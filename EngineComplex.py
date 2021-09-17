import balancer.ThirdBalancer as sb
from mpi4py import MPI
import sequential.main as sl
import communicator.SimpleCommunicator as com
import time
import route.TraceCollector as rc
import communicator.Message as me
# import route.CommunicationCollector as cc


class Engine:

    def __init__(self,
                 proc_amount,
                 comm,
                 arg=7):
        self.comm = comm
        self.rank = comm.Get_rank()
        self.arg = arg
        self.processes_amount = proc_amount  # amount of simulated processes

        self.route_collector = rc.TraceCollector('Trace.csv', self.rank)
        # self.comm_collector = cc.CommunicationCollector('Communication.csv')
        self.balancer = None
        self.communicator = None
        self.solver = None

        self.isSentRequest = []
        self.state = ""
        self.timer = time.time()

    # TODO: вынести в отдельный метод вне ENGINE
    def initializeAll(self) -> None:
        if self.rank == 0:
            self.balancer = sb.MasterBalancer("start", max_depth=0, proc_am=self.processes_amount,
                                              prc_blnc=0)
            self.solver = sl.Solver(subproblems=[])
            root = sl.Node(0, self.solver.arr[0].value, 0, self.solver.arr[0].weight)
            root.bound = self.solver.bound(root)
            self.solver.putSubproblems([root])

            self.communicator = com.SimpleCommunicator(comm=comm)
        else:
            self.balancer = sb.SlaveBalancer("start", max_depth=0, proc_am=self.processes_amount,
                                             prc_blnc=0)
            self.solver = sl.Solver(subproblems=[])

            self.communicator = com.SimpleCommunicator(comm=comm)
        t = time.time()
        if self.rank == 0:
            t = time.time()
        self.timer = comm.bcast(t, 0)
        self.state = "starting"

    def run(self) -> None:
        self.initializeAll()
        while True:
            state = self.state
            self.route_collector.write(self.rank, time.time() - self.timer, "balance", f"state={self.state}")
            command, outputs = self.balancer.balance(state=state,
                                                     subs_amount=self.solver.get_sub_amount(),
                                                     add_args=[[], False, self.rank]
                                                     )
            if command == "receive":
                receive_status, message, sender = self.communicator.receive()
                self.route_collector.write(self.rank, time.time() - self.timer, "receive", f"mes_type={message.message_type}")
                if receive_status != "received_exit_command":
                    if receive_status == "received_subproblems":
                        self.solver.putSubproblems(message.payload)

                    self.route_collector.write(self.rank, time.time() - self.timer, "balance", f"state={receive_status}")
                    command, outputs = self.balancer.balance(state=receive_status,
                                                             subs_amount=self.solver.get_sub_amount(),
                                                             add_args=[[message.payload, sender], False, self.rank]
                                                             )
                    if command == "send_subproblems":
                        self.state = self.send_subs(subs_am=outputs[1], dest=outputs[0])
                        self.route_collector.write(self.rank, time.time() - self.timer, command,
                                                   f"subs_am={outputs[1]}, dest={outputs[0]}")
                    elif command == "send_get_request":
                        receiver = outputs[0]
                        amount_of_tasks = outputs[1]
                        self.state, outputs = self.communicator.send(
                            receiver,
                            me.Message(
                                message_type="get_request",
                                payload=amount_of_tasks
                            )
                        )
                        self.route_collector.write(self.rank, time.time() - self.timer, command,
                                                   f"subs_am={outputs[1]}, dest={outputs[0]}")
                    elif command == "send_exit_command":
                        receiver = outputs[0]
                        self.state, outputs = self.communicator.send(
                            receiver,
                            me.Message(message_type="exit_command")
                        )
                        self.route_collector.write(self.rank, time.time() - self.timer, command,
                                                   f"dest={receiver}")
                    elif command == "solve":
                        tasks_am = outputs[0]
                        self.state = self.solver.solve(tasks_am)
                        self.route_collector.write(self.rank, time.time() - self.timer, command,
                                                   f"tasks_am={tasks_am}")
                else:
                    break
            elif command == "send_subproblems":
                self.state = self.send_subs(subs_am=outputs[1], dest=outputs[0])
                self.route_collector.write(self.rank, time.time() - self.timer, command,
                                           f"subs_am={outputs[1]}, dest={outputs[0]}")
            elif command == "send_all":
                self.state = self.send_all_subs_to_all_proc()
            elif command == "send_get_request":
                receiver = outputs[0]
                amount_of_tasks = outputs[1]
                self.state, outputs = self.communicator.send(
                    receiver,
                    me.Message(
                        message_type="get_request",
                        payload=amount_of_tasks
                    )
                )
                self.route_collector.write(self.rank, time.time() - self.timer, command,
                                           f"subs_am={amount_of_tasks}, dest={receiver}")
            elif command == "send_exit_command":
                receiver = outputs[0]
                self.state, outputs = self.communicator.send(
                    receiver,
                    me.Message(message_type="exit_command")
                )
                self.route_collector.write(self.rank, time.time() - self.timer, command,
                                           f"dest={outputs[0]}")
            elif command == "solve":
                tasks_am = outputs[0]
                self.state = self.solver.solve(tasks_am)
                self.route_collector.write(self.rank, time.time() - self.timer, command,
                                           f"tasks_am={tasks_am}")
            elif command == "exit":
                self.route_collector.write(self.rank, time.time() - self.timer, command,
                                           "")
                break
        profit = self.comm.reduce(self.solver.max_profit, MPI.MAX, root=0)
        if self.rank == 0:
            print(f"maximum profit  : {profit}")
            max_time = self.route_collector.frame['timestamp0'][-1]
            print(f"maximum time    : {max_time}")
        traces = self.comm.gather(self.route_collector.frame, root=0)
        if self.rank == 0:
            res = {}
            for d in traces:
                res.update(d)
            self.route_collector.frame = res
            self.route_collector.save()
        # self.comm_collector.save()

    def send_subs(self, subs_am, dest):
        message = me.Message(
            message_type="subproblems",
            payload=self.solver.getSubproblems(subs_am)
        )
        state, outputs = self.communicator.send(
            receiver=dest,
            message=message
        )
        return state

    def send_all_subs_to_all_proc(self):
        probs = self.solver.getSubproblems(-1)
        probs_amnt = len(probs)
        part = 1 / (self.processes_amount - 1)
        state = ""
        for dest_proc in range(1, self.processes_amount):
            message_list = probs[int((dest_proc - 1) * probs_amnt * part): int(dest_proc * probs_amnt * part)]
            message = me.Message(
                message_type="subproblems",
                payload=message_list
            )
            state, outputs = self.communicator.send(
                receiver=dest_proc,
                message=message
            )
        return state


if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    eng = Engine(proc_amount=size, comm=comm)
    eng.run()
