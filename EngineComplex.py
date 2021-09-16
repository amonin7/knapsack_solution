import balancer.ThirdBalancer as sb
from mpi4py import MPI
import sequential.main as sl
import communicator.SimpleCommunicator as com
# import route.TraceCollector as rc
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

        # self.route_collector = rc.TraceCollector('Trace.csv', self.processes_amount)
        # self.comm_collector = cc.CommunicationCollector('Communication.csv')
        self.balancer = None
        self.communicator = None
        self.solver = None

        self.isSentRequest = []
        self.state = ""

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
        self.state = "starting"

    def run(self) -> None:
        self.initializeAll()
        while True:
            state = self.state
            command, outputs = self.balancer.balance(state=state,
                                                     subs_amount=self.solver.get_sub_amount(),
                                                     add_args=[[7, 3], False, self.rank]
                                                     )
            if command == "receive":
                receive_status, message, sender = self.communicator.receive()
                if receive_status != "received_exit_command":
                    if receive_status == "received_subproblems":
                        self.solver.putSubproblems(message.payload)

                    command, outputs = self.balancer.balance(state=receive_status,
                                                             subs_amount=self.solver.get_sub_amount(),
                                                             add_args=[[message.payload, sender], False, self.rank])
                    if command == "send_subproblems":
                        self.state = self.send_subs(subs_am=outputs[1], dest=outputs[0])
                    elif command == "send_get_request":
                        print("send GR")
                        receiver = outputs[0]
                        amount_of_tasks = outputs[1]
                        self.state, outputs = self.communicator.send(
                            receiver,
                            me.Message(
                                message_type="get_request",
                                payload=amount_of_tasks
                            )
                        )
                    elif command == "send_exit_command":
                        print("send EC")
                        receiver = outputs[0]
                        self.state, outputs = self.communicator.send(
                            receiver,
                            me.Message(message_type="exit_command")
                        )
                    elif command == "solve":
                        tasks_am = outputs[0]
                        self.state = self.solver.solve(tasks_am)
                else:
                    break
            elif command == "send_subproblems":
                self.state = self.send_subs(subs_am=outputs[1], dest=outputs[0])
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
            elif command == "send_exit_command":
                receiver = outputs[0]
                self.state, outputs = self.communicator.send(
                    receiver,
                    me.Message(message_type="exit_command")
                )
            elif command == "solve":
                tasks_am = outputs[0]
                self.state = self.solver.solve(tasks_am)
            elif command == "exit":
                break
        profit = self.comm.reduce(self.solver.max_profit, MPI.MAX, root=0)
        if self.rank == 0:
            print(f"maximum profit: {profit}")
        # self.route_collector.save()
        # self.comm_collector.save()

    def send_subs(self, subs_am, dest):
        print("send S")
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
