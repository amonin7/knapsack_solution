import time

from mpi4py import MPI
import balancer.SimpleBalancer as sb
import sequential.main as sl
import communicator.SimpleCommunicator as com
import communicator.Message as me


class Engine:

    def __init__(self,
                 proc_amount,
                 max_depth,
                 comm,
                 arg=7,
                 price_receive=0.005,
                 price_send=0.005,
                 price_put=0.005,
                 price_get=0.005,
                 price_balance=0.05,
                 price_solve=5.0):
        self.comm = comm
        self.rank = comm.Get_rank()
        self.arg = arg
        self.processes_amount = proc_amount  # amount of simulated processes
        self.max_depth = max_depth  # max depth of solving tree
        self.price_rcv = price_receive  # price of receiving message
        self.price_snd = price_send  # price of sending message
        self.price_put = price_put  # price of putting message into solver
        self.price_get = price_get  # price of getting message from solver
        self.price_blc = price_balance  # price of balancing
        self.price_slv = price_solve  # price of solving

        # self.mes_service = ms.MessageService()
        # self.route_collector = rc.TraceCollector('Trace.csv', self.processes_amount)
        # self.comm_collector = cc.CommunicationCollector('Communication.csv')
        self.balancer = None
        self.communicator = None
        self.solver = None

        # self.timers = []
        # self.downtime = []  # amount of time when process was without any tasks
        # self.isDoneStatuses = []
        self.isSentRequest = []
        self.state = ""

    # TODO: вынести в отдельный метод вне ENGINE
    def initializeAll(self) -> None:
        if self.rank == 0:
            self.balancer = sb.MasterBalancer("start", max_depth=self.max_depth, proc_am=self.processes_amount,
                                              prc_blnc=self.price_blc, arg=self.arg)
            self.solver = sl.Solver(subproblems=[])
            root = sl.Node(0, self.solver.arr[0].value, 0, self.solver.arr[0].weight)
            root.bound = self.solver.bound(root)
            self.solver.putSubproblems([root])

            self.communicator = com.SimpleCommunicator()
        else:
            self.balancer = sb.SlaveBalancer("start", max_depth=self.max_depth, proc_am=self.processes_amount,
                                             prc_blnc=self.price_blc, arg=self.arg)
            self.solver = sl.Solver(subproblems=[])

            self.communicator = com.SimpleCommunicator()
        self.state = "starting"

    def run(self) -> None:
        self.initializeAll()
        while True:
            state = self.state
            command, outputs = self.balancer.balance(state=state,
                                                     subs_amount=self.solver.get_sub_amount(),
                                                     add_args=[[], self.isSentRequest, self.rank])
            if command == "start" or command == "receive":
                receive_status, message, sender = self.communicator.receive(comm)
                if receive_status != "received_exit_command":
                    if receive_status == "received_subproblems":
                        self.solver.putSubproblems(message.payload)

                    command, outputs = self.balancer.balance(state=receive_status,
                                                             subs_amount=self.solver.get_sub_amount(),
                                                             add_args=message)
                    if command == "send_subs":
                        pass
                        # self.state = self.send_subs(proc_id=proc_ind, subs_am=outputs[1], dest_id=outputs[0])
                    # elif command == "send_get_request":
                    #     self.state = self.send_get_request(dest_proc_id=outputs[0],
                    #                                                  sender_proc_id=proc_ind,
                    #                                                  tasks_amount=outputs[1])
                    # elif command == "send_exit":
                    #     self.state = self.send_exit(proc_id=proc_ind, dest_id=outputs[0])
                    elif command == "solve":
                        tasks_am = outputs[0]
                        self.state = self.solver.solve(tasks_am)
                else:
                    break
            # elif command == "send_subs":
            #     self.state = self.send_subs(proc_id=proc_ind, subs_am=outputs[1], dest_id=outputs[0])
            elif command == "send_all":
                self.state = self.send_all_subs_to_all_proc()
            # elif command == "send_get_request":
            #     self.state = self.send_get_request(dest_proc_id=outputs[0],
            #                                                  sender_proc_id=proc_ind,
            #                                                  tasks_amount=outputs[1])
            # elif command == "send_exit":
            #     self.state = self.send_exit(proc_id=proc_ind, dest_id=outputs[0])
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

    # def start(self, proc_id, state):
    #     rcv_output = self.receive_message(proc_id=proc_id)
    #     command, outputs = self.balance(proc_id,
    #                                     state,
    #                                     subs_amount=self.solver.get_sub_amount(),
    #                                     add_args=[[], self.isSentRequest, proc_id])
    #     return command, outputs
    #
    # def receive_message(self, proc_id):
    #     command, message, time_for_rcv = self.communicator.receive_one(proc_id, self.mes_service)
    #     if command == "put_message":
    #         if self.timers[proc_id] < message.timestamp:
    #             self.route_collector.write(proc_id,
    #                                        str(round(self.timers[proc_id], 3)) + '-' + str(
    #                                            round(message.timestamp, 3)),
    #                                        'Await for receive',
    #                                        '-')
    #             self.route_collector.write(proc_id,
    #                                        str(round(message.timestamp, 3)) + '-' + str(
    #                                            round(message.timestamp + time_for_rcv, 3)),
    #                                        'Receive',
    #                                        message.mes_type)
    #             self.downtime[proc_id] += message.timestamp - self.timers[proc_id]
    #             self.timers[proc_id] = message.timestamp + time_for_rcv
    #         else:
    #             self.route_collector.write(proc_id,
    #                                        str(round(self.timers[proc_id], 3)) + '-' + str(
    #                                            round(self.timers[proc_id] + time_for_rcv, 3)),
    #                                        'Receive',
    #                                        message.mes_type)
    #             self.timers[proc_id] += time_for_rcv
    #         self.comm_collector.write_recv(message.sender, proc_id, message.timestamp, self.timers[proc_id])
    #         if message.mes_type == "get_request":
    #             return "received_get_request", [message.payload, message.sender]
    #         elif message.mes_type == "subproblems":
    #             self.solvers[proc_id].putSubproblems(message.payload)
    #             return "received_put_subs_and_rec", []
    #         elif message.mes_type == "exit_command":
    #             return "received_exit_command", []
    #
    #         return "received", []
    #     elif command == "continue":
    #         return "nothing_to_receive", []

    # def solve(self, proc_id, tasks_amount):
    #     state, _, time = self.solver.solve(tasks_amount)
    #     if state == "solved":
    #         # command = "balance"
    #         self.route_collector.write(proc_id,
    #                                    str(round(self.timers[proc_id], 3)) + '-' + str(
    #                                        round(self.timers[proc_id] + time, 3)),
    #                                    'Solve',
    #                                    'tasks_am=' + str(tasks_amount))
    #         self.timers[proc_id] += time
    #     else:
    #         raise Exception('Solving went wrong')
    #     return state

    # def balance(self, proc_id, state, subs_amount, add_args=None):
    #     command, outputs, time = self.balancer.balance(state=state,
    #                                                    subs_amount=subs_amount,
    #                                                    add_args=add_args)
    #     self.route_collector.write(proc_id,
    #                                str(round(self.timers[proc_id], 3)) + '-' + str(
    #                                    round(self.timers[proc_id] + time, 3)),
    #                                'Balance',
    #                                'state=' + state)
    #     self.timers[proc_id] += time
    #     return command, outputs

    # def send_get_request(self, dest_proc_id, sender_proc_id, tasks_amount):
    #     state, _, time = self.communicator.send(
    #         receiver=dest_proc_id,
    #         message=sm.Message2(sender=sender_proc_id,
    #                             dest=dest_proc_id,
    #                             mes_type="get_request",
    #                             payload=tasks_amount,
    #                             timestamp=self.timers[sender_proc_id]),
    #         ms=self.mes_service
    #     )
    #     if state != "sent":
    #         raise Exception('Sending went wrong')
    #     self.save_time(proc_id=sender_proc_id, timestamp=time, dest_proc=dest_proc_id)
    #     return "sent_get_request"

    # def send_subs(self, proc_id, subs_am, dest_id):
    #     message = sm.Message2(sender=proc_id,
    #                           dest=dest_id,
    #                           mes_type="subproblems",
    #                           payload=self.solver.getSubproblems(subs_am),
    #                           timestamp=self.timers[proc_id])
    #     state, outputs, time = self.communicator.send(
    #         receiver=dest_id,
    #         message=message,
    #         ms=self.mes_service
    #     )
    #     if state != "sent":
    #         raise Exception('Sending went wrong')
    #     self.save_time(proc_id=proc_id, timestamp=time, dest_proc=dest_id)
    #     return "sent_subs"

    # def send_exit(self, proc_id, dest_id):
    #     state, _, time = self.communicator.send(
    #         receiver=dest_id,
    #         message=sm.Message2(sender=proc_id,
    #                             dest=dest_id,
    #                             mes_type="exit_command",
    #                             payload=None,
    #                             timestamp=self.timers[proc_id]),
    #         ms=self.mes_service
    #     )
    #     if state != "sent":
    #         raise Exception('Sending went wrong')
    #     self.save_time(proc_id=proc_id, timestamp=time, dest_proc=proc_id)
    #     return "sent_exit"

    def send_all_subs_to_all_proc(self):
        probs = self.solver.getSubproblems(-1)
        probs_amnt = len(probs)
        part = 1 / (self.processes_amount - 1)
        state = ""
        print(f"self.processes_amount: {self.processes_amount}")
        print(f"len(probs): {len(probs)}")
        for dest_proc in range(1, self.processes_amount):
            message_list = probs[int((dest_proc - 1) * probs_amnt * part): int(dest_proc * probs_amnt * part)]
            message = me.Message(
                message_type="subproblems",
                payload=message_list
            )
            state, outputs = self.communicator.send(
                receiver=dest_proc,
                message=message,
                comm=self.comm
            )
        return state

    # def save_time(self, proc_id, timestamp, dest_proc):
    #     self.route_collector.write(proc_id,
    #                                str(round(self.timers[proc_id], 3)) + '-' + str(
    #                                    round(self.timers[proc_id] + timestamp, 3)),
    #                                'Send',
    #                                'dest=' + str(dest_proc))
    #     self.timers[proc_id] += timestamp


if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    eng = Engine(proc_amount=size, max_depth=9, comm=comm)
    eng.run()
