# import balancer.ThirdBalancer as sb
import balancer.ComplexBalancer as sb
from mpi4py import MPI
import sequential.main as sl
import communicator.SimpleCommunicator as com
import time
import route.TraceCollector as rc
import communicator.Message as me
import sys

# import route.CommunicationCollector as cc


class Engine:

    def __init__(self,
                 proc_amount,
                 comm,
                 T=200,
                 S=10,
                 arg=7):
        self.T = T
        self.S = S

        self.subs_am = 0
        self.comm = comm
        self.rank = comm.Get_rank()
        self.arg = arg
        self.processes_amount = proc_amount  # amount of simulated processes

        self.route_collector = rc.TraceCollector('TraceC.csv', self.rank)
        # self.comm_collector = cc.CommunicationCollector('Communication.csv')
        self.balancer = None
        self.communicator = None
        self.solver = None

        self.slv_cnt = 0.0
        self.blc_cnt = 0.0
        self.snd_cnt = 0.0
        self.rcv_cnt = 0.0

        self.slv_act = 0
        self.blc_act = 0
        self.snd_act = 0
        self.rcv_act = 0

        self.rcvs = []
        self.rcvl = []

        self.isSentRequest = []
        self.state = ""
        self.timer = time.time()

    # TODO: вынести в отдельный метод вне ENGINE
    def initializeAll(self) -> None:
        if self.rank == 0:
            self.balancer = sb.MasterBalancer("start", max_depth=0, proc_am=self.processes_amount,
                                              prc_blnc=0, T=self.T, S=self.S)
            self.solver = sl.Solver(subproblems=[])
            root = sl.Node(0, self.solver.arr[0].value, 0, self.solver.arr[0].weight)
            root.bound = self.solver.bound(root)
            self.solver.putSubproblems([root])

            self.communicator = com.SimpleCommunicator(comm=comm)
        else:
            self.balancer = sb.SlaveBalancer("start", max_depth=0, proc_am=self.processes_amount,
                                             prc_blnc=0, T=self.T, S=self.S)
            self.solver = sl.Solver(subproblems=[])

            self.communicator = com.SimpleCommunicator(comm=comm)
        self.timer = time.time()
        self.state = "starting"

    def run(self) -> None:
        self.initializeAll()
        while True:
            state = self.state
            if state != "receiving":
                start = round(time.time() - self.timer, 7)
                command, outputs = self.balancer.balance(state=state,
                                                         subs_amount=self.solver.get_sub_amount(),
                                                         add_args=[[], False, self.rank])
                end = round(time.time() - self.timer, 8)
                self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7):.7f}", "balance",
                                           f"state={self.state}")
                self.blc_cnt += end - start
                self.blc_act += 1
            else:
                command = "receive"
            if command == "receive":
                start = round(time.time() - self.timer, 7)
                receive_status, message, sender = self.communicator.receive()
                end = round(time.time() - self.timer, 7)
                self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}", "receive",
                                           f"mes_type={message.message_type}")
                self.rcv_cnt += (end - start) / len(str(message))
                self.rcvs.append(f"{(end - start):.7f}")
                self.rcvl.append(len(str(me.pack(message))))
                self.rcv_act += 1
                if receive_status != "received_exit_command":
                    if receive_status == "received_subproblems":
                        self.solver.putSubproblems(message.payload['problems'])
                        self.update_record(message)
                    elif receive_status == 'received_S':
                        self.balancer.S = message.payload['S']
                        self.update_record(message)

                    start = round(time.time() - self.timer, 7)
                    command, outputs = self.balancer.balance(state=receive_status,
                                                             subs_amount=self.solver.get_sub_amount(),
                                                             add_args=[[message.payload, sender], False, self.rank])
                    end = round(time.time() - self.timer, 8)
                    self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                               "balance", f"state={receive_status}")
                    self.blc_cnt += end - start
                    self.blc_act += 1
                    if command == "send_subproblems":
                        start = round(time.time() - self.timer, 7)
                        self.state = self.send_subs(subs_am=outputs[1], dest=outputs[0])
                        end = round(time.time() - self.timer, 7)
                        self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                                   command,
                                                   f"subs_am={outputs[1]}, dest={outputs[0]}")
                    elif command == "send_S":
                        [S, receiver] = outputs
                        start = round(time.time() - self.timer, 7)
                        self.state, outputs = self.communicator.send(
                            receiver,
                            me.Message(
                                message_type="S",
                                payload={'S': S, 'record': self.solver.max_profit}
                            )
                        )
                        end = round(time.time() - self.timer, 7)
                        self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                                   command,
                                                   f"subs_am={S}, dest={receiver}")
                        self.snd_cnt += (end - start) / len(str(me.Message(
                                message_type="S",
                                payload={'S': S, 'record': self.solver.max_profit}
                            )))
                        self.snd_act += 1
                    elif command == "send_get_request":
                        receiver = outputs[0]
                        amount_of_tasks = outputs[1]
                        start = round(time.time() - self.timer, 7)
                        message = me.Message(message_type="get_request", payload=amount_of_tasks)
                        self.state, outputs = self.communicator.send(
                            receiver,
                            message
                        )
                        end = round(time.time() - self.timer, 7)
                        self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                                   command,
                                                   f"subs_am={outputs[1]}, dest={outputs[0]}")
                        self.snd_cnt += (end - start) / len(str(message))
                        self.snd_act += 1
                    elif command == "send_exit_command":
                        receiver = outputs[0]
                        start = round(time.time() - self.timer, 7)
                        message = me.Message(message_type="exit_command")
                        self.state, outputs = self.communicator.send(
                            receiver,
                            message
                        )
                        end = round(time.time() - self.timer, 7)
                        self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                                   command,
                                                   f"dest={receiver}")
                        self.snd_cnt += (end - start) / len(str(message))
                        self.snd_act += 1
                    elif command == "solve":
                        tasks_am = outputs[0]
                        start = round(time.time() - self.timer, 7)
                        self.state, solved_amount = self.solver.solve(tasks_am)
                        self.subs_am += solved_amount
                        end = round(time.time() - self.timer, 8)
                        self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                                   command,
                                                   f"tasks_am={tasks_am}")
                        self.slv_cnt += (end - start) / solved_amount
                        self.slv_act += 1
                    elif command == "receive":
                        self.state = "receiving"
                    elif command == "send_all_exit_command":
                        while not outputs[0].empty():
                            (receiver, get_amount) = outputs[0].get()
                            start = round(time.time() - self.timer, 7)
                            message = me.Message(message_type="exit_command")
                            self.state, _ = self.communicator.send(
                                receiver,
                                message
                            )
                            end = round(time.time() - self.timer, 7)
                            self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                                       command,
                                                       f"dest={receiver}")
                            self.snd_cnt += (end - start) / len(str(message))
                            self.snd_act += 1
                        self.state = "sent_all_exit_command"
                    else:
                        raise Exception(f"wrong command={command}, state={receive_status}")
                else:
                    break
            elif command == "send_subproblems":
                self.state = self.send_subproblems(outputs[0], outputs[1])
            elif command == "send_all":
                self.state = self.send_all_subs_to_all_proc()
            elif command == "send_get_request":
                receiver = outputs[0]
                amount_of_tasks = outputs[1]
                start = round(time.time() - self.timer, 7)
                message = me.Message(message_type="get_request", payload=amount_of_tasks)
                self.state, outputs = self.communicator.send(
                    receiver,
                    message
                )
                end = round(time.time() - self.timer, 7)
                self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}", command,
                                           f"subs_am={amount_of_tasks}, dest={receiver}")
                self.snd_cnt += (end - start) / len(str(message))
                self.snd_act += 1
            elif command == "send_exit_command":
                receiver = outputs[0]
                start = round(time.time() - self.timer, 7)
                message = me.Message(message_type="exit_command")
                self.state, outputs = self.communicator.send(
                    receiver,
                    message
                )
                end = round(time.time() - self.timer, 7)
                self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}", command,
                                           f"dest={outputs[0]}")
                self.snd_cnt += (end - start) / len(str(message))
                self.snd_act += 1
            elif command == "solve":
                tasks_am = outputs[0]
                start = round(time.time() - self.timer, 7)
                self.state, solved_amount = self.solver.solve(tasks_am)
                self.subs_am += solved_amount
                end = round(time.time() - self.timer, 8)
                self.route_collector.write(self.rank, f"{start:.7f}-{end:.7f}",
                                           command, f"tasks_am={tasks_am}")
                self.slv_cnt += (end - start) / solved_amount
                self.slv_act += 1
            elif command == "send_all_exit_command":
                while not outputs[0].empty():
                    (receiver, get_amount) = outputs[0].get()
                    # print(f"copysize={outputs[0].qsize()}, size={self.balancer.poor_proc.qsize()}")
                    start = round(time.time() - self.timer, 7)
                    message = me.Message(message_type="exit_command")
                    self.state, outputs = self.communicator.send(
                        receiver,
                        message
                    )
                    end = round(time.time() - self.timer, 7)
                    self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}", command,
                                               f"dest={outputs[0]}")
                    self.snd_cnt += (end - start) / len(str(message))
                    self.snd_act += 1
                self.state = "sent_all_exit_command"
            elif command == "try_send_subproblems":
                q = outputs[0]
                # print(f"try_send_subproblems, s_am={self.solver.get_sub_amount()}, q_s={q.qsize()}")
                while not q.empty() and self.solver.get_sub_amount() > 0:
                    (receiver, get_amount) = q.get()
                    # print(f"copysize={q.qsize()}, size={self.balancer.poor_proc.qsize()}")
                    self.send_subproblems(receiver, get_amount)
                self.state = "sent_subproblems"
            elif command == "exit":
                start = round(time.time() - self.timer, 7)
                self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}", command,
                                           "")
                break
            else:
                raise Exception(f"wrong command={command}")

        if self.slv_act == 0: self.slv_act = 1
        if self.blc_act == 0: self.blc_act = 1
        if self.rcv_act == 0: self.rcv_act = 1
        if self.snd_act == 0: self.snd_act = 1

        profit = self.comm.reduce(self.solver.max_profit, MPI.MAX, root=0)
        slv = self.comm.reduce(self.slv_cnt / self.slv_act, MPI.SUM, root=0)
        blc = self.comm.reduce(self.blc_cnt / self.blc_act, MPI.SUM, root=0)
        rcv = self.comm.reduce(self.rcv_cnt / self.rcv_act, MPI.SUM, root=0)
        snd = self.comm.reduce(self.snd_cnt / self.snd_act, MPI.SUM, root=0)

        subs_total = self.comm.reduce(self.subs_am, MPI.SUM, root=0)

        rcvs = self.comm.gather(self.rcvs, root=0)
        rcvl = self.comm.gather(self.rcvl, root=0)
        if self.rank == 0:
            # print(f"maximum profit: {profit}")
            # print(f"price_solve={(slv / self.comm.size):.7f},")
            # print(f"price_balance={(blc / self.comm.size):.7f},")
            # print(f"price_receive={(rcv / self.comm.size):.7f},")
            # print(f"price_send={(snd / self.comm.size):.7f}):")
            #
            # print(f"subs_am={subs_total}")

            with open("rcvc.txt", "w") as file:
                result1 = []
                for lst in rcvs:
                    result1.extend(lst)
                file.write(f"rcvs=np.array( {result1} )\n")
                result1 = []
                for lst in rcvl:
                    result1.extend(lst)
                file.write(f"rcvl=np.array( {result1} )")

            max_time = float(self.route_collector.frame['timestamp0'][-1].split('-')[1])
            # print(f"maximum time    : {max_time}")
            with open("ts_times.csv", "a") as f:
                f.write(f'{max_time},{self.T},{self.S}\n')
                f.close()
        # traces = self.comm.gather(self.route_collector.frame, root=0)
        # if self.rank == 0:
        #     res = {}
        #     for d in traces:
        #         res.update(d)
        #     self.route_collector.frame = res
        #     self.route_collector.save()

        # self.comm_collector.save()

    def send_subproblems(self, sender, amount):
        start = round(time.time() - self.timer, 7)
        state = self.send_subs(subs_am=amount, dest=sender)
        # self.state = self.send_subs(subs_am=outputs[1], dest=outputs[0])
        end = round(time.time() - self.timer, 7)
        self.route_collector.write(self.rank, f"{start:.7f}-{end}", "send_subproblems",
                                   f"subs_am={amount}, dest={sender}")
        return state

    def update_record(self, message):
        if self.solver.max_profit < message.payload['record']:
            self.solver.max_profit = message.payload['record']

    def send_subs(self, subs_am, dest):
        message = me.Message(
            message_type="subproblems",
            payload={'problems': self.solver.getSubproblems(subs_am), 'record': self.solver.max_profit}
        )
        start = round(time.time() - self.timer, 7)
        state, outputs = self.communicator.send(
            receiver=dest,
            message=message
        )
        end = round(time.time() - self.timer, 7)
        self.snd_cnt += (end - start) / len(str(message))
        self.snd_act += 1
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
    if len(sys.argv) == 3:
        _, t, s = sys.argv
        eng = Engine(proc_amount=size, comm=comm, T=int(t), S=int(s))
        eng.run()
