import time
from mpi4py import MPI
import balancer.SecondBalancer as sb
import sequential.Solver as sl
import communicator.SimpleCommunicator as com
import communicator.Message as me
import route.TraceCollector as rc
import sys


class Engine:

    def __init__(self,
                 proc_amount,
                 comm,
                 arg=7):
        self.comm = comm
        self.rank = comm.Get_rank()
        self.arg = arg
        self.processes_amount = proc_amount  # amount of simulated processes
        self.route_collector = rc.TraceCollector('TraceS.csv', self.rank)

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

        self.isSentRequest = []
        self.state = ""
        self.timer = time.time()

        self.subs_am = 0

    # TODO: вынести в отдельный метод вне ENGINE
    def initializeAll(self) -> None:
        if self.rank == 0:
            self.balancer = sb.MasterBalancer(max_depth=0, proc_am=self.processes_amount,
                                              prc_blnc=0, arg=self.arg)
            self.solver = sl.Solver(subproblems=[], I=24)
            root = sl.Node(0, self.solver.arr[0].value, 0, self.solver.arr[0].weight)
            root.bound = self.solver.bound(root)
            self.solver.putSubproblems([root])

            self.communicator = com.SimpleCommunicator(self.comm)
        else:
            self.balancer = sb.SlaveBalancer(max_depth=0, proc_am=self.processes_amount,
                                             prc_blnc=0, arg=self.arg)
            self.solver = sl.Solver(subproblems=[], I=24)

            self.communicator = com.SimpleCommunicator(self.comm)
        self.timer = time.time()
        self.state = "starting"

    def run(self) -> None:
        self.initializeAll()
        while True:
            state = self.state
            if state != "receiving":
                command, outputs = self.balance([[], self.isSentRequest, self.rank], state)
            else:
                command = "receive"
                outputs = []
            if command == "receive":
                message, receive_status, sender = self.receive_message()
                if receive_status != "received_exit_command":
                    if receive_status == "received_subproblems":
                        self.solver.putSubproblems(message.payload['problems'])
                        if self.solver.max_profit > message.payload['record']:
                            self.solver.max_profit = message.payload['record']

                    command, outputs = self.balance([[message.payload, sender], False, self.rank], receive_status)
                    if command == "send_subs":
                        pass
                    elif command == "send_exit_command":
                        receiver = outputs[0]
                        outputs = self.send_exit_command(receiver)
                    elif command == "receive":
                        self.state = "receiving"
                    elif command == "send_subproblems":
                        start = round(time.time() - self.timer, 7)
                        self.state = self.send_subs(subs_am=outputs[1], dest=outputs[0])
                        end = round(time.time() - self.timer, 7)
                        self.route_collector.write(self.rank, f"{start:.7f}-{end:.7f}",
                                                   command,
                                                   f"subs_am={outputs[1]}, dest={outputs[0]}")
                    elif command == "solve":
                        tasks_am = outputs[0]
                        self.solve(tasks_am)
                    elif command == "send_get_request":
                        receiver = outputs[0]
                        amount_of_tasks = outputs[1]
                        outputs = self.send_get_request(amount_of_tasks, receiver)
                    else:
                        raise Exception(f"wrong command={command}")
                else:
                    break
            elif command == "send_all":
                self.state = self.send_all_subs_to_all_proc()
            elif command == "solve":
                tasks_am = outputs[0]
                self.solve(tasks_am)
            elif command == "send_get_request":
                receiver = outputs[0]
                amount_of_tasks = outputs[1]
                outputs = self.send_get_request(amount_of_tasks, receiver)
            elif command == "exit":
                start = round(time.time() - self.timer, 7)
                self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}", command,
                                           "")
                break
            else:
                raise Exception(f"wrong command={command}")
        # if self.slv_act == 0: self.slv_act = 1
        # if self.blc_act == 0: self.blc_act = 1
        # if self.rcv_act == 0: self.rcv_act = 1
        # if self.snd_act == 0: self.snd_act = 1
        #
        # profit = self.comm.reduce(self.solver.max_profit, MPI.MAX, root=0)
        # slv = self.comm.reduce(self.slv_cnt / self.slv_act, MPI.SUM, root=0)
        # blc = self.comm.reduce(self.blc_cnt / self.blc_act, MPI.SUM, root=0)
        # rcv = self.comm.reduce(self.rcv_cnt / self.rcv_act, MPI.SUM, root=0)
        # snd = self.comm.reduce(self.snd_cnt / self.snd_act, MPI.SUM, root=0)
        #
        # subs_total = self.comm.reduce(self.subs_am, MPI.SUM, root=0)
        m_time = self.comm.reduce(
            float(self.route_collector.frame[f'timestamp{self.rank}'][-1].split('-')[1]),
            MPI.MAX,
            root=0
        )
        if self.rank == 0:
            # print(f"maximum profit: {profit}")
            # print(f"price_solve={(slv / self.comm.size):.7f},")
            # print(f"price_balance={(blc / self.comm.size):.7f},")
            # print(f"price_receive={(rcv / self.comm.size):.7f},")
            # print(f"price_send={(snd / self.comm.size):.7f}):")
            #
            # print(f"subs_am={subs_total}")
            #
            # max_time = float(self.route_collector.frame['timestamp0'][-1].split('-')[1])
            # print(f"maximum time    : {max_time}")
            with open('experimental_data/argtime-ls.csv', 'a') as f:
                f.write(f'\n{m_time},{self.arg}')
            # print(m_time)
        # traces = self.comm.gather(self.route_collector.frame, root=0)
        # if self.rank == 0:
        #     res = {}
        #     for d in traces:
        #         res.update(d)
        #     self.route_collector.frame = res
        #     self.route_collector.save()

        # self.comm_collector.save()

    def send_get_request(self, amount_of_tasks, receiver):
        start = round(time.time() - self.timer, 7)
        message = me.Message(message_type="get_request", payload=amount_of_tasks)
        self.state, outputs = self.communicator.send(
            receiver,
            message
        )
        end = round(time.time() - self.timer, 7)
        self.route_collector.write(self.rank, f"{start:.7f}-{end:.7f}",
                                   "send_get_request",
                                   f"subs_am={amount_of_tasks}, dest={receiver}")
        return outputs

    def solve(self, tasks_am):
        start = round(time.time() - self.timer, 8)
        self.state, solved_amount = self.solver.solve(tasks_am)
        self.subs_am += solved_amount
        end = round(time.time() - self.timer, 8)
        self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                   "solve",
                                   f"tasks_am={tasks_am}")
        self.slv_cnt += (end - start) / solved_amount
        self.slv_act += 1

    def send_exit_command(self, receiver):
        start = round(time.time() - self.timer, 7)
        message = me.Message(message_type="exit_command")
        self.state, outputs = self.communicator.send(
            receiver,
            message
        )
        end = round(time.time() - self.timer, 7)
        self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                   "send_exit_command",
                                   f"dest={receiver}")
        self.snd_cnt += (end - start) / len(str(me.pack(message)))
        self.snd_act += 1
        return outputs

    def balance(self, add_args, state):
        start = round(time.time() - self.timer, 8)
        command, outputs = self.balancer.balance(state=state,
                                                 subs_amount=self.solver.get_sub_amount(),
                                                 add_args=add_args)
        end = round(time.time() - self.timer, 8)
        self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                   "balance", f"state={state}")
        self.blc_cnt += end - start
        self.blc_act += 1
        return command, outputs

    def receive_message(self):
        start = round(time.time() - self.timer, 8)
        receive_status, message, sender = self.communicator.receive()
        end = round(time.time() - self.timer, 8)
        self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}", "receive",
                                   f"mes_type={message.message_type}")
        self.rcv_cnt += (end - start) / len(str(message))
        self.rcv_act += 1
        return message, receive_status, sender

    def send_all_subs_to_all_proc(self):
        probs = self.solver.getSubproblems(-1)
        probs_amnt = len(probs)
        part = 1 / (self.processes_amount - 1)
        state = ""
        for dest_proc in range(1, self.processes_amount):
            message_list = probs[int((dest_proc - 1) * probs_amnt * part): int(dest_proc * probs_amnt * part)]
            message = me.Message(
                message_type="subproblems",
                payload={
                    'problems': message_list,
                    'record': self.solver.max_profit
                }
            )
            start = round(time.time() - self.timer, 7)
            state, outputs = self.communicator.send(
                receiver=dest_proc,
                message=message
            )
            end = round(time.time() - self.timer, 7)
            self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                       "send_subproblems", f"subs_am={len(message_list)}, dest={dest_proc}")
            self.snd_cnt += (end - start) / len(str(message))
            self.snd_act += 1
        return state

    def send_all_subs_to_all_proc_rr(self):
        probs = self.solver.getSubproblems(-1)
        probs_amnt = len(probs)
        subs_to_send = {x: [] for x in range(self.processes_amount)}
        subs_to_send.pop(self.rank)

        state = ""
        cnt = 0
        while cnt < probs_amnt:
            index = cnt % self.processes_amount
            if index == self.rank:
                cnt += 1
                continue
            subs_to_send[index].append(probs[cnt])
            cnt += 1

        for dest_proc in range(0, self.processes_amount):
            if dest_proc == self.rank:
                continue
            message = me.Message(
                message_type="subproblems",
                payload={
                    'problems': subs_to_send[dest_proc],
                    'record': self.solver.max_profit
                }
            )
            start = round(time.time() - self.timer, 7)
            state, outputs = self.communicator.send(
                receiver=dest_proc,
                message=message
            )
            end = round(time.time() - self.timer, 7)
            self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                       "send_subproblems", f"subs_am={len(subs_to_send[dest_proc])}, dest={dest_proc}")
            self.snd_cnt += (end - start) / len(str(message))
            self.snd_act += 1
        return state

    def send_subs(self, subs_am, dest):
        message = me.Message(
            message_type="subproblems",
            payload={'problems': self.solver.getSubproblems(subs_am), 'record': self.solver.max_profit}
        )
        state, outputs = self.communicator.send(
            receiver=dest,
            message=message
        )
        return state


if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    if len(sys.argv) == 2:
        _, arg = sys.argv
        eng = Engine(proc_amount=size, comm=comm, arg=int(arg))
        eng.run()
    else:
        eng = Engine(proc_amount=size, comm=comm)
        eng.run()
