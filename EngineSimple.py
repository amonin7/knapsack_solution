import time
from mpi4py import MPI
import balancer.SimpleBalancer as sb
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
            start = round(time.time() - self.timer, 8)
            command, outputs = self.balancer.balance(state=state,
                                                     subs_amount=self.solver.get_sub_amount(),
                                                     add_args=[[], self.isSentRequest, self.rank])
            end = round(time.time() - self.timer, 8)
            self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7):.7f}", "balance",
                                       f"state={self.state}")
            self.blc_cnt += end - start
            self.blc_act += 1
            if command == "receive":
                start = round(time.time() - self.timer, 8)
                receive_status, message, sender = self.communicator.receive()
                end = round(time.time() - self.timer, 8)
                self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}", "receive",
                                           f"mes_type={message.message_type}")
                self.rcv_cnt += (end - start) / len(str(message))
                self.rcv_act += 1
                if receive_status != "received_exit_command":
                    if receive_status == "received_subproblems":
                        self.solver.putSubproblems(message.payload['problems'])
                        if self.solver.max_profit > message.payload['record']:
                            self.solver.max_profit = message.payload['record']

                    start = round(time.time() - self.timer, 8)
                    command, outputs = self.balancer.balance(state=receive_status,
                                                             subs_amount=self.solver.get_sub_amount(),
                                                             add_args=message)
                    end = round(time.time() - self.timer, 8)
                    self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                               "balance", f"state={receive_status}")
                    self.blc_cnt += end - start
                    self.blc_act += 1
                    if command == "send_subs":
                        pass
                    elif command == "solve":
                        tasks_am = outputs[0]
                        start = round(time.time() - self.timer, 8)
                        self.state, solved_amount = self.solver.solve(tasks_am)
                        self.subs_am += solved_amount
                        end = round(time.time() - self.timer, 8)
                        self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                                   command,
                                                   f"tasks_am={tasks_am}")
                        self.slv_cnt += (end - start) / solved_amount
                        self.slv_act += 1
                else:
                    break
            elif command == "send_all":
                self.state = self.send_all_subs_to_all_proc_rr()
            elif command == "solve":
                tasks_am = outputs[0]
                start = round(time.time() - self.timer, 8)
                self.state, solved_amount = self.solver.solve(tasks_am)
                self.subs_am += solved_amount
                end = round(time.time() - self.timer, 8)
                self.route_collector.write(self.rank, f"{start:.7f}-{end}", command,
                                           f"tasks_am={tasks_am}")
                self.slv_cnt += (end - start) / solved_amount
                self.slv_act += 1
            elif command == "exit":
                start = round(time.time() - self.timer, 7)
                self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}", command,
                                           "")
                break
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
        subs_total = self.comm.reduce(self.subs_am, MPI.SUM, root=0)
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
            print(f"subs_am={subs_total}")
            #
            # max_time = float(self.route_collector.frame['timestamp0'][-1].split('-')[1])
            # print(f"maximum time    : {max_time}")
            with open('experimental_data/argtime-rr-big-values.csv', 'a') as f:
                f.write(f'\n{m_time},{self.arg}')
            print(m_time)
        traces = self.comm.gather(self.route_collector.frame, root=0)
        if self.rank == 0:
            res = {}
            for d in traces:
                res.update(d)
            self.route_collector.frame = res
            self.route_collector.save()
        # self.comm_collector.save()

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
            outputs = []
            state = self.communicator.send(
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
            outputs = []
            state = self.communicator.send(
                receiver=dest_proc,
                message=message
            )
            end = round(time.time() - self.timer, 7)
            self.route_collector.write(self.rank, f"{start:.7f}-{round(time.time() - self.timer, 7)}",
                                       "send_subproblems", f"subs_am={len(subs_to_send[dest_proc])}, dest={dest_proc}")
            self.snd_cnt += (end - start) / len(str(message))
            self.snd_act += 1
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
