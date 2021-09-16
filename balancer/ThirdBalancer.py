import balancer.SimpleBalancer as sb


class MasterBalancer(sb.SimpleBalancer):
    def __init__(self, state, max_depth, proc_am, prc_blnc, alive_proc_am=0, T=0, S=0, m=0, M=0, arg=5):
        super().__init__(state, max_depth, proc_am, prc_blnc)
        if alive_proc_am == 0:
            self.alive_proc_am = proc_am - 1
        else:
            self.alive_proc_am = alive_proc_am
        self.T = T
        self.S = S
        self.M = M
        self.m = m
        self.arg = arg

    def balance(self, state, subs_amount, add_args=None):
        self.state = state
        if state == "starting":
            return "solve", [self.proc_am]
        elif state == "solved" or state == "nothing_to_receive":
            return "receive", []
        elif state == "received_get_request":
            if isinstance(add_args, list) and len(add_args) == 3 \
                    and isinstance(add_args[0], list) and len(add_args[0]) == 2:
                info = add_args[0]
                get_amount, sender = info[0], info[1]
                if subs_amount == 0:
                    self.alive_proc_am -= 1
                    return "send_exit_command", [sender]
                elif subs_amount >= get_amount:
                    return "send_subproblems", [sender, get_amount]
                elif subs_amount < get_amount:
                    return "send_subproblems", [sender, subs_amount]
            else:
                raise Exception(f"Wrong args list format: {add_args}")
        elif state == "received_subproblems":
            self.state = "receive"
            return "receive", []
        elif state == "sent_subproblems" or state == "sent_get_request" or state == "sent_exit_command":
            if self.alive_proc_am == 0:
                self.state = "exit"
                return "exit", []
            else:
                self.state = "receive"
                return "receive", []


class SlaveBalancer(sb.SimpleBalancer):

    def __init__(self, state, max_depth, proc_am, prc_blnc, alive_proc_am=0, T=5, S=5, m=0, M=0, arg=5):
        super().__init__(state, max_depth, proc_am, prc_blnc)
        self.alive_proc_am = alive_proc_am
        self.T = T
        self.S = S
        self.M = M
        self.m = m
        self.arg = arg

    def balance(self, state, subs_amount, add_args=None):
        self.state = state
        if self.state == "sent_get_request" or self.state == "sent":
            return "receive", []
        elif self.state == "starting":
            if isinstance(add_args, list) and len(add_args) == 3 \
                    and isinstance(add_args[0], list):
                isSentGR = add_args[1]
                if not isSentGR:
                    return "send_get_request", [0, 2]
                else:
                    raise Exception(f"Double needance of sending GR")
            else:
                raise Exception(f"Wrong args list format: {add_args}")
        elif state == "received_subproblems":
            return "solve", [self.T]
        elif self.state == "solved" or self.state == 'sent_subproblems':
            if subs_amount > 0:
                if subs_amount > self.S:
                    return "send_subproblems", [0, self.S]
                else:
                    return "solve", [self.T]
            else:
                if isinstance(add_args, list) and len(add_args) == 3 \
                        and isinstance(add_args[0], list):
                    if not add_args[1]:
                        return "send_get_request", [0, 2]
                    else:
                        raise Exception(f"Wrong args list format: {add_args}")
                else:
                    raise Exception(f"Wrong args list format: {add_args}")
        elif state == "received_exit_command":
            return "exit", []
        else:
            print(state)
            raise Exception("no suitable state discovered")
