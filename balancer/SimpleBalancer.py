class SimpleBalancer:

    def __init__(self, state, max_depth, proc_am, prc_blnc=0):
        self._state = state
        self.max_depth = max_depth
        self.prc_blnc = prc_blnc
        self.proc_am = proc_am

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    def balance(self, state, subs_amount, add_args=None):
        print("Balancing")


class MasterBalancer(SimpleBalancer):

    def __init__(self, state, max_depth, proc_am, prc_blnc, arg=5):
        super().__init__(state, max_depth, proc_am, prc_blnc)
        self.arg = arg

    '''
    :returns status, where to send, how many to send
    where to send -- either list of proc numbers or -1 (means all others)
    how many to send -- either list of amounts of tasks to each process to send
    or -1 (means all tasks should be separated into equal groups and send to all processes)
    '''

    def balance(self, state, subs_amount, add_args=None):
        self.state = state
        if self.state == "starting":
            return "solve", [self.proc_am * self.arg]
        if self.state == "solved":
            return "send_all", [[-1], [-1]]
        if self.state == "sent_subproblems":
            return "exit", []


class SlaveBalancer(SimpleBalancer):
    def __init__(self, state, max_depth, proc_am, prc_blnc, arg=5):
        super().__init__(state, max_depth, proc_am, prc_blnc)
        self.arg = arg

    def balance(self, state, subs_amount, add_args=None):
        self.state = state
        if self.state == "starting":
            return "receive", []
        elif self.state == "received_subproblems":
            return "solve", [-1]
        elif self.state == "solved":
            return "exit", []
        else:
            return "bound", []
