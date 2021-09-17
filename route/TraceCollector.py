import pandas as pd


class TraceCollector:

    def __init__(self, filename, proc_id):
        self.filename = filename
        self.frame = {'timestamp' + str(proc_id): [0], 'state' + str(proc_id): ['init'], 'args' + str(proc_id): ['-']}

    def write(self, proc_num, timestamp, state, args):
        self.frame['timestamp' + str(proc_num)].append(timestamp)
        self.frame['state' + str(proc_num)].append(state)
        self.frame['args' + str(proc_num)].append(args)

    def save(self):
        df = pd.DataFrame.from_dict(self.frame, orient='index').transpose()
        df.to_csv(self.filename)
