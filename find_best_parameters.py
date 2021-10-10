import subprocess


class Experiment:

    def __init__(self, time, t, s) -> None:
        self.time = time
        self.t = t
        self.s = s

    def __lt__(self, other):
        return self.t < other.t or self.t == other.t and self.s < other.s

    def __le__(self, other):
        return self.t < other.t or self.t == other.t and self.s <= other.s

    def __eq__(self, other):
        return self.t == other.t and self.s == other.s



def make_experiment():
    T = list(range(100, 501, 40))
    S = list(range(10, 31, 5))
    I = list(range(18, 27, 2))
    # for i in I:
    i = 26
    for t in T:
        for s in S:
            bashCommand = f'mpiexec --hostfile hostfile -n 10 python EngineComplex.py {t} {s} {i}'
            process = subprocess.Popen(bashCommand.split())
            output, error = process.communicate()
            print(f'[*] Step done:  T={t},  S={s},  I={i}')


def sort_values():
    exp = []
    with open('ts_times2.csv', 'r') as f:
        for line in f.read().split('\n')[:-1]:
            time, t, s = line.split(',')
            exp.append(Experiment(float(time), int(t), int(s)))
    sorted(exp)
    res = []
    cur = []
    t_in = 220
    ts = set()
    ss = set()
    for e in exp:
        ts.add(e.t)
        ss.add(e.s)
        if e.t == t_in:
            cur.append(e.time)
        else:
            res.append(cur)
            cur = [e.time]
            t_in = e.t
    res.append(cur)
    ts = sorted(list(ts))
    ss = sorted(list(ss))
    print(f'times={res}')
    print(f'ts={ts}')
    print(f'ss={ss}')


if __name__ == "__main__":
    # sort_values()
    make_experiment()