import subprocess


class Experiment:

    def __init__(self, time, t, s, i) -> None:
        self.time = time
        self.t = t
        self.s = s
        self.i = i

    def __lt__(self, other):
        return self.t < other.t or self.t == other.t and self.s < other.s

    def __le__(self, other):
        return self.t < other.t or self.t == other.t and self.s <= other.s

    def __eq__(self, other):
        return self.t == other.t and self.s == other.s



def make_experiment():
    T = list(range(400, 1001, 100))
    S = list(range(30, 91, 10))
    # I = list(range(18, 27, 2))
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
    with open('experiments.csv', 'r') as f:
        for line in f.read().split('\n')[:-1]:
            time, t, s, i = line.split(',')
            exp.append(Experiment(float(time), int(t), int(s), int(i)))
    sorted(exp)
    result = dict()
    res = []
    cur = []
    i_in = 18
    t_in = 100
    ts = set()
    ss = set()
    for s in range(18, 27, 2):
        result[s] = []

    for e in exp:
        ts.add(e.t)
        ss.add(e.s)
        if e.i == i_in:
            if e.t == t_in:
                cur.append(e.time)
            else:
                res.append(cur)
                cur = [e.time]
                t_in = e.t
        else:
            result[i_in] = res
            res = []
            cur = [e.time]
            i_in = e.i
            t_in = 100
    res.append(cur)
    result[26] = res

    ts = sorted(list(ts))
    ss = sorted(list(ss))
    print(f'times={result}')
    print(f'ts={ts}')
    print(f'ss={ss}')


if __name__ == "__main__":
    # sort_values()
    make_experiment()
