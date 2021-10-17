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


def find_best_arg():
    cur_arg = 1
    new_arg = 2
    coeff = 2
    multi = 1
    bashCommand = f'mpiexec --hostfile hostfile -n 10 python EngineSimple.py {cur_arg}'
    process = subprocess.Popen(bashCommand.split())
    output, error = process.communicate()
    multi *= coeff
    while cur_arg != new_arg:
        bashCommand = f'mpiexec --hostfile hostfile -n 10 python EngineSimple.py {new_arg}'
        process = subprocess.Popen(bashCommand.split())
        output, error = process.communicate()

        with open('argtime.csv', 'r') as f:
            line = f.read().split('\n')[-1]
            time, arg = line.split(',')
            time1 = float(time)
            arg1 = int(arg)

            line = f.read().split('\n')[-2]
            time, arg = line.split(',')
            time0 = float(time)
            arg0 = int(arg)

            if time1 >= time0:
                multi /= coeff
                cur_arg = arg1
                new_arg = round(arg0 * multi)


def find_best_arg_range():
    for i in range(40, 150, 3):
        bashCommand = f'mpiexec --hostfile hostfile -n 10 python EngineSimple.py {i}'
        process = subprocess.Popen(bashCommand.split())
        output, error = process.communicate()
        # if i // 3 <= 2:
        #     print(f'[*] step with arg={i} is done')
        #     continue
        #
        # with open('argtime.csv', 'r') as f:
        #     text = f.read().split('\n')
        #     line = text[-1]
        #     time, arg = line.split(',')
        #     time1 = float(time)
        #
        #     line = text[-2]
        #     time, arg = line.split(',')
        #     time0 = float(time)
        #
        #     if time1 >= time0:
        #         line = text[-3]
        #         time, arg = line.split(',')
        #         time_1 = float(time)
        #         if time0 >= time_1:
        #             break
        print(f'[*] step with arg={i} is done')


def sort_values():
    exp = []
    with open('experiments26.csv', 'r') as f:
        for line in f.read().split('\n')[:-1]:
            time, t, s, i = line.split(',')
            exp.append(Experiment(float(time), int(t), int(s), int(i)))
    sorted(exp)
    result = dict()
    res = []
    cur = []
    i_in = 26
    t_in = 400
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
    # make_experiment()
    # find_best_arg()
    find_best_arg_range()
