import subprocess


if __name__ == "__main__":
    T = list(range(220, 401, 30))
    S = list(range(13, 39, 5))
    for t in T:
        for s in S:
            bashCommand = f'mpiexec --hostfile hostfile -n 10 python EngineComplex.py {t} {s}'
            process = subprocess.Popen(bashCommand.split())
            output, error = process.communicate()
            print(f'[*] Step done:  T={t},  S={s}')
