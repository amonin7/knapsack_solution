import time
from typing import List
from queue import Queue
import random as rnd
from math import floor


class Item:
    weight: float
    value: int

    def __init__(self, w: float, v: int):
        self.weight = w
        self.value = v

    def __str__(self):
        return "{" + str(self.weight) + ", " + str(self.value) + "}"

    def __repr__(self):
        return "{" + str(self.weight) + ", " + str(self.value) + "}"

    def __cmp__(self, other):
        return self.value / self.weight > other.value / other.weight

    def __lt__(self, other):
        return self.value / self.weight < other.value / other.weight

    def __le__(self, other):
        return self.value / self.weight <= other.value / other.weight

    def __eq__(self, other):
        return self.value / self.weight == other.value / other.weight


class Node:
    level: int
    profit: int
    bound: int
    weight: float

    def __init__(self, le: int, p: int, b: int, w: float):
        self.level = le
        self.profit = p
        self.bound = b
        self.weight = w


def nodeToDict(v: Node) -> dict:
    return {
        "level": v.level,
        "profit": v.profit,
        "bound": v.bound,
        "weight": v.weight
    }


def dictToNode(d: dict) -> Node:
    return Node(d['level'], d['profit'], d['bound'], d['weight'])


class Solver:

    def __init__(self, subproblems):
        self.n = self.initialize_amount()
        self.arr, total_w = self.initialize_items()
        self.arr.sort()
        self.arr.reverse()
        self.w = self.initialize_weight(total_w)
        self.max_profit = 0
        self.tasks_q = Queue()
        for el in subproblems:
            self.tasks_q.put(el)

    def getSubproblems(self, subprobs_amount):
        result = []
        if subprobs_amount == -1:
            while not self.tasks_q.empty():
                result.append(self.tasks_q.get())
        else:
            while not self.tasks_q.empty() and subprobs_amount > 0:
                result.append(self.tasks_q.get())
                subprobs_amount -= 1
        return result

    def putSubproblems(self, new_subproblems):
        for node in new_subproblems:
            self.tasks_q.put(node)

    def get_sub_amount(self):
        return self.tasks_q.qsize()

    def initialize_amount(self):
        return 20

    def initialize_weight(self, total_w=10):
        return 2 * floor(self.n / 2) + 1
    #
    # def initialize_weight(self, total_w=10):
    #     return 1 / 2 * total_w

    # ToDo: отделить функцию (вынести)
    # def initialize_items(self):
    #     rnd.seed(42)
    #     R = 1000
    #     items = list()
    #     total_w = 0
    #     for i in range(self.initialize_amount()):
    #         w = int(rnd.uniform(1, R) * 100) / 100
    #         p = 0
    #         while p <= 1:
    #             p = rnd.randint(int(w) - R // 100, int(w) + R // 100)
    #         i = Item(w, p)
    #         items.append(i)
    #         total_w += w
    #     return items, total_w

    # def initialize_items(self):
    #     return [Item(2, 40), Item(3.1, 50), Item(1.98, 100), Item(5, 95), Item(3, 30), Item(3, 30), Item(3, 30), Item(3, 30), Item(3, 30), Item(3, 30), Item(3, 30), Item(3, 30), Item(3, 30), Item(3, 30), Item(3, 30)]
    #
    # def initialize_items(self):
    #     return [Item(2, 40), Item(3.1, 50), Item(1.98, 100), Item(5, 95), Item(3, 30)]

    def initialize_items(self):
        items = list()
        total_w = 0
        n = self.initialize_amount()
        for i in range(n):
            w = 2
            p = 2
            i = Item(w, p)
            items.append(i)
        return items, total_w

    def cmp(self, i1: Item, i2: Item):
        return i1.value / i1.weight > i2.value / i2.weight

    def bound(self, u: Node) -> int:
        if u.weight > self.w:
            return 0
        profit_bound = u.profit
        j = u.level + 1
        total_w = u.weight

        while j < self.n and (total_w + self.arr[j].weight <= self.w):
            total_w += self.arr[j].weight
            profit_bound += self.arr[j].value
            j += 1

        if j < self.n:
            profit_bound += int((self.w - total_w) * self.arr[j].value / self.arr[j].weight)

        return profit_bound

    def branch(self, u: Node) -> None:
        v_in = Node(0, 0, 0, 0)
        if u.level == self.n - 1:
            return

        v_in.level = u.level + 1
        v_in.weight = u.weight + self.arr[v_in.level].weight
        v_in.profit = u.profit + self.arr[v_in.level].value

        if v_in.weight <= self.w and v_in.profit > self.max_profit:
            self.max_profit = v_in.profit

        v_in.bound = self.bound(v_in)
        if v_in.bound > self.max_profit:
            self.tasks_q.put(v_in)

        v_out = Node(0, 0, 0, 0)
        v_out.level = u.level + 1
        v_out.weight = u.weight
        v_out.profit = u.profit
        v_out.bound = self.bound(v_out)
        if v_out.bound > self.max_profit:
            self.tasks_q.put(v_out)

    def solve(self, n):
        cnt = 0
        if n > 0:
            while n > 0 and not self.tasks_q.empty():
                self.branch(self.tasks_q.get())
                cnt += 1
                n -= 1
        else:
            while not self.tasks_q.empty():
                self.branch(self.tasks_q.get())
                cnt += 1
        return "solved", cnt


if __name__ == '__main__':
    solver = Solver([])
    s2 = Solver([])
    root = Node(0, solver.arr[0].value, 0, solver.arr[0].weight)
    root.bound = solver.bound(root)

    solver.tasks_q.put(root)
    start = time.time()
    while not solver.tasks_q.empty():
        solver.branch(solver.tasks_q.get())
    print(solver.max_profit)
    print(f"time: {int((time.time() - start) * 1000) / 1000}")
