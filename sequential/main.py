from typing import List
from queue import Queue


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


class Solver:

    def __init__(self):
        self.max_profit = 0
        self.tasks_q = Queue()

    def initialize_amount(self):
        return 5

    def initialize_weight(self):
        return 10

    def initialize_items(self):
        return [Item(2, 40), Item(3.1, 50), Item(1.98, 100), Item(5, 95), Item(3, 30)]

    def cmp(self, i1: Item, i2: Item):
        return i1.value / i1.weight > i2.value / i2.weight

    def bound(self, u: Node, n: int, w: int, arr: List[Item]) -> int:
        if u.weight > w:
            return 0
        profit_bound = u.profit
        j = u.level + 1
        total_w = u.weight

        while j < n and (total_w + arr[j].weight <= w):
            total_w += arr[j].weight
            profit_bound += arr[j].value
            j += 1

        if j < n:
            profit_bound += int((w - total_w) * arr[j].value / arr[j].weight)

        return profit_bound

    def ramify(self, u: Node, n: int, arr: List[Item], W: int) -> None:
        v_in = Node(0, 0, 0, 0)
        if u.level == n - 1:
            return

        v_in.level = u.level + 1
        v_in.weight = u.weight + arr[v_in.level].weight
        v_in.profit = u.profit + arr[v_in.level].value

        if v_in.weight <= W and v_in.profit > self.max_profit:
            self.max_profit = v_in.profit

        v_in.bound = self.bound(v_in, n, W, arr)
        if v_in.bound > self.max_profit:
            self.tasks_q.put(v_in)

        v_out = Node(0, 0, 0, 0)
        v_out.level = u.level + 1
        v_out.weight = u.weight
        v_out.profit = u.profit
        v_out.bound = self.bound(v_out, n, W, arr)
        if v_out.bound > self.max_profit:
            self.tasks_q.put(v_out)


if __name__ == '__main__':
    solver = Solver()
    n = solver.initialize_amount()
    W = solver.initialize_weight()
    items = solver.initialize_items()
    items.sort()
    items.reverse()

    # root = Node(-1, 0, 0, 0)
    root = Node(0, items[0].value, 0, items[0].weight)
    root.bound = solver.bound(root, n, W, items)

    print(items)
    solver.tasks_q.put(root)
    while not solver.tasks_q.empty():
        solver.ramify(solver.tasks_q.get(), n, items, W)
    print(solver.max_profit)

