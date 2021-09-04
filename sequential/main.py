import queue
from functools import cmp_to_key

from typing import List


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
        self.weight = w
        self.profit = p
        self.bound = b


class Solver:

    def __init__(self):
        self.max_profit = 0

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
        v = Node(0, 0, 0, 0)
        if u.level == n - 1:
            return

        v.level = u.level + 1
        v.weight = u.weight + arr[v.level].weight
        v.profit = u.profit + arr[v.level].value

        if v.weight <= W and v.profit > self.max_profit:
            self.max_profit = v.profit

        v.bound = self.bound(v, n, W, arr)
        max_profit1 = 0
        if v.bound > self.max_profit:
            self.ramify(v, n, arr, W)
            # max_profit1 = ramify(v, n, arr, max_profit, W)

        v.weight = u.weight
        v.profit = u.profit
        v.bound = self.bound(v, n, W, arr)
        max_profit2 = 0
        if v.bound > self.max_profit:
            self.ramify(v, n, arr, W)
            # max_profit2 = ramify(v, n, arr, max_profit, W)
        #
        # max_profit = max(max_profit1, max_profit2, max_profit)
        # return max_profit


if __name__ == '__main__':
    solver = Solver()
    n = solver.initialize_amount()
    W = solver.initialize_weight()
    items = solver.initialize_items()
    items.sort()
    items.reverse()

    root = Node(-1, 0, 0, 0)

    print(items)
    solver.ramify(root, n, items, W)
    print(solver.max_profit)

