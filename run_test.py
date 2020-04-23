#!/usr/bin/env python3

import sys
import random
from functools import reduce
import json
import hashlib

from cluster import Cluster, Host, Function, Invocation, metrics, stats
import workload
import plot

def test():
    # create cluster
    hosts = []
    for i in range(3):
        hosts.append(Host(i, 10))
    c = Cluster(hosts)

    f1 = Function(1, 2)
    f2 = Function(2, 1)
    # define invocations
    invocations =  {0:[Invocation(f1, 2) for _ in range(20)],
                    10:[Invocation(f2, 2) for _ in range(20)]}

    # ticks
    while len(invocations) > 0 or not c.is_idle():
        for i in invocations.get(c.epoch, []):
            c.request(i)
        invocations.pop(c.epoch, None)

        c.tick()
        c.describe()
    c.dashboard()
    plot.plot(c.epoch, metrics, stats, 'test.png')


if __name__ == '__main__':
    # import cProfile

    # cProfile.run('azuretest()', 'restats')
    test()
