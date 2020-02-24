#!/usr/bin/env python3

import sys
import random
from functools import reduce
import workloads
from cluster import Cluster, Host, Function, Invocation, metrics, stats
import plot

def test():
    # create cluster
    hosts = []
    for i in range(3):
        hosts.append(Host(i, 10))
    c = Cluster(hosts)

    # define functions
    functions = []
    for i in range(1):
        functions.append(Function(1, 2))
        functions.append(Function(3, 2))
        # functions.append(Function(3, 2))
        # functions.append(Function(4, 2))
        
    # define invocations
    invocations = {0:[]}
    for n in range(16):
        invocations[0].append(Invocation(functions[n%len(functions)], 4))

    # ticks
    while len(invocations) > 0 or not c.is_idle():
        for i in invocations.get(c.epoch, []):
            c.request(i)
        invocations.pop(c.epoch, None)

        c.tick()
        c.describe()
    c.dashboard()
    plot.plot(c.epoch, metrics, stats, 'test.png')

def run(seed):
    # create cluster
    hosts = []
    for i in range(105):
        hosts.append(Host(i, 4))
    cluster = Cluster(hosts)

    # workloads
    gen = random.Random(seed)
    loads = []
    for i in range(20): # 20 apps
        func = Function(gen.randint(0, 2**31), 1)
        loads.append(workloads.burst_parallel_app(func, 100, start=gen.randint(0, 300)))
    merged = reduce(workloads.merge_invocs, loads)

    # ticks
    while len(merged) > 0 or not cluster.is_idle():
        for i in merged.get(cluster.epoch, []):
            cluster.request(i)
        merged.pop(cluster.epoch, None)
        cluster.tick()

    cluster.dashboard()
    plot.plot(cluster.epoch, metrics, stats, 'seed'+str(seed)+'.png')

if __name__ == '__main__':
    run(sys.argv[1])
