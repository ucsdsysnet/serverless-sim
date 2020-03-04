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
    # for i in range(1250):
    #     hosts.append(Host(i, 48))
    # for i in range(1250):
    #     hosts.append(Host(i, 16))
    for i in range(2500):
        hosts.append(Host(i, 32))    
    cluster = Cluster(hosts)

    # workloads
    gen = random.Random(seed)
    loads = []
    for i in range(9000): # apps 8000, 9000
        func = Function(gen.randint(0, 2**31), 1)
        loads.append(workloads.burst_parallel_app(func, 100, 0, 600, gen, 10, 5))
    merged = reduce(workloads.merge_invocs, loads)

    # ticks
    while len(merged) > 0 or not cluster.is_idle():
        for i in merged.get(cluster.epoch, []):
            cluster.request(i)
        merged.pop(cluster.epoch, None)
        cluster.tick()
        print('epoch:', cluster.epoch, file=sys.stderr)

    name = 'seed'+str(seed)+'homo-overload'
    dash = cluster.dashboard()
    with open(name, 'w') as f:
        f.write(dash)
    plot.plot(cluster.epoch, metrics, stats, name+'.png')

def run_mixed(seed):
    # create cluster
    hosts = []
    for i in range(2500):
        hosts.append(Host(i, 32))    
    cluster = Cluster(hosts)

    # workloads
    gen = random.Random(seed)
    loads = []
    for i in range(5000): # apps
        func = Function(gen.randint(0, 2**31), 1)
        loads.append(workloads.burst_parallel_app(func, 100, 0, 600, gen, 10, 5))

    for i in range(2000): # some larger apps
        func = Function(gen.randint(0, 2**31), 20)
        loads.append(workloads.burst_parallel_app(func, 1, 0, 600, gen, 10, 50))

    merged = reduce(workloads.merge_invocs, loads)

    # ticks
    while len(merged) > 0 or not cluster.is_idle():
        for i in merged.get(cluster.epoch, []):
            cluster.request(i)
        merged.pop(cluster.epoch, None)
        cluster.tick()
        print('epoch:', cluster.epoch, file=sys.stderr)

    name = 'seed'+str(seed)+'homo-mix-overload'
    dash = cluster.dashboard()
    with open(name, 'w') as f:
        f.write(dash)
    plot.plot(cluster.epoch, metrics, stats, name+'.png')


if __name__ == '__main__':
    run_mixed(sys.argv[1])
