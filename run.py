#!/usr/bin/env python3

from functools import reduce
import workloads
from cluster import Cluster, Host, Function, Invocation, dashboard

def main():
    # create cluster
    hosts = []
    for i in range(3):
        hosts.append(Host(i, 10))
    c = Cluster(hosts)

    # define functions
    functions = []
    for i in range(1):
        functions.append(Function(1, 2))
        # functions.append(Function(2, 2))
        # functions.append(Function(3, 2))
        # functions.append(Function(4, 2))
        
    # define invocations
    invocations = {0:[]}
    for n in range(12):
        invocations[0].append(Invocation(functions[n%len(functions)], 4))

    # ticks
    for _ in range(10):
        for i in invocations.get(c.epoch, []):
            c.request(i)
        c.tick()
        c.describe()
    dashboard()

def main2():
    # create cluster
    hosts = []
    for i in range(10):
        hosts.append(Host(i, 4))
    cluster = Cluster(hosts)

    # workloads
    requests = [workloads.burst_parallel(0, 22), 
                workloads.burst_parallel(3, 25), 
                workloads.burst_parallel(4, 18), 
                workloads.burst_parallel(6, 16)]
    merged = reduce(workloads.merge_invocs, requests)

    # ticks
    for _ in range(20):
        for i in merged.get(cluster.epoch, []):
            cluster.request(i)
        cluster.tick()
        cluster.describe()

    dashboard()


if __name__=='__main__':
    main2()
