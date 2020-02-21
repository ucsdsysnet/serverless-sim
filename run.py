#!/usr/bin/env python3

import cluster
from cluster import Cluster, Host, Function, Invocation, dashboard, update_metric
import workload

def main():
    # create cluster
    hosts = []
    for i in range(5):
        hosts.append(Host(i, 10))
    cluster = Cluster(hosts)

    # define functions
    functions = []
    for i in range(1):
        functions.append(Function(1, 2))

    # define invocations
    invocations = {3:[]}
    for _ in range(26):
        invocations[3].append(Invocation(functions[0], 4))

    invoke_queue = []
    # ticks
    for e in range(15):
        cluster.epoch = e
        cluster.tick()
        for i in invocations.get(cluster.epoch, []):
            invoke_queue.append(i)
        
        print('scheduling...')
        i = 0
        while i < len(invoke_queue):
            if cluster.schedule(invoke_queue[i]):
                invoke_queue.pop(i)
            else:
                update_metric('inqueue')
                i += 1
    
        print('\n======== epoch', cluster.epoch, '========')
        print('in queue:', len(invoke_queue))
        cluster.describe()

    dashboard()

def main2():
    # create cluster
    hosts = []
    for i in range(10):
        hosts.append(Host(i, 4))
    cluster = Cluster(hosts)

    # workloads
    requests1 = workload.burst_parallel(100)
    requests2 = workload.burst_parallel(100)

    # ticks
    invoke_queue = []
    for e in range(40):
        cluster.epoch = e
        cluster.tick()
        for i in requests1.get(cluster.epoch-2, []):
            invoke_queue.append(i)
        for i in requests2.get(cluster.epoch-4, []):
            invoke_queue.append(i)
        
        print('scheduling...')
        i = 0
        while i < len(invoke_queue):
            if cluster.schedule(invoke_queue[i]):
                invoke_queue.pop(i)
            else:
                update_metric('inqueue')
                i += 1
    
        print('\n======== epoch', cluster.epoch, '========')
        print('in queue:', len(invoke_queue))
        cluster.describe()

    dashboard()


if __name__=='__main__':
    main2()
