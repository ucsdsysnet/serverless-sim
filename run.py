#!/usr/bin/env python3

import cluster
from cluster import Cluster, Host, Function, Invocation, dashboard, update_metric
import workload

def main():
    # create cluster
    hosts = []
    for i in range(3):
        hosts.append(Host(i, 10))
    cluster = Cluster(hosts)

    # define functions
    functions = []
    for i in range(1):
        functions.append(Function(1, 2))
        functions.append(Function(2, 2))
        functions.append(Function(3, 2))
        functions.append(Function(4, 2))
        

    # define invocations
    invocations = {1:[]}
    for n in range(12):
        invocations[1].append(Invocation(functions[n%4], 4))

    invoke_queue = []
    # ticks
    for e in range(10):
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
    for i in range(50):
        hosts.append(Host(i, 4))
    cluster = Cluster(hosts)

    # workloads
    requests1 = workload.burst_parallel(22)
    requests2 = workload.burst_parallel(25)
    requests3 = workload.burst_parallel(18)

    # ticks
    invoke_queue = []
    for e in range(50):
        cluster.epoch = e
        cluster.tick()
        for i in requests1.get(cluster.epoch-2, []):
            invoke_queue.append(i)
        for i in requests2.get(cluster.epoch-4, []):
            invoke_queue.append(i)
        for i in requests3.get(cluster.epoch-5, []):
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
