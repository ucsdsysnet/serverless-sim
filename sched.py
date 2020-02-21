#!/usr/bin/env python3

import os, sys
import math
from collections import OrderedDict

SANDBOX_CAP = 10

epoch = 0

metrics = { 'invoke':[],
            'finish':[],
            'cold-start':[], 
            'evict':[], 
            'inqueue':[],
            'non-home':[]}

def update_metric(metric):
    metrics[metric].append(epoch)

def dashboard():
    print('\n======== DASHBOARD ========')
    for k, l in metrics.items():
        print(k, ':', len(l))

class Function(object):
    def __init__(self, function_id, demand):
        self.function_id = function_id
        self.demand = demand

class Invocation(object):
    def __init__(self, function, duration):
        self.function = function
        self.duration = duration

class Host(object):
    def __init__(self, host_id, host_size):
        self.host_id = host_id
        self.host_size = host_size
        self.load = 0
        self.sandboxes = OrderedDict() # function_id -> # of active_invocations, also an LRU
        self.invocations = set()
    
    def tick(self):
        for i in list(self.invocations):
            i.duration -= 1
            if i.duration <= 0:
                self.finish(i)

    def install(self, function):
        self.sandboxes[function.function_id] = 1
        update_metric('cold-start')

    def evict(self):
        for k, v in self.sandboxes.items():
            if v == 0:
                self.sandboxes.pop(k)
        else:
            raise RuntimeError('cannot evict any sandbox')
        update_metric('evict')

    def invoke(self, invocation):
        function = invocation.function
        if function.function_id in self.sandboxes: # warm start
            self.sandboxes.move_to_end(function.function_id) # update LRU
            self.sandboxes[function.function_id] += 1 # update count
        else: # cold start
            if len(self.sandboxes) >= SANDBOX_CAP:
                self.evict() # evict
            self.install(function)
        self.invocations.add(invocation)
        self.load += function.demand
        update_metric('invoke')

    def finish(self, invocation):
        self.load -= invocation.function.demand
        self.sandboxes[invocation.function.function_id] -= 1
        self.invocations.remove(invocation)
        update_metric('finish')
    
    def full(self, function):
        if self.load + function.demand > self.host_size: # overload
            return True
        if len(self.sandboxes) < SANDBOX_CAP:
            return False
        for v in self.sandboxes.values():
            if v == 0: # has an evictable sandbox
                return False
        else:
            return True
    
    def describe(self):
        print('host: ', self.host_id, 'running functions:', len(self.invocations),'resources:', self.load, 'sandboxes:', len(self.sandboxes))

class Cluster(object):
    def __init__(self, hosts):
        self.hosts = hosts
        n = len(hosts)
        self.co_primes = [k for k in range(2, n) if math.gcd(k, n) == 1]

    def schedule(self, invocation):
        function = invocation.function
        chosen = function.function_id % len(self.hosts)
        stride = self.co_primes[function.function_id % len(self.co_primes)]
        remaining = len(self.hosts)
        while remaining > 0:
            if not self.hosts[chosen].full(function):
                break
            remaining -= 1
            stride = self.co_primes[function.function_id % len(self.co_primes)]
            chosen = (chosen + stride) % len(self.hosts)
            update_metric('non-home')
        else:
            return False
        target = self.hosts[chosen]
        target.invoke(invocation)
        return True

    def tick(self):
        for h in self.hosts:
            h.tick()
 
    def describe(self):
        for h in self.hosts:
            h.describe()

def main():
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
    global epoch
    for e in range(15):
        epoch = e
        cluster.tick()
        for i in invocations.get(epoch, []):
            invoke_queue.append(i)
        
        print('scheduling...')
        i = 0
        while i < len(invoke_queue):
            if cluster.schedule(invoke_queue[i]):
                invoke_queue.pop(i)
            else:
                update_metric('inqueue')
                i += 1
    
        print('\n======== epoch', epoch, '========')
        print('in queue:', len(invoke_queue))
        cluster.describe()

    dashboard()

if __name__=='__main__':
    main()
