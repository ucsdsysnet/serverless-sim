#!/usr/bin/env python3

import os, sys
import math
import statistics
from collections import OrderedDict

SANDBOX_CAP = 10

metrics = { 'request':[],
            'invoke':[],
            'finish':[],
            'cold-start':[], 
            'evict':[], 
            'non-home':[]}

stats  =  { 'load':[],
            'inqueue':[],
            'average-delay':[]}

class Function(object):
    def __init__(self, function_id, demand):
        self.function_id = function_id
        self.demand = demand

class Invocation(object):
    def __init__(self, function, duration):
        self.function = function
        self.duration = duration
        self.requested = 0

class Host(object):
    def __init__(self, host_id, capacity):
        self.host_id = host_id
        self.capacity = capacity
        self.cluster = None
        self.load = 0
        self.sandboxes = OrderedDict() # function_id -> # of invocations, also an LRU
        self.invocations = set()
    
    def tick(self):
        for i in list(self.invocations):
            i.duration -= 1
            if i.duration <= 0:
                self.finish(i)

    def install(self, function):
        self.sandboxes[function.function_id] = 1
        metrics['cold-start'].append(self.cluster.epoch)

    def evict(self):
        for k, v in list(self.sandboxes.items()):
            if v == 0:
                self.sandboxes.pop(k)
                break
        else:
            raise RuntimeError('cannot evict any sandbox')
        metrics['evict'].append(self.cluster.epoch)

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
        self.cluster.load += function.demand
        metrics['invoke'].append(self.cluster.epoch)

    def finish(self, invocation):
        self.load -= invocation.function.demand
        self.cluster.load -= invocation.function.demand
        self.sandboxes[invocation.function.function_id] -= 1
        self.invocations.remove(invocation)
        metrics['finish'].append(self.cluster.epoch)
    
    def full(self, function):
        if self.load + function.demand > self.capacity: # overload
            return True
        if len(self.sandboxes) < SANDBOX_CAP:
            return False
        for v in self.sandboxes.values():
            if v == 0: # has an evictable sandbox
                return False
        else:
            return True
    
    def describe(self):
        print('host: ', self.host_id, 'running:', len(self.invocations),'utilization:', self.load, 'sandboxes:', len(self.sandboxes))

class Cluster(object):
    def __init__(self, hosts):
        self.capacity = 0
        self.load = 0
        for h in hosts:
            h.cluster = self
            self.capacity += h.capacity
        self.hosts = hosts
        n = len(hosts)
        self.co_primes = [k for k in range(2, n) if math.gcd(k, n) == 1]
        self.request_queue = []
        self.epoch = 0

    def request(self, invocation):
        invocation.requested = self.epoch
        self.request_queue.append(invocation)
        metrics['request'].append(self.epoch)

    def tick(self):
        for h in self.hosts:
            h.tick()
        i = 0
        delay_sum = 0
        invoked = 0
        while i < len(self.request_queue):
            if self.schedule(self.request_queue[i]):
                invoked += 1
                delay_sum += self.epoch - self.request_queue[i].requested
                self.request_queue.pop(i)
            else:
                i += 1
        stats['inqueue'].append(len(self.request_queue))
        stats['load'].append(self.load)
        if invoked == 0:
            stats['average-delay'].append(0.0)
        else:
            stats['average-delay'].append(delay_sum / invoked)

        self.epoch += 1

    def is_idle(self):
        if not len(self.request_queue) == 0:
            return False
        return all([len(h.invocations) == 0 for h in self.hosts])

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
            metrics['non-home'].append(self.epoch)
        else:
            return False
        target = self.hosts[chosen]
        target.invoke(invocation)
        return True
 
    def describe(self):
        print('======== epoch', self.epoch, '========')
        print('cluster load :\t\t', self.load/self.capacity)
        print('waiting: \t\t', len(self.request_queue))
        for h in self.hosts:
            h.describe()

    def dashboard(self):
        print('\n======== DASHBOARD ========')
        print('finish epoch  \t\t', self.epoch)
        for k, l in metrics.items():
            print(k, '  \t\t', len(l))
        print('average load: \t\t', statistics.mean(stats['load'])/self.capacity)
