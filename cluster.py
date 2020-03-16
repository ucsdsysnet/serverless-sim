#!/usr/bin/env python3

import os, sys
import math
import statistics
from collections import OrderedDict

SANDBOX_CAP = 50
INVOCATIONS_CAP_PER_SB = 8
INSTALL_TIME = 3

metrics = { 'request':[],
            'start':[],
            'finish':[],
            'cold-start':[], 
            'evict':[], 
            'non-home':[]}

stats  =  { 'load':[],
            'inqueue':[],
            'delay':[],
            'distance':[]}

temp_stats = {  'delay':[],
                'distance':[]}

class Function(object):
    def __init__(self, function_id, demand):
        self.function_id = function_id
        self.demand = demand

class Invocation(object):
    def __init__(self, function, duration):
        self.function = function
        self.duration = duration
        self.requested = 0
        self.started = 0

class Sandbox(object):
    def __init__(self, host, function):
        self.host = host
        self.function = function
        self.state = 'installing' # installing, idle, active
        self.time_to_install = INSTALL_TIME
        self.invocations = set()

class Host(object):
    def __init__(self, host_id, capacity):
        self.host_id = host_id
        self.capacity = capacity
        self.cluster = None
        self.load = 0
        self.sandboxes = OrderedDict() # function_id -> Sandbox, also an LRU
        self.invocations = set()
    
    def tick(self):
        for i in list(self.invocations):
            if i.started:
                i.duration -= 1
                if i.duration <= 0:
                    self.finish(i)

        for sb in self.sandboxes.values():
            if sb.time_to_install > 0:
                sb.time_to_install -= 1
                if sb.time_to_install == 0:
                    for i in sb.invocations:
                        self.start(i)

    def install(self, function):
        self.sandboxes[function.function_id] = Sandbox(self, function)
        metrics['cold-start'].append(self.cluster.epoch)

    def evict(self):
        for k, sb in list(self.sandboxes.items()):
            if sb.state == 'idle':
                self.sandboxes.pop(k)
                break
        else:
            raise RuntimeError('cannot evict any sandbox')
        metrics['evict'].append(self.cluster.epoch)

    def start(self, invocation):
        invocation.started = self.cluster.epoch
        temp_stats['delay'].append(invocation.started - invocation.requested)
        self.sandboxes[invocation.function.function_id].state = 'active'
        metrics['start'].append(self.cluster.epoch)

    def invoke(self, invocation):
        function = invocation.function
        self.invocations.add(invocation)
        self.load += function.demand
        self.cluster.load += function.demand

        if function.function_id in self.sandboxes: # warm start
            sb = self.sandboxes[function.function_id]
            self.sandboxes.move_to_end(function.function_id) # update LRU
            sb.invocations.add(invocation)
            if sb.state != 'installing': # sandbox ready
                self.start(invocation)

        else: # cold start
            if len(self.sandboxes) >= SANDBOX_CAP:
                self.evict() # evict
            self.install(function)
            self.sandboxes[function.function_id].invocations.add(invocation)

    def finish(self, invocation):
        self.load -= invocation.function.demand
        self.cluster.load -= invocation.function.demand
        sb = self.sandboxes[invocation.function.function_id]
        sb.invocations.remove(invocation)
        if len(sb.invocations) == 0:
            sb.state = 'idle'
        self.invocations.remove(invocation)
        metrics['finish'].append(self.cluster.epoch)
    
    def full(self, function):
        if self.load + function.demand > self.capacity: # overload
            return True
        if function.function_id in self.sandboxes:
            return len(self.sandboxes[function.function_id].invocations) >= INVOCATIONS_CAP_PER_SB # consider full if too many invocations on SB already
        if len(self.sandboxes) < SANDBOX_CAP:
            return False
        for sb in self.sandboxes.values():
            if sb.state == 'idle': # has an evictable sandbox
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
        while i < len(self.request_queue):
            if self.schedule(self.request_queue[i]):
                self.request_queue.pop(i)
            else:
                i += 1
        stats['inqueue'].append(len(self.request_queue))
        stats['load'].append(self.load / self.capacity)
        for k, v in temp_stats.items():
            if len(v) == 0:
                stats[k].append(0.0)
            else:
                stats[k].append(sum(v) / len(v))
            temp_stats[k] = list()

        self.epoch += 1 # tick

    def is_idle(self):
        if not len(self.request_queue) == 0:
            return False
        return all([len(h.invocations) == 0 for h in self.hosts])

    def schedule(self, invocation):
        function = invocation.function
        # overload fast path
        if self.load + function.demand > self.capacity:
            return False
        chosen = function.function_id % len(self.hosts)
        stride = self.co_primes[function.function_id % len(self.co_primes)]
        remaining = len(self.hosts)
        while remaining > 0:
            if not self.hosts[chosen].full(function):
                break
            remaining -= 1
            chosen = (chosen + stride) % len(self.hosts)
        else:
            return False
        target = self.hosts[chosen]
        target.invoke(invocation)
        temp_stats['distance'].append(len(self.hosts) - remaining)
        if remaining < len(self.hosts):
            metrics['non-home'].append(self.epoch)
        return True
 
    def describe(self):
        print('======== epoch', self.epoch, '========')
        print('cluster load :\t\t', self.load/self.capacity)
        print('waiting: \t\t', len(self.request_queue))
        for h in self.hosts:
            h.describe()

    def dashboard(self):
        ret = '======== DASHBOARD ========\n'
        ret += 'finish epoch  \t\t {0}\n'.format(self.epoch)
        for k, l in metrics.items():
            ret += '{0}  \t\t {1}\n'.format(k, len(l))
        ret += 'average load: \t\t {0}\n'.format(statistics.mean(stats['load']))
        return ret
