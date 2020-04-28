#!/usr/bin/env python3

import os, sys
import math
import statistics
from collections import OrderedDict, deque
import common

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
    def __init__(self, host, function, install_time):
        self.host = host
        self.function = function
        self.time_to_install = install_time
        self.state = 'installing' # installing, idle, active
        self.invocation = None

class Host(object):
    def __init__(self, host_id, configs):
        self.host_id = host_id
        self.capacity = configs['capacity']
        self.invocation_per_host_cap = configs['invocation_per_host_cap']
        self.install_time = configs['install_time']
        self.cluster = None
        self.load = 0
        self.sb_load = 0
        self.sandboxes = list() # LRU
        self.invocations = set()
        self.request_queue = deque()
    
    def tick(self):
        for sb in list(self.sandboxes): # be sure to use a copy of list because self.sandboxes changes in iteration
            if sb.state == 'active':
                sb.invocation.duration -= 1
                if sb.invocation.duration <= 0:
                    self.finish(sb)

            if sb.time_to_install > 0:
                sb.time_to_install -= 1
                if sb.time_to_install == 0:
                    self.start(sb)
        
        if self.cluster.host_local_queue:
            if self.load == self.capacity:
                return
            while len(self.request_queue) > 0:
                req = self.request_queue.popleft()
                if not self.full(req.function):
                    self.invoke(req)
                else:
                    self.request_queue.appendleft(req)
                    break

    def install(self, invocation):
        sb = Sandbox(self, invocation.function, self.install_time)
        sb.invocation = invocation
        self.sandboxes.append(sb)
        metrics['cold-start'].append(self.cluster.epoch)

    def evict(self, load2evict):
        i = 0
        while i < len(self.sandboxes):
            if self.sandboxes[i].state == 'idle':
                released_load = self.sandboxes.pop(i).function.demand
                self.sb_load -= released_load
                load2evict -= released_load
                metrics['evict'].append(self.cluster.epoch)
                if load2evict <= 0:
                    break
            else:
                i += 1
        else:
            raise RuntimeError('cannot evict enough sandbox load')

    def start(self, sb):
        invocation = sb.invocation
        invocation.started = self.cluster.epoch
        sb.state = 'active'
        self.sandboxes.remove(sb)
        self.sandboxes.append(sb) # update LRU
        temp_stats['delay'].append(invocation.started - invocation.requested)
        metrics['start'].append(self.cluster.epoch)

    def invoke(self, invocation):
        function = invocation.function
        function_id = function.function_id
        self.invocations.add(invocation)
        self.load += function.demand
        self.cluster.load += function.demand

        available_sb = [sb for sb in self.sandboxes if sb.function.function_id == function_id and sb.state == 'idle']
        if len(available_sb) > 0: # warm start
            chosen_sb = common.gen.choice(available_sb) # OW currently randomly chooses warm sb
            chosen_sb.invocation = invocation
            self.start(chosen_sb)
            self.sb_load -= function.demand

        else: # cold start
            if self.load + self.sb_load > self.capacity:
                self.evict(self.load + self.sb_load - self.capacity) # evict
            self.install(invocation)

    def finish(self, sb):
        invocation = sb.invocation
        self.load -= invocation.function.demand
        self.cluster.load -= invocation.function.demand
        self.sb_load += invocation.function.demand
        sb.invocation = None
        sb.state = 'idle'
        self.invocations.remove(invocation)
        metrics['finish'].append(self.cluster.epoch)
    
    def full(self, function):
        if self.load + function.demand > self.capacity: # overload
            return True
        return len([sb for sb in self.sandboxes if sb.function.function_id == function.function_id and
            sb.state == 'active']) >= self.invocation_per_host_cap # consider full for the function if too many invocations on host already
    
    def describe(self):
        print('host: ', self.host_id, 'running:', len(self.invocations),'utilization:', self.load, 'sandboxes:', len(self.sandboxes))

class Cluster(object):
    def __init__(self, hosts, configs):
        self.capacity = 0
        self.load = 0
        for h in hosts:
            h.cluster = self
            self.capacity += h.capacity
        self.hosts = hosts
        n = len(hosts)
        self.co_primes = [k for k in range(2, n) if math.gcd(k, n) == 1]
        self.host_local_queue = configs['host_local_queue']
        self.request_queue = deque()
        self.epoch = 0

    def request(self, invocation):
        invocation.requested = self.epoch
        self.request_queue.append(invocation)
        metrics['request'].append(self.epoch)

    def tick(self):
        for h in self.hosts:
            h.tick()

        if self.host_local_queue:
            # process request queue
            for req in self.request_queue:
                self.schedule(req) # here schedule() should always return True
            self.request_queue.clear()
            # stats
            stats['inqueue'].append(sum([len(h.request_queue) for h in self.hosts]))
            stats['load'].append(self.load / self.capacity)

        else: # global queue
            # process request queue
            remaining_queue = deque()
            while len(self.request_queue) > 0:
                req = self.request_queue.popleft()
                if not self.schedule(req):
                    remaining_queue.append(req)
            self.request_queue = remaining_queue
            # stats
            stats['inqueue'].append(len(remaining_queue))
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
        def fallback_to_random(invoc):
            common.gen.choice(self.hosts).request_queue.append(invoc)
            temp_stats['distance'].append(len(self.hosts)/2)
            metrics['non-home'].append(self.epoch)
            
        function = invocation.function
        # overload fast path
        if self.load + function.demand > self.capacity:
            if self.host_local_queue:
                fallback_to_random(invocation)
                return True
            else:
                return False
        # choose target
        chosen = function.function_id % len(self.hosts)
        stride = self.co_primes[function.function_id % len(self.co_primes)]
        remaining = len(self.hosts)
        while remaining > 0:
            if not self.hosts[chosen].full(function):
                break
            remaining -= 1
            chosen = (chosen + stride) % len(self.hosts)
        else:
            if self.host_local_queue:
                fallback_to_random(invocation)
                return True
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
