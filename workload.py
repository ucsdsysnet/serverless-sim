#!/usr/bin/env python3

import math
import common
from cluster import Function, Invocation

def burst(func, start, duration, parallelism):
    invocs = {}
    invocs[start] = [Invocation(func, duration) for _ in range(parallelism)]
    return invocs

def burst_parallel_app(func, parallelism, start, end, n_bursts, duration):
    invocs = {}
    for i in range(n_bursts):
        invocs = merge_invocs(invocs, burst(func, int(common.random.randint(start, end)), duration, parallelism))
    return invocs

def faas(ntasks):
    pass

"""
See Azure's "Serverless in the Wild" paper.
Knobs:
    - timespan in seconds
    - number of functions (possibly not all functions have invocations)
    - number of invocations (possibly less than this number)
"""
def azure(span, n_functions, n_invocations, dist_mu, dist_sigma, CV, start_window, start_load, BP_percentage, **kwargs):
    dist_mu = -7.85
    dist_sigma = 2.75
    # create functions
    fns = [Function(i, 1) for i in range(n_functions)] # TODO: use memory distribution as "demand"
    function_dist = [0] * n_functions # fn -> invocations of that fn

    # allocate invocations to functions
    created = 0
    while created < n_invocations:
        fnid = int(common.random.lognormvariate(dist_mu, dist_sigma) * n_functions)
        if fnid < n_functions:
            function_dist[fnid] += 1
            created += 1

    # allocate CVs of IAT for each function
    CVs = []
    for _ in range(n_functions):
        CVs.append(1) # TODO: use CV distribution, using random.choice(), make sure in [0, 10]
    
    # distribution function
    def dist_func(x):
        # if x >= 0.2 and x < 0.3:
        #     return 7.5 * x - 1.25
        # else:
        #     return 1
        if x >= start_window:
            return 1
        else:
            return start_load + x * (1 - start_load) / start_window

    # create {Tn} using function_dist and CVs, use lognormal distribution for inter-arrival time
    invocs = {} # invocs w/ timestamps
    for i in range(n_functions):
        new_invocs = {}
        if function_dist[i] == 0: break
        if function_dist[i] >= 300 and function_dist[i] < 3000 and common.random.random() < BP_percentage: # assume half of these are burst-parallel apps
            # calculate A2, A3
            step = span // 3
            startpoint = common.random.randint(0, step)
            batch1 = []
            batch2 = []
            batch3 = []
            for _ in range(int(function_dist[i]/3)):
                dur = 2 # TODO: use distribution
                batch1.append(Invocation(fns[i], dur))
                batch2.append(Invocation(fns[i], dur))
                batch3.append(Invocation(fns[i], dur))
            existing_list = invocs.get(startpoint, list())
            existing_list += batch1
            invocs[startpoint] = existing_list
            existing_list = invocs.get(startpoint+step, list())
            existing_list += batch2
            invocs[startpoint+step] = existing_list
            existing_list = invocs.get(startpoint+step*2, list())
            existing_list += batch3
            invocs[startpoint+step*2] = existing_list
            print('bursty app of', function_dist[i], 'invocations')
            continue
        # normal apps
        sigma = math.sqrt(math.log(CVs[i] ** 2 + 1))
        mean = span / function_dist[i] # inter-arrival time expectation
        mu = math.log(mean) - sigma ** 2 / 2
        current_ts = 0.0 # start point

        # create IAT sequence {An}, then calculate {Tn}
        while True:
            interval = common.random.lognormvariate(mu, sigma)
            interval = interval / dist_func(current_ts / span) # reshape by dist function
            current_ts += interval
            ts = round(current_ts)
            if ts >= span: break # will have more or less invocations
            # duration
            dur = 2 # TODO: use duration distribution
            invoc = Invocation(fns[i], dur)
            existing_list = new_invocs.get(ts, list())
            existing_list.append(invoc)
            new_invocs[ts] = existing_list

        invocs = merge_invocs(invocs, new_invocs)

    return invocs

def merge_invocs(invoc1, invoc2):
    ret = invoc1.copy()
    for k, v in invoc2.items():
        if k in ret:
            ret[k].extend(v)
        else:
            ret[k] = v
    return ret
