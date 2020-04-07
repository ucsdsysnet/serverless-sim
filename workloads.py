#!/usr/bin/env python3

import random, math
from cluster import Function, Invocation

def burst(func, start, duration, parallelism):
    invocs = {}
    invocs[start] = [Invocation(func, duration) for _ in range(parallelism)]
    return invocs

def burst_parallel_app(func, parallelism, start, end, gen, n_bursts, duration):
    invocs = {}
    for i in range(n_bursts):
        invocs = merge_invocs(invocs, burst(func, int(gen.randint(start, end)), duration, parallelism))
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
def azure(gen, span, n_functions, n_invocations):
    INVOC_MU = -7.85
    INVOC_SIGMA = 2.75
    # create functions
    fns = [Function(i, 1) for i in range(n_functions)] # TODO: use memory distribution as "demand"
    invocation_count = [0] * n_functions # fn -> invocations of that fn

    # allocate invocations to functions
    created = 0
    while created < n_invocations:
        fnid = int(gen.lognormvariate(INVOC_MU, INVOC_SIGMA) * n_functions)
        if fnid < n_functions:
            invocation_count[fnid] += 1
            created += 1

    # allocate CVs of IAT for each function
    CVs = []
    for _ in range(n_functions):
        CVs.append(0) # TODO: use CV distribution, using random.choice(), make sure in [0, 10]

    # create invocations using counts and CVs, use lognormal distribution for inter-arrival time
    invocs = {} # invocs w/ timestamps
    for i in range(n_functions):
        if invocation_count[i] == 0: break
        sigma = math.sqrt(math.log(CVs[i] ** 2 + 1))
        mean = span / invocation_count[i] # inter-arrival time expectation
        mu = math.log(mean) - sigma ** 2 / 2
        current_ts = 0.0 # start point
        # for _ in range(invocation_count[i]):
        while True:
            interval = gen.lognormvariate(mu, sigma)
            current_ts += interval
            ts = round(current_ts)
            if ts >= span: break # will have more or less invocations
            # duration
            dur = 2 # TODO: use duration distribution
            invoc = Invocation(fns[i], dur)
            existing_list = invocs.get(ts, list())
            existing_list.append(invoc)
            invocs[ts] = existing_list

    return invocs

def merge_invocs(invoc1, invoc2):
    ret = invoc1.copy()
    for k, v in invoc2.items():
        if k in ret:
            ret[k].extend(v)
        else:
            ret[k] = v
    return ret
