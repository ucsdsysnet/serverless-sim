#!/usr/bin/env python3

import math
import common
from cluster import Function, Invocation

fn_counter = 0

def new_fnid():
    global fn_counter
    fn_counter += 1
    return fn_counter - 1

def burst(func, start, duration, parallelism):
    invocs = {}
    invocs[start] = [Invocation(func, duration) for _ in range(parallelism)]
    return invocs

def burst_parallel_app(func, parallelism, start, end, n_bursts, duration):
    invocs = {}
    for i in range(n_bursts):
        extend_workload(invocs, burst(func, int(common.gen.randint(start, end)), duration, parallelism))
    return invocs

def faas(ntasks):
    pass

"""
See Azure's "Serverless in the Wild" paper.
Knobs:
    - timespan in seconds
    - number of functions (possibly not all functions have invocations)
    - number of invocations (possibly more or less than this number)
"""
def azure(span, n_functions, n_invocations, mem_hist, mem_bins, dist_mu, dist_sigma, CV, dur_mu, dur_sigma, start_window, start_load, BP_percentage, **kwargs):
    # memory demand distribution, stats from https://www.datadoghq.com/state-of-serverless/. the paper has memory usage stats, consistent w/ demand from datadog.
    mems = common.random_from_histogram(mem_hist, mem_bins, n_functions)

    # create functions
    fns = [Function(new_fnid(), mems[i]) for i in range(n_functions)]

    # allocate invocations to functions
    function_dist = [0] * n_functions # fn -> invocations of that fn
    created = 0
    while created < n_invocations:
        fnid = int(common.gen.lognormvariate(dist_mu, dist_sigma) * n_functions)
        if fnid < n_functions:
            function_dist[fnid] += 1
            created += 1

    # allocate CVs of IAT for each function
    CVs = []
    for _ in range(n_functions):
        CVs.append(1) # TODO: use CV distribution, using random.choice(), make sure in [0, 10]

    # duration distribution
    durations = []
    while len(durations) < n_functions:
        dur = common.gen.lognormvariate(dist_mu, dist_sigma)
        if dur > 60.0: # timeout
            continue
        if dur < 1.0:
            durations.append(1)
        else:
            durations.append(round(dur))

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
        if function_dist[i] >= 300 and function_dist[i] < 3000 and common.gen.random() < BP_percentage: # assume half of these are burst-parallel apps
            # calculate A2, A3
            step = span // 3
            startpoint = common.gen.randint(0, step)
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
            interval = common.gen.lognormvariate(mu, sigma)
            interval = interval / dist_func(current_ts / span) # reshape by dist function
            current_ts += interval
            ts = round(current_ts)
            if ts >= span: break # will have more or less invocations
            # duration
            dur = durations[i]
            invoc = Invocation(fns[i], dur)
            existing_list = new_invocs.get(ts, list())
            existing_list.append(invoc)
            new_invocs[ts] = existing_list

        extend_workload(invocs, new_invocs)

    return invocs

def extend_workload(wl1, wl2):
    for k, v in wl2.items():
        if k in wl1:
            wl1[k].extend(v)
        else:
            wl1[k] = v
    return wl1
