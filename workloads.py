#!/usr/bin/env python3

import random
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

def merge_invocs(invoc1, invoc2):
    ret = invoc1.copy()
    for k, v in invoc2.items():
        if k in ret:
            ret[k].extend(v)
        else:
            ret[k] = v
    return ret
