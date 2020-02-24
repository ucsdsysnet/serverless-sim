#!/usr/bin/env python3

import random
from cluster import Function, Invocation

def burst_parallel(func, start, duration, parallelism):
    invocs = {}
    invocs[start] = [Invocation(func, duration) for _ in range(parallelism)]
    return invocs

def burst_parallel_app(func, parallelism, seed=10, start=0, spacing=10, n_bursts=10, duration=5):
    invocs = {}
    gen = random.Random(seed)
    for i in range(n_bursts):
        invocs = merge_invocs(invocs, burst_parallel(func, int(start+i*spacing), duration, parallelism))
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
