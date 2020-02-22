#!/usr/bin/env python3

import random
from cluster import Function, Invocation

def burst_parallel(start, ntasks):
    invocs = {}
    f = Function(int(random.random()*65536), 1)
    invocs[start] = [Invocation(f, 5) for _ in range(ntasks)]
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
