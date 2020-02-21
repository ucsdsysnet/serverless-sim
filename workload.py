#!/usr/bin/env python3

import random
from cluster import Function, Invocation

def burst_parallel(ntasks):
    invocs = {}
    f = Function(int(random.random()*65536), 1)
    invocs[0] = [Invocation(f, 5) for _ in range(ntasks)]
    return invocs

def faas(ntasks):
    pass
