#!/usr/bin/env python3

import sys, os
import random
from functools import reduce
import json
import hashlib

from cluster import Cluster, Host, Function, Invocation, metrics, stats
import workload
import plot

def main(digest, seed, workloads, resources, **kwargs):
    gen = random.Random(seed)

    wl_gen = getattr(workload, workloads['type'])
    wl = wl_gen(gen, **workloads['parameters'])

    hosts = []
    i = 0
    for r in resources:
        for _ in range(r['amount']):
            hosts.append(Host(i, r['configs']))
            i += 1
    cluster = Cluster(hosts)

    # ticks
    while len(wl) > 0 or not cluster.is_idle():
        for i in wl.get(cluster.epoch, []):
            cluster.request(i)
        wl.pop(cluster.epoch, None)
        cluster.tick()
        print('epoch:', cluster.epoch, file=sys.stderr)

    return cluster.epoch, metrics, stats

if __name__ == '__main__':
    params = json.load(sys.stdin)
    digest = hashlib.md5(json.dumps(params).encode('utf-8')).hexdigest()

    try:
        os.mkdir('runs')
    except FileExistsError:
        pass
    with open('runs/' + digest + '.json', 'w') as f:
        json.dump(params, f)

    plot.plot(*main(digest, **params), 'runs/' + digest + '.pdf')
    print('finished. digest:', digest, file=sys.stderr)
