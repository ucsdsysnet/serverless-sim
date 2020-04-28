#!/usr/bin/env python3

import sys, os
from functools import reduce
import json
import hashlib

from common import init_gen
from cluster import Cluster, Host, Function, Invocation, metrics, stats
import workload
import plot

def main(seed, workloads, hosts, cluster, **kwargs):
    init_gen(seed)

    wl_gen = getattr(workload, workloads['type'])
    wl = wl_gen(**workloads['parameters'])

    host_list = []
    i = 0
    for host_type in hosts:
        for _ in range(host_type['amount']):
            host_list.append(Host(i, host_type['configs']))
            i += 1
    clstr = Cluster(host_list, cluster['configs'])

    # ticks
    while len(wl) > 0 or not clstr.is_idle():
        for i in wl.get(clstr.epoch, []):
            clstr.request(i)
        wl.pop(clstr.epoch, None)
        clstr.tick()
        print('epoch:', clstr.epoch, file=sys.stderr)

    return clstr.epoch, metrics, stats

if __name__ == '__main__':
    params = json.load(sys.stdin)
    digest = hashlib.md5(json.dumps(params).encode('utf-8')).hexdigest()

    try:
        os.mkdir('runs')
    except FileExistsError:
        pass
    with open('runs/' + digest + '.json', 'w') as f:
        json.dump(params, f)

    plot.plot(*main(**params), 'runs/' + digest + '.pdf')
    print('finished. digest:', digest, file=sys.stderr)
