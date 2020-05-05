#!/usr/bin/env python3

import sys, os
import time
from functools import reduce
import json
import hashlib

import common
from cluster import Cluster, Host, Function, Invocation, logs, metrics
import workload
import plot

def create_function(fnid, demand):
    pass

def request(invocation):
    print('requesting:', invocation, time.time(), file=sys.stderr)

def main(seed, workloads, *args, **kwargs):
    common.init_gen(seed)

    wklds = {}
    all_functions = set()
    for wl_type in workloads:
        wl_gen = getattr(workload, wl_type['type'])
        wl, fns = wl_gen(**wl_type['parameters'])
        workload.extend_workload(wklds, wl)
        all_functions.update(fns)

    for f in all_functions:
        create_function(f.function_id, f.demand)

    start = time.time() + 5

    def sleep_till(ts):
        # print('sleep_till: ', ts)
        cur = time.time()
        # print('current:', cur)
        if ts <= cur:
            print('overflown to next tick!', file=sys.stderr)
            return
        time.sleep(ts - cur)

    # start requests
    epoch = 0
    while len(wklds) > 0:
        sleep_till(start + epoch)
        for i in wklds.get(epoch, []):
            request(i)
        wklds.pop(epoch, None)
        epoch += 1
        print('epoch:', epoch, file=sys.stderr)

    return

if __name__ == '__main__':
    params = json.load(sys.stdin)
    if len(sys.argv) == 2:
        run_id = sys.argv[1]
    else:
        run_id = hashlib.md5(json.dumps(params).encode('utf-8')).hexdigest()

    try:
        os.mkdir('real_runs')
    except FileExistsError:
        pass
    with open('real_runs/' + run_id + '.json', 'w') as f:
        json.dump(params, f)

    main(**params)
    print('finished. run_id:', run_id, file=sys.stderr)
