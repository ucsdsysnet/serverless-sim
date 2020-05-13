#!/usr/bin/env python3

import sys, os
import time
from functools import reduce
import json
import hashlib
import requests
import urllib3


import common
from cluster import Cluster, Host, Function, Invocation, logs, metrics
import workload
import plot

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=100,
    pool_maxsize=100)
session.mount('http://', adapter)
session.verify = False

APIHOST = None

assigned_count = [0] * 10
function_pool = {}

def get_func(invocation):
    return function_pool[invocation.function.function_id]

def create_function(fnid, demand):
    assigned_count[demand] += 1
    function_pool[fnid] = 'ram_walk_%d_%d' % (demand * 128, assigned_count[demand])

def request(invocation):
    print('requesting:', invocation, time.time(), file=sys.stderr)
    fn = get_func(invocation)
    url = 'https://' + APIHOST + '/api/v1/namespaces/_/actions/' + fn
    headers = {'Content-Type':'application/json'}
    jsondata = {"duration":invocation.duration}
    resp = session.request('post', url, params={'blocking':'false'}, headers=headers, json=jsondata)
    print('response:', resp.json(), file=sys.stderr)
    return resp

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
        create_function(f.function_id, int(round(f.demand)))

    start = time.time() + 5
    print('waiting 5 seconds', file=sys.stderr)
#    wklds = {3:[Invocation(list(all_functions)[0], 2)], 13:[Invocation(list(all_functions)[0], 1)]}
    def sleep_till(ts):
        cur = time.time()
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
    session.auth = tuple(os.environ['AUTH'].split(':'))
    APIHOST = os.environ['APIHOST']
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
