#!/usr/bin/env python3

import sys, os
import time
import datetime as DT
from functools import reduce
import json
import hashlib
import requests
import urllib3
import subprocess
from collections import defaultdict

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

assigned_count = defaultdict(lambda: 0)
n_requested = 0

def get_func(invocation):
    return invocation.function.function_name

def create_function(function):
    if not function.function_name:
        demand = function.demand
        function.function_name = 'loop_new_%d_%d' % (demand * 128, function.function_id)
        assigned_count[demand] += 1
    res = subprocess.run(['wsk', '-i', 'action', 'update', function.function_name, '--kind', 'python:3', '-m', str(int(function.demand*128)), '-t', '300000', 'functions/sleep.py'])
    res.check_returncode()


def request(invocation):
    global n_requested
    n_requested += 1
    fn = get_func(invocation)
    print('requesting:', fn, 'at', time.time(), 'for', invocation.duration/1000, 'seconds', file=sys.stderr)
    url = 'https://' + APIHOST + '/api/v1/namespaces/_/actions/' + fn
    headers = {'Content-Type':'application/json'}
    jsondata = {"duration":invocation.duration/1000, "long":False}
    resp = session.request('post', url, params={'blocking':'false'}, headers=headers, json=jsondata)
    print('response:', resp.json(), file=sys.stderr)
    return resp

def main(seed, workloads, *args, **kwargs):
    common.init_gen(seed)

    wklds = {}
    all_functions = set() # order is arbitrary
    for wl_type in workloads:
        wl_gen = getattr(workload, wl_type['type'])
        wl, fns = wl_gen(**wl_type['parameters'])
        workload.extend_workload(wklds, wl)
        all_functions.update(fns)

    for f in all_functions:
        create_function(f)

    list_of_workloads = list(wklds.values())
    lengths = [len(s) for s in list_of_workloads]
    print('average', sum(lengths)/len(lengths), 'invocations, highest', max(lengths), file=sys.stderr)

    start = time.time() + 1
    print('starting at', start, '...', file=sys.stderr)
    
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

    print('SINCE=%sZ' % DT.datetime.utcfromtimestamp(start).isoformat(), file=sys.stderr)
    print('requested', n_requested, 'invocations', file=sys.stderr)
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
    print('Finished. run_id:', run_id, file=sys.stderr)
