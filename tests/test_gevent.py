# -*- coding: utf-8 -*-

import gevent
import gevent_tarantool
import time

def insert_worker(tnt, worker_idx):
    for i in range(1000):
        tnt.store(1, (worker_idx + i, worker_idx + i))

tnt = gevent_tarantool.connect('127.0.0.1', 33013)

t0 = time.time()
jobs = [gevent.spawn(insert_worker, tnt, worker_idx) for worker_idx in range(10)]
gevent.joinall(jobs)
t1 = time.time() - t0

print("total time for 10000 inserts: {}".format(t1))
