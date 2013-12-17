# -*- coding: utf-8 -*-

import time
import tarantool

def insert_worker(tnt, worker_idx):
    for i in range(100):
        tnt.store(1, (worker_idx + i, worker_idx + i))

tnt = tarantool.connect('127.0.0.1', 33013)

t0 = time.time()
for worker_idx in range(10):
    for i in range(1000):
        tnt.store(1, (worker_idx + i, worker_idx + i))
t1 = time.time() - t0

print("total time for 1000 inserts: {}".format(t1))
