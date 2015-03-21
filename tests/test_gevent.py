# -*- coding: utf-8 -*-

import gevent
import gtarantool
import time

cnt = 0


def insert_job(tnt):
    global cnt

    for i in range(10000):
        cnt += 1
        tnt.insert("tester", (cnt, cnt))


tnt = gtarantool.connect("127.0.0.1", 3301)

t0 = time.time()
jobs = [gevent.spawn(insert_job, tnt)
        for _ in range(10)]

gevent.joinall(jobs)
t1 = time.time() - t0

print("total time for 100000 inserts: {0}".format(t1))
