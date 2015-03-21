# -*- coding: utf-8 -*-

import time
import tarantool
import gtarantool
import gevent

benchmark = {
    "tarantool": {},
    "gtarantool": {},
}

cnt = 0

tnt = tarantool.connect("127.0.0.1", 3301)

import string
mod_len = len(string.printable)
data = [string.printable[it] * 1536 for it in xrange(mod_len)]

# sync benchmark
# insert test
print "tarantool insert test"
t1 = time.time()
for it in xrange(100000):
    r = tnt.insert("tester", (it, data[it % mod_len]))

t2 = time.time()
benchmark["tarantool"]["insert"] = t2 - t1

# select test
print "tarantool select test"
t1 = time.time()
for it in xrange(100000):
    r = tnt.select("tester", it)

t2 = time.time()
benchmark["tarantool"]["select"] = t2 - t1

# delete test
print "tarantool delete test"
t1 = time.time()
for it in xrange(100000):
    r = tnt.delete("tester", it)

t2 = time.time()
benchmark["tarantool"]["delete"] = t2 - t1

# gevent benchmark


def insert_job(tnt):
    global cnt

    for i in range(2500):
        cnt += 1
        tnt.insert("tester", (cnt, data[cnt % mod_len]))


def select_job(tnt):
    global cnt

    for i in range(2500):
        cnt += 1
        r = tnt.select("tester", cnt)


def delete_job(tnt):
    global cnt

    for i in range(2500):
        cnt += 1
        r = tnt.delete("tester", cnt)


tnt = gtarantool.connect("127.0.0.1", 3301)

# insert test
print "gtarantool insert test"
t1 = time.time()
cnt = 0
jobs = [gevent.spawn(insert_job, tnt)
        for _ in range(40)]

gevent.joinall(jobs)
t2 = time.time()
benchmark["gtarantool"]["insert"] = t2 - t1

# select test
print "gtarantool select test"
t1 = time.time()
cnt = 0
jobs = [gevent.spawn(select_job, tnt)
        for _ in range(40)]

gevent.joinall(jobs)
t2 = time.time()
benchmark["gtarantool"]["select"] = t2 - t1

# delete test
print "gtarantool delete test"
t1 = time.time()
cnt = 0
jobs = [gevent.spawn(delete_job, tnt)
        for _ in range(40)]

gevent.joinall(jobs)
t2 = time.time()
benchmark["gtarantool"]["delete"] = t2 - t1

print("\nbenchmark results:")
print("op      tarantool  gtarantool")
for k in ("insert", "select", "delete"):
    print("{2:6}: {0:0.6f}  {1:0.6f}".format(benchmark["tarantool"][k], benchmark["gtarantool"][k], k))