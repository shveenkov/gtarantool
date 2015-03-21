# -*- coding: utf-8 -*-

import time
import tarantool

tnt = tarantool.connect("127.0.0.1", 3301)

t0 = time.time()
for cnt in range(100000):
    tnt.insert("tester", (cnt, cnt))

t1 = time.time() - t0

print("total time for 100000 inserts: {0}".format(t1))
