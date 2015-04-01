# -*- coding: utf-8 -*-

import os
import gevent
import gtarantool
import string
import multiprocessing
import time


class Bench(object):
    def __init__(self, aiotnt):
        self.tnt = aiotnt
        self.mod_len = len(string.printable)
        self.data = [string.printable[it] * 1536 for it in range(self.mod_len)]

        self.cnt_i = 0
        self.cnt_s = 0
        self.cnt_u = 0
        self.cnt_d = 0

        self.iter_max = 10000

    def select_insert_job(self):
        for it in range(self.iter_max):
            rs = self.tnt.select("tester", it)
            if len(rs):
                self.cnt_s += 1
            else:
                try:
                    self.tnt.insert("tester", (it, self.data[it % self.mod_len]))
                    self.cnt_i += 1
                except self.tnt.DatabaseError:
                    pass

    def insert_job(self):
        for it in range(self.iter_max):
            try:
                self.tnt.insert("tester", (it, self.data[it % self.mod_len]))
                self.cnt_i += 1
            except self.tnt.DatabaseError:
                pass

    def select_job(self):
        for it in range(self.iter_max):
            rs = self.tnt.select("tester", it)
            if len(rs):
                self.cnt_s += 1

    def update_job(self):
        for it in range(self.iter_max):
            try:
                self.tnt.update("tester", it, [("=", 2, it)])
                self.cnt_u += 1
            except self.tnt.DatabaseError:
                pass

    def delete_job(self):
        for it in range(0, self.iter_max, 2):
            rs = self.tnt.delete("tester", it)
            if len(rs):
                self.cnt_d += 1


def target_bench():
    print("run process:", os.getpid())
    tnt = gtarantool.connect("127.0.0.1", 3301)
    bench = Bench(tnt)

    tasks = []
    tasks += [gevent.spawn(bench.insert_job)
              for _ in range(20)]

    tasks += [gevent.spawn(bench.select_job)
              for _ in range(20)]

    tasks += [gevent.spawn(bench.update_job)
              for _ in range(20)]

    tasks += [gevent.spawn(bench.delete_job)
              for _ in range(20)]

    t1 = time.time()
    gevent.joinall(tasks)
    t2 = time.time()

    print("select=%d; insert=%d; update=%d; delete=%d; total=%d" % (
        bench.cnt_s, bench.cnt_i, bench.cnt_u, bench.cnt_d, t2 - t1))


workers = [multiprocessing.Process(target=target_bench)
           for _ in range(22)]

for worker in workers:
    worker.start()

for worker in workers:
    worker.join()
