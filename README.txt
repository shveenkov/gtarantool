Tarantool connection driver for work with gevent framework
----------------------------------------------------------
Try it example !

.. code:: python

    import gevent
    import gtarantool

    cnt = 0

    def insert_job(tnt):
        global cnt

        for i in range(10000):
            tnt.insert("tester", (cnt, cnt))
            cnt += 1


    tnt = gtarantool.connect("127.0.0.1", 3301)

    jobs = [gevent.spawn(insert_job, tnt)
            for _ in range(10)]

    gevent.joinall(jobs)

it's cool!