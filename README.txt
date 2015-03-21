Tarantool connection driver for work with gevent framework
----------------------------------------------------------
Connector required tarantool version 1.6:

    $ pip install gtarantool

Try it example:

.. code:: python

    import gevent
    import gtarantool

    cnt = 0

    def insert_job(tnt):
        global cnt

        for i in range(10000):
            # make io job here
            cnt += 1
            tnt.insert("tester", (cnt, cnt))


    tnt = gtarantool.connect("127.0.0.1", 3301)

    jobs = [gevent.spawn(insert_job, tnt)
            for _ in range(10)]

    gevent.joinall(jobs)

Under this scheme the gtarantool driver makes a smaller number of read/write tarantool socket.

See benchmark results time for insert/select/delete 100K tuples on 1.5KBytes:

=========  =========  ==========
call       tarantool  gtarantool
=========  =========  ==========
insert     32.448247  10.072774
select     22.326968  9.305423
delete     33.535188  9.464293
=========  =========  ==========

In this case, your code does not contain callbacks and remains synchronous!
