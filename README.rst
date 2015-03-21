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
            tnt.insert("tester", (cnt, cnt))
            cnt += 1


    tnt = gtarantool.connect("127.0.0.1", 3301)

    jobs = [gevent.spawn(insert_job, tnt)
            for _ in range(10)]

    gevent.joinall(jobs)

it's cool!
see benchmark results:

=====  =====  ======
call tarantool gtarantool
=====  =====  ======
insert 33.123570 11.574602
select 22.030409 10.305281
delete 32.839110 11.233938
=====  =====  ======
