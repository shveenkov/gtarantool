# -*- coding: utf-8 -*-

import gevent
import gevent.lock
import gevent.socket as socket
from gevent.event import AsyncResult, Event

import errno
import time
import msgpack
import base64

import tarantool
from tarantool.utils import check_key
from tarantool.response import Response
from tarantool.request import (
    Request,
    RequestSelect,
    RequestReplace,
    RequestUpdate,
    RequestInsert,
    RequestDelete,
    RequestCall,
    RequestPing,
    RequestAuthenticate)

from tarantool.error import (
    NetworkError,
    DatabaseError,
    warn,
    RetryWarning)

from tarantool.const import (
    SOCKET_TIMEOUT,
    IPROTO_CODE,
    IPROTO_SYNC,
    RETRY_MAX_ATTEMPTS,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_DELAY,
    IPROTO_GREETING_SIZE)


def connect(host="localhost", port=3301):
    return TarantoolCoroConnection(host, port,
                                   user=None,
                                   password=None,
                                   socket_timeout=SOCKET_TIMEOUT,
                                   reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                                   reconnect_delay=RECONNECT_DELAY,
                                   connect_now=True)


class GResponse(Response):
    def get_sync(self):
        return self._sync


class GRequest(Request):
    def header(self, length):
        self.id = self.conn.gen_req_event()

        header = msgpack.dumps({IPROTO_CODE: self.request_type, IPROTO_SYNC: self.id})

        return msgpack.dumps(length + len(header)) + header


class GRequestAuthenticate(GRequest, RequestAuthenticate):
    pass


class GRequestReplace(GRequest, RequestReplace):
    pass


class GRequestSelect(GRequest, RequestSelect):
    pass


class GRequestUpdate(GRequest, RequestUpdate):
    pass


class GRequestInsert(GRequest, RequestInsert):
    pass


class GRequestDelete(GRequest, RequestDelete):
    pass


class GRequestCall(GRequest, RequestCall):
    pass


class GRequestPing(GRequest, RequestPing):
    pass


class TarantoolCoroConnection(tarantool.Connection):
    def __init__(self, *args, **kwargs):
        self.lock = gevent.lock.Semaphore()
        self._reader = None
        self._writer = None
        self._write_buffer = b""
        self._write_event = Event()
        self.req_num = 0
        self.req_event = {}
        self._send_request = self._send_request_check_connected

        self.gbuf_size = kwargs.pop("gbuf_size", 16384)

        assert isinstance(self.gbuf_size, int);

        super(TarantoolCoroConnection, self).__init__(*args, **kwargs)

        # important not raise exception in reader
        self.error = False

    def gen_req_event(self):
        self.req_num += 1
        if self.req_num > 10000000:
            self.req_num = 0
        self.req_event[self.req_num] = AsyncResult()
        return self.req_num

    def close(self):
        if not self.connected:
            return

        self.connected = False
        super(TarantoolCoroConnection, self).close()

    def connect(self):
        if self.connected:
            return

        with self.lock:
            if self.connected:
                return

            # duplicate tarantool connect vs gevent monkey patch
            self.connected = False
            try:
                if self._socket:
                    self._socket.close()

                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
                self._socket.connect((self.host, self.port))
                self._socket.settimeout(self.socket_timeout)
                self.connected = True
            except socket.error as ex:
                raise NetworkError(ex)

            # separate gevent thread for write request to tarantool socket
            if self._writer and not self._writer.ready():
                self._writer.kill()

            self._writer = gevent.spawn(self.request_writer)

            # separate gevent thread for read response from tarantool socket
            if self._reader and not self._reader.ready():
                self._reader.kill()

            self._reader = gevent.spawn(self.response_reader)

            self._write_buffer = b""

    def request_writer(self):
        try:
            while self.connected:
                self._write_event.wait()

                while len(self._write_buffer) > 0 and self.connected:
                    sent = self._socket.send(self._write_buffer)
                    self._write_buffer = self._write_buffer[sent:]

                self._write_event.clear()
        except socket.error, ex:
            if self._reader and not self._reader.ready():
                self._reader.kill()

            with self.lock:
                self.close()

                for event in self.req_event.values():
                    event.set(("", ex))

    def response_reader(self):
        try:
            # need handshake here
            greeting = self._socket.recv(IPROTO_GREETING_SIZE)
            self._salt = base64.decodestring(greeting[64:])[:20]
            if self.user:
                self.authenticate(self.user, self.password)

            buf = b""
            while self.connected:
                # chunk socket read
                tmp_buf = self._socket.recv(self.gbuf_size)
                if not tmp_buf:
                    raise NetworkError(socket.error(errno.ECONNRESET, "Lost connection to server during query"))

                buf += tmp_buf
                len_buf = len(buf)
                curr = 0

                while len_buf - curr >= 5:
                    length_pack = buf[curr:curr + 5]
                    length = msgpack.unpackb(length_pack)

                    if len_buf - curr < 5 + length:
                        break

                    body = buf[curr + 5:curr + 5 + length]
                    curr += 5 + length

                    response = GResponse(self, body)  # unpack response

                    # set AsyncResult
                    request_id = response.get_sync()
                    if request_id in self.req_event:
                        self.req_event[request_id].set((response, None))

                # one cut for buffer
                if curr:
                    buf = buf[curr:]
        except socket.error, ex:
            if self._writer and not self._writer.ready():
                self._writer.kill()

            with self.lock:
                self.close()

                for event in self.req_event.values():
                    event.set((None, ex))

    def _read_response(self, request_id):
        response, ex = self.req_event[request_id].get()
        del self.req_event[request_id]

        return response, ex

    def _send_request_check_connected(self, request):
        assert isinstance(request, Request)

        if not self.connected:
            self.connect()

        return self._send_request_wo_reconnect(request)

    def _send_request_wo_reconnect(self, request):
        assert isinstance(request, Request)

        for attempt in xrange(RETRY_MAX_ATTEMPTS):
            self._write_buffer += bytes(request)
            self._write_event.set()

            response, ex = self._read_response(request.id)
            if ex is not None:
                raise ex

            if response.completion_status != 1:
                if response.return_code != 0:
                    raise DatabaseError(response.return_code, response.return_message)

                return response

            warn(response.return_message, RetryWarning)

        # Raise an error if the maximum number of attempts have been made
        raise DatabaseError(response.return_code, response.return_message)

    def call(self, func_name, *args):
        assert isinstance(func_name, str)

        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            args = args[0]

        request = GRequestCall(self, func_name, args)
        return self._send_request(request)

    def replace(self, space_name, values):
        if isinstance(space_name, basestring):
            space_name = self.schema.get_space(space_name).sid

        request = GRequestReplace(self, space_name, values)
        return self._send_request(request)

    def authenticate(self, user, password):
        self.user = user
        self.password = password

        if not self.connected:
            self.connect()

        request = GRequestAuthenticate(self, self._salt, user, password)
        return self._send_request(request)

    def insert(self, space_name, values):
        if isinstance(space_name, basestring):
            space_name = self.schema.get_space(space_name).sid

        request = GRequestInsert(self, space_name, values)
        return self._send_request(request)

    def delete(self, space_name, key, **kwargs):
        index_name = kwargs.get("index", 0)

        key = check_key(key)
        if isinstance(space_name, basestring):
            space_name = self.schema.get_space(space_name).sid

        if isinstance(index_name, basestring):
            index_name = self.schema.get_index(space_name, index_name).iid

        request = GRequestDelete(self, space_name, index_name, key)
        return self._send_request(request)

    def update(self, space_name, key, op_list, **kwargs):
        index_name = kwargs.get("index", 0)
        key = check_key(key)

        if isinstance(space_name, basestring):
            space_name = self.schema.get_space(space_name).sid

        if isinstance(index_name, basestring):
            index_name = self.schema.get_index(space_name, index_name).iid

        request = GRequestUpdate(self, space_name, index_name, key, op_list)
        return self._send_request(request)

    def ping(self, notime=False):
        request = GRequestPing(self)
        t0 = time.time()
        self._send_request(request)
        t1 = time.time()

        if notime:
            return "Success"

        return t1 - t0

    def select(self, space_name, key=None, **kwargs):
        offset = kwargs.get("offset", 0)
        limit = kwargs.get("limit", 0xffffffff)
        index_name = kwargs.get("index", 0)
        iterator_type = kwargs.get("iterator", 0)

        key = check_key(key, select=True)

        if isinstance(space_name, basestring):
            space_name = self.schema.get_space(space_name).sid

        if isinstance(index_name, basestring):
            index_name = self.schema.get_index(space_name, index_name).iid

        request = GRequestSelect(self, space_name, index_name, key, offset, limit, iterator_type)
        response = self._send_request(request)
        return response
