# -*- coding: utf-8 -*-

import gevent
import gevent.lock
import gevent.socket as socket
from gevent.event import AsyncResult, Event

import errno
import time

import tarantool
from tarantool.const import struct_LLL
from tarantool.response import Response
from tarantool.request import (
    Request,
    RequestSelect,
    RequestUpdate,
    RequestInsert,
    RequestDelete,
    RequestCall,
    RequestPing)

from tarantool.error import (
    NetworkError,
    DatabaseError,
    warn,
    RetryWarning,
    NetworkWarning)

from tarantool.const import (
    SOCKET_TIMEOUT,
    RECONNECT_DELAY,
    RECONNECT_MAX_ATTEMPTS,
    RETRY_MAX_ATTEMPTS,
    BOX_RETURN_TUPLE,
    BOX_ADD,
    BOX_REPLACE,
    REQUEST_TYPE_PING)

def connect(host="localhost", port=33013, schema=None, return_tuple=True):
        return Connection(host, port,
            socket_timeout=SOCKET_TIMEOUT,
            reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
            reconnect_delay=RECONNECT_DELAY,
            connect_now=True,
            schema=schema,
            return_tuple=return_tuple)


class GeventRequest(Request):
    def header(self, body_length):
        self.id = self.conn.gen_req_event()
        return struct_LLL.pack(self.request_type, body_length, self.id)


class GeventRequestSelect(GeventRequest, RequestSelect):
    pass


class GeventRequestUpdate(GeventRequest, RequestUpdate):
    pass


class GeventRequestInsert(GeventRequest, RequestInsert):
    pass


class GeventRequestDelete(GeventRequest, RequestDelete):
    pass


class GeventRequestCall(GeventRequest, RequestCall):
    pass


class GeventRequestPing(RequestPing):
    def __init__(self, conn):
        super(GeventRequestPing, self).__init__(conn)
        self.id = self.conn.gen_req_event()
        self._bytes = struct_LLL.pack(REQUEST_TYPE_PING, 0, self.id)


class Connection(tarantool.Connection):
    def __init__(self, *args, **kwargs):
        self.lock = gevent.lock.Semaphore()
        self._reader = None
        self._writer = None
        self._write_buffer = b""
        self._write_event = Event()
        self.req_num = 0
        self.req_event = {}
        self._send_request = self._send_request_check_connected
        super(Connection, self).__init__(*args, **kwargs)

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
        super(Connection, self).close()
    
    def connect(self):
        if self.connected:
            return

        with self.lock:
            if self.connected:
                return

            # duplicate tarantool connect vs monkey patch
            self.connected = False
            try:
                if self._socket:
                    self._socket.close()
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
                self._socket.connect((self.host, self.port))
                self._socket.settimeout(self.socket_timeout)
                self.connected = True
            except socket.error as e:
                raise NetworkError(e)

            # separate gevent thread for coro write request to tarantool socket
            if self._writer and not self._writer.ready():
                self._writer.kill()
            self._writer = gevent.spawn(self.request_writer)

            # separate gevent thread for coro read response from tarantool socket
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
        except Exception, ex:
            if self._reader and not self._reader.ready():
                self._reader.kill()
            with self.lock:
                self.close()
                for event in self.req_event.values():
                    event.set(("", ex))

    def response_reader(self):
        try:
            buf = b""
            while self.connected:
                tmp_buf = self._socket.recv(4096)
                if not tmp_buf:
                    raise NetworkError(socket.error(errno.ECONNRESET, "Lost connection to server during query"))

                buf += tmp_buf
                len_buf = len(buf)
                curr = 0

                while len_buf - curr >= 12:
                    header = buf[curr:curr + 12]
                    request_type, body_length, request_id = struct_LLL.unpack(header)

                    if len_buf - curr < 12 + body_length:
                        break

                    body = buf[curr + 12:curr + 12 + body_length]
                    curr += 12 + body_length

                    # set AsyncResult
                    if request_id in self.req_event:
                        self.req_event[request_id].set((header, body))

                # one cut for buffer
                if curr:
                    buf = buf[curr:]
        except Exception, ex:
            if self._writer and not self._writer.ready():
                self._writer.kill()
            with self.lock:
                self.close()
                for event in self.req_event.values():
                    event.set(("", ex))

    def _read_response(self, request_id):
        # wait AsyncEvent from response_reader
        header, body = self.req_event[request_id].get()
        del self.req_event[request_id]
        return header, body

    def _send_request_check_connected(self, request, space_name=None, field_defs=None, default_type=None):
        assert isinstance(request, Request)

        if not self.connected:
            self.connect()

        return self._send_request_wo_reconnect(request, space_name, field_defs, default_type)

    def _send_request_wo_reconnect(self, request, space_name=None, field_defs=None, default_type=None):
        assert isinstance(request, Request)

        for attempt in xrange(RETRY_MAX_ATTEMPTS):
            self._write_buffer += bytes(request)
            self._write_event.set()
            
            header, body = self._read_response(request.id)
            if not header:
                ex = body or DatabaseError(0, "request aborted")
                raise ex
            response = Response(
                self, header, body, space_name, field_defs, default_type)

            if response.completion_status != 1:
                return response
            warn(response.return_message, RetryWarning)

        # Raise an error if the maximum number of attempts have been made
        raise DatabaseError(response.return_code, response.return_message)

    def _select(self, space_name, index_name, values, offset=0, limit=0xffffffff):
        assert isinstance(values, (list, tuple, set, frozenset))

        request = GeventRequestSelect(
            self, space_name, index_name, values, offset, limit)
        response = self._send_request(request, space_name)
        return response

    def _insert(self, space_name, values, flags):
        assert isinstance(values, tuple)
        assert (flags & (BOX_RETURN_TUPLE | BOX_ADD | BOX_REPLACE)) == flags

        request = GeventRequestInsert(self, space_name, values, flags)
        return self._send_request(request, space_name)

    def call(self, func_name, *args, **kwargs):
        assert isinstance(func_name, str)
        assert len(args) != 0

        if isinstance(args[0], (list, tuple)):
            args = args[0]

        field_defs = kwargs.get("field_defs", None)
        default_type = kwargs.get("default_type", None)
        space_name = kwargs.get("space_name", None)
        return_tuple = kwargs.get("return_tuple", self.return_tuple)

        request = GeventRequestCall(self, func_name, args, return_tuple)
        response = self._send_request(request, space_name=space_name,
                                      field_defs=field_defs,
                                      default_type=default_type)
        return response

    def update(self, space_name, key, op_list, return_tuple=None):
        assert isinstance(key, (int, long, basestring))

        if return_tuple is None:
            return_tuple = self.return_tuple
        request = GeventRequestUpdate(self, space_name, key, op_list, return_tuple)
        return self._send_request(request, space_name)

    def delete(self, space_name, key, return_tuple=None):
        assert isinstance(key, (int, long, basestring))

        if return_tuple is None:
            return_tuple = self.return_tuple
        request = GeventRequestDelete(self, space_name, key, return_tuple)
        return self._send_request(request, space_name)

    def ping(self, notime=False):
        request = GeventRequestPing(self)
        t0 = time.time()
        response = self._send_request(request)
        t1 = time.time()

        assert response._request_type == REQUEST_TYPE_PING
        assert response._body_length == 0

        if notime:
            return "Success"
        return t1 - t0
