# -*- coding: utf-8 -*-

"""
http/json protocol
==================

http headers:

    Content-Length
    Content-Encoding: UTF-8
    Content-Type: application/json


thrift -> json:

    struct User {
        1: string name
        2: i32 id
    }

    struct Info {
        1: string f1
        2: i32 f2
        3: list<string> f3
        4: map<string, i32> f4
        5: User f5
    }

    =>

    {
        "f1": "hello",
        "f2": 123,
        "f3": ["a", "b", "c"],
        "f4": {"foo": 34, "bar": 123},
        "f5": {"name": "jane", "id": 10032}
    }

args:
    c.ping(23, "ping", User(name="jane", id=10032))

    =>

    ["23", '"ping"', "{\"name\": \"jane\", \"id\": 10032}"]
"""


from __future__ import absolute_import

try:
    import httplib
except ImportError:
    import http.client as httplib

import json

from thriftpy.protocol.exc import TProtocolException
from thriftpy.protocol.json import JsonConverter
from thriftpy.thrift import TMessageType, TType, TException

VERSION = "1.0"


class HTTPJsonException(TException):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return self.msg


class _JsonHTTP(httplib.HTTPConnection):
    def __init__(self, trans, *args, **kwargs):
        httplib.HTTPConnection.__init__(self, '', *args, **kwargs)

        self.trans = trans

    def send(self, data):
        self.trans.write(data)

    def _send_request(self, method, url, body, headers):
        self.putrequest(method, url, skip_host=True, skip_accept_encoding=True)
        for hdr, value in headers.items():
            self.putheader(hdr, value)
        try:
            self.endheaders(body)
        except TypeError:
            self.endheaders()
            if body:
                self.send(body)


class _Stream(object):
    def __init__(self, stream):
        self.stream = stream

    def makefile(self, *args, **kwargs):
        return self

    def readline(self, size=-1):
        res = []
        data = None
        while data != b'\n':
            if len(res) == size:
                break

            data = self.stream.read(1)
            if not data:
                break
            res.append(data)
        return b''.join(res)

    def close(self):
        pass

    def read(self, size=-1):
        if size < 0:
            raise HTTPJsonException("`size` must be greater or equal than 0")
        return self.stream.read(size)

    def fileno(self):
        raise HTTPJsonException("Not supported")

    def unread(self):
        raise HTTPJsonException("Not supported")


class Converter(JsonConverter):
    COMPLEX = (TType.STRUCT, TType.SET, TType.LIST, TType.MAP)

    @classmethod
    def json_map(cls, val, spec):
        key_type, key_spec = cls.parse_spec(spec[0], True)

        if key_type in cls.COMPLEX:
            raise TProtocolException("%r in map key, not supported" %
                                     TType._VALUES_TO_NAMES[key_type])

        value_type, value_spec = cls.parse_spec(spec[1], True)

        res = {}
        for k, v in val.items():
            key = str(cls.json_value(key_type, k, None))
            res[key] = cls.json_value(value_type, v, value_spec)
        return res

    @classmethod
    def thrift_map(cls, val, spec):
        res = {}

        key_type, key_spec = cls.parse_spec(spec[0], True)
        value_type, value_spec = cls.parse_spec(spec[1], True)

        for k, v in val.items():
            key = cls.thrift_value(key_type, k, key_spec)
            res[key] = cls.thrift_value(value_type, v, value_spec)

        return res


def serialize_args(struct):
    args = []

    for fid in sorted(struct.thrift_spec):
        field_spec = struct.thrift_spec[fid]

        ttype, field_name, type_spec = Converter.parse_spec(field_spec)

        v = getattr(struct, field_name)
        if v is not None:
            v = Converter.json_value(ttype, v, type_spec)

        args.append(json.dumps(v))

    return args


class THTTPJsonProtocol(object):
    """http/json protocol

    .. note::

        This protocol can only be used at client side!
    """
    def __init__(self, trans, uri="/rpc"):
        self.trans = trans
        self.uri = uri

        self.data = {
            "ver": VERSION,
            "soa": {},
            "method": '',
            "args": [],
            "metas": {}
        }

        self.headers = {
            "Content-Length": 0,
            "Content-Encoding": "UTF-8",
            "Content-type": "application/json",
            "Connection": "Keep-Alive"
        }

        self._writing = False

    @staticmethod
    def _thrift_spec_names(spec):
        res = []
        for v in spec.values():
            res.append(v[1])
        return res

    def read_message_begin(self):
        response = httplib.HTTPResponse(_Stream(self.trans))
        response.begin()

        assert response.status == 200

        content_encoding = response.getheader("Content-Encoding")
        content_type = response.getheader("Content-Type")

        assert content_type == "application/json"

        payload = response.read()
        val = json.loads(payload.decode(content_encoding))

        assert val["ver"] == VERSION

        self._payload = val

        return '', TMessageType.REPLY, None

    def read_message_end(self):
        self._payload = None

    def write_message_begin(self, name, ttype, seqid):
        if ttype == TMessageType.CALL:
            self._writing = True

        self.data["method"] = name

    def write_message_end(self):
        assert self._writing

        self._writing = False
        data = json.dumps(self.data)

        self.headers["Content-Length"] = len(data)

        http = _JsonHTTP(self.trans)
        http.request("POST", self.uri, body=data.encode("utf-8"),
                     headers=self.headers)

    def read_struct(self, obj):
        assert hasattr(self, "_payload") and self._payload

        res = self._payload["result"]

        try:
            res = json.loads(res)
        except (ValueError, TypeError):
            pass

        if self._payload["ex"]:
            names = self._thrift_spec_names(obj.thrift_spec)
            if not set(names).difference(["success"]):
                raise HTTPJsonException(
                    "Undefined exception %s(%r) received." % (
                        self._payload["ex"]["cl"],
                        self._payload["ex"]["msg"]))

            exc_name = names[-1]
            res = {exc_name: self._payload["ex"]}
        else:
            res = {"success": res}

        res = Converter.thrift_struct(res, obj)
        return res

    def write_struct(self, obj):
        assert self._writing

        self.data["args"] = serialize_args(obj)


class THTTPJsonProtocolFactory(object):
    def get_protocol(self, trans, uri="/rpc"):
        return THTTPJsonProtocol(trans, uri)
