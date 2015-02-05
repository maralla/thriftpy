# -*- coding: utf-8 -*-

from __future__ import absolute_import

import contextlib
import os
import multiprocessing
import time
import tempfile
import pickle

try:
    import dbm
except ImportError:
    import dbm.ndbm as dbm

import pytest

import thriftpy

from thriftpy.transport import TServerSocket, TBufferedTransportFactory, \
    TTransportException, TSocket
from thriftpy.protocol import TBinaryProtocolFactory
from thriftpy.thrift import TTrackedProcessor, TTrackedClient, \
    TProcessorFactory, TClient, TProcessor
from thriftpy.server import TThreadedServer
from thriftpy.trace.tracker import TrackerBase


addressbook = thriftpy.load(os.path.join(os.path.dirname(__file__),
                                         "addressbook.thrift"))
_, db_file = tempfile.mkstemp()


class SampleTracker(TrackerBase):
    def record(self, header):
        db = dbm.open(db_file, 'w')
        key = "%s:%d" % (header.request_id, header.seq)
        db[key.encode("ascii")] = pickle.dumps(header.__dict__)
        db.close()

tracker = SampleTracker("test_client", "test_server")


class Dispatcher(object):
    def __init__(self):
        self.ab = addressbook.AddressBook()
        self.ab.people = {}

    def ping(self):
        return True

    def hello(self, name):
        return "hello %s" % name

    def remove(self, name):
        person = addressbook.Person(name="marry")
        with client(port=6098) as c:
            c.add(person)
        return True

    def add(self, person):
        with client(port=6099) as c:
            c.hello("jane")
        return True

    def get(self, name):
        e = addressbook.PersonNotExistsError()
        raise e


class TSampleServer(TThreadedServer):
    def __init__(self, processor_factory, trans, trans_factory, prot_factory):
        self.daemon = False
        self.processor_factory = processor_factory
        self.trans = trans

        self.itrans_factory = self.otrans_factory = trans_factory
        self.iprot_factory = self.oprot_factory = prot_factory
        self.closed = False

    def handle(self, client):
        processor = self.processor_factory.get_processor()
        itrans = self.itrans_factory.get_transport(client)
        otrans = self.otrans_factory.get_transport(client)
        iprot = self.iprot_factory.get_protocol(itrans)
        oprot = self.oprot_factory.get_protocol(otrans)
        try:
            while True:
                processor.process(iprot, oprot)
        except TTransportException:
            pass
        except Exception:
            raise

        itrans.close()
        otrans.close()


def gen_server(port=6029, tracker=tracker, processor=TTrackedProcessor):
    processor = TProcessorFactory(addressbook.AddressBookService, Dispatcher(),
                                  tracker, processor)
    server_socket = TServerSocket(host="localhost", port=port)
    server = TSampleServer(processor, server_socket,
                           prot_factory=TBinaryProtocolFactory(),
                           trans_factory=TBufferedTransportFactory())
    ps = multiprocessing.Process(target=server.serve)
    ps.start()
    return ps, server


@pytest.fixture(scope="module")
def server(request):
    ps, ser = gen_server(port=6029)

    time.sleep(0.5)

    def fin():
        if ps.is_alive():
            ps.terminate()
    request.addfinalizer(fin)
    return ser


@pytest.fixture(scope="module")
def server1(request):
    ps, ser = gen_server(port=6098)

    time.sleep(0.5)

    def fin():
        if ps.is_alive():
            ps.terminate()
    request.addfinalizer(fin)
    return ser


@pytest.fixture(scope="module")
def server2(request):
    ps, ser = gen_server(port=6099)

    time.sleep(0.5)

    def fin():
        if ps.is_alive():
            ps.terminate()
    request.addfinalizer(fin)
    return ser


@pytest.fixture(scope="module")
def not_tracked_server(request):
    ps, ser = gen_server(port=6030, tracker=None, processor=TProcessor)

    time.sleep(0.5)

    def fin():
        if ps.is_alive():
            ps.terminate()
    request.addfinalizer(fin)

    return ser


@contextlib.contextmanager
def client(client_class=TTrackedClient, port=6029):
    socket = TSocket("localhost", port)

    try:
        trans = TBufferedTransportFactory().get_transport(socket)
        proto = TBinaryProtocolFactory().get_protocol(trans)
        trans.open()
        args = [addressbook.AddressBookService, proto]
        if client_class.__name__ == TTrackedClient.__name__:
            args.insert(0, tracker)
        yield client_class(*args)
    finally:
        trans.close()


@pytest.fixture
def dbm_db(request):
    db = dbm.open(db_file, 'n')
    db.close()

    def fin():
        try:
            os.remove(db_file)
        except OSError:
            pass
    request.addfinalizer(fin)


def test_negotiation(server):
    with client() as c:
        assert c._upgraded is True


def test_tracker(server, dbm_db):
    with client() as c:
        c.ping()

    time.sleep(0.6)

    db = dbm.open(db_file, 'r')
    headers = list(db.keys())
    assert len(headers) == 1

    request_id = headers[0]
    data = pickle.loads(db[request_id])

    assert "start" in data and "end" in data

    data.pop("start")
    data.pop("end")

    assert data == {
        "request_id": request_id.decode("ascii").split(':')[0],
        "seq": 0,
        "client": "test_client",
        "server": "test_server",
        "api": "ping",
        "status": True
    }


def test_tracker_chain(server, server1, server2, dbm_db):
    with client() as c:
        c.remove("jane")

    time.sleep(0.6)

    db = dbm.open(db_file, 'r')
    headers = list(db.keys())
    assert len(headers) == 3

    headers.sort()

    header0 = pickle.loads(db[headers[0]])
    header1 = pickle.loads(db[headers[1]])
    header2 = pickle.loads(db[headers[2]])

    for h in (header0, header1, header2):
        h.pop("start")
        h.pop("end")

    assert header0["request_id"] == header1["request_id"] == \
        header2["request_id"] == headers[0].decode("ascii").split(':')[0]
    assert header0["seq"] == 0 and header1["seq"] == 1 and header2["seq"] == 2


def test_exception(server, dbm_db):
    with pytest.raises(addressbook.PersonNotExistsError):
        with client() as c:
            c.get("jane")

    db = dbm.open(db_file, 'r')
    headers = list(db.keys())
    assert len(headers) == 1

    header = pickle.loads(db[headers[0]])
    assert header["status"] is False


def test_not_tracked_client_tracked_server(server):
    with client(TClient) as c:
        c.ping()
        c.hello("world")


def test_tracked_client_not_tracked_server(not_tracked_server):
    with client(port=6030) as c:
        assert c._upgraded is False
        c.ping()
        c.hello("cat")
