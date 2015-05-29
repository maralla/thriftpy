# -*- coding: utf-8 -*-

import time

from thriftpy.thrift import TClient
from thriftpy.contrib.tracking import track_thrift, RequestInfo


class Client(TClient):
    def __init__(self, service_name, tracker_handler, *args, **kwargs):
        super(Client, self).__init__(*args, **kwargs)

        self.tracker = tracker_handler
        self.service_name = service_name

    def _send(self, _api, **kwargs):
        self._header = track_thrift.RequestHeader()
        self.tracker.gen_header(self._header)

        soa = {"req": self._header.request_id, "rpc": self._header.seq}
        self._oprot.write_metadata(soa=soa, iface=self.service_name)

        self.send_start = int(time.time() * 1000)
        super(Client, self)._send(_api, **kwargs)

    def _req(self, _api, *args, **kwargs):
        exception, status = None, False

        try:
            res = super(Client, self)._req(_api, *args, **kwargs)
            status = True
            return res
        except BaseException as e:
            exception = e
            raise
        finally:
            header_info = RequestInfo(
                request_id=self._header.request_id,
                seq=self._header.seq,
                client=self.tracker.client,
                server=self.tracker.server,
                api=_api,
                status=status,
                start=self.send_start,
                end=int(time.time() * 1000),
                annotation=self.tracker.annotation
            )
            self.tracker.record(header_info, exception)
