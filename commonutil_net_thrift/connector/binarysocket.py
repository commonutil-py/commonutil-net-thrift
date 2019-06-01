# -*- coding: utf-8 -*-
"""
Connector for socket transport with binary protocol
"""

import logging

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

from commonutil_net_thrift.connector.exceptions import ClientCheckException

_log = logging.getLogger(__name__)


def binary_socket_connector(client_cls, client_check_callable, host, port, timeout_seconds=3):
	transport = TSocket.TSocket(host, port)
	if timeout_seconds:
		transport.setTimeout(timeout_seconds * 1000.0)
	protocol = TBinaryProtocol.TBinaryProtocol(transport)
	client = client_cls(protocol)
	transport.open()  # connect
	try:
		if not client_check_callable(client):
			raise ClientCheckException()
	except Exception:
		transport.close()
		raise
	return (client, transport)
