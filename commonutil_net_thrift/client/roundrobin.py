# -*- coding: utf-8 -*-
"""
Round-robin client
"""

from contextlib import closing
import logging

_log = logging.getLogger(__name__)


class RoundRobinClient:
	def __init__(self, connector_callable, client_cls, client_check_callable, server_locations, timeout_seconds=3, *args, **kwds):  # pylint: disable=too-many-arguments
		super().__init__(*args, **kwds)
		self._connector_callable = connector_callable
		self._client_cls = client_cls
		self._client_check_callable = client_check_callable
		self._server_locations = server_locations
		self._connect_kwds = {
				"timeout_seconds": timeout_seconds,
		}
		self._next_server_index = 0
		self._client = None
		self._transport = None

	def __enter__(self):
		return self

	def __exit__(self, *exc_info):
		self.close()

	def open(self):
		return closing(self)

	def close(self):
		try:
			self._transport.close()
		except Exception:
			pass
		self._client = None
		self._transport = None

	def _reconnect(self):
		self.close()
		srv_loc = self._server_locations[self._next_server_index]
		self._next_server_index = (self._next_server_index + 1) % len(self._server_locations)
		self._client, self._transport = self._connector_callable(self._client_cls, self._client_check_callable, *srv_loc, **self._connect_kwds)

	def _invoke_impl(self, method_name, *args, **kwds):
		if not self._client:
			self._reconnect()
		client_callable = getattr(self._client, method_name)
		return client_callable(*args, **kwds)

	def _invoke(self, method_name, *args, **kwds):
		remain_count = len(self.server_locations)
		while remain_count > 0:
			remain_count = remain_count - 1
			try:
				return self._invoke_impl(method_name, *args, **kwds)
			except Exception:
				_log.exception("invoke %r failed", method_name)
				self._reconnect()
		return self._invoke_impl(method_name, *args, **kwds)
