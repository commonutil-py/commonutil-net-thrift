# -*- coding: utf-8 -*-
"""
Types for setting up wrapped client instances
"""

from collections import namedtuple

ClientSetup = namedtuple(
		"ClientSetup", (
				"client_class",
				"check_callable",
				"prepare_callable",
		), defaults=(
				None,
				None,
		))
