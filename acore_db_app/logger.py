# -*- coding: utf-8 -*-

from .vendor.nested_logger import NestedLogger

logger = NestedLogger(name="acore_db_app", log_format="%(message)s")
