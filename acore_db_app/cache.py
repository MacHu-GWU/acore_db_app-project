# -*- coding: utf-8 -*-

from diskcache import Cache
from .paths import dir_disk_cache

cache = Cache(str(dir_disk_cache))
