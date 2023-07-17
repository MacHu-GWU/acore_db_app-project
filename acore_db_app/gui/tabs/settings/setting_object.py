# -*- coding: utf-8 -*-

import typing as T
import dataclasses
from boto_session_manager import BotoSesManager
from acore_server.api import Server
from ....compat import cached_property


@dataclasses.dataclass
class SettingObject:
    aws_profile: T.Optional[str]
    server_id: T.Optional[str]

    bsm: BotoSesManager = dataclasses.field(init=False)
    server: Server = dataclasses.field(init=False)


