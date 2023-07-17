# -*- coding: utf-8 -*-

import typing as T

import json
import dataclasses

from boto_session_manager import BotoSesManager
from acore_server.api import Server

from ....compat import cached_property
from ....paths import path_gui_settings_json


@dataclasses.dataclass
class SettingObject:
    # static fields
    aws_profile: T.Optional[str] = dataclasses.field(default=None)
    server_id: str = dataclasses.field(default="sbx-blue")
    locale: str = dataclasses.field(default="enUS")

    # derived fields
    bsm: T.Optional[BotoSesManager] = dataclasses.field(default=None)
    server: T.Optional[Server] = dataclasses.field(default=None)

    def __post_init__(self):
        self.bsm = BotoSesManager(profile_name=self.aws_profile)
        if self.server_id:
            self.server = Server.get(
                bsm=self.bsm,
                server_id=self.server_id,
            )

    @classmethod
    def read_settings(cls):
        try:
            data = json.loads(path_gui_settings_json.read_text())
            return cls(**data)
        except FileNotFoundError:
            setting_object = cls()
            setting_object.write_settings()
            return setting_object

    def write_settings(self):
        data = {
            field: getattr(self, field)
            for field in self.static_fields
        }
        path_gui_settings_json.write_text(json.dumps(data, indent=4))

    @cached_property
    def static_fields(self) -> T.List[str]:
        return [
            "aws_profile",
            "server_id",
            "locale",
        ]

    def iter_static_items(self) -> T.Iterator[T.Tuple[str, str]]:
        for field in self.static_fields:
            yield field, getattr(self, field)


setting_object = SettingObject.read_settings()
