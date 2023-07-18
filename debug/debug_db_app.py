# -*- coding: utf-8 -*-

from boto_session_manager import BotoSesManager
from acore_db_ssh_tunnel.api import create_engine
from acore_server.api import Server

from acore_db_app.api import app, get_orm_for_ssh_tunnel
from rich import print as rprint

bsm = BotoSesManager(profile_name="bmt_app_dev_us_east_1")
env_name = "sbx"
server_name = "green"
server_id = f"{env_name}-{server_name}"

orm = get_orm_for_ssh_tunnel(bsm=bsm, server_id=server_id)

res = app.quest.list_quest_by_character(orm, "sa")
# rprint(res)

app.quest.complete_latest_n_quest(
    orm=orm,
    character="shootingrab",
    locale=app.LocaleEnum.zhTW,
)
