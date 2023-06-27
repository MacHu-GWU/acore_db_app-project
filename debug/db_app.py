# -*- coding: utf-8 -*-

from boto_session_manager import BotoSesManager
from acore_db_ssh_tunnel.api import create_engine
from acore_server.api import Server
from acore_db_app.orm import Orm
from acore_db_app.app.locale import LocaleEnum
from acore_db_app.app.quest import list_quest_by_character, complete_latest_n_quest
from rich import print as rprint

bsm = BotoSesManager(profile_name="bmt_app_dev_us_east_1")
env_name = "sbx"
server_name = "green"
server_id = f"{env_name}-{server_name}"

server = Server.get(bsm, server_id)

engine = create_engine(
    host="127.0.0.1",
    port=3306,
    username=server.config.db_username,
    password=server.config.db_password,
    db_name="acore_auth",
)
orm = Orm(engine=engine)

# res = list_quest_by_character(orm, "sa")
# rprint(res)

# complete_latest_n_quest(
#     orm=orm,
#     character="sa",
#     locale=LocaleEnum.zhCN,
# )
