# -*- coding: utf-8 -*-

from boto_session_manager import BotoSesManager
import acore_db_app.api as acore_db_app


bsm = BotoSesManager(profile_name="bmt_app_dev_us_east_1")
env_name = "sbx"
server_name = "blue"
server_id = f"{env_name}-{server_name}"

orm = acore_db_app.get_orm_for_ssh_tunnel(bsm=bsm, server_id=server_id)
