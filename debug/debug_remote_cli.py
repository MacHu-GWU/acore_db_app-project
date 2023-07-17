# -*- coding: utf-8 -*-

from boto_session_manager import BotoSesManager
from rich import print as rprint
from acore_server_metadata.api import Server as ServerMetadata
from acore_db_app.app.api import LocaleEnum
from acore_db_app.remote_cli.impl import (
    get_latest_n_request,
)

bsm = BotoSesManager(profile_name="bmt_app_dev_us_east_1")
env_name = "sbx"
server_name = "green"
server_id = f"{env_name}-{server_name}"

instance_id = ServerMetadata.get_ec2(ec2_client=bsm.ec2_client, id=server_id).id
enriched_quest_data_list = get_latest_n_request(
    bsm=bsm,
    instance_id=instance_id,
    character="shootingrab",
    locale=LocaleEnum.zhTW.value,
    n=25,
)
rprint(enriched_quest_data_list)
