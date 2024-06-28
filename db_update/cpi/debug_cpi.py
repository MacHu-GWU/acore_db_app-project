# -*- coding: utf-8 -*-

from pathlib_mate import Path
import acore_db_app.api as acore_db_app
from acore_db_app.update.projects.cpi import CpiWorkflow
from boto_session_manager import BotoSesManager

aws_profile = "bmt_app_dev_us_east_1"
server_id = "sbx-black"
bsm = BotoSesManager(profile_name=aws_profile)
orm = acore_db_app.get_orm_for_ssh_tunnel(
    bsm=bsm,
    server_id=server_id,
)

wf = CpiWorkflow(orm=orm, dir_workspace=Path.dir_here(__file__))
# wf.backup_item_template()
# wf.generate_base_price_table()
# wf.generate_final_price_table()
