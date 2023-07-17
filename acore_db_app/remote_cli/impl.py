# -*- coding: utf-8 -*-

import typing as T
import json
from boto_session_manager import BotoSesManager
from acore_paths.api import path_acore_db_app_cli
from acore_server_metadata.api import Server as ServerMetadata
import aws_ssm_run_command.api as aws_ssm_run_command

from ..app.quest import EnrichedQuestData


def get_latest_n_request(
    bsm: BotoSesManager,
    server_id: str,
    character: str,
    locale: str,
    n: int,
) -> T.List[EnrichedQuestData]:
    command_invocation = aws_ssm_run_command.better_boto.send_command_sync(
        ssm_client=bsm.ssm_client,
        instance_id=ServerMetadata.get_ec2(ec2_client=bsm.ec2_client, id=server_id).id,
        commands=[
            (
                f"{path_acore_db_app_cli} "
                f"quest get-latest-n-quest --char {character} --locale {locale} --n {n}"
            ),
        ],
        delays=1,
        timeout=10,
    )
    enriched_quest_data_list = [
        EnrichedQuestData(**dct)
        for dct in json.loads(command_invocation.StandardOutputContent)
    ]
    return enriched_quest_data_list
