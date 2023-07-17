# -*- coding: utf-8 -*-

import typing as T
import json
import dataclasses

from acore_db_ssh_tunnel.api import create_engine
from acore_server.api import Server

from ..api import Orm, app
from ..cache import cache


@cache.memoize(name="db_info", expire=3600)
def get_db_info() -> dict:
    server = Server.from_ec2_inside()
    return {
        "db_host": server.metadata.rds_inst.endpoint,
        "db_username": server.config.db_username,
        "db_password": server.config.db_password,
    }


def get_orm_from_ec2_inside() -> Orm:
    db_info = get_db_info()
    engine = create_engine(
        host=db_info["db_host"],
        port=3306,
        username=db_info["db_username"],
        password=db_info["db_password"],
        db_name="acore_auth",
    )
    return Orm(engine=engine)


def get_latest_n_quest(
    character: str,
    locale: str,
    n: int = 3,
):
    filtered_enriched_quest_data_list = app.get_latest_n_quest_enriched_quest_data(
        orm=get_orm_from_ec2_inside(),
        character=character,
        locale=app.LocaleEnum[locale],
        n=n,
    )
    print(
        json.dumps(
            [
                dataclasses.asdict(enriched_quest_data)
                for enriched_quest_data in filtered_enriched_quest_data_list
            ],
            ensure_ascii=False,
        )
    )
