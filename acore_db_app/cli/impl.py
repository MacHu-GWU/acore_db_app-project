# -*- coding: utf-8 -*-

import typing as T
import json
import dataclasses
from datetime import datetime

from acore_db_ssh_tunnel.api import create_engine
from acore_server.api import Server

from ..api import Orm, app
from ..paths import path_sqlalchemy_engine_json


def get_orm() -> T.Tuple[Orm, str, str, str]:
    server = Server.from_ec2_inside()
    engine = create_engine(
        host=server.metadata.rds_inst.endpoint,
        port=3306,
        username=server.config.db_username,
        password=server.config.db_password,
        db_name="acore_auth",
    )
    orm = Orm(engine=engine)
    return (
        orm,
        server.metadata.rds_inst.endpoint,
        server.config.db_username,
        server.config.db_password,
    )


def get_orm_from_ec2_inside() -> Orm:
    # no cache
    if path_sqlalchemy_engine_json.exists() is False:
        orm, host, username, password = get_orm()
        cache_data = {
            "host": host,
            "username": username,
            "password": password,
            "update_time": datetime.now().isoformat(),
        }
        path_sqlalchemy_engine_json.write_text(json.dumps(cache_data))
        return orm

    # cache exists, and is not expired
    cache_data = json.loads(path_sqlalchemy_engine_json.read_text())
    update_time = datetime.fromisoformat(cache_data["update_time"])

    if (datetime.now() - update_time).total_seconds() <= 3600:
        engine = create_engine(
            host=cache_data["host"],
            port=3306,
            username=cache_data["username"],
            password=cache_data["password"],
            db_name="acore_auth",
        )
        orm = Orm(engine=engine)
        return orm

    # cache is expired
    orm, host, username, password = get_orm()
    cache_data = {
        "host": host,
        "username": username,
        "password": password,
        "update_time": datetime.now().isoformat(),
    }
    path_sqlalchemy_engine_json.write_text(json.dumps(cache_data))
    return orm


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
