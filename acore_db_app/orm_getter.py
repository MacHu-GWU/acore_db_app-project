# -*- coding: utf-8 -*-

from boto_session_manager import BotoSesManager
from acore_db_ssh_tunnel.api import create_engine
from acore_server.api import Server

from .orm import Orm
from .cache import cache


DB_INFO_CACHE_EXPIRE = 3600


@cache.memoize(name="db_info_from_ec2_inside", expire=DB_INFO_CACHE_EXPIRE)
def get_db_info_from_ec2_inside() -> dict:
    server = Server.from_ec2_inside()
    return {
        "db_host": server.metadata.rds_inst.endpoint,
        "db_username": server.config.db_username,
        "db_password": server.config.db_password,
    }


def get_orm_from_ec2_inside() -> Orm:
    db_info = get_db_info_from_ec2_inside()
    engine = create_engine(
        host=db_info["db_host"],
        port=3306,
        username=db_info["db_username"],
        password=db_info["db_password"],
        db_name="acore_auth",
    )
    return Orm(engine=engine)


def get_db_info_from_ec2_outside(
    bsm: BotoSesManager,
    server_id: str,
) -> dict:
    key = "db_info_from_ec2_outside"
    value = cache.get(key)
    if value is None:
        server = Server.get(bsm=bsm, server_id=server_id)
        value = {
            "db_host": server.metadata.rds_inst.endpoint,
            "db_username": server.config.db_username,
            "db_password": server.config.db_password,
        }
        cache.set(key=key, value=value, expire=DB_INFO_CACHE_EXPIRE)
    return value


def get_orm_for_ssh_tunnel(
    bsm: BotoSesManager,
    server_id: str,
) -> Orm:
    db_info = get_db_info_from_ec2_outside(bsm=bsm, server_id=server_id)
    engine = create_engine(
        host="127.0.0.1",
        port=3306,
        username=db_info["db_username"],
        password=db_info["db_password"],
        db_name="acore_auth",
    )
    return Orm(engine=engine)


def get_orm_for_lambda(
    bsm: BotoSesManager,
    server_id: str,
) -> Orm:
    db_info = get_db_info_from_ec2_outside(bsm=bsm, server_id=server_id)
    engine = create_engine(
        host=db_info["db_host"],
        port=3306,
        username=db_info["db_username"],
        password=db_info["db_password"],
        db_name="acore_auth",
    )
    return Orm(engine=engine)
