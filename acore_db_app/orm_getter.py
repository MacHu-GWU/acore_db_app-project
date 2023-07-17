# -*- coding: utf-8 -*-

"""
该模块用于创建 ORM 对象的实例. 该模块的函数会根据当前运行环境的不同, 选择不同的方式来创建 ORM
对象的实例.
"""

from boto_session_manager import BotoSesManager
from acore_db_ssh_tunnel.api import create_engine
from acore_server.api import Server

from .orm import Orm
from .cache import cache


DB_INFO_CACHE_EXPIRE = 3600


@cache.memoize(name="db_info_from_ec2_inside", expire=DB_INFO_CACHE_EXPIRE)
def _get_db_info_from_ec2_inside() -> dict:
    server = Server.from_ec2_inside()
    return {
        "db_host": server.metadata.rds_inst.endpoint,
        "db_username": server.config.db_username,
        "db_password": server.config.db_password,
    }


def get_orm_from_ec2_inside() -> Orm:
    """
    从 EC2 实例内部获取数据库信息, 并创建 ORM 对象的实例.
    """
    db_info = _get_db_info_from_ec2_inside()
    engine = create_engine(
        host=db_info["db_host"],
        port=3306,
        username=db_info["db_username"],
        password=db_info["db_password"],
        db_name="acore_auth",
    )
    return Orm(engine=engine)


def _get_db_info_from_ec2_outside(
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
    """
    创建基于 SSH Tunnel 的 ORM 对象的实例. 该函数常用于在本地开发电脑上连接数据库.

    :param bsm: BotoSesManager 对象的实例.
    :param server_id: 服务器 ID. Example: ``${env_name}-${server_name}``.
    """
    db_info = _get_db_info_from_ec2_outside(bsm=bsm, server_id=server_id)
    engine = create_engine(
        host="127.0.0.1",
        port=3306,
        username=db_info["db_username"],
        password=db_info["db_password"],
        db_name="acore_auth",
    )
    return Orm(engine=engine)


def get_orm_for_vpc(
    bsm: BotoSesManager,
    server_id: str,
) -> Orm:
    """
    创建基于 VPC 的 ORM 对象的实例. 该函数常用于在与数据库同处于一个 VPC 下的 EC2 或 Lambda
    中连接数据库.

    :param bsm: BotoSesManager 对象的实例.
    :param server_id: 服务器 ID. Example: ``${env_name}-${server_name}``.
    """
    db_info = _get_db_info_from_ec2_outside(bsm=bsm, server_id=server_id)
    engine = create_engine(
        host=db_info["db_host"],
        port=3306,
        username=db_info["db_username"],
        password=db_info["db_password"],
        db_name="acore_auth",
    )
    return Orm(engine=engine)
