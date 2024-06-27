# -*- coding: utf-8 -*-

"""
这个模块负责从 GitHub release 上下载数据文件.
"""

from pathlib import Path
from urllib.request import urlopen

from ..._version import __version__

dir_tmp = Path.home().joinpath("tmp", "acore_db_app")


def get_download_url(file_name: str, version: str = __version__) -> str:
    return (
        f"https://github.com/MacHu-GWU/acore_db_app-project"
        f"/releases/download/{version}/{file_name}"
    )


def get_download_path(file_name: str, version: str = __version__) -> Path:
    return dir_tmp.joinpath(version, file_name)


def download_file(file_name: str, version: str = __version__) -> Path:
    """
    尝试下载文件. 如果文件已经存在, 则不会重复下载 (因为 release 是 immutable 的),
    如果存在了, 内容就肯定是一样的.
    """
    url = get_download_url(file_name=file_name, version=version)
    path = get_download_path(file_name=file_name, version=version)
    if path.exists() is False:
        with urlopen(url) as response:
            try:
                path.write_bytes(response.read())
            except FileNotFoundError:
                path.parent.mkdir(parents=True)
                path.write_bytes(response.read())
    return path
