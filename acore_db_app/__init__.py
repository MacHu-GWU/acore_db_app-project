# -*- coding: utf-8 -*-

"""
Azerothcore World of Warcraft Database Application.

代码结构:

- orm, orm_getter: 对底层数据表以及数据库连接的封装.
- app: 数据库 app 的实现, 并将其封装成用户友好的 Python 函数.
- cli: 命令行接口, 部署在在 EC2 游戏服务器上.
- sdk: 通过 SSM Run Command 远程调用 cli 的 Python SDK.
- gui: 一个图形界面的 App.
"""

from ._version import __version__

__short_description__ = "Azerothcore World of Warcraft Database Application."
__license__ = "MIT"
__author__ = "Sanhe Hu"
__author_email__ = "husanhe@gmail.com"
__maintainer__ = "Sanhe Hu"
__maintainer_email__ = "husanhe@gmail.com"
__github_username__ = "MacHu-GWU"
