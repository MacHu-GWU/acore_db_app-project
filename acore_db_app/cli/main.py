# -*- coding: utf-8 -*-

"""
Acore DB App CLI interface
"""

import fire

from ..app.api import LocaleEnum


class Quest:
    """
    A collection of canned SOAP Agent commands.
    """

    def get_latest_n_quest(
        self,
        char: str,
        locale: str = LocaleEnum.enUS.value,
        n: int = 3,
    ):
        """
        Get the online players and characters in world. Also, you can use this
         command to check whether server is online.

        Example::

            acoredb quest get_latest_n_quest --help

            acoredb quest get_latest_n_quest --char mychar --locale enUS --n 3
        """
        # 注: 这段代码不能放在文件开头, 因为这段代码会 import cache. 如果我们放在文件开头,
        # 那么在 bootstrap 的时候是 root user 创建的 cache 数据库, 会导致普通用户没有权限
        # 使用
        from .impl import get_latest_n_quest

        get_latest_n_quest(
            character=char,
            locale=locale,
            n=n,
        )


class Command:
    """
    Example:

    - acoredb
    """

    def __init__(self):
        self.quest = Quest()

    def hello(self):
        """
        Print welcome message.
        """
        print("Hello acore db app user!")


def run():
    fire.Fire(Command)
