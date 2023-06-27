# -*- coding: utf-8 -*-

"""
Acore DB App CLI interface
"""

import fire

from .impl import (
    get_latest_n_quest,
)


class Quest:
    """
    A collection of canned SOAP Agent commands.
    """

    def get_latest_n_quest(
        self,
        char: str,
        locale: str,
        n: int = 3,
    ):
        """
        Get the online players and characters in world. Also, you can use this
         command to check whether server is online.

        Example::

            acoredb quest get_latest_n_quest --help

            acoredb quest get_latest_n_quest --char mychar --locale enUS --n 3
        """
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


def run():
    fire.Fire(Command)
