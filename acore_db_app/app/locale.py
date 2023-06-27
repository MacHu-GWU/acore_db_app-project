# -*- coding: utf-8 -*-

import enum


class LocaleEnum(str, enum.Enum):
    """
    Reference:

    - https://www.azerothcore.org/wiki/acore_string
    """
    enUS = "enUS"
    deDE = "deDE"
    esES = "esES"
    esMX = "esMX"
    frFR = "frFR"
    ruRU = "ruRU"
    zhCN = "zhCN"
    zhTW = "zhTW"
