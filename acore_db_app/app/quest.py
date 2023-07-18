# -*- coding: utf-8 -*-

"""
该模块用于实现跟 Quest 相关的 App
"""

import typing as T
import enum
import dataclasses

import sqlalchemy as sa

from ..orm import Orm
from ..logger import logger

from .locale import LocaleEnum


def _normalize_character(name: str) -> str:
    """
    将角色名称标准化为数据库中的值. 第一个字母大写, 后面的字母小写.
    """
    return name[0].upper() + name[1:].lower()


class CharacterQuestStatusEnum(int, enum.Enum):
    QUEST_STATUS_NONE = 0  # Quest isn't shown in quest list; default
    QUEST_STATUS_COMPLETE = 1  # Quest has been completed
    QUEST_STATUS_UNAVAILABLE = 2  # NOT USED
    QUEST_STATUS_INCOMPLETE = 3  # Quest is active in quest log but incomplete
    QUEST_STATUS_AVAILABLE = 4  # NOT USED
    QUEST_STATUS_FAILED = 5  # Player failed to complete the quest


@dataclasses.dataclass
class CharacterQuestStatus:
    """
    Reference: https://www.azerothcore.org/wiki/character_queststatus

    :param quest: Quest ID
    :param status: Quest status code, see :class:`CharacterQuestStatusEnum`
    """

    quest: int
    status: int

    def is_complete(self) -> bool:
        return self.status == CharacterQuestStatusEnum.QUEST_STATUS_COMPLETE.value

    def is_incomplete(self) -> bool:
        return self.status == CharacterQuestStatusEnum.QUEST_STATUS_INCOMPLETE.value

    def is_failed(self) -> bool:
        return self.status == CharacterQuestStatusEnum.QUEST_STATUS_FAILED.value


def list_quest_by_character(
    orm: Orm,
    character: str,
) -> T.List[CharacterQuestStatus]:
    """
    列出指定角色的所有任务的状态信息. 按照接任务的事件排序, 最新的任务在最前面.
    """
    with orm.engine.connect() as connect:
        stmt = (
            sa.select(
                orm.t_character_queststatus.c.quest,
                orm.t_character_queststatus.c.status,
            )
            .select_from(
                orm.t_characters.join(
                    orm.t_character_queststatus,
                    orm.t_character_queststatus.c.guid == orm.t_characters.c.guid,
                )
            )
            .where(orm.t_characters.c.name == _normalize_character(character))
            .order_by(orm.t_character_queststatus.c.timer.desc())
        )
        return [CharacterQuestStatus(**row) for row in connect.execute(stmt).mappings()]


@dataclasses.dataclass
class EnrichedQuestData:
    """
    代表着一个魔兽世界任务的数据. 并将额外的相关信息也整合到了一起. 该 data model 不包含与
    玩家相关的信息, 例如任务状态, 任务是否完成等等. 它是一个完全无状态的数据.
    """

    quest_id: T.Optional[int] = dataclasses.field(default=None)
    quest_title_enUS: T.Optional[str] = dataclasses.field(default=None)
    quest_title_locale: T.Optional[str] = dataclasses.field(default=None)
    locale: T.Optional[str] = dataclasses.field(default=None)
    starter_creature_id: T.Optional[int] = dataclasses.field(default=None)
    starter_guid: T.Optional[int] = dataclasses.field(default=None)
    starter_position_x: T.Optional[float] = dataclasses.field(default=None)
    starter_position_y: T.Optional[float] = dataclasses.field(default=None)
    starter_position_z: T.Optional[float] = dataclasses.field(default=None)
    starter_map: T.Optional[int] = dataclasses.field(default=None)
    ender_creature_id: T.Optional[int] = dataclasses.field(default=None)
    ender_guid: T.Optional[int] = dataclasses.field(default=None)
    ender_position_x: T.Optional[float] = dataclasses.field(default=None)
    ender_position_y: T.Optional[float] = dataclasses.field(default=None)
    ender_position_z: T.Optional[float] = dataclasses.field(default=None)
    ender_map: T.Optional[int] = dataclasses.field(default=None)

    @property
    def quest_title(self) -> str:
        if self.quest_title_locale is None:
            return self.quest_title_enUS
        else:
            return self.quest_title_locale

    @logger.pretty_log()
    def get_gm_commands(self):
        quest_complete_cmd = f".quest complete {self.quest_id}"
        go_starter_cmd = ".go xyz {} {} {} {}".format(
            self.starter_position_x,
            self.starter_position_y,
            self.starter_position_z,
            self.starter_map,
        )
        go_ender_cmd = ".go xyz {} {} {} {}".format(
            self.ender_position_x,
            self.ender_position_y,
            self.ender_position_z,
            self.ender_map,
        )
        logger.info(f"完成任务 {self.quest_title!r}:")
        logger.info(f"  {quest_complete_cmd}")
        logger.info("传送至任务开始 NPC:")
        logger.info(f"  {go_starter_cmd}")
        logger.info("传送至任务结束 NPC:")
        logger.info(f"  {go_ender_cmd}")


def get_enriched_quest_data(
    orm: Orm,
    character: str,
    locale: LocaleEnum = LocaleEnum.enUS,
    quest_title: T.Optional[str] = None,
    quest_objective: T.Optional[str] = None,
    quest_detail: T.Optional[str] = None,
    limit: int = 25,
) -> T.List[EnrichedQuestData]:
    """
    给定数据库连接, 和一个魔兽世界游戏角色的名字. 根据这些信息对获得该玩家所有的任务的详细信息,
    并将跟任务相关的其他数据都整合到一起.

    :param orm:
    :param character: 魔兽世界角色名字
    :param locale: 本地化语言, 默认为英文
    :param quest_title: 可选参数, 根据任务标题对任务进行过滤
    :param quest_objective: 可选参数, 根据任务目标对任务进行过滤
    :param quest_detail: 可选参数, 根据任务详情对任务进行过滤
    :param limit: 返回的任务数量的最大限制
    """
    with orm.engine.connect() as connect:
        selects = list()
        selects.append(orm.t_character_queststatus.c.quest.label("quest_id"))

        wheres = list()
        wheres.append(orm.t_characters.c.name == _normalize_character(character))

        joins = orm.t_characters.join(
            # 获得只属于该角色的任务状态
            orm.t_character_queststatus,
            orm.t_character_queststatus.c.guid == orm.t_characters.c.guid,
        )

        # 获得任务的英文文本信息
        selects.append(orm.t_quest_template.c.LogTitle.label("quest_title_enUS"))
        joins = joins.join(
            orm.t_quest_template,
            orm.t_quest_template.c.ID == orm.t_character_queststatus.c.quest,
        )

        # 若指定了除英文以外的语言, 则获得任务的本地化文本信息
        # the main table has to be on the left
        if locale is not LocaleEnum.enUS:
            # 先对 quest_template_locale 子表进行 filter, 只要指定的 locale
            subquery = sa.select(
                orm.t_quest_template_locale.c.ID,
                orm.t_quest_template_locale.c.Title,
            ).where(
                orm.t_quest_template_locale.c.locale == locale.value,
            ).subquery()

            selects.extend([
                subquery.c.Title.label("quest_title_locale"),
            ])

            # 然后再进行 left outer JOIN
            joins = joins.join(
                # 获得任务的文本信息
                subquery,
                subquery.c.ID == orm.t_character_queststatus.c.quest,
                isouter=True,
            )

        t_creature_quest_starter_entry = orm.t_creature.alias()
        t_creature_quest_ender_entry = orm.t_creature.alias()

        selects.extend(
            [
                orm.t_creature_queststarter.c.id.label("starter_creature_id"),
                t_creature_quest_starter_entry.c.guid.label("starter_guid"),
                t_creature_quest_starter_entry.c.position_x.label("starter_position_x"),
                t_creature_quest_starter_entry.c.position_y.label("starter_position_y"),
                t_creature_quest_starter_entry.c.position_z.label("starter_position_z"),
                t_creature_quest_starter_entry.c.map.label("starter_map"),
                orm.t_creature_questender.c.id.label("ender_creature_id"),
                t_creature_quest_ender_entry.c.guid.label("ender_guid"),
                t_creature_quest_ender_entry.c.position_x.label("ender_position_x"),
                t_creature_quest_ender_entry.c.position_y.label("ender_position_y"),
                t_creature_quest_ender_entry.c.position_z.label("ender_position_z"),
                t_creature_quest_ender_entry.c.map.label("ender_map"),
            ]
        )

        joins = (
            joins.join(
                # 获得任务给与者的信息
                orm.t_creature_queststarter,
                orm.t_creature_queststarter.c.quest
                == orm.t_character_queststatus.c.quest,
            )
            .join(
                # 获得任务结束者的信息
                orm.t_creature_questender,
                orm.t_creature_questender.c.quest
                == orm.t_character_queststatus.c.quest,
            )
            .join(
                # 获得任务给与者 NPC 的坐标信息
                t_creature_quest_starter_entry,
                t_creature_quest_starter_entry.c.id1
                == orm.t_creature_queststarter.c.id,
            )
            .join(
                # 获得任务结束者 NPC 的坐标信息
                t_creature_quest_ender_entry,
                t_creature_quest_ender_entry.c.id1 == orm.t_creature_queststarter.c.id,
            )
        )


        if quest_title is not None:
            if locale is LocaleEnum.enUS:
                wheres.append(orm.t_quest_template.c.Title.like(f"%{quest_title}%"))
            else:
                wheres.append(
                    orm.t_quest_template_locale.c.Title.like(f"%{quest_title}%")
                )
        if quest_objective is not None:
            if locale is LocaleEnum.enUS:
                wheres.append(orm.t_quest_template.c.Title.like(f"%{quest_title}%"))
            else:
                wheres.append(
                    orm.t_quest_template_locale.c.Objectives.like(
                        f"%{quest_objective}%"
                    )
                )
        if quest_detail is not None:
            if locale is LocaleEnum.enUS:
                wheres.append(orm.t_quest_template.c.Title.like(f"%{quest_title}%"))
            else:
                wheres.append(
                    orm.t_quest_template_locale.c.Details.like(f"%{quest_detail}%")
                )

        stmt = sa.select(*selects).select_from(joins).where(*wheres).limit(limit)
        # print(stmt)
        enriched_quest_data_list = list()
        for row in connect.execute(stmt).mappings():
            # print(row)
            enriched_quest_data = EnrichedQuestData(**row)
            enriched_quest_data_list.append(enriched_quest_data)
        return enriched_quest_data_list


def get_latest_n_quest_enriched_quest_data(
    orm: Orm,
    character: str,
    locale: LocaleEnum,
    n: int = 3,
) -> T.List[EnrichedQuestData]:
    """
    给定数据库连接, 和一个魔兽世界游戏角色的名字. 根据这些信息对获得该玩家所有的任务的详细信息,
    并将跟任务相关的其他数据都整合到一起. 最终按照接任务的事件顺序返回最新的 n 个任务的详细信息.
    """
    character_quest_status_list = list_quest_by_character(
        orm=orm,
        character=character,
    )
    enriched_quest_data_list = get_enriched_quest_data(
        orm=orm,
        character=character,
        locale=locale,
    )
    enriched_quest_data_mapper = {
        enriched_quest_data.quest_id: enriched_quest_data
        for enriched_quest_data in enriched_quest_data_list
    }
    ordered_enriched_quest_data_list = [
        enriched_quest_data_mapper[character_quest_status.quest]
        for character_quest_status in character_quest_status_list
        if character_quest_status.quest in enriched_quest_data_mapper
    ]
    ordered_enriched_quest_data_list = ordered_enriched_quest_data_list[:n]
    return ordered_enriched_quest_data_list


@logger.pretty_log()
def _print_complete_latest_n_quest_gm_commands(
    character: str,
    n: int,
    enriched_quest_data_list: T.List[EnrichedQuestData],
):
    logger.info(f"打印完成 {character!r} 的最新的 {n} 个任务的 GM 命令:")
    for enriched_quest_data in enriched_quest_data_list:
        with logger.nested():
            enriched_quest_data.get_gm_commands()


def complete_latest_n_quest(
    orm: Orm,
    character: str,
    locale: LocaleEnum,
    n: int = 3,
):
    """
    给定数据库连接, 和一个魔兽世界游戏角色的名字. 打印完成该玩家最新的 n 个任务的 GM 命令.
    其中包含完成任务的命令, 传送到接任务, 交任务的 NPC 处的命令.
    """
    ordered_enriched_quest_data_list = get_latest_n_quest_enriched_quest_data(
        orm=orm,
        character=character,
        locale=locale,
        n=n,
    )
    _print_complete_latest_n_quest_gm_commands(
        character=character,
        n=n,
        enriched_quest_data_list=ordered_enriched_quest_data_list,
    )
