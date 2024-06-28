# -*- coding: utf-8 -*-

"""
该脚本通过筛选 Spell.dbc 文件, 筛选出所有创造物品的技能, 并且获得它们的配方数据. 这样我们
就不需要手动输入配方了.

Ref:

- Spell.csv 数据来源: https://drive.google.com/drive/folders/13eOXHKWCyYSGTrsETCx6yKTfVuuqeQrl
"""

import typing as T
import gzip
import json
import dataclasses
from pathlib import Path
from collections import Counter

import polars as pl

from ..._version import __version__
from ..common.api import download_file


@dataclasses.dataclass
class Reagent:
    """
    表示一个配方中的一个材料的 ID 和数量.

    :param id: 材料的 ID.
    :param count: 材料的数量.
    """

    id: int = dataclasses.field()
    count: int = dataclasses.field()


@dataclasses.dataclass
class Recipe:
    """
    表示一个制造物品的配方.

    :param id: 造出的物品的 ID
    :param count: 造出的物品的数量
    :param reagents: 制造这个物品所需要的材料, 每个材料是一个 (item_id, item_count) 的元组.
    """

    id: int = dataclasses.field()
    count: int = dataclasses.field()
    reagents: T.List[Reagent] = dataclasses.field()

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, dct: dict):
        return cls(
            id=dct["id"],
            count=dct["count"],
            reagents=[Reagent(**reagent) for reagent in dct["reagents"]],
        )

    @classmethod
    def dump_many(
        cls,
        recipes: T.List["Recipe"],
        path_json_gz: Path,
    ):
        """
        Dump recipes to a ``.json.gz`` file.
        """
        path_json_gz.write_bytes(
            gzip.compress(
                json.dumps([dataclasses.asdict(recipe) for recipe in recipes]).encode(
                    "utf-8"
                )
            )
        )

    @classmethod
    def load_many(
        cls,
        path_json_gz: Path,
    ) -> T.List["Recipe"]:
        """
        Load recipes from a ``.json.gz`` file.
        """
        return [
            Recipe.from_dict(dct)
            for dct in json.loads(gzip.decompress(path_json_gz.read_bytes()))
        ]

    @classmethod
    def to_dataframe(
        cls,
        recipes: T.List["Recipe"],
    ) -> pl.DataFrame:
        """
        将 Recipe 对象转换为 Polars 的 DataFrame 对象, 以便于和数据库中的表做 JOIN.
        """
        n = 14  # 2 for id and count, 2 * 6 = 12 for reagents
        rows = list()
        for recipe in recipes:
            row = [recipe.id, recipe.count]
            for reagent in recipe.reagents:
                row.append(reagent.id)
                row.append(reagent.count)
            row.extend([None] * (n - len(row)))
            rows.append(row)
        schema = {
            "id": int,
            "count": int,
            "reagent_id_1": int,
            "reagent_count_1": int,
            "reagent_id_2": int,
            "reagent_count_2": int,
            "reagent_id_3": int,
            "reagent_count_3": int,
            "reagent_id_4": int,
            "reagent_count_4": int,
            "reagent_id_5": int,
            "reagent_count_5": int,
            "reagent_id_6": int,
            "reagent_count_6": int,
        }
        df = pl.DataFrame(rows, schema=schema)
        return df


def extract_recipe(
    path_spell_csv_gz: T.Optional[Path] = None,
) -> T.List[Recipe]:
    """
    Spell.dbc 中的 column 的定义: https://wowdev.wiki/DB/Spell#3.3.5.12340

    - col 75: 是 EffectDieSides, 也就是说这是一个几个面的色子 (N 个面就是说能丢出来的数在 1-N 之间),
    - col 81: 是 EffectBasePoints, 这就是 DNF 规则下丢色子的意思.
        例如 (EffectBasePoints, EffectDieSides) 分别是 (49, 26), 那么丢出来的随机数就是 (50, 75) 之间的一个.
    """
    if path_spell_csv_gz is None:
        path_spell_csv_gz = download_file(
            file_name="Spell.csv.gz",
            version=__version__,
        )

    df_spell = pl.read_csv(
        str(path_spell_csv_gz),
        # separator=",",
        # CSV 的读取引擎需要扫描一定数量的数据才能做出数据类型推理, 这里我们设定扫描全部数据后再做推理.
        infer_schema_length=99999,
    )

    df_spell.columns = [str(i) for i in range(1, 1 + len(df_spell.columns))]

    _wanted_cols = [
        # 施法材料
        53,
        54,
        55,
        56,
        57,
        58,
        59,
        60,
        61,
        62,
        63,
        64,
        65,
        66,
        67,
        68,
        # 造出的物品的数量
        75,
        81,
        # 造出的物品 ID
        108,
    ]

    reagent_item_id_list = [53, 54, 55, 56, 57, 58, 59, 60]
    reagent_item_id_list = [str(i) for i in reagent_item_id_list]
    reagent_item_count_list = [61, 62, 63, 64, 65, 66, 67, 68]
    reagent_item_count_list = [str(i) for i in reagent_item_count_list]

    wanted_cols = [str(i) for i in _wanted_cols]

    df_spell_subset = (
        df_spell.filter(
            (
                # 效果的类型是创建物品
                (pl.col("72") == 24)
                # 生产出来的物品的数量是确定的
                & (pl.col("75") == 1)
                # 生产出来的物品只有一种
                & (pl.col("109") == "0x0")
                & (pl.col("110") == "0x0")
                # pl.col(df_spell.columns[0]).is_in([8366, 56475, 53901]), # for debug only, 用于定位特殊的 row
            )
        )
        .select(wanted_cols)
        .limit(99999)
    )

    recipe_list: T.List[Recipe] = list()
    for record in df_spell_subset.to_dicts():
        reagents = list()

        # 从 record 中提取跟 Reagent 相关的数据
        for col1, col2 in zip(reagent_item_id_list, reagent_item_count_list):
            reagent_id, reagent_count = record[col1], record[col2]
            if reagent_id != 0:
                reagents.append(Reagent(id=reagent_id, count=reagent_count))

        # 对 reagent 数据进行处理, 创建对象
        if 1 <= len(reagents) <= 6:
            recipe = Recipe(
                id=record["108"],
                count=record["75"] + record["81"],
                reagents=reagents,
            )
            recipe_list.append(recipe)

    # 我们只关注只有一个配方的物品
    counter = Counter([recipe.id for recipe in recipe_list])
    single_spell_recipe_set = {k for k, v in counter.items() if v == 1}
    single_spell_recipe_list = [
        recipe for recipe in recipe_list if recipe.id in single_spell_recipe_set
    ]
    return single_spell_recipe_list


def get_dataframe() -> pl.DataFrame:
    path = download_file(file_name="recipe.json.gz")
    recipe_list = Recipe.load_many(path)
    return Recipe.to_dataframe(recipe_list)


def get_recipe_list() -> T.List[Recipe]:
    path = download_file(file_name="recipe.json.gz")
    return Recipe.load_many(path)
