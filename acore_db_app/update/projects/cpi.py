# -*- coding: utf-8 -*-

"""
Consumer Price Index (CPI) 是一个对魔兽世界中的物品价格进行修正, 按照比较稳定的私服物价
给这些物品的价格赋值.
"""

import typing as T
import json
import gzip
import dataclasses

import polars as pl
import sqlalchemy as sa
from pathlib_mate import Path

from acore_df.api import Lookup

from ..common.api import craft_spell_recipe
from ...logger import logger

if T.TYPE_CHECKING:  # pragma: no cover
    from ...orm import Orm

lookup = Lookup.new()


@dataclasses.dataclass
class CpiWorkflow:
    """
    CPI db update workflow.
    """

    orm: "Orm" = dataclasses.field()
    dir_workspace: Path = dataclasses.field()

    @property
    def path_item_template_backup_json_gz(self) -> Path:
        return self.dir_workspace.joinpath("item_template_backup.json.gz")

    @property
    def path_lookup_table_tsv(self) -> Path:
        return self.dir_workspace.joinpath("lookup_table.tsv")

    @property
    def path_base_price_table_tsv(self) -> Path:
        return self.dir_workspace.joinpath("base-price-table.tsv")

    @property
    def path_price_table_tsv(self) -> Path:
        return self.dir_workspace.joinpath("price-table.tsv")

    @property
    def final_price_table_tsv(self) -> Path:
        return self.dir_workspace.joinpath("final-price-table.tsv")

    @logger.emoji_block(
        msg="{func_name}",
        emoji="📄",
    )
    def backup_item_template(
        self,
        _limit: T.Optional[int] = None,
    ) -> T.List[T.Dict[str, T.Any]]:
        """
        将数据库中的 ItemTemplate 表的数据导出并备份到 json 文件中.
        """
        engine = self.orm.engine
        with engine.connect() as conn:
            t_item_template = self.orm.t_item_template
            t_item_template_locale = self.orm.t_item_template_locale
            sql_stmt = (
                sa.select(
                    t_item_template, t_item_template_locale.c.Name.label("name_cn")
                )
                .join(
                    t_item_template_locale,
                    onclause=t_item_template.c.entry == t_item_template_locale.c.ID,
                )
                .where(t_item_template_locale.c.locale == "zhCN")
            )
            logger.info("Read data from item_template table ...")
            item_template_records = [row._asdict() for row in conn.execute(sql_stmt)]
            logger.info(f"Done, got {len(item_template_records)} records.")

            logger.info(f"Dump data to {self.path_item_template_backup_json_gz} ...")
            self.path_item_template_backup_json_gz.write_bytes(
                gzip.compress(
                    json.dumps(item_template_records, ensure_ascii=False).encode(
                        "utf-8"
                    )
                )
            )
            logger.info(f"Done, see: file://{self.path_item_template_backup_json_gz}")
        return item_template_records

    @logger.emoji_block(
        msg="{func_name}",
        emoji="📄",
    )
    def generate_base_price_table(self) -> pl.DataFrame:
        """
        生成一个包含所有物品列表, 但是没有价格的基础 price table. 然后我才能在这个表中
        填入物品价值. 在 price table 中的物品都是我认为 "可以" 从 NPC 处购买的物品. 包括:

        1. 各种商品.
        2. 各种配方.
        3. 各种每个版本满级后的装备.

        生成的表结构可以参考: https://docs.google.com/spreadsheets/d/1e4I2-d4JyVbsvOcdePruqev-rkyYYMUPrwkI_fieIYw/edit?gid=2104698923#gid=2104698923
        """
        logger.info(f"Load data from {self.path_item_template_backup_json_gz} ...")
        records = json.loads(
            gzip.decompress(self.path_item_template_backup_json_gz.read_bytes()).decode(
                "utf-8"
            )
        )
        logger.info(f"total records = {len(records)}")

        logger.info(f"Transform data ...")
        df = pl.DataFrame(records)

        df = df.select(
            [
                "entry",
                "name_cn",
                "class",
                "subclass",
                "Quality",
                "bonding",
                "ItemLevel",
                "RequiredLevel",
            ]
        )
        df = df.rename(
            mapping={
                "entry": "编号",
                "name_cn": "名字",
                "class": "大类",
                "subclass": "小类",
                "Quality": "品质",
                "bonding": "绑定",
                "ItemLevel": "iLvl",
                "RequiredLevel": "rLvl",
            }
        )

        df = df.with_columns(
            pl.concat_str(
                pl.col("大类"),
                pl.lit("-"),
                pl.col("小类"),
            ).alias("item_class")
        )

        logger.info(f"Filter data by item class and subclass ...")
        df = df.filter(
            # https://docs.google.com/spreadsheets/d/1XevE2tFnjCSf0paizwanCJMgmBdChunCgAp7BvqqD0M/edit?gid=0#gid=0
            pl.col("item_class").is_in(
                [
                    "0-0",  # 消耗品-消耗品
                    "0-1",  # 消耗品-药水
                    "0-2",  # 消耗品-药剂
                    "0-3",  # 消耗品-合剂
                    "0-4",  # 消耗品-卷轴
                    "0-5",  # 消耗品-食物和饮料
                    "0-6",  # 消耗品-物品强化
                    "0-7",  # 消耗品-绷带
                    "0-8",  # 消耗品-其他
                    "1-0",  # 容器-背包
                    "1-1",  # 容器-灵魂袋
                    "1-2",  # 容器-草药包
                    "1-3",  # 容器-附魔材料包
                    "1-4",  # 容器-工程学材料包
                    "1-5",  # 容器-宝石包
                    "1-6",  # 容器-矿石袋
                    "1-7",  # 容器-制皮材料包
                    "1-8",  # 容器-铭文包
                    "2-0",  # 武器-单手斧
                    "2-1",  # 武器-双手斧
                    "2-2",  # 武器-弓
                    "2-3",  # 武器-枪
                    "2-4",  # 武器-单手锤
                    "2-5",  # 武器-双手锤
                    "2-6",  # 武器-长柄武器
                    "2-7",  # 武器-单手剑
                    "2-8",  # 武器-双手剑
                    # "2-9",  # -
                    "2-10",  # 武器-法杖
                    # "2-11",  # -
                    # "2-12",  # -
                    "2-13",  # 武器-拳套
                    "2-14",  # 武器-杂项
                    "2-15",  # 武器-匕首
                    "2-16",  # 武器-投掷武器
                    # "2-17",  # -
                    "2-18",  # 武器-弩
                    "2-19",  # 武器-魔杖
                    "2-20",  # 武器-钓鱼竿
                    "3-0",  # 珠宝-红色
                    "3-1",  # 珠宝-蓝色
                    "3-2",  # 珠宝-黄色
                    "3-3",  # 珠宝-紫色
                    "3-4",  # 珠宝-绿色
                    "3-5",  # 珠宝-橙色
                    "3-6",  # 珠宝-多彩
                    "3-7",  # 珠宝-简单
                    "3-8",  # 珠宝-棱彩
                    "4-0",  # 护甲-杂项
                    "4-1",  # 护甲-布甲
                    "4-2",  # 护甲-皮甲
                    "4-3",  # 护甲-锁甲
                    "4-4",  # 护甲-板甲
                    # "4-5",  # -
                    "4-6",  # 护甲-盾牌
                    "4-7",  # 护甲-圣契
                    "4-8",  # 护甲-神像
                    "4-9",  # 护甲-图腾
                    "4-10",  # 护甲-魔印
                    "5-0",  # 材料-施法材料
                    # "6-0",  # -
                    # "6-1",  # -
                    "6-2",  # 弹药-箭
                    "6-3",  # 弹药-子弹
                    # "6-4",  # -
                    # "7-0",  # 商品-商品
                    "7-1",  # 商品-零件
                    "7-2",  # 商品-爆炸物
                    "7-3",  # 商品-装置
                    "7-4",  # 商品-珠宝加工
                    "7-5",  # 商品-布料
                    "7-6",  # 商品-皮革
                    "7-7",  # 商品-金属与石头
                    "7-8",  # 商品-肉类
                    "7-9",  # 商品-草药
                    "7-10",  # 商品-元素
                    "7-11",  # 商品-其他
                    "7-12",  # 商品-附魔
                    "7-13",  # 商品-原料
                    "7-14",  # 商品-护甲附魔
                    "7-15",  # 商品-武器附魔
                    # "8-0",  # -
                    "9-0",  # 配方-书籍
                    "9-1",  # 配方-制皮
                    "9-2",  # 配方-裁缝
                    "9-3",  # 配方-工程学
                    "9-4",  # 配方-锻造
                    "9-5",  # 配方-烹饪
                    "9-6",  # 配方-炼金术
                    "9-7",  # 配方-急救
                    "9-8",  # 配方-附魔
                    "9-9",  # 配方-钓鱼
                    "9-10",  # 配方-珠宝加工
                    # "10-0",  # -
                    # "11-0",  # -
                    # "11-1",  # -
                    "11-2",  # 箭袋-箭袋
                    "11-3",  # 箭袋-弹药包
                    # "12-0",  # 任务-任务
                    "13-0",  # 钥匙-钥匙
                    # "13-1",  # 钥匙-开锁工具
                    # "14-0",  # 永久-永久
                    # "15-0",  # 其它-垃圾
                    "15-1",  # 其它-施法材料
                    "15-2",  # 其它-宠物
                    "15-3",  # 其它-节日
                    "15-4",  # 其它-其他
                    "15-5",  # 其它-坐骑
                    "16-1",  # 雕文-战士
                    "16-2",  # 雕文-圣骑士
                    "16-3",  # 雕文-猎人
                    "16-4",  # 雕文-盗贼
                    "16-5",  # 雕文-牧师
                    "16-6",  # 雕文-死亡骑士
                    "16-7",  # 雕文-萨满
                    "16-8",  # 雕文-法师
                    "16-9",  # 雕文-术士
                    "16-11",  # 雕文-德鲁伊
                ]
            ),
            pl.col("品质").is_in(
                [
                    # 0, # 灰色
                    1,  # 白色
                    2,  # 绿色
                    3,  # 蓝色
                    4,  # 紫色
                    5,  # 橙色
                    # 6, # 红色
                    # 7, # 金色
                ]
            ),
        )
        print(f"filtered records = {df.shape[0]}")

        # Join with recipe
        logger.info(f"Add recipe data by join with craft spell recipe data ...")
        df_recipe = craft_spell_recipe.get_dataframe()
        df_recipe = df_recipe.rename({"count": "product_count"})
        df = df.join(
            df_recipe,
            left_on="编号",
            right_on="id",
            how="left",
        )

        # Convert value and select columns
        id_to_name_mapping = {
            dct["编号"]: dct["名字"] for dct in df.select(["编号", "名字"]).to_dicts()
        }
        df = (
            df.drop("小类")
            .with_columns(
                pl.col("大类").replace(
                    {
                        id: row.name_cn
                        for id, row in lookup.item_template_class.row_map.items()
                    }
                ),
                pl.col("item_class")
                .replace(
                    {
                        id: row.subclass_name_cn
                        for id, row in lookup.item_template_subclass.row_map.items()
                    }
                )
                .alias("小类"),
                pl.col("品质").replace(
                    {
                        id: row.name_cn
                        for id, row in lookup.item_template_quality.row_map.items()
                    }
                ),
                pl.col("绑定").replace(
                    {
                        id: row.name_cn
                        for id, row in lookup.item_template_bonding.row_map.items()
                    }
                ),
                pl.col("reagent_id_1")
                .replace(id_to_name_mapping)
                .alias("reagent_name_1"),
                pl.col("reagent_id_2")
                .replace(id_to_name_mapping)
                .alias("reagent_name_2"),
                pl.col("reagent_id_3")
                .replace(id_to_name_mapping)
                .alias("reagent_name_3"),
                pl.col("reagent_id_4")
                .replace(id_to_name_mapping)
                .alias("reagent_name_4"),
                pl.col("reagent_id_5")
                .replace(id_to_name_mapping)
                .alias("reagent_name_5"),
                pl.col("reagent_id_6")
                .replace(id_to_name_mapping)
                .alias("reagent_name_6"),
            )
            .select(
                [
                    "编号",
                    "名字",
                    pl.lit(-1).alias("单价"),
                    pl.lit(1).alias("增值"),
                    pl.lit(None).alias("可买"),
                    "大类",
                    "小类",
                    "品质",
                    "绑定",
                    "iLvl",
                    "rLvl",
                    "product_count",
                    "reagent_id_1",
                    "reagent_name_1",
                    "reagent_count_1",
                    "reagent_id_2",
                    "reagent_name_2",
                    "reagent_count_2",
                    "reagent_id_3",
                    "reagent_name_3",
                    "reagent_count_3",
                    "reagent_id_4",
                    "reagent_name_4",
                    "reagent_count_4",
                    "reagent_id_5",
                    "reagent_name_5",
                    "reagent_count_5",
                    "reagent_id_6",
                    "reagent_name_6",
                    "reagent_count_6",
                ]
            )
        )

        logger.info(f"Dump data to {self.path_base_price_table_tsv} ...")
        df.write_csv(
            str(self.path_base_price_table_tsv),
            separator="\t",
        )
        logger.info(f"Done, see: file://{self.path_base_price_table_tsv}")

        return df

    @logger.emoji_block(
        msg="{func_name}",
        emoji="📄",
    )
    def generate_final_price_table(self):
        """
        生成包含所有物品的单价的 final price table. 这个表是根据 base price table
        计算得来. 在 base price table 中只有原子类的物品的单价 (原子类物品就是无法通过
        其他物品合成的物品). 在 final price table 中, 我们会尽可能的计算出所有物品的
        单价.

        生成的表结构可以参考: https://docs.google.com/spreadsheets/d/1e4I2-d4JyVbsvOcdePruqev-rkyYYMUPrwkI_fieIYw/edit?gid=1949060708#gid=1949060708
        """
        # 从 price table 中读取所需的数据
        logger.info(f"Load data from {self.path_price_table_tsv} ...")
        df = pl.read_csv(
            str(self.path_price_table_tsv),
            separator="\t",
            schema={
                "编号": pl.Int32,
                "名字": pl.Utf8,
                "单价": pl.Float32,
                "增值": pl.Float32,
                "可买": pl.Int32,
                "大类": pl.Utf8,
                "小类": pl.Utf8,
                "品质": pl.Utf8,
                "绑定": pl.Utf8,
                "iLvl": pl.Int32,
                "rLvl": pl.Int32,
                "product_count": pl.Int32,
                "reagent_id_1": pl.Int32,
                "reagent_name_1": pl.Utf8,
                "reagent_count_1": pl.Float32,
                "reagent_id_2": pl.Int32,
                "reagent_name_2": pl.Utf8,
                "reagent_count_2": pl.Float32,
                "reagent_id_3": pl.Int32,
                "reagent_name_3": pl.Utf8,
                "reagent_count_3": pl.Float32,
                "reagent_id_4": pl.Int32,
                "reagent_name_4": pl.Utf8,
                "reagent_count_4": pl.Float32,
                "reagent_id_5": pl.Int32,
                "reagent_name_5": pl.Utf8,
                "reagent_count_5": pl.Float32,
                "reagent_id_6": pl.Int32,
                "reagent_name_6": pl.Utf8,
                "reagent_count_6": pl.Float32,
            },
        )
        # 建立一个内存中的 id -> row 的映射
        mapping = {row["编号"]: row for row in df.to_dicts()}

        logger.info(f"extract reagents ...")
        def extract_reagents(row: T.Dict[str, T.Any]) -> T.Dict[int, int]:
            """
            从 row 中把多个 column 的值合并成一个 reagents 字段. reagents 字段是一个字典,
            key 是 reagent_id, value 是 reagent_count.
            """
            reagents = {}
            for i in range(1, 1 + 6):
                id = row[f"reagent_id_{i}"]
                if id:
                    reagents[id] = row[f"reagent_count_{i}"]
            return reagents

        # 先获得所有 item 的 reagents
        for id, row in mapping.items():
            reagents = extract_reagents(row)
            mapping[id]["reagents"] = reagents

        logger.info(f"resolve price ...")
        def resolve_price():
            """
            这个函数会遍历所有 item, 尝试计算出单价. 由于很可能 item 配方中的 reagent
            的单价还没有计算出来, 要正确计算就需要用到递归编程. 这里我们偷懒了, 就用多次
            循环来模拟递归, 直到再也无法找到任何一个新的 item 能计算出单价为止.
            """
            counter = 0
            for id, row in mapping.items():
                price = row["单价"]
                if price == -1:
                    reagents = row["reagents"]
                    if reagents:
                        total_price = 0
                        found_total_price = True
                        for reg_id, reg_count in reagents.items():
                            try:
                                reg_price = mapping[reg_id]["单价"]
                            except KeyError:
                                found_total_price = False
                                break
                            if reg_price == -1:
                                found_total_price = False
                                break
                            else:
                                total_price += reg_price * reg_count
                        if found_total_price:
                            unit_price = total_price / row["product_count"]
                            mapping[id]["单价"] = unit_price
                            counter += 1
            return counter

        # 循环十次, 直到没有新物品既可
        for ith in range(1, 1 + 10):
            logger.info(f"--- {ith} iteration ---")
            counter = resolve_price()
            logger.info(f"resolved {counter} items")
            if counter == 0:
                break

        # 我们不需要 reagents 列了
        for id, row in mapping.items():
            del row["reagents"]

        # 重新排列一下 column 准备输出
        df = pl.DataFrame(list(mapping.values()), infer_schema_length=99999)
        df = df.select(
            [
                "编号",
                "名字",
                "单价",
                "增值",
                "可买",
            ]
        )
        df = df.filter(pl.col("单价") != -1)
        df = df.with_columns((pl.col("单价") * 10000).cast(int))

        logger.info(f"Dump data to {self.final_price_table_tsv} ...")
        df.write_csv(str(self.final_price_table_tsv), separator="\t")
        logger.info(f"Done, see: file://{self.final_price_table_tsv}")
