# -*- coding: utf-8 -*-


import json
import dataclasses

from ..app import api as app
from ..orm_getter import get_orm_from_ec2_inside


def get_latest_n_quest(
    character: str,
    locale: str = app.LocaleEnum.enUS.value,
    n: int = 3,
):
    filtered_enriched_quest_data_list = app.quest.get_latest_n_quest_enriched_quest_data(
        orm=get_orm_from_ec2_inside(),
        character=character,
        locale=app.LocaleEnum[locale],
        n=n,
    )
    print(
        json.dumps(
            [
                dataclasses.asdict(enriched_quest_data)
                for enriched_quest_data in filtered_enriched_quest_data_list
            ],
            ensure_ascii=False,
        )
    )
