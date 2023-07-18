# -*- coding: utf-8 -*-

from acore_db_app import api


def test():
    _ = api

    _ = api.Orm
    _ = api.get_orm_from_ec2_inside
    _ = api.get_orm_for_ssh_tunnel
    _ = api.get_orm_for_vpc

    _ = api.app
    _ = api.app.LocaleEnum
    _ = api.app.quest.CharacterQuestStatusEnum
    _ = api.app.quest.CharacterQuestStatus
    _ = api.app.quest.list_quest_by_character
    _ = api.app.quest.EnrichedQuestData
    _ = api.app.quest.get_enriched_quest_data
    _ = api.app.quest.get_latest_n_quest_enriched_quest_data
    _ = api.app.quest.complete_latest_n_quest

    _ = api.sdk
    _ = api.sdk.quest.get_latest_n_request


if __name__ == "__main__":
    from acore_db_app.tests import run_cov_test

    run_cov_test(__file__, "acore_db_app.api", preview=False)
