# -*- coding: utf-8 -*-

"""
该模块使用 Sqlalchemy 枚举了所有 azerothcore 中的数据库和数据表对象. 由于里面的表众多,
我们不可能一一的根据表名, 列名, 列类型来定义, 所以我们用了 metadata.reflect 的方式来自动
获得所有的表的 Metadata, 并将其缓存到磁盘上, 以便下次使用.

所有的数据库 App 都要使用这个模块来构造 SQL query.
"""

import pickle

import dataclasses
import sqlalchemy as sa

from .paths import (
    path_metadata_cache,
)
from .compat import cached_property


@dataclasses.dataclass
class Orm:
    """
    一个可以访问所有的数据表对象 ``sqlalchemy.Table`` 的 namespace 类.
    """

    engine: sa.engine.Engine

    _metadata: sa.MetaData = dataclasses.field(init=False, repr=False)

    def _reflect(self) -> sa.MetaData:
        metadata = sa.MetaData()
        # 注: 只有指定 schema 才能用一个 metadata 来管理多个数据库 (在 MySQL 中是
        # database, 但在数据库学术领域叫 schema, 例如 Postgres 中就是 schema)
        metadata.reflect(self.engine, schema="acore_auth")
        metadata.reflect(self.engine, schema="acore_characters")
        metadata.reflect(self.engine, schema="acore_world")
        with path_metadata_cache.open("wb") as f:
            pickle.dump(metadata, f)
        return metadata

    def __post_init__(self):
        # 如果缓存存在则从缓存读取 metadata, 否则从数据库中 reflect 出来, 再写入缓存
        if path_metadata_cache.exists():
            try:
                with path_metadata_cache.open("rb") as f:
                    self._metadata = pickle.load(f)
            except pickle.UnpicklingError:
                self._metadata = self._reflect()
        else:
            self._metadata = self._reflect()

    @cached_property
    def t_account(self) -> sa.Table:
        return self._metadata.tables["acore_auth.account"]

    @cached_property
    def t_account_access(self) -> sa.Table:
        return self._metadata.tables["acore_auth.account_access"]

    @cached_property
    def t_account_banned(self) -> sa.Table:
        return self._metadata.tables["acore_auth.account_banned"]

    @cached_property
    def t_account_muted(self) -> sa.Table:
        return self._metadata.tables["acore_auth.account_muted"]

    @cached_property
    def t_autobroadcast(self) -> sa.Table:
        return self._metadata.tables["acore_auth.autobroadcast"]

    @cached_property
    def t_build_info(self) -> sa.Table:
        return self._metadata.tables["acore_auth.build_info"]

    @cached_property
    def t_ip_banned(self) -> sa.Table:
        return self._metadata.tables["acore_auth.ip_banned"]

    @cached_property
    def t_logs(self) -> sa.Table:
        return self._metadata.tables["acore_auth.logs"]

    @cached_property
    def t_logs_ip_actions(self) -> sa.Table:
        return self._metadata.tables["acore_auth.logs_ip_actions"]

    @cached_property
    def t_motd(self) -> sa.Table:
        return self._metadata.tables["acore_auth.motd"]

    @cached_property
    def t_realmcharacters(self) -> sa.Table:
        return self._metadata.tables["acore_auth.realmcharacters"]

    @cached_property
    def t_realmlist(self) -> sa.Table:
        return self._metadata.tables["acore_auth.realmlist"]

    @cached_property
    def t_secret_digest(self) -> sa.Table:
        return self._metadata.tables["acore_auth.secret_digest"]

    @cached_property
    def t_updates(self) -> sa.Table:
        return self._metadata.tables["acore_auth.updates"]

    @cached_property
    def t_updates_include(self) -> sa.Table:
        return self._metadata.tables["acore_auth.updates_include"]

    @cached_property
    def t_uptime(self) -> sa.Table:
        return self._metadata.tables["acore_auth.uptime"]

    @cached_property
    def t_account_data(self) -> sa.Table:
        return self._metadata.tables["acore_characters.account_data"]

    @cached_property
    def t_account_instance_times(self) -> sa.Table:
        return self._metadata.tables["acore_characters.account_instance_times"]

    @cached_property
    def t_account_tutorial(self) -> sa.Table:
        return self._metadata.tables["acore_characters.account_tutorial"]

    @cached_property
    def t_addons(self) -> sa.Table:
        return self._metadata.tables["acore_characters.addons"]

    @cached_property
    def t_arena_team(self) -> sa.Table:
        return self._metadata.tables["acore_characters.arena_team"]

    @cached_property
    def t_arena_team_member(self) -> sa.Table:
        return self._metadata.tables["acore_characters.arena_team_member"]

    @cached_property
    def t_auctionhouse(self) -> sa.Table:
        return self._metadata.tables["acore_characters.auctionhouse"]

    @cached_property
    def t_banned_addons(self) -> sa.Table:
        return self._metadata.tables["acore_characters.banned_addons"]

    @cached_property
    def t_battleground_deserters(self) -> sa.Table:
        return self._metadata.tables["acore_characters.battleground_deserters"]

    @cached_property
    def t_bugreport(self) -> sa.Table:
        return self._metadata.tables["acore_characters.bugreport"]

    @cached_property
    def t_calendar_events(self) -> sa.Table:
        return self._metadata.tables["acore_characters.calendar_events"]

    @cached_property
    def t_calendar_invites(self) -> sa.Table:
        return self._metadata.tables["acore_characters.calendar_invites"]

    @cached_property
    def t_channels(self) -> sa.Table:
        return self._metadata.tables["acore_characters.channels"]

    @cached_property
    def t_channels_bans(self) -> sa.Table:
        return self._metadata.tables["acore_characters.channels_bans"]

    @cached_property
    def t_channels_rights(self) -> sa.Table:
        return self._metadata.tables["acore_characters.channels_rights"]

    @cached_property
    def t_character_account_data(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_account_data"]

    @cached_property
    def t_character_achievement(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_achievement"]

    @cached_property
    def t_character_achievement_progress(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_achievement_progress"]

    @cached_property
    def t_character_action(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_action"]

    @cached_property
    def t_character_arena_stats(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_arena_stats"]

    @cached_property
    def t_character_aura(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_aura"]

    @cached_property
    def t_character_banned(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_banned"]

    @cached_property
    def t_character_battleground_random(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_battleground_random"]

    @cached_property
    def t_character_brew_of_the_month(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_brew_of_the_month"]

    @cached_property
    def t_character_declinedname(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_declinedname"]

    @cached_property
    def t_character_entry_point(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_entry_point"]

    @cached_property
    def t_character_equipmentsets(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_equipmentsets"]

    @cached_property
    def t_character_gifts(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_gifts"]

    @cached_property
    def t_character_glyphs(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_glyphs"]

    @cached_property
    def t_character_homebind(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_homebind"]

    @cached_property
    def t_character_instance(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_instance"]

    @cached_property
    def t_character_inventory(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_inventory"]

    @cached_property
    def t_character_pet(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_pet"]

    @cached_property
    def t_character_pet_declinedname(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_pet_declinedname"]

    @cached_property
    def t_character_queststatus(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_queststatus"]

    @cached_property
    def t_character_queststatus_daily(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_queststatus_daily"]

    @cached_property
    def t_character_queststatus_monthly(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_queststatus_monthly"]

    @cached_property
    def t_character_queststatus_rewarded(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_queststatus_rewarded"]

    @cached_property
    def t_character_queststatus_seasonal(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_queststatus_seasonal"]

    @cached_property
    def t_character_queststatus_weekly(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_queststatus_weekly"]

    @cached_property
    def t_character_reputation(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_reputation"]

    @cached_property
    def t_character_settings(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_settings"]

    @cached_property
    def t_character_skills(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_skills"]

    @cached_property
    def t_character_social(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_social"]

    @cached_property
    def t_character_spell(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_spell"]

    @cached_property
    def t_character_spell_cooldown(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_spell_cooldown"]

    @cached_property
    def t_character_stats(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_stats"]

    @cached_property
    def t_character_talent(self) -> sa.Table:
        return self._metadata.tables["acore_characters.character_talent"]

    @cached_property
    def t_characters(self) -> sa.Table:
        return self._metadata.tables["acore_characters.characters"]

    @cached_property
    def t_corpse(self) -> sa.Table:
        return self._metadata.tables["acore_characters.corpse"]

    @cached_property
    def t_creature_respawn(self) -> sa.Table:
        return self._metadata.tables["acore_characters.creature_respawn"]

    @cached_property
    def t_game_event_condition_save(self) -> sa.Table:
        return self._metadata.tables["acore_characters.game_event_condition_save"]

    @cached_property
    def t_game_event_save(self) -> sa.Table:
        return self._metadata.tables["acore_characters.game_event_save"]

    @cached_property
    def t_gameobject_respawn(self) -> sa.Table:
        return self._metadata.tables["acore_characters.gameobject_respawn"]

    @cached_property
    def t_gm_subsurvey(self) -> sa.Table:
        return self._metadata.tables["acore_characters.gm_subsurvey"]

    @cached_property
    def t_gm_survey(self) -> sa.Table:
        return self._metadata.tables["acore_characters.gm_survey"]

    @cached_property
    def t_gm_ticket(self) -> sa.Table:
        return self._metadata.tables["acore_characters.gm_ticket"]

    @cached_property
    def t_group_member(self) -> sa.Table:
        return self._metadata.tables["acore_characters.group_member"]

    @cached_property
    def t_groups(self) -> sa.Table:
        return self._metadata.tables["acore_characters.groups"]

    @cached_property
    def t_guild(self) -> sa.Table:
        return self._metadata.tables["acore_characters.guild"]

    @cached_property
    def t_guild_bank_eventlog(self) -> sa.Table:
        return self._metadata.tables["acore_characters.guild_bank_eventlog"]

    @cached_property
    def t_guild_bank_item(self) -> sa.Table:
        return self._metadata.tables["acore_characters.guild_bank_item"]

    @cached_property
    def t_guild_bank_right(self) -> sa.Table:
        return self._metadata.tables["acore_characters.guild_bank_right"]

    @cached_property
    def t_guild_bank_tab(self) -> sa.Table:
        return self._metadata.tables["acore_characters.guild_bank_tab"]

    @cached_property
    def t_guild_eventlog(self) -> sa.Table:
        return self._metadata.tables["acore_characters.guild_eventlog"]

    @cached_property
    def t_guild_member(self) -> sa.Table:
        return self._metadata.tables["acore_characters.guild_member"]

    @cached_property
    def t_guild_member_withdraw(self) -> sa.Table:
        return self._metadata.tables["acore_characters.guild_member_withdraw"]

    @cached_property
    def t_guild_rank(self) -> sa.Table:
        return self._metadata.tables["acore_characters.guild_rank"]

    @cached_property
    def t_instance(self) -> sa.Table:
        return self._metadata.tables["acore_characters.instance"]

    @cached_property
    def t_instance_reset(self) -> sa.Table:
        return self._metadata.tables["acore_characters.instance_reset"]

    @cached_property
    def t_instance_saved_go_state_data(self) -> sa.Table:
        return self._metadata.tables["acore_characters.instance_saved_go_state_data"]

    @cached_property
    def t_item_instance(self) -> sa.Table:
        return self._metadata.tables["acore_characters.item_instance"]

    @cached_property
    def t_item_loot_storage(self) -> sa.Table:
        return self._metadata.tables["acore_characters.item_loot_storage"]

    @cached_property
    def t_item_refund_instance(self) -> sa.Table:
        return self._metadata.tables["acore_characters.item_refund_instance"]

    @cached_property
    def t_item_soulbound_trade_data(self) -> sa.Table:
        return self._metadata.tables["acore_characters.item_soulbound_trade_data"]

    @cached_property
    def t_lag_reports(self) -> sa.Table:
        return self._metadata.tables["acore_characters.lag_reports"]

    @cached_property
    def t_lfg_data(self) -> sa.Table:
        return self._metadata.tables["acore_characters.lfg_data"]

    @cached_property
    def t_log_arena_fights(self) -> sa.Table:
        return self._metadata.tables["acore_characters.log_arena_fights"]

    @cached_property
    def t_log_arena_memberstats(self) -> sa.Table:
        return self._metadata.tables["acore_characters.log_arena_memberstats"]

    @cached_property
    def t_log_encounter(self) -> sa.Table:
        return self._metadata.tables["acore_characters.log_encounter"]

    @cached_property
    def t_log_money(self) -> sa.Table:
        return self._metadata.tables["acore_characters.log_money"]

    @cached_property
    def t_mail(self) -> sa.Table:
        return self._metadata.tables["acore_characters.mail"]

    @cached_property
    def t_mail_items(self) -> sa.Table:
        return self._metadata.tables["acore_characters.mail_items"]

    @cached_property
    def t_mail_server_character(self) -> sa.Table:
        return self._metadata.tables["acore_characters.mail_server_character"]

    @cached_property
    def t_mail_server_template(self) -> sa.Table:
        return self._metadata.tables["acore_characters.mail_server_template"]

    @cached_property
    def t_pet_aura(self) -> sa.Table:
        return self._metadata.tables["acore_characters.pet_aura"]

    @cached_property
    def t_pet_spell(self) -> sa.Table:
        return self._metadata.tables["acore_characters.pet_spell"]

    @cached_property
    def t_pet_spell_cooldown(self) -> sa.Table:
        return self._metadata.tables["acore_characters.pet_spell_cooldown"]

    @cached_property
    def t_petition(self) -> sa.Table:
        return self._metadata.tables["acore_characters.petition"]

    @cached_property
    def t_petition_sign(self) -> sa.Table:
        return self._metadata.tables["acore_characters.petition_sign"]

    @cached_property
    def t_pool_quest_save(self) -> sa.Table:
        return self._metadata.tables["acore_characters.pool_quest_save"]

    @cached_property
    def t_profanity_name(self) -> sa.Table:
        return self._metadata.tables["acore_characters.profanity_name"]

    @cached_property
    def t_pvpstats_battlegrounds(self) -> sa.Table:
        return self._metadata.tables["acore_characters.pvpstats_battlegrounds"]

    @cached_property
    def t_pvpstats_players(self) -> sa.Table:
        return self._metadata.tables["acore_characters.pvpstats_players"]

    @cached_property
    def t_quest_tracker(self) -> sa.Table:
        return self._metadata.tables["acore_characters.quest_tracker"]

    @cached_property
    def t_recovery_item(self) -> sa.Table:
        return self._metadata.tables["acore_characters.recovery_item"]

    @cached_property
    def t_reserved_name(self) -> sa.Table:
        return self._metadata.tables["acore_characters.reserved_name"]

    @cached_property
    def t_updates(self) -> sa.Table:
        return self._metadata.tables["acore_characters.updates"]

    @cached_property
    def t_updates_include(self) -> sa.Table:
        return self._metadata.tables["acore_characters.updates_include"]

    @cached_property
    def t_warden_action(self) -> sa.Table:
        return self._metadata.tables["acore_characters.warden_action"]

    @cached_property
    def t_worldstates(self) -> sa.Table:
        return self._metadata.tables["acore_characters.worldstates"]

    @cached_property
    def t_achievement_category_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.achievement_category_dbc"]

    @cached_property
    def t_achievement_criteria_data(self) -> sa.Table:
        return self._metadata.tables["acore_world.achievement_criteria_data"]

    @cached_property
    def t_achievement_criteria_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.achievement_criteria_dbc"]

    @cached_property
    def t_achievement_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.achievement_dbc"]

    @cached_property
    def t_achievement_reward(self) -> sa.Table:
        return self._metadata.tables["acore_world.achievement_reward"]

    @cached_property
    def t_achievement_reward_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.achievement_reward_locale"]

    @cached_property
    def t_acore_string(self) -> sa.Table:
        return self._metadata.tables["acore_world.acore_string"]

    @cached_property
    def t_areagroup_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.areagroup_dbc"]

    @cached_property
    def t_areapoi_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.areapoi_dbc"]

    @cached_property
    def t_areatable_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.areatable_dbc"]

    @cached_property
    def t_areatrigger(self) -> sa.Table:
        return self._metadata.tables["acore_world.areatrigger"]

    @cached_property
    def t_areatrigger_involvedrelation(self) -> sa.Table:
        return self._metadata.tables["acore_world.areatrigger_involvedrelation"]

    @cached_property
    def t_areatrigger_scripts(self) -> sa.Table:
        return self._metadata.tables["acore_world.areatrigger_scripts"]

    @cached_property
    def t_areatrigger_tavern(self) -> sa.Table:
        return self._metadata.tables["acore_world.areatrigger_tavern"]

    @cached_property
    def t_areatrigger_teleport(self) -> sa.Table:
        return self._metadata.tables["acore_world.areatrigger_teleport"]

    @cached_property
    def t_auctionhouse_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.auctionhouse_dbc"]

    @cached_property
    def t_bankbagslotprices_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.bankbagslotprices_dbc"]

    @cached_property
    def t_barbershopstyle_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.barbershopstyle_dbc"]

    @cached_property
    def t_battleground_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.battleground_template"]

    @cached_property
    def t_battlemaster_entry(self) -> sa.Table:
        return self._metadata.tables["acore_world.battlemaster_entry"]

    @cached_property
    def t_battlemasterlist_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.battlemasterlist_dbc"]

    @cached_property
    def t_broadcast_text(self) -> sa.Table:
        return self._metadata.tables["acore_world.broadcast_text"]

    @cached_property
    def t_broadcast_text_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.broadcast_text_locale"]

    @cached_property
    def t_charstartoutfit_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.charstartoutfit_dbc"]

    @cached_property
    def t_chartitles_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.chartitles_dbc"]

    @cached_property
    def t_chatchannels_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.chatchannels_dbc"]

    @cached_property
    def t_chrclasses_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.chrclasses_dbc"]

    @cached_property
    def t_chrraces_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.chrraces_dbc"]

    @cached_property
    def t_cinematiccamera_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.cinematiccamera_dbc"]

    @cached_property
    def t_cinematicsequences_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.cinematicsequences_dbc"]

    @cached_property
    def t_command(self) -> sa.Table:
        return self._metadata.tables["acore_world.command"]

    @cached_property
    def t_conditions(self) -> sa.Table:
        return self._metadata.tables["acore_world.conditions"]

    @cached_property
    def t_creature(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature"]

    @cached_property
    def t_creature_addon(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_addon"]

    @cached_property
    def t_creature_classlevelstats(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_classlevelstats"]

    @cached_property
    def t_creature_equip_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_equip_template"]

    @cached_property
    def t_creature_formations(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_formations"]

    @cached_property
    def t_creature_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_loot_template"]

    @cached_property
    def t_creature_model_info(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_model_info"]

    @cached_property
    def t_creature_movement_override(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_movement_override"]

    @cached_property
    def t_creature_onkill_reputation(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_onkill_reputation"]

    @cached_property
    def t_creature_questender(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_questender"]

    @cached_property
    def t_creature_questitem(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_questitem"]

    @cached_property
    def t_creature_queststarter(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_queststarter"]

    @cached_property
    def t_creature_summon_groups(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_summon_groups"]

    @cached_property
    def t_creature_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_template"]

    @cached_property
    def t_creature_template_addon(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_template_addon"]

    @cached_property
    def t_creature_template_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_template_locale"]

    @cached_property
    def t_creature_template_movement(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_template_movement"]

    @cached_property
    def t_creature_template_resistance(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_template_resistance"]

    @cached_property
    def t_creature_template_spell(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_template_spell"]

    @cached_property
    def t_creature_text(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_text"]

    @cached_property
    def t_creature_text_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.creature_text_locale"]

    @cached_property
    def t_creaturedisplayinfo_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.creaturedisplayinfo_dbc"]

    @cached_property
    def t_creaturedisplayinfoextra_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.creaturedisplayinfoextra_dbc"]

    @cached_property
    def t_creaturefamily_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.creaturefamily_dbc"]

    @cached_property
    def t_creaturemodeldata_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.creaturemodeldata_dbc"]

    @cached_property
    def t_creaturespelldata_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.creaturespelldata_dbc"]

    @cached_property
    def t_creaturetype_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.creaturetype_dbc"]

    @cached_property
    def t_currencytypes_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.currencytypes_dbc"]

    @cached_property
    def t_destructiblemodeldata_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.destructiblemodeldata_dbc"]

    @cached_property
    def t_disables(self) -> sa.Table:
        return self._metadata.tables["acore_world.disables"]

    @cached_property
    def t_disenchant_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.disenchant_loot_template"]

    @cached_property
    def t_dungeon_access_requirements(self) -> sa.Table:
        return self._metadata.tables["acore_world.dungeon_access_requirements"]

    @cached_property
    def t_dungeon_access_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.dungeon_access_template"]

    @cached_property
    def t_dungeonencounter_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.dungeonencounter_dbc"]

    @cached_property
    def t_durabilitycosts_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.durabilitycosts_dbc"]

    @cached_property
    def t_durabilityquality_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.durabilityquality_dbc"]

    @cached_property
    def t_emotes_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.emotes_dbc"]

    @cached_property
    def t_emotestext_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.emotestext_dbc"]

    @cached_property
    def t_event_scripts(self) -> sa.Table:
        return self._metadata.tables["acore_world.event_scripts"]

    @cached_property
    def t_exploration_basexp(self) -> sa.Table:
        return self._metadata.tables["acore_world.exploration_basexp"]

    @cached_property
    def t_faction_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.faction_dbc"]

    @cached_property
    def t_factiontemplate_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.factiontemplate_dbc"]

    @cached_property
    def t_fishing_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.fishing_loot_template"]

    @cached_property
    def t_game_event(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event"]

    @cached_property
    def t_game_event_arena_seasons(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_arena_seasons"]

    @cached_property
    def t_game_event_battleground_holiday(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_battleground_holiday"]

    @cached_property
    def t_game_event_condition(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_condition"]

    @cached_property
    def t_game_event_creature(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_creature"]

    @cached_property
    def t_game_event_creature_quest(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_creature_quest"]

    @cached_property
    def t_game_event_gameobject(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_gameobject"]

    @cached_property
    def t_game_event_gameobject_quest(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_gameobject_quest"]

    @cached_property
    def t_game_event_model_equip(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_model_equip"]

    @cached_property
    def t_game_event_npc_vendor(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_npc_vendor"]

    @cached_property
    def t_game_event_npcflag(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_npcflag"]

    @cached_property
    def t_game_event_pool(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_pool"]

    @cached_property
    def t_game_event_prerequisite(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_prerequisite"]

    @cached_property
    def t_game_event_quest_condition(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_quest_condition"]

    @cached_property
    def t_game_event_seasonal_questrelation(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_event_seasonal_questrelation"]

    @cached_property
    def t_game_graveyard(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_graveyard"]

    @cached_property
    def t_game_tele(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_tele"]

    @cached_property
    def t_game_weather(self) -> sa.Table:
        return self._metadata.tables["acore_world.game_weather"]

    @cached_property
    def t_gameobject(self) -> sa.Table:
        return self._metadata.tables["acore_world.gameobject"]

    @cached_property
    def t_gameobject_addon(self) -> sa.Table:
        return self._metadata.tables["acore_world.gameobject_addon"]

    @cached_property
    def t_gameobject_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.gameobject_loot_template"]

    @cached_property
    def t_gameobject_questender(self) -> sa.Table:
        return self._metadata.tables["acore_world.gameobject_questender"]

    @cached_property
    def t_gameobject_questitem(self) -> sa.Table:
        return self._metadata.tables["acore_world.gameobject_questitem"]

    @cached_property
    def t_gameobject_queststarter(self) -> sa.Table:
        return self._metadata.tables["acore_world.gameobject_queststarter"]

    @cached_property
    def t_gameobject_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.gameobject_template"]

    @cached_property
    def t_gameobject_template_addon(self) -> sa.Table:
        return self._metadata.tables["acore_world.gameobject_template_addon"]

    @cached_property
    def t_gameobject_template_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.gameobject_template_locale"]

    @cached_property
    def t_gameobjectartkit_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gameobjectartkit_dbc"]

    @cached_property
    def t_gameobjectdisplayinfo_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gameobjectdisplayinfo_dbc"]

    @cached_property
    def t_gemproperties_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gemproperties_dbc"]

    @cached_property
    def t_glyphproperties_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.glyphproperties_dbc"]

    @cached_property
    def t_glyphslot_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.glyphslot_dbc"]

    @cached_property
    def t_gossip_menu(self) -> sa.Table:
        return self._metadata.tables["acore_world.gossip_menu"]

    @cached_property
    def t_gossip_menu_option(self) -> sa.Table:
        return self._metadata.tables["acore_world.gossip_menu_option"]

    @cached_property
    def t_gossip_menu_option_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.gossip_menu_option_locale"]

    @cached_property
    def t_graveyard_zone(self) -> sa.Table:
        return self._metadata.tables["acore_world.graveyard_zone"]

    @cached_property
    def t_gtbarbershopcostbase_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gtbarbershopcostbase_dbc"]

    @cached_property
    def t_gtchancetomeleecrit_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gtchancetomeleecrit_dbc"]

    @cached_property
    def t_gtchancetomeleecritbase_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gtchancetomeleecritbase_dbc"]

    @cached_property
    def t_gtchancetospellcrit_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gtchancetospellcrit_dbc"]

    @cached_property
    def t_gtchancetospellcritbase_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gtchancetospellcritbase_dbc"]

    @cached_property
    def t_gtcombatratings_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gtcombatratings_dbc"]

    @cached_property
    def t_gtnpcmanacostscaler_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gtnpcmanacostscaler_dbc"]

    @cached_property
    def t_gtoctclasscombatratingscalar_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gtoctclasscombatratingscalar_dbc"]

    @cached_property
    def t_gtoctregenhp_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gtoctregenhp_dbc"]

    @cached_property
    def t_gtregenhpperspt_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gtregenhpperspt_dbc"]

    @cached_property
    def t_gtregenmpperspt_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.gtregenmpperspt_dbc"]

    @cached_property
    def t_holiday_dates(self) -> sa.Table:
        return self._metadata.tables["acore_world.holiday_dates"]

    @cached_property
    def t_holidays_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.holidays_dbc"]

    @cached_property
    def t_instance_encounters(self) -> sa.Table:
        return self._metadata.tables["acore_world.instance_encounters"]

    @cached_property
    def t_instance_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.instance_template"]

    @cached_property
    def t_item_enchantment_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.item_enchantment_template"]

    @cached_property
    def t_item_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.item_loot_template"]

    @cached_property
    def t_item_set_names(self) -> sa.Table:
        return self._metadata.tables["acore_world.item_set_names"]

    @cached_property
    def t_item_set_names_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.item_set_names_locale"]

    @cached_property
    def t_item_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.item_template"]

    @cached_property
    def t_item_template_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.item_template_locale"]

    @cached_property
    def t_itembagfamily_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.itembagfamily_dbc"]

    @cached_property
    def t_itemdisplayinfo_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.itemdisplayinfo_dbc"]

    @cached_property
    def t_itemextendedcost_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.itemextendedcost_dbc"]

    @cached_property
    def t_itemlimitcategory_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.itemlimitcategory_dbc"]

    @cached_property
    def t_itemrandomproperties_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.itemrandomproperties_dbc"]

    @cached_property
    def t_itemrandomsuffix_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.itemrandomsuffix_dbc"]

    @cached_property
    def t_itemset_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.itemset_dbc"]

    @cached_property
    def t_lfg_dungeon_rewards(self) -> sa.Table:
        return self._metadata.tables["acore_world.lfg_dungeon_rewards"]

    @cached_property
    def t_lfg_dungeon_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.lfg_dungeon_template"]

    @cached_property
    def t_lfgdungeons_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.lfgdungeons_dbc"]

    @cached_property
    def t_light_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.light_dbc"]

    @cached_property
    def t_linked_respawn(self) -> sa.Table:
        return self._metadata.tables["acore_world.linked_respawn"]

    @cached_property
    def t_liquidtype_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.liquidtype_dbc"]

    @cached_property
    def t_lock_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.lock_dbc"]

    @cached_property
    def t_mail_level_reward(self) -> sa.Table:
        return self._metadata.tables["acore_world.mail_level_reward"]

    @cached_property
    def t_mail_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.mail_loot_template"]

    @cached_property
    def t_mailtemplate_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.mailtemplate_dbc"]

    @cached_property
    def t_map_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.map_dbc"]

    @cached_property
    def t_mapdifficulty_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.mapdifficulty_dbc"]

    @cached_property
    def t_milling_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.milling_loot_template"]

    @cached_property
    def t_movie_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.movie_dbc"]

    @cached_property
    def t_namesprofanity_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.namesprofanity_dbc"]

    @cached_property
    def t_namesreserved_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.namesreserved_dbc"]

    @cached_property
    def t_npc_spellclick_spells(self) -> sa.Table:
        return self._metadata.tables["acore_world.npc_spellclick_spells"]

    @cached_property
    def t_npc_text(self) -> sa.Table:
        return self._metadata.tables["acore_world.npc_text"]

    @cached_property
    def t_npc_text_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.npc_text_locale"]

    @cached_property
    def t_npc_trainer(self) -> sa.Table:
        return self._metadata.tables["acore_world.npc_trainer"]

    @cached_property
    def t_npc_vendor(self) -> sa.Table:
        return self._metadata.tables["acore_world.npc_vendor"]

    @cached_property
    def t_outdoorpvp_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.outdoorpvp_template"]

    @cached_property
    def t_overridespelldata_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.overridespelldata_dbc"]

    @cached_property
    def t_page_text(self) -> sa.Table:
        return self._metadata.tables["acore_world.page_text"]

    @cached_property
    def t_page_text_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.page_text_locale"]

    @cached_property
    def t_pet_levelstats(self) -> sa.Table:
        return self._metadata.tables["acore_world.pet_levelstats"]

    @cached_property
    def t_pet_name_generation(self) -> sa.Table:
        return self._metadata.tables["acore_world.pet_name_generation"]

    @cached_property
    def t_pet_name_generation_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.pet_name_generation_locale"]

    @cached_property
    def t_pickpocketing_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.pickpocketing_loot_template"]

    @cached_property
    def t_player_class_stats(self) -> sa.Table:
        return self._metadata.tables["acore_world.player_class_stats"]

    @cached_property
    def t_player_classlevelstats(self) -> sa.Table:
        return self._metadata.tables["acore_world.player_classlevelstats"]

    @cached_property
    def t_player_factionchange_achievement(self) -> sa.Table:
        return self._metadata.tables["acore_world.player_factionchange_achievement"]

    @cached_property
    def t_player_factionchange_items(self) -> sa.Table:
        return self._metadata.tables["acore_world.player_factionchange_items"]

    @cached_property
    def t_player_factionchange_quests(self) -> sa.Table:
        return self._metadata.tables["acore_world.player_factionchange_quests"]

    @cached_property
    def t_player_factionchange_reputations(self) -> sa.Table:
        return self._metadata.tables["acore_world.player_factionchange_reputations"]

    @cached_property
    def t_player_factionchange_spells(self) -> sa.Table:
        return self._metadata.tables["acore_world.player_factionchange_spells"]

    @cached_property
    def t_player_factionchange_titles(self) -> sa.Table:
        return self._metadata.tables["acore_world.player_factionchange_titles"]

    @cached_property
    def t_player_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.player_loot_template"]

    @cached_property
    def t_player_race_stats(self) -> sa.Table:
        return self._metadata.tables["acore_world.player_race_stats"]

    @cached_property
    def t_player_xp_for_level(self) -> sa.Table:
        return self._metadata.tables["acore_world.player_xp_for_level"]

    @cached_property
    def t_playercreateinfo(self) -> sa.Table:
        return self._metadata.tables["acore_world.playercreateinfo"]

    @cached_property
    def t_playercreateinfo_action(self) -> sa.Table:
        return self._metadata.tables["acore_world.playercreateinfo_action"]

    @cached_property
    def t_playercreateinfo_cast_spell(self) -> sa.Table:
        return self._metadata.tables["acore_world.playercreateinfo_cast_spell"]

    @cached_property
    def t_playercreateinfo_item(self) -> sa.Table:
        return self._metadata.tables["acore_world.playercreateinfo_item"]

    @cached_property
    def t_playercreateinfo_skills(self) -> sa.Table:
        return self._metadata.tables["acore_world.playercreateinfo_skills"]

    @cached_property
    def t_playercreateinfo_spell_custom(self) -> sa.Table:
        return self._metadata.tables["acore_world.playercreateinfo_spell_custom"]

    @cached_property
    def t_points_of_interest(self) -> sa.Table:
        return self._metadata.tables["acore_world.points_of_interest"]

    @cached_property
    def t_points_of_interest_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.points_of_interest_locale"]

    @cached_property
    def t_pool_creature(self) -> sa.Table:
        return self._metadata.tables["acore_world.pool_creature"]

    @cached_property
    def t_pool_gameobject(self) -> sa.Table:
        return self._metadata.tables["acore_world.pool_gameobject"]

    @cached_property
    def t_pool_pool(self) -> sa.Table:
        return self._metadata.tables["acore_world.pool_pool"]

    @cached_property
    def t_pool_quest(self) -> sa.Table:
        return self._metadata.tables["acore_world.pool_quest"]

    @cached_property
    def t_pool_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.pool_template"]

    @cached_property
    def t_powerdisplay_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.powerdisplay_dbc"]

    @cached_property
    def t_prospecting_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.prospecting_loot_template"]

    @cached_property
    def t_pvpdifficulty_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.pvpdifficulty_dbc"]

    @cached_property
    def t_quest_details(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_details"]

    @cached_property
    def t_quest_greeting(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_greeting"]

    @cached_property
    def t_quest_greeting_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_greeting_locale"]

    @cached_property
    def t_quest_mail_sender(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_mail_sender"]

    @cached_property
    def t_quest_money_reward(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_money_reward"]

    @cached_property
    def t_quest_offer_reward(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_offer_reward"]

    @cached_property
    def t_quest_offer_reward_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_offer_reward_locale"]

    @cached_property
    def t_quest_poi(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_poi"]

    @cached_property
    def t_quest_poi_points(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_poi_points"]

    @cached_property
    def t_quest_request_items(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_request_items"]

    @cached_property
    def t_quest_request_items_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_request_items_locale"]

    @cached_property
    def t_quest_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_template"]

    @cached_property
    def t_quest_template_addon(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_template_addon"]

    @cached_property
    def t_quest_template_locale(self) -> sa.Table:
        return self._metadata.tables["acore_world.quest_template_locale"]

    @cached_property
    def t_questfactionreward_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.questfactionreward_dbc"]

    @cached_property
    def t_questsort_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.questsort_dbc"]

    @cached_property
    def t_questxp_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.questxp_dbc"]

    @cached_property
    def t_randproppoints_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.randproppoints_dbc"]

    @cached_property
    def t_reference_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.reference_loot_template"]

    @cached_property
    def t_reputation_reward_rate(self) -> sa.Table:
        return self._metadata.tables["acore_world.reputation_reward_rate"]

    @cached_property
    def t_reputation_spillover_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.reputation_spillover_template"]

    @cached_property
    def t_scalingstatdistribution_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.scalingstatdistribution_dbc"]

    @cached_property
    def t_scalingstatvalues_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.scalingstatvalues_dbc"]

    @cached_property
    def t_script_waypoint(self) -> sa.Table:
        return self._metadata.tables["acore_world.script_waypoint"]

    @cached_property
    def t_skill_discovery_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.skill_discovery_template"]

    @cached_property
    def t_skill_extra_item_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.skill_extra_item_template"]

    @cached_property
    def t_skill_fishing_base_level(self) -> sa.Table:
        return self._metadata.tables["acore_world.skill_fishing_base_level"]

    @cached_property
    def t_skill_perfect_item_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.skill_perfect_item_template"]

    @cached_property
    def t_skillline_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.skillline_dbc"]

    @cached_property
    def t_skilllineability_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.skilllineability_dbc"]

    @cached_property
    def t_skillraceclassinfo_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.skillraceclassinfo_dbc"]

    @cached_property
    def t_skilltiers_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.skilltiers_dbc"]

    @cached_property
    def t_skinning_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.skinning_loot_template"]

    @cached_property
    def t_smart_scripts(self) -> sa.Table:
        return self._metadata.tables["acore_world.smart_scripts"]

    @cached_property
    def t_soundentries_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.soundentries_dbc"]

    @cached_property
    def t_spell_area(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_area"]

    @cached_property
    def t_spell_bonus_data(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_bonus_data"]

    @cached_property
    def t_spell_cooldown_overrides(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_cooldown_overrides"]

    @cached_property
    def t_spell_custom_attr(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_custom_attr"]

    @cached_property
    def t_spell_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_dbc"]

    @cached_property
    def t_spell_enchant_proc_data(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_enchant_proc_data"]

    @cached_property
    def t_spell_group(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_group"]

    @cached_property
    def t_spell_group_stack_rules(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_group_stack_rules"]

    @cached_property
    def t_spell_linked_spell(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_linked_spell"]

    @cached_property
    def t_spell_loot_template(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_loot_template"]

    @cached_property
    def t_spell_mixology(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_mixology"]

    @cached_property
    def t_spell_pet_auras(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_pet_auras"]

    @cached_property
    def t_spell_proc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_proc"]

    @cached_property
    def t_spell_proc_event(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_proc_event"]

    @cached_property
    def t_spell_ranks(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_ranks"]

    @cached_property
    def t_spell_required(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_required"]

    @cached_property
    def t_spell_script_names(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_script_names"]

    @cached_property
    def t_spell_scripts(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_scripts"]

    @cached_property
    def t_spell_target_position(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_target_position"]

    @cached_property
    def t_spell_threat(self) -> sa.Table:
        return self._metadata.tables["acore_world.spell_threat"]

    @cached_property
    def t_spellcasttimes_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spellcasttimes_dbc"]

    @cached_property
    def t_spellcategory_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spellcategory_dbc"]

    @cached_property
    def t_spelldifficulty_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spelldifficulty_dbc"]

    @cached_property
    def t_spellduration_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spellduration_dbc"]

    @cached_property
    def t_spellfocusobject_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spellfocusobject_dbc"]

    @cached_property
    def t_spellitemenchantment_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spellitemenchantment_dbc"]

    @cached_property
    def t_spellitemenchantmentcondition_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spellitemenchantmentcondition_dbc"]

    @cached_property
    def t_spellradius_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spellradius_dbc"]

    @cached_property
    def t_spellrange_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spellrange_dbc"]

    @cached_property
    def t_spellrunecost_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spellrunecost_dbc"]

    @cached_property
    def t_spellshapeshiftform_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spellshapeshiftform_dbc"]

    @cached_property
    def t_spellvisual_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.spellvisual_dbc"]

    @cached_property
    def t_stableslotprices_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.stableslotprices_dbc"]

    @cached_property
    def t_summonproperties_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.summonproperties_dbc"]

    @cached_property
    def t_talent_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.talent_dbc"]

    @cached_property
    def t_talenttab_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.talenttab_dbc"]

    @cached_property
    def t_taxinodes_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.taxinodes_dbc"]

    @cached_property
    def t_taxipath_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.taxipath_dbc"]

    @cached_property
    def t_taxipathnode_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.taxipathnode_dbc"]

    @cached_property
    def t_teamcontributionpoints_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.teamcontributionpoints_dbc"]

    @cached_property
    def t_totemcategory_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.totemcategory_dbc"]

    @cached_property
    def t_transportanimation_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.transportanimation_dbc"]

    @cached_property
    def t_transportrotation_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.transportrotation_dbc"]

    @cached_property
    def t_transports(self) -> sa.Table:
        return self._metadata.tables["acore_world.transports"]

    @cached_property
    def t_updates(self) -> sa.Table:
        return self._metadata.tables["acore_world.updates"]

    @cached_property
    def t_updates_include(self) -> sa.Table:
        return self._metadata.tables["acore_world.updates_include"]

    @cached_property
    def t_vehicle_accessory(self) -> sa.Table:
        return self._metadata.tables["acore_world.vehicle_accessory"]

    @cached_property
    def t_vehicle_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.vehicle_dbc"]

    @cached_property
    def t_vehicle_template_accessory(self) -> sa.Table:
        return self._metadata.tables["acore_world.vehicle_template_accessory"]

    @cached_property
    def t_vehicleseat_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.vehicleseat_dbc"]

    @cached_property
    def t_version(self) -> sa.Table:
        return self._metadata.tables["acore_world.version"]

    @cached_property
    def t_warden_checks(self) -> sa.Table:
        return self._metadata.tables["acore_world.warden_checks"]

    @cached_property
    def t_waypoint_data(self) -> sa.Table:
        return self._metadata.tables["acore_world.waypoint_data"]

    @cached_property
    def t_waypoint_scripts(self) -> sa.Table:
        return self._metadata.tables["acore_world.waypoint_scripts"]

    @cached_property
    def t_waypoints(self) -> sa.Table:
        return self._metadata.tables["acore_world.waypoints"]

    @cached_property
    def t_wmoareatable_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.wmoareatable_dbc"]

    @cached_property
    def t_worldmaparea_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.worldmaparea_dbc"]

    @cached_property
    def t_worldmapoverlay_dbc(self) -> sa.Table:
        return self._metadata.tables["acore_world.worldmapoverlay_dbc"]
