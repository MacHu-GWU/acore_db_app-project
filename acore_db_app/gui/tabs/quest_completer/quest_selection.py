# -*- coding: utf-8 -*-

import typing as T

from PySide6 import QtCore, QtWidgets, QtGui

from ....sdk.api import quest

if T.TYPE_CHECKING:
    from .main import QuestCompleterWidget


class QuestCompleterWidgetQuestSelection:
    def add_quest_selection(self):
        self._add_quest_selection_widgets()
        self._add_quest_selection_layout()

    def _add_quest_selection_widgets(self):
        self.quest_selection_item_list_header = QtWidgets.QLabel("Quests")
        self.quest_selection_item_list_header.setAlignment(QtCore.Qt.AlignCenter)

        self.quest_selection_item_list = QtWidgets.QListWidget()
        self.quest_selection_item_list.setSelectionMode(
            QtWidgets.QAbstractItemView.SelectionMode.SingleSelection
        )

        self.quest_selection_item_list.itemSelectionChanged.connect(
            self._quest_selection_item_selection_changed_event_handler
        )

    def _quest_selection_item_selection_changed_event_handler(self: "QuestCompleterWidget"):
        """
        当用户双击 items 中的某个 item 的时候会触发这个函数.
        """
        selected_items = self.quest_selection_item_list.selectedItems()
        item = selected_items[0]
        text = item.text()
        print(f"item list item selection changed: {text!r}")

        self.quest_action_item_list.clear()

        enriched_quest_data: quest.EnrichedQuestData = item.enriched_quest_data

        copy_text = f".quest complete {enriched_quest_data.quest_id}"
        item = QtWidgets.QListWidgetItem(f"double click to copy COMPLETE QUEST command: {copy_text}")
        item.copy_text = copy_text
        item.setTextAlignment(QtCore.Qt.AlignLeft)
        self.quest_action_item_list.addItem(item)

        copy_text = (
            f".go xyz "
            f"{enriched_quest_data.starter_position_x} "
            f"{enriched_quest_data.starter_position_y} "
            f"{enriched_quest_data.starter_position_z} "
            f"{enriched_quest_data.starter_map}"
        )
        item = QtWidgets.QListWidgetItem(f"double click to copy TELEPORT TO QUEST STARTER command: {copy_text}")
        item.copy_text = copy_text
        item.setTextAlignment(QtCore.Qt.AlignLeft)
        self.quest_action_item_list.addItem(item)

        copy_text = (
            f".go xyz "
            f"{enriched_quest_data.ender_position_x} "
            f"{enriched_quest_data.ender_position_y} "
            f"{enriched_quest_data.ender_position_z} "
            f"{enriched_quest_data.ender_map}"
        )
        item = QtWidgets.QListWidgetItem(f"double click to copy TELEPORT TO QUEST ENDER command: {copy_text}")
        item.copy_text = copy_text
        item.setTextAlignment(QtCore.Qt.AlignLeft)
        self.quest_action_item_list.addItem(item)

    def _add_quest_selection_layout(self):
        self.quest_selection_layout = QtWidgets.QVBoxLayout()
        self.quest_selection_layout.addWidget(self.quest_selection_item_list_header)
        self.quest_selection_layout.addWidget(self.quest_selection_item_list)
