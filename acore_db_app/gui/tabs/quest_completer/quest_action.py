# -*- coding: utf-8 -*-

import typing as T

from PySide6 import QtCore, QtWidgets, QtGui

if T.TYPE_CHECKING:
    from .main import QuestCompleterWidget


class QuestCompleterWidgetQuestAction:
    def add_quest_action(self):
        self._add_quest_action_widgets()
        self._add_quest_action_layout()

    def _add_quest_action_widgets(self):
        self.quest_action_item_list_header = QtWidgets.QLabel("Actions")
        self.quest_action_item_list_header.setAlignment(QtCore.Qt.AlignCenter)

        self.quest_action_item_list = QtWidgets.QListWidget()
        self.quest_action_item_list.setSelectionMode(
            QtWidgets.QAbstractItemView.SelectionMode.SingleSelection
        )

        self.quest_action_item_list.itemDoubleClicked.connect(
            self._quest_action_item_double_clicked_event_handler
        )

    def _quest_action_item_double_clicked_event_handler(self):
        """
        当用户双击 Quest Action 中的某个 item 的时候会触发这个函数.
        """
        selected_items = self.quest_action_item_list.selectedItems()
        item = selected_items[0]
        text = item.text()
        print(f"item list item double clicked: {text!r}")

        clipboard = QtWidgets.QApplication.clipboard()
        clipboard.setText(item.copy_text)

    def _add_quest_action_layout(self):
        self.quest_action_layout = QtWidgets.QVBoxLayout()
        self.quest_action_layout.addWidget(self.quest_action_item_list_header)
        self.quest_action_layout.addWidget(self.quest_action_item_list)
