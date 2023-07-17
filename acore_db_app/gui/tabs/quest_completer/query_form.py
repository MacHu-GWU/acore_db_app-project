# -*- coding: utf-8 -*-

import typing as T
import json
from PySide6 import QtCore, QtWidgets, QtGui

from ....remote_cli.impl import get_latest_n_request

if T.TYPE_CHECKING:
    from .main import QuestCompleterWidget


from boto_session_manager import BotoSesManager
bsm = BotoSesManager(profile_name="bmt_app_dev_us_east_1")

class QuestCompleterWidgetQueryForm:
    def add_query_form(self):
        self._add_query_form_widgets()
        self._add_query_form_layout()

    def _add_query_form_widgets(self):
        self.query_form_header = QtWidgets.QLabel("Search Quest")

        self.query_form_character_label = QtWidgets.QLabel("Character")
        self.query_form_character_label.setAlignment(QtCore.Qt.AlignCenter)
        self.query_form_character_label.setFixedWidth(60)
        self.query_form_character_value = QtWidgets.QLineEdit()
        self.query_form_character_value.setPlaceholderText("enter Character name here")

        self.search_button = QtWidgets.QPushButton("Search")
        self.search_button.clicked.connect(self._search_button_clicked_event_handler)

    def _search_button_clicked_event_handler(self: "QuestCompleterWidget"):
        print("search button clicked")
        enriched_quest_data_list = get_latest_n_request(
            bsm=bsm,
            server_id="sbx-green",
            character="shootingrab",
            locale="zhTW",
            n=25,
        )

        self.quest_selection_item_list.clear()
        for enriched_quest_data in enriched_quest_data_list:
            item = QtWidgets.QListWidgetItem(f"{enriched_quest_data}")
            item.enriched_quest_data = enriched_quest_data
            item.setTextAlignment(QtCore.Qt.AlignLeft)
            self.quest_selection_item_list.addItem(item)

    def _add_query_form_layout(self):
        self.query_form_layout = QtWidgets.QVBoxLayout()

        self.query_form_layout.addWidget(self.query_form_header)

        pairs = [
            (self.query_form_character_label, self.query_form_character_value),
        ]
        for label, value in pairs:
            query_form_row_layout = QtWidgets.QHBoxLayout()
            query_form_row_layout.addWidget(label)
            query_form_row_layout.addWidget(value)
            self.query_form_layout.addLayout(query_form_row_layout)

        self.query_form_layout.addWidget(self.search_button)
