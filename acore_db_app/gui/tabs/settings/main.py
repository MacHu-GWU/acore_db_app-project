# -*- coding: utf-8 -*-

from PySide6 import QtCore, QtWidgets, QtGui
from .json_setting import SettingsWidgetJsonSetting

# from .quest_selection import QuestCompleterWidgetQuestSelection
# from .quest_action import QuestCompleterWidgetQuestAction


class SettingsWidget(
    QtWidgets.QWidget,
    SettingsWidgetJsonSetting,
):
    """
    Settings 主要是用来修改我们的 GUI.
    """

    def __init__(self, parent: QtWidgets.QWidget):
        super().__init__(parent)
        self.add_json_setting()
        self.set_layout()

    def set_layout(self):
        layout = QtWidgets.QVBoxLayout()
        layout.addLayout(self.json_setting_layout)
        self.setLayout(layout)
