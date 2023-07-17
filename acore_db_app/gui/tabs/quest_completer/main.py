# -*- coding: utf-8 -*-

from PySide6 import QtCore, QtWidgets, QtGui
from .query_form import QuestCompleterWidgetQueryForm
from .quest_selection import QuestCompleterWidgetQuestSelection
from .quest_action import QuestCompleterWidgetQuestAction


class QuestCompleterWidget(
    QtWidgets.QWidget,
    QuestCompleterWidgetQueryForm,
    QuestCompleterWidgetQuestSelection,
    QuestCompleterWidgetQuestAction,
):
    """
    Quest Completer 是主要帮助角色完成任务的 App.

    1. 一个顶部的搜索框, 用于搜索任务
    2. 一个中间左边的任务列表, 用于显示搜索结果
    3. 一个底部的动作列表, 用于点击后执行动作
    """

    def __init__(self, parent: QtWidgets.QWidget):
        super().__init__(parent)
        self.add_query_form()
        self.add_quest_selection()
        self.add_quest_action()
        self.set_layout()

    def set_layout(self):
        layout = QtWidgets.QVBoxLayout()
        layout.addLayout(self.query_form_layout)
        layout.addLayout(self.quest_selection_layout)
        layout.addLayout(self.quest_action_layout)
        self.setLayout(layout)
