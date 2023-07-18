# -*- coding: utf-8 -*-

from PySide6 import QtCore, QtWidgets, QtGui

from .quest_completer.main import QuestCompleterWidget
from .settings.main import SettingsWidget


class TabDialog(QtWidgets.QDialog):
    """
    这是我们的 App 的主要的 Widget, 它包含了几个 Tab.

    1. Quest Completer Tab, 用于搜索任务, 并且完成任务
    2. Settings Tab, 主要 GUI 的配置管理

    每个 Tab 本身是一个子 Widget.
    """

    def __init__(self, parent: QtWidgets.QWidget = None):
        super().__init__(parent)
        self.tab_widget = QtWidgets.QTabWidget()

        # 一共有两个 Tab, 一个用于搜索, 一个用于设置
        self.tab_widget.addTab(QuestCompleterWidget(self), "Quest Completer")
        self.tab_widget.addTab(SettingsWidget(self), "Settings")

        # 这个 Widget 只有一个 Layout, 用于放置 Tab
        main_layout = QtWidgets.QVBoxLayout()
        main_layout.addWidget(self.tab_widget)
        self.setLayout(main_layout)
