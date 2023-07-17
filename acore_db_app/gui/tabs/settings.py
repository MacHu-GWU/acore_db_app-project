# -*- coding: utf-8 -*-

import json
from PySide6 import QtCore, QtWidgets, QtGui

from ...paths import path_settings_json


def read_settings() -> dict:
    try:
        return json.loads(path_settings_json.read_text())
    except FileNotFoundError:
        path_settings_json.write_text(json.dumps("{}"))
        return {}


def write_settings(data: dict):
    path_settings_json.write_text(json.dumps(data, indent=4))


class SettingsWidget(QtWidgets.QWidget):
    """
    用于设置的 Widget, 包含了许多 Key Value 的输入框.
    """

    def __init__(self, parent: QtWidgets.QWidget):
        super().__init__(parent)
        self.setting_keys = ["key1", "key2"]  # 定义我们的 config 有哪些 Key
        self.add_settings_form()
        self.add_buttons()
        self.set_layout()

    def add_settings_form(self):
        # 创建一个用于展示 settings 中的 key value 的表格
        self.setting_form: dict[str, tuple[QtWidgets.QLabel, QtWidgets.QLineEdit]] = {}
        data = read_settings()
        # 从列表动态生成 key value 的表单
        for key in self.setting_keys:
            # 其中 key 是一个 label
            label = QtWidgets.QLabel(f"{key}:")
            # 而 value 是一个输入框
            edit = QtWidgets.QLineEdit()
            edit.setPlaceholderText("type something here")
            # 默认第一次打开的时候会从 settings 中读取数据
            if key in data:
                edit.setText(str(data.get(key)))
            self.setting_form[key] = (label, edit)

    def add_buttons(self):
        # 添加两个 button, 一个用于从 settings 中加载数据, 另一个用于将数据写入 settings
        self.load_button = QtWidgets.QPushButton("Load")
        self.load_button.clicked.connect(self.load_button_clicked_event_handler)
        self.apply_button = QtWidgets.QPushButton("Apply")
        self.apply_button.clicked.connect(self.apply_button_clicked_event_handler)

    @QtCore.Slot()
    def load_button_clicked_event_handler(self):
        data = read_settings()
        for key, value in self.setting_form.items():
            if key in data:
                value[1].setText(str(data.get(key)))

    @QtCore.Slot()
    def apply_button_clicked_event_handler(self):
        write_settings(
            {key: value[1].text() for key, value in self.setting_form.items()}
        )

    def set_layout(self):
        # 设置一个嵌套的 layout, 其中 form layout 是子 layout
        form_layout = QtWidgets.QVBoxLayout()
        for key, value in self.setting_form.items():
            form_layout.addWidget(value[0])
            form_layout.addWidget(value[1])

        # button 的 layout 也是一个子 layout
        button_layout = QtWidgets.QHBoxLayout()
        button_layout.addWidget(self.load_button)
        button_layout.addWidget(self.apply_button)

        # 最后将两个子 layout 组合起来
        layout = QtWidgets.QVBoxLayout()
        layout.addLayout(form_layout)
        layout.addLayout(button_layout)

        self.setLayout(layout)
