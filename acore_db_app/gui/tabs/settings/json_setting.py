# -*- coding: utf-8 -*-

import typing as T
import json
from PySide6 import QtCore, QtWidgets, QtGui

from ....paths import path_gui_settings_json
from .setting_object import SettingObject


def read_settings() -> dict:
    try:
        return json.loads(path_gui_settings_json.read_text())
    except FileNotFoundError:
        path_gui_settings_json.write_text(json.dumps("{}"))
        return {}


def write_settings(data: dict):
    path_gui_settings_json.write_text(json.dumps(data, indent=4))


class SettingsWidgetJsonSetting:
    def add_json_setting(self):
        self._add_json_setting_widgets()
        self._add_json_setting_layout()

    def _add_json_setting_widgets(self):
        # 创建一个用于展示 settings 中的 key value 的表格
        self.json_setting_keys = ["aws_profile", "server_id"]
        self.json_setting_form: T.Dict[
            str, T.Tuple[QtWidgets.QLabel, QtWidgets.QLineEdit]
        ] = {}
        data = read_settings()
        # 从列表动态生成 key value 的表单
        for key in self.json_setting_keys:
            # 其中 key 是一个 label
            label = QtWidgets.QLabel(f"{key}:")
            label.setAlignment(QtCore.Qt.AlignCenter)
            label.setFixedWidth(120)

            # 而 value 是一个输入框
            edit = QtWidgets.QLineEdit()
            edit.setPlaceholderText(f"enter {key!r} here")
            # 默认第一次打开的时候会从 settings 中读取数据
            if key in data:
                edit.setText(str(data.get(key)))
            self.json_setting_form[key] = (label, edit)

        # 添加两个 button, 一个用于从 settings 中加载数据, 另一个用于将数据写入 settings
        self.load_button = QtWidgets.QPushButton("Load")
        self.load_button.clicked.connect(self._load_button_clicked_event_handler)
        self.apply_button = QtWidgets.QPushButton("Apply")
        self.apply_button.clicked.connect(self._apply_button_clicked_event_handler)

    @QtCore.Slot()
    def _load_button_clicked_event_handler(self):
        data = read_settings()
        for key, value in self.json_setting_form.items():
            if key in data:
                value[1].setText(str(data.get(key)))

    @QtCore.Slot()
    def _apply_button_clicked_event_handler(self):
        write_settings(
            {key: value[1].text() for key, value in self.json_setting_form.items()}
        )

    def _add_json_setting_layout(self):
        # 将 json setting form 按照数列排版
        json_setting_form_layout = QtWidgets.QVBoxLayout()
        for key, (label, edit) in self.json_setting_form.items():
            json_setting_form_row_layout = QtWidgets.QHBoxLayout()
            json_setting_form_row_layout.addWidget(label)
            json_setting_form_row_layout.addWidget(edit)
            json_setting_form_layout.addLayout(json_setting_form_row_layout)

        # button 的 layout 也是一个子 layout
        button_layout = QtWidgets.QHBoxLayout()
        button_layout.addWidget(self.load_button)
        button_layout.addWidget(self.apply_button)

        # 将 json setting form 和 button 组合起来
        self.json_setting_layout = QtWidgets.QVBoxLayout()
        self.json_setting_layout.addLayout(json_setting_form_layout)
        self.json_setting_layout.addLayout(button_layout)
