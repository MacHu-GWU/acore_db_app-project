# -*- coding: utf-8 -*-

# 任何面向最终用户的 app 基本上都是从命令行接入的, 所以 sys 模块是必须的
import sys

# QtCore, QtWidgets, QtGui 是 PySide 的三个主要模块, 一般每一个项目都会用到这三个模块.
# 所以我一般在开始就会 Import 它们, 不管用不用得到.
# 全部的模块列表: https://doc.qt.io/qtforpython-6/modules.html#
from PySide6 import QtCore, QtWidgets, QtGui

from .tabs.main import TabDialog


class MainWindow(QtWidgets.QMainWindow):
    """
    一般任何一个 App 都有一个主窗口. 在这个主窗口里我们可以塞下各种各样的 Widget.
    """

    def __init__(self, widget: QtWidgets.QWidget):
        QtWidgets.QMainWindow.__init__(self)
        self.setWindowTitle("Acore Database App")
        self.add_menu()
        self.setCentralWidget(widget)

    def add_menu(self):
        # Create the tray
        self.tray = QtWidgets.QSystemTrayIcon()
        self.tray.setIcon(QtGui.QIcon("icon.png"))
        self.tray.setVisible(True)

        # Create the menu
        self.menu = QtWidgets.QMenu()
        self.file_menu = self.menu.addMenu("File")

        settings_action = QtGui.QAction("settings", self)
        settings_action.triggered.connect(self.update_settings)

        self.file_menu.addAction(settings_action)
        self.tray.setContextMenu(self.menu)

    @QtCore.Slot()
    def update_settings(self, checked):
        print("update settings")


def run():
    app = QtWidgets.QApplication(sys.argv)

    tab_dialog = TabDialog()

    window = MainWindow(tab_dialog)  # 将主要的 widget 添加到 MainWindow 中
    window.resize(1200, 800)
    window.show()

    sys.exit(app.exec())
