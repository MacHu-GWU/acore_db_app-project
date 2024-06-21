
.. image:: https://readthedocs.org/projects/acore-db-app/badge/?version=latest
    :target: https://acore-db-app.readthedocs.io/en/latest/
    :alt: Documentation Status

.. image:: https://github.com/MacHu-GWU/acore_db_app-project/actions/workflows/main.yml/badge.svg
    :target: https://github.com/MacHu-GWU/acore_db_app-project/actions?query=workflow:CI

.. image:: https://codecov.io/gh/MacHu-GWU/acore_db_app-project/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/MacHu-GWU/acore_db_app-project

.. image:: https://img.shields.io/pypi/v/acore-db-app.svg
    :target: https://pypi.python.org/pypi/acore-db-app

.. image:: https://img.shields.io/pypi/l/acore-db-app.svg
    :target: https://pypi.python.org/pypi/acore-db-app

.. image:: https://img.shields.io/pypi/pyversions/acore-db-app.svg
    :target: https://pypi.python.org/pypi/acore-db-app

.. image:: https://img.shields.io/badge/Release_History!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/acore_db_app-project/blob/main/release-history.rst

.. image:: https://img.shields.io/badge/STAR_Me_on_GitHub!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/acore_db_app-project

.. image:: https://img.shields.io/badge/Acore_Doc--None.svg?style=social&logo=readthedocs
    :target: https://acore-doc.readthedocs.io/en/latest/

------

.. image:: https://img.shields.io/badge/Link-Document-blue.svg
    :target: https://acore-db-app.readthedocs.io/en/latest/

.. image:: https://img.shields.io/badge/Link-API-blue.svg
    :target: https://acore-db-app.readthedocs.io/en/latest/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Install-blue.svg
    :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
    :target: https://github.com/MacHu-GWU/acore_db_app-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
    :target: https://github.com/MacHu-GWU/acore_db_app-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
    :target: https://github.com/MacHu-GWU/acore_db_app-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
    :target: https://pypi.org/pypi/acore-db-app#files


Welcome to ``acore_db_app`` Documentation
==============================================================================
.. image:: https://acore-server.readthedocs.io/en/latest/_static/acore_server-logo.png
    :target: https://acore-server.readthedocs.io/en/latest/

AzerothCore 魔兽世界服务器后端有一个数据库. 基于数据库我们可以开发出很多有创造力的 App. 这里有两个痛点:

1. 出于安全考虑, 我们只能允许位于 AWS EC2 上的游戏服务器能跟数据库网络直连. 在本地的 App 开发过程连接到数据库, 以及让最终的 App 连接到数据库都是一个挑战.
2. 当基于数据库的 App 开发完毕后, 这个 App 以什么形式给最终用户使用? Web App? 桌面 GUI? 网络安全又如何保障?

该项目是一个针对这个需求的完整解决方案, 它包含两个组件 **CLI** 和 **SDK**.

1. **CLI** 是在 AWS EC2 游戏服务器上安装的一个命令行工具. 把常用的数据库 App 功能以及输入输出用 CLI 包装好供外部用户调用. 这些 CLI 命令的输入通常是和对应 Python 函数一致的参数, 而输出通常是将数据序列化成 JSON 然后打印到 stdout.
2. **SDK** 则是可以在任何地方运行的一系列 API. 这些 API 会用 SSM Run Command 远程调用位于 EC2 上的 CLI, 然后将 stdout 中的数据解析并返回.

简而言之, CLI 是服务端的 App, SDK 是客户端的 App. 并且 SDK 是基于 CLI 的封装. 两者结合就能实现任何有权限的开发者都能从任何地方安全地运行数据库 App 的功能, 而无需位于数据库所在的 VPC 中.

AzerothCore Database Schema Reference:

- https://www.azerothcore.org/wiki/database-auth
- https://www.azerothcore.org/wiki/database-characters
- https://www.azerothcore.org/wiki/database-world

在 EC2 上安装完 acore_db_app CLI 之后, 你可以用下面的命令测试:

.. code-block:: bash

    /home/ubuntu/git_repos/acore_db_app-project/.venv/bin/acdb hello

Get latest N quests for a character:

.. code-block:: bash

    /home/ubuntu/git_repos/acore_db_app-project/.venv/bin/acdb quest get-latest-n-quest --char ${character}


.. _install:

Install
------------------------------------------------------------------------------

``acore_db_app`` is released on PyPI, so all you need is to:

.. code-block:: console

    $ pip install acore-db-app

To upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade acore-db-app
