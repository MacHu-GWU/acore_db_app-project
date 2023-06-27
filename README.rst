
.. image:: https://readthedocs.org/projects/acore-db-app/badge/?version=latest
    :target: https://acore-db-app.readthedocs.io/en/latest/
    :alt: Documentation Status

.. image:: https://github.com/MacHu-GWU/acore_db_app-project/workflows/CI/badge.svg
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
AzerothCore 魔兽世界服务器后端有一个数据库. 基于数据库我们可以开发出很多有创造力的 App. 这里有两个痛点:

1. 出于安全考虑, 我们只能允许位于 AWS EC2 上的游戏服务器能跟数据库网络直连. 本地的 App 开发, 以及最终的 App 部署都是一个挑战.
2. 当基于数据库的 App 开发完毕后, 这个 App 以什么形式给最终用户使用? Web App? 桌面 GUI? 网络安全又如何保障?

这个项目就是为了解决这两个痛点而生的. 它本身包含两个组件. 一个是在 AWS EC2 游戏服务器上安装的一套 CLI 程序. 把常用的功能以及输入输出用 CLI 包装好供外部用户调用. 另一个是一个 Remote CLI, 可以让有权限的用户从任何地方远程调用服务器上的 CLI 并获取返回的结果. 这个组件的底层是通过 AWS SSM Run Command 来实现的.

AzerothCore Database Schema Reference:

- https://www.azerothcore.org/wiki/database-auth
- https://www.azerothcore.org/wiki/database-characters
- https://www.azerothcore.org/wiki/database-world


.. _install:

Install
------------------------------------------------------------------------------

``acore_db_app`` is released on PyPI, so all you need is to:

.. code-block:: console

    $ pip install acore-db-app

To upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade acore-db-app
