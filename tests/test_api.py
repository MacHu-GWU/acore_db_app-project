# -*- coding: utf-8 -*-

from acore_db_app import api


def test():
    _ = api


if __name__ == "__main__":
    from acore_db_app.tests import run_cov_test

    run_cov_test(__file__, "acore_db_app.api", preview=False)
