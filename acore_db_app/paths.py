# -*- coding: utf-8 -*-

from pathlib import Path

dir_here = Path(__file__).absolute().parent
PACKAGE_NAME = dir_here.name

dir_project_root = dir_here.parent

# ------------------------------------------------------------------------------
# Virtual Environment Related
# ------------------------------------------------------------------------------
dir_venv = dir_project_root / ".venv"
dir_venv_bin = dir_venv / "bin"

# virtualenv executable paths
bin_pytest = dir_venv_bin / "pytest"

# test related
dir_htmlcov = dir_project_root / "htmlcov"
path_cov_index_html = dir_htmlcov / "index.html"
dir_unit_test = dir_project_root / "tests"


# ------------------------------------------------------------------------------
# acore_db_app Related
# ------------------------------------------------------------------------------
path_metadata_cache = dir_project_root.joinpath("metadata.pickle")
path_metadata_auth_cache = dir_project_root.joinpath("metadata_auth.pickle")
path_metadata_char_cache = dir_project_root.joinpath("metadata_char.pickle")
path_metadata_world_cache = dir_project_root.joinpath("metadata_world.pickle")

# a local cache of sqlalchemy engine connection info
path_sqlalchemy_engine_json = dir_project_root.joinpath("sqlalchemy_engine.json")

# diskcache.Cache location
dir_disk_cache = dir_project_root / ".disk-cache"

# ------------------------------------------------------------------------------
# GUI Related
# ------------------------------------------------------------------------------
path_gui_settings_json = dir_project_root / "gui_settings.json"
