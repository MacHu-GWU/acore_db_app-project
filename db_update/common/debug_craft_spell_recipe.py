# -*- coding: utf-8 -*-

from pathlib import Path
from acore_db_app.update.common.api import download_file
from acore_db_app.update.common.craft_spell_recipe import (
    Recipe,
    extract_recipe,
)

# --- first time creating the recipe.json.gz file ---
dir_here = Path(__file__).absolute().parent
path_recipe_json_gz = dir_here.joinpath("recipe.json.gz")
recipe_list = extract_recipe()
Recipe.dump_many(recipe_list, path_recipe_json_gz)
df = Recipe.to_dataframe(recipe_list)
print(df)

# --- reuse existing recipe.json.gz file ---
# path_recipe_json_gz = download_file("recipe.json.gz")
# recipe_list = Recipe.load_many(path_recipe_json_gz)
# df = Recipe.to_dataframe(recipe_list)
# print(df)
