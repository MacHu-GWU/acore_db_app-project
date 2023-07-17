# -*- coding: utf-8 -*-

from pathlib import Path
from boto_session_manager import BotoSesManager
from acore_server.api import Server

bsm = BotoSesManager(profile_name="bmt_app_dev_us_east_1")
env_name = "sbx"
server_name = "green"
path_pem_file = Path.home() / "ec2-pem" / "bmt-app-dev" / "us-east-1" / "sanhe-dev.pem"
server_id = f"{env_name}-{server_name}"

server = Server.get(bsm, server_id)

# server.create_ssh_tunnel(path_pem_file)
# server.list_ssh_tunnel(path_pem_file)
server.test_ssh_tunnel()
# server.kill_ssh_tunnel(path_pem_file)
