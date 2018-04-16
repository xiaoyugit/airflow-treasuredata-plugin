# -*- coding: utf-8 -*-

from airflow.plugins_manager import AirflowPlugin
from treasuredata_plugin.hooks.td_hook import TreasureHook
from treasuredata_plugin.operators.td_to_postgres_operator import TreasureToPostgresOperator


class TreasureOperatorPlugin(AirflowPlugin):
    name = "treasuredata_plugin"
    operators = [TreasureToPostgresOperator]
    flask_blueprints = []
    hooks = [TreasureHook]
    executors = []
    admin_views = []
    menu_links = []
