# -*- coding: utf-8 -*-
import csv
from io import StringIO

import tdclient
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from treasuredata_plugin.hooks.td_hook import TreasureHook


class TreasureToPostgresOperator(BaseOperator):
    """
    Moves data from TreasureData to Postgres Database.
    """

    template_fields = ('sql', 'postgres_table', 'postgres_preoperator',
                       'postgres_postoperator')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            sql,
            postgres_table,
            td_conn_id='td_default',
            postgres_conn_id='postgres_default',
            postgres_preoperator=None,
            postgres_postoperator=None,
            td_apikey=None,
            td_database='sample_datasets',
            sql_type='presto',
            *args, **kwargs):
        super(TreasureToPostgresOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_table = postgres_table
        self.td_database = td_database
        # self.td_apikey = td_apikey
        self.td_conn_id = td_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.postgres_preoperator = postgres_preoperator
        self.postgres_postoperator = postgres_postoperator
        self.sql_type = sql_type
        self.td_apikey = td_apikey
        self.td_database = td_database

    def execute(self, context):
        td = TreasureHook(
            td_conn_id=self.td_conn_id,
            sql_type=self.sql_type,
            td_apikey=self.td_apikey,
            schema=self.td_database
        )

        self.log.info("Extracting data from TreasureData: %s", self.sql)

        results = td.get_records(self.sql)

        tmpfile = StringIO()
        writer = csv.writer(tmpfile, delimiter='\t')
        writer.writerows(results)
        # back to the top of fileobj
        tmpfile.seek(0)

        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        if self.postgres_preoperator:
            self.log.info("Running Postgres preoperator")
            postgres.run(self.postgres_preoperator)

        self.log.info("Loading into Postgres")

        with postgres.get_conn() as pg_conn:
            with pg_conn.cursor() as cur:
                cur.copy_from(tmpfile, self.postgres_table, sep='\t', null='')

        if self.postgres_postoperator:
            self.log.info("Running Postgres postoperator")
            postgres.run(self.postgres_postoperator)

        self.log.info("Done.")
