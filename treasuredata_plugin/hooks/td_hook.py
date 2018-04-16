# -*- coding: utf-8 -*-
import csv

import tdclient
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.utils.decorators import apply_defaults


class TreasureException(Exception):
    pass


class TreasureHook(DbApiHook):
    """
    Interact with TreasureData through tdclient!
    """

    conn_name_attr = 'td_conn_id'
    default_conn_name = 'td_default'

    def __init__(self, *args, **kwargs):
        super(TreasureHook, self).__init__(*args, **kwargs)
        self.sql_type = kwargs['sql_type']
        self.td_apikey = kwargs['td_apikey']
        self.schema = kwargs['schema']

    def get_conn(self):
        """Returns a connection object"""
        try:
            db = self.get_connection(self.td_conn_id)
            conn = tdclient.connect(
                apikey=db.login,
                type=self.sql_type,
                wait_callback=self._on_waiting,
                db=db.schema)
        except Exception:
            self.log.info("Swith to non-hook connection mode")
            if self.td_apikey:
                conn = tdclient.connect(
                    apikey=self.td_apikey,
                    type=self.sql_type,
                    wait_callback=self._on_waiting,
                    db=self.schema)
            else:
                raise TreasureException('No td_apikey specified')
        return conn

    def _on_waiting(self, cursor):
        self.log.info("Current Job status: %s", cursor.job_status())

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    def get_records(self, hql, parameters=None):
        """
        Get a set of records from TreasureData
        """
        try:
            return super(TreasureHook, self).get_records(
                self._strip_sql(hql), parameters)
        except Exception:
            raise TreasureException("get_records returns nothing")

    def get_first(self, hql, parameters=None):
        """
        Returns only the first row, regardless of how many rows the query
        returns.
        """
        try:
            return super(TreasureHook, self).get_first(
                self._strip_sql(hql), parameters)
        except Exception:
            raise TreasureException("get_first returns nothing")

    def get_pandas_df(self, hql, parameters=None):
        """
        Get a pandas dataframe from a sql query.
        """
        import pandas
        cursor = self.get_cursor()
        try:
            cursor.execute(self._strip_sql(hql), parameters)
            data = cursor.fetchall()
        except Exception:
            raise TreasureException("get_pandas_df returns nothing")
        column_descriptions = cursor.description
        if data:
            df = pandas.DataFrame(data)
            df.columns = [c[0] for c in column_descriptions]
        else:
            df = pandas.DataFrame()
        return df

    def run(self, hql, parameters=None):
        """
        Simply execute the statement.
        """
        return super(TreasureHook, self).run(self._strip_sql(hql), parameters)

    def to_tsv(self, hql, filepath, delimiter='\t',
               lineterminator='\r\n', output_header=True, fetch_size=1000):
        """
        Dump the query results to a file object
        """
        with self.get_conn() as conn:
            cur = conn.cursor()
            self.log.info("Running query: %s", hql)
            cur.execute(hql)
            schema = cur.description
            with open(filepath, 'w') as f:
                writer = csv.writer(f, delimiter=delimiter,
                                    lineterminator=lineterminator)
                if output_header:
                    writer.writerow([c[0] for c in cur.description])
                i = 0
                while i < cur.rowcount:
                    fetch_size = min(fetch_size, cur.rowcount-i)
                    rows = cur.fetchmany(fetch_size)
                    if not rows:
                        break
                    writer.writerows(rows)
                    i += fetch_size
                    self.log.info("Written %s rows so far.", i)
                self.log.info("Done. Loaded a total of %s rows.", i)
            cur.close()
