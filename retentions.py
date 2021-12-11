"""
Populate Customers data, 
Calculate retention & create table & populate it 
"""
import psycopg2
import pandas_cohort  # TO USE <dataframe>.cohort.retention
import pandas as pd
from dotenv import dotenv_values
import psycopg2.extras as extras
from memorize import Memorize


DATA_PATH = './data/'


class Cohort:

    def __init__(self, data, user_col, date_col):
        try:
            self.df = pd.read_csv(data,)
            self.df = self.df.dropna(subset=[user_col])
            self.user_col = user_col
            self.date_col = date_col
        except Exception:
            # pass for now, TODO: add more configuration on reading data
            pass

    def _create_table_command(self, columns=None, summary=False):
        if summary:
            _command = """ CREATE TABLE IF NOT EXISTS retenetions_summary (
                        month_run VARCHAR(255) NOT NULL,
                        retention float ,
                        country VARCHAR(255) )"""
            return _command
        _command = """ CREATE TABLE IF NOT EXISTS retenetions (
            cohort_month VARCHAR(255) NOT NULL """
        for col in columns[1:-1]:
            _command += ',' + str(col) + ' float'
        _command += ',' + str(columns[-1]) + ' VARCHAR(350) '
        _command += ')'
        return _command

    def retention(self, filter_by=None):
        if filter_by is None:
            self.retention = self.df.cohort.retention(
                self.user_col,
                self.date_col,)
            self.retention.columns = self.retention.columns.str.replace(
                ' ', '_')
            cmd = self._create_table_command(self.retention.columns)
            self.retention = self.retention.fillna(0)
            return self.retention, cmd
        else:
            retentions = self.df.cohort.retention(
                self.user_col,
                self.date_col,)
            retentions_summary = retentions.mean(axis=0).to_frame().reset_index().rename(
                columns={'index': 'month_run', 0: 'retention'})
            retentions[filter_by] = 'all'
            retentions_summary[filter_by] = 'all'
            _filters = self.df[filter_by].unique().tolist()
            for _filter in _filters:
                try:
                    retention_ = self.df.cohort.retention(
                        self.user_col,
                        self.date_col,
                        country=_filter
                    )
                except IndexError:
                    pass  # pass for now: TODO fix pandas-cohort
                retentions_summary_ = retention_.mean(axis=0).to_frame().reset_index().rename(
                    columns={'index': 'month_run', 0: 'retention'})
                retention_[filter_by] = _filter
                retentions_summary_[filter_by] = _filter
                retentions = pd.concat([retentions, retention_], axis=0)
                retentions_summary = pd.concat(
                    [retentions_summary, retentions_summary_], axis=0)
            
            self.retention = retentions
            self.retentions_summary = retentions_summary
            self.retention.columns = self.retention.columns.str.replace(
                ' ', '_')
            cmd = self._create_table_command(self.retention.columns)
            cmd_summary = self._create_table_command(summary=True)
            self.retention = self.retention.fillna(0)

            return self.retention, cmd, self.retentions_summary, cmd_summary


class Postgres:
    def __init__(self, database, host, port, user, password,):
        self._connect(
            database, host, port, user, password
        )

    def _connect(self,
                 database, host, port, user, password):
        self.conn = psycopg2.connect(
            database=database, user=user, password=password, host=host, port=port)

    @property
    def close(self):
        self.conn.close()

    def create_table(self, cmd):
        cur = self.conn.cursor()
        try:
            cur.execute(cmd)
            self.conn.commit()
            print('create_table() done')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.conn.rollback()
            cur.close()

    @Memorize
    def execute_values(self, df, table):
        tuples = [tuple(x) for x in df.to_numpy()]

        cols = ','.join(list(df.columns))
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cur = self.conn.cursor()
        try:
            extras.execute_values(cur, query, tuples)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cur.close()
            return 1
        print("execute_values() done")
        cur.close()


if __name__ == '__main__':
    config = dotenv_values(".env")
    database, host, user, password, port = config['DB_NAME'], config['DB_HOST'], config[
        'DB_USER'], config['DB_PASSWORD'], config['DB_PORT']
    postgres = Postgres(
        database, host, port, user, password,
    )
    cohort = Cohort(
        data=DATA_PATH + 'data.csv', 
        user_col='CustomerID', date_col='InvoiceDate'
    )
    # TODO prvent pandas-cohort from adding new columns to original df
    postgres.execute_values(cohort.df, 'Customers')
    retention, cmd, retention_summary, cmd_summary = cohort.retention(filter_by='Country')
    retention['Cohort_month'] = retention['Cohort_month'].dt.strftime('%Y-%m')
    postgres.create_table(cmd)
    postgres.execute_values(retention, 'Retenetions')

    postgres.create_table(cmd_summary)
    postgres.execute_values(retention_summary, 'retenetions_summary')

    postgres.close
