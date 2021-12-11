import pandas as pd
import datetime as dt

@pd.api.extensions.register_dataframe_accessor("cohort")
class CohortAnalysis:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def _get_month(self, series):
        return dt.datetime(series.year, series.month, 1)

    def _calc(self, user_col, date_column, country):
        if country:
            _filter = (self._obj == country).any(axis=1)
            self._obj = self._obj[_filter].copy()
        self._obj['Inital_date'] = pd.to_datetime(self._obj[date_column])
        self._obj['Inital_month'] = self._obj['Inital_date'].apply(
            lambda x: self._get_month(x))
        self._obj['Cohort_month'] = self._obj.groupby(
            user_col)['Inital_month'].transform('min')
        self._diff()
        self._obj['months_Initial_date'] = self._years_diff * \
            12 + self._months_diff + 1
        self._cohorts = self._obj.groupby(['Cohort_month', 'months_Initial_date'])[
            user_col].nunique().reset_index()
        self._cohorts = self._cohorts.pivot(
            index='Cohort_month', columns='months_Initial_date', values=user_col)

    def retention(self, user_col, date_column, country=None):
        # Future -> implement for days
        self._calc(user_col, date_column, country)
        self.retention_ratio = self._retention_ratio()
        self._prepare()
        return self.retention_ratio

    def _validate(self,):
        pass

    def _prepare(self):
        self.retention_ratio.columns = self.retention_ratio.columns.rename('')
        _new_cols = []
        for column in self.retention_ratio.columns:
            try:
                int(column)
                _new_cols.append('Month ' + str(column))
            except ValueError:
                _new_cols.append(column)
        self.retention_ratio.columns = _new_cols
        self.retention_ratio = self.retention_ratio.reset_index()

    def _retention_ratio(self,):
        self._cohorts_sizes = self._cohorts.iloc[:, 0]
        self._retention = self._cohorts.divide(self._cohorts_sizes, axis=0)
        return self._retention.round(3) * 100

    def _diff(self, columns=['Inital_month', 'Cohort_month']):
        self._years_diff = self._obj[columns[0]
                                     ].dt.year - self._obj[columns[1]].dt.year
        self._months_diff = self._obj[columns[0]
                                      ].dt.month - self._obj[columns[1]].dt.month
