import datetime
from typing import List

import pandas as pd
from pandas.api.types import is_datetime64_ns_dtype


class DateHelper:
    @staticmethod
    def add_timestamp_suffix(string: str) -> str:
        """Add timestamp suffix to input string."""

        utc_now = datetime.datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')
        return f"{string}_{utc_now}"

    @staticmethod
    def get_today_date(utc: bool = True):
        if utc:
            today = pd.Timestamp.utcnow().date()
        else:
            today = pd.Timestamp.now().date()
        return today

    @staticmethod
    def get_today_datetime(utc: bool = True):
        if utc:
            today = pd.Timestamp.utcnow()
        else:
            today = pd.Timestamp.now()
        return today

    @staticmethod
    def get_yesterday():
        today = pd.Timestamp.utcnow().date()
        yesterday = today - pd.to_timedelta('1 days')
        return yesterday

    @staticmethod
    def get_last_week_date():
        today = DateHelper.get_today_date()
        last_week_date = today - pd.to_timedelta('7 days')
        return last_week_date

    def get_last_two_week_date():
        today = DateHelper.get_today_date()
        last_week_date = today - pd.to_timedelta('14 days')
        return last_week_date

    @staticmethod
    def get_last_30_days_date():
        today = DateHelper.get_today_date()
        last_month_date = today - pd.to_timedelta('30 days')
        return last_month_date

    @staticmethod
    def get_last_60_days_date():
        today = DateHelper.get_today_date()
        last_month_date = today - pd.to_timedelta('90 days')
        return last_month_date

    @staticmethod
    def get_first_day_of_current_month():
        today = DateHelper.get_today_date()
        last_month_date = today.replace(day=1)
        return last_month_date

    @staticmethod
    def calc_days_since(s_col: pd.Series):
        """Calc number of days elapsed since the date in the given column."""

        def convert_today_to_date(today: datetime.datetime):
            """Convert today from timestamp to date if the column has date format."""

            if len(s_col) > 0:
                if isinstance(s_col.iloc[0], datetime.date):
                    today = today.date()
            return today

        today = datetime.datetime.today()
        # today = convert_today_to_date(today)
        s_days_since = (today - s_col).dt.components['days']
        return s_days_since

    @staticmethod
    def get_hours_since(s_col: pd.Series):
        """Calc number of hours elapsed since the timestamp in the given column."""

        now = datetime.datetime.utcnow()
        s_hours_since = (now - s_col).dt.components['hours']
        s_days_since = (now - s_col).dt.components['days']

        s_cumulated_hours_since = s_days_since * 24 + s_hours_since
        s_cumulated_hours_since.name = 'hours_since_update'
        return s_cumulated_hours_since

    @staticmethod
    def convert_to_datetime_and_remove_tz(s_col: pd.Series) -> pd.Series:
        """Convert series to datetime and remove timezone."""

        s_col = pd.to_datetime(s_col, utc=True)

        if pd.api.types.is_datetime64tz_dtype(s_col):
            """Remove timezone 'tz' if specified."""
            s_col = s_col.dt.tz_convert(None)

        assert is_datetime64_ns_dtype(s_col)

        return s_col

    @staticmethod
    def convert_columns_to_datetime(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Convert columns to datetime."""

        for col in columns:
            df[col] = DateHelper.convert_to_datetime_and_remove_tz(df[col])
        return df

    @staticmethod
    def column_to_datetime(s: pd.Series) -> pd.Series:
        """Convert columns to datetime."""

        s = DateHelper.convert_to_datetime_and_remove_tz(s)
        return s

    @staticmethod
    def convert_datetime_to_date(df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Convert columns to datetime."""
        df[col] = df[col].dt.date
        return df

    @staticmethod
    def convert_to_date(s: pd.Series) -> pd.Series:
        """Convert column to date."""
        s = s.dt.date
        return s

    @staticmethod
    def convert_to_month_day(date: datetime.date) -> str:
        """Convert to month-day format."""

        return date.strftime('%m/%d')

    @staticmethod
    def get_last_week_until_today_interval() -> str:

        today_date = DateHelper.get_today_date()
        last_week_date = DateHelper.get_last_week_date()
        last_week_month_day = DateHelper.convert_to_month_day(last_week_date)
        today_month_day = DateHelper.convert_to_month_day(today_date)

        interval = f"({last_week_month_day} - {today_month_day})"
        return interval

    @staticmethod
    def get_last_week_until_yesterday_interval() -> str:

        yesterday_date = DateHelper.get_yesterday()
        last_week_date = DateHelper.get_last_week_date()
        last_week_month_day = DateHelper.convert_to_month_day(last_week_date)
        yesterday_month_day = DateHelper.convert_to_month_day(yesterday_date)

        interval = f"({last_week_month_day} - {yesterday_month_day})"
        return interval

    @staticmethod
    def remove_time_from_datetime_column(s: pd.Series) -> pd.Series:
        """Remove time part of datetime column."""
        return pd.to_datetime(pd.to_datetime(s).dt.date)
