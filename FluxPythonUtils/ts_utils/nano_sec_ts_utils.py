# standard imports
import datetime

# 3rd party imports
import pendulum
import pandas as pd

# project imports


def get_epoch_from_pendulum_dt(date_time: pendulum.DateTime) -> int:
    epoch_dt = int(date_time.timestamp() * 1_000_000_000)
    return epoch_dt


def get_epoch_from_standard_dt(date_time: datetime.datetime) -> int:
    epoch_dt = int(date_time.timestamp() * 1_000_000_000)
    return epoch_dt


def get_epoch_from_pandas_timestamp(date_time: pd.Timestamp) -> int:
    epoch_dt = int(date_time.timestamp() * 1_000_000_000)
    return epoch_dt


def get_pendulum_dt_from_epoch(epoch_nanoseconds: int) -> pendulum.DateTime:
    datetime = pendulum.from_timestamp(epoch_nanoseconds / 1_000_000_000, tz="UTC")
    return datetime


def get_standard_dt_from_epoch(epoch_nanoseconds: int) -> datetime.datetime:
    date_time = datetime.datetime.fromtimestamp(epoch_nanoseconds / 1_000_000_000, tz=datetime.timezone.utc)
    return date_time


def get_pandas_timestamp_from_epoch(epoch_nanoseconds: int) -> pd.Timestamp:
    date_time = pd.Timestamp(epoch_nanoseconds / 1_000_000_000, unit='s', tz='UTC')
    return date_time
