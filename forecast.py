import datetime
import re
import warnings

import openmeteo_requests
import pandas as pd
import requests
from retry_requests import retry as retry_request

from coordinates import LOCATIONS

URL = "https://api.open-meteo.com/v1/forecast"
TIMEZONE = "America/Los_Angeles"

WIND_SPEED = "wind_speed_10m"
WIND_GUSTS = "wind_gusts_10m"
WIND_DIRECTION = "wind_direction_10m"
COLUMNS = [WIND_SPEED, WIND_GUSTS, WIND_DIRECTION]

DAY_START = 6  # 6 am
DAY_END = 20  # 8 pm

def request_forecast(location):

    # HRDPS update times: observationally, seeing ~36 hours of predictions.
    #
    # Observations are assimilated every 6 hours (00, 06, 12, 18 UTC) with a 7-hour observation cut-off to ensure real-time updates.
    # https://docs.therisk.global/nexus-initiatives/heatwaves-prediction/appendix-a-data-source/numerical-deterministic/hrdps

    retry_session = retry_request(requests.Session(), retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    loc = LOCATIONS[location]
    lat, long = zip(*loc)

    params = {
        "latitude": lat,
        "longitude": long,
        "hourly": COLUMNS,
        "forecast_days": 3,
        "timezone": TIMEZONE,
        "wind_speed_unit": "kn",
        #"models": "gem_seamless",
        "models": "gem_hrdps_continental",
    }
    return openmeteo.weather_api(URL, params=params)

def parse_location(response):
    hourly = response.Hourly()
    hourly_data = {"timestamp": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end = pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq = pd.Timedelta(seconds=hourly.Interval()),
        inclusive = "left"
    ).tz_convert(tz=TIMEZONE)}

    for i, p in enumerate(COLUMNS):
        hourly_data[p] = hourly.Variables(i).ValuesAsNumpy().round()

    hourly_dataframe = pd.DataFrame(data=hourly_data).dropna(axis=0).set_index("timestamp")
    return hourly_dataframe

def filter_daytime(df):
    day_start = datetime.time(DAY_START)
    day_end = datetime.time(DAY_END)
    use_rows = (df.index > datetime.datetime.now(datetime.timezone.utc)) & (df.index.time >= day_start) & (df.index.time <= day_end)
    return df[use_rows]

def filter_time_window_max(df, hours=2):
    start_row = hours - 1
    hours_str = f"{hours}h"  # This won't combine days if already filtered to daytime
    # Mean would make more sense for direction but 10 degrees doesn't matter to us
    return(df.rolling(hours_str, center=True).max().iloc[start_row::hours,:])

def compress_to_diff(df):
    df.loc[:, WIND_DIRECTION] = (df[WIND_DIRECTION] / 10).round()
    diff = df.diff()
    diff.iloc[0,:] = df.iloc[0,:]
    return diff.astype(int)

def format_string(diff):
    str_diff = diff.astype(str)
    str_diff[diff == 0] = None
    str_diff.rename(
        columns={WIND_SPEED: "W", WIND_GUSTS: "G", WIND_DIRECTION: "D"},
        inplace=True)
    csv = str_diff.T.to_csv(header=False).replace("\n", "")
    return re.sub(r"([A-Z]),", r"\1", csv)

def concat(responses):
    dfs = []
    for res in responses:
        df = parse_location(res)
        daytime = filter_daytime(df)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=pd.errors.SettingWithCopyWarning)
            daytime["location"] = str((res.Latitude(), res.Longitude()))
        dfs.append(daytime)
    df = pd.concat(dfs)
    return df

def extract_message(df, location):
    # Send today's forecast for all locations with 2 hour resolution and tomorrow's with 4.

    # Prefix with a letter key for the location and a number for the start time
    loc_abbr = "".join([loc[0] for loc in location.split("_")])
    messages = [loc_abbr, df.index[0].strftime("%H").lstrip("0")]

    days = iter(df.groupby(lambda x: x.date()))

    def single_day_message(day, hours):
        for _, loc in day.groupby("location"):
            time_windowed_max = filter_time_window_max(loc.drop(columns="location"), hours=hours)
            if not len(time_windowed_max):
                continue
            diff = compress_to_diff(time_windowed_max)
            messages.append(format_string(diff))

    _, today = next(days)
    single_day_message(today, 2)

    messages.append("T")

    _, tomorrow = next(days)
    single_day_message(tomorrow, 4)

    return "".join(messages)
