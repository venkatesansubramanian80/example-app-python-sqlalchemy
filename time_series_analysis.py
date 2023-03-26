import pandas as pd
import datetime as dt
from datetime import timedelta


def process_row(row):
    metric_value_daily = [float(single_data) for single_data in row['metric_value_daily'].split('|')]
    day_range = list(pd.date_range((dt.datetime.now().date() - timedelta(days=len(metric_value_daily)-1)), dt.datetime.now().date(), freq='d'))
    df_telemetry_data_full = pd.DataFrame({
        'day_range': day_range,
        'metric_value_daily': metric_value_daily
    })
    df_telemetry_data_full = df_telemetry_data_full.loc[df_telemetry_data_full['metric_value_daily'] >= 0]
    df_miss_simulation = pd.DataFrame({
        'day_range': day_range
    })
    df_telemetry_data = df_miss_simulation.merge(df_telemetry_data_full, on='day_range', how='left')
    df_telemetry_data.set_index('day_range', inplace=True)
    df_weekly_telemetry = df_telemetry_data.resample('W').quantile(0.95)
    df_weekly_telemetry.rename(columns={'metric_value_daily': 'metric_value_weekly'}, inplace= True)
    df_monthly_telemetry = df_telemetry_data.resample('M').quantile(0.95)
    df_monthly_telemetry.rename(columns={'metric_value_daily': 'metric_value_monthly'}, inplace=True)
    df_joined = df_telemetry_data.join(df_weekly_telemetry).join(df_monthly_telemetry)
    print(len(df_telemetry_data))


df_data_from_csv = pd.read_csv('data_to_import.csv')
df_data_from_csv.apply(process_row, axis=1)
print(df_data_from_csv.columns)

