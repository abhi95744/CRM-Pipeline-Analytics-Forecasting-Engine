import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

INPUT_CSV = "crm_leads.csv"
OUTPUT_DIR = "output"
FORECAST_WEEKS = 4
DATE_COLS = ["created_at", "mql_at", "sql_at", "won_at"]
CATEGORICAL_COLS = ["channel", "region"]
ID_COL = "lead_id"

def ensure_output_dir(path):
    os.makedirs(path, exist_ok=True)

def parse_date(s):
    if pd.isna(s) or s == '':
        return pd.NaT
    return pd.to_datetime(s, errors="coerce", utc=True)

def get_week_start(dt):
    if pd.isna(dt):
        return pd.NaT
    d = dt.tz_convert(None) if hasattr(dt, "tz_convert") else dt
    try:
        d = d.tz_localize(None) if hasattr(d, "tz_localize") else d
    except:
        pass
    return (d - pd.to_timedelta(d.weekday(), unit="D")).normalize()

def safe_rate(num, den):
    return num / den if den > 0 else 0.0

def load_and_clean_data(csv_path):
    df = pd.read_csv(csv_path)

    for col in DATE_COLS:
        if col in df.columns:
            df[col] = df[col].apply(parse_date)
        else:
            df[col] = pd.NaT

    for col in CATEGORICAL_COLS:
        if col not in df.columns:
            df[col] = "Unknown"
        df[col] = df[col].fillna("Unknown").astype(str).str.strip()

    df[ID_COL] = df[ID_COL].astype(str)
    return df

def add_derived_fields(df):
    df["week_created"] = df["created_at"].apply(get_week_start)
    df["week_mql"] = df["mql_at"].apply(get_week_start)
    df["week_sql"] = df["sql_at"].apply(get_week_start)
    df["week_won"] = df["won_at"].apply(get_week_start)

    today = pd.Timestamp.utcnow().normalize()
    end_date = df["won_at"].fillna(today)
    df["lead_age"] = (end_date - df["created_at"]).dt.days.clip(lower=0)
    return df

def compute_weekly_metrics(df):
    created = df.groupby("week_created", dropna=True).agg(new_leads=(ID_COL, "nunique"))
    mql = df.groupby("week_mql", dropna=True).agg(mql=(ID_COL, "nunique"))
    sql = df.groupby("week_sql", dropna=True).agg(sql=(ID_COL, "nunique"))
    won = df.groupby("week_won", dropna=True).agg(won=(ID_COL, "nunique"))

    summary = created.join([mql, sql, won], how="outer").fillna(0).sort_index()

    summary["mql_rate"] = summary.apply(lambda r: safe_rate(r["mql"], r["new_leads"]), axis=1)
    summary["sql_rate"] = summary.apply(lambda r: safe_rate(r["sql"], r["mql"]), axis=1)
    summary["win_rate"] = summary.apply(lambda r: safe_rate(r["won"], r["sql"]), axis=1)

    avg_age = df.dropna(subset=["week_created"]).groupby("week_created")["lead_age"].mean()
    summary = summary.join(avg_age.rename("avg_lead_age"), how="left").fillna({"avg_lead_age": 0})

    summary = summary.reset_index()
    summary.rename(columns={"index": "week_created"}, inplace=True)

    return summary

def breakdown_by_dimension(df, dim_col):
    grp_cols = [dim_col, "week_created"]

    base = df.groupby(grp_cols, dropna=True).agg(new_leads=(ID_COL, "nunique")).reset_index()
    mql_df = df.groupby([dim_col, "week_mql"], dropna=True).agg(mql=(ID_COL, "nunique")).reset_index()
    mql_df.rename(columns={"week_mql": "week_created"}, inplace=True)

    sql_df = df.groupby([dim_col, "week_sql"], dropna=True).agg(sql=(ID_COL, "nunique")).reset_index()
    sql_df.rename(columns={"week_sql": "week_created"}, inplace=True)

    won_df = df.groupby([dim_col, "week_won"], dropna=True).agg(won=(ID_COL, "nunique")).reset_index()
    won_df.rename(columns={"week_won": "week_created"}, inplace=True)

    result = base.merge(mql_df, on=grp_cols, how="outer")
    result = result.merge(sql_df, on=grp_cols, how="outer")
    result = result.merge(won_df, on=grp_cols, how="outer")
    result = result.fillna(0).sort_values(grp_cols)

    result["mql_rate"] = result.apply(lambda r: safe_rate(r["mql"], r["new_leads"]), axis=1)
    result["sql_rate"] = result.apply(lambda r: safe_rate(r["sql"], r["mql"]), axis=1)
    result["win_rate"] = result.apply(lambda r: safe_rate(r["won"], r["sql"]), axis=1)

    return result

def generate_forecast(weekly_df, periods=4, window=4):
    if weekly_df.empty:
        return pd.DataFrame(columns=["week_start", "forecast_leads", "forecast_wins"])

    def ma_forecast(series, w, p):
        vals = series.dropna().astype(float)
        if len(vals) == 0:
            return [0] * p
        avg = vals.tail(w).mean() if len(vals) >= w else vals.mean()
        return [avg] * p

    last_week = weekly_df["week_created"].max()
    future_weeks = [last_week + timedelta(weeks=i) for i in range(1, periods + 1)]

    leads_forecast = ma_forecast(weekly_df.set_index("week_created")["new_leads"], window, periods)
    wins_forecast = ma_forecast(weekly_df.set_index("week_created")["won"], window, periods)

    return pd.DataFrame({
        "week_start": future_weeks,
        "forecast_leads": leads_forecast,
        "forecast_wins": wins_forecast
    })

def export_results(weekly_df, channel_df, region_df, forecast_df, output_dir):
    ensure_output_dir(output_dir)

    weekly_df.to_csv(f"{output_dir}/weekly_summary.csv", index=False)
    channel_df.to_csv(f"{output_dir}/channel_breakdown.csv", index=False)
    region_df.to_csv(f"{output_dir}/region_breakdown.csv", index=False)
    forecast_df.to_csv(f"{output_dir}/forecast.csv", index=False)

def run_pipeline():
    df = load_and_clean_data(INPUT_CSV)
    df = add_derived_fields(df)

    weekly_summary = compute_weekly_metrics(df)
    channel_breakdown = breakdown_by_dimension(df, "channel")
    region_breakdown = breakdown_by_dimension(df, "region")

    forecast = generate_forecast(weekly_summary, FORECAST_WEEKS)

    export_results(weekly_summary, channel_breakdown, region_breakdown, forecast, OUTPUT_DIR)

    print(f"Pipeline completed. {len(df)} records processed.")
    print(f"Generated {len(weekly_summary)} weeks of data.")
    print(f"Results saved to {OUTPUT_DIR}/")

    return weekly_summary, channel_breakdown, region_breakdown, forecast

if __name__ == "__main__":
    weekly, channel, region, forecast = run_pipeline()
