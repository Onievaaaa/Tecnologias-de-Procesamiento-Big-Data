import os
from pathlib import Path
import boto3
import pandas as pd
from TradingviewData import TradingViewData, Interval  

SYMBOL = "BTCUSD"
EXCHANGE = "BINANCE"
INTERVAL = Interval.daily
N_BARS = 1461  

OUT_DIR = Path("data_out") 

DATE_FMT = "%Y-%m-%d"

S3_BUCKET = "btc-imat-bucket"
S3_PREFIX = "btc"  
AWS_PROFILE = "Comillas-BIGDATA-Alumnos-354918392915"
AWS_REGION = "eu-south-2"  

def split_by_year_month(df):
    if df.index.name == "datetime":
        df2 = df.reset_index()
    else:
        df2 = df.copy()


    if "symbol" in df2.columns:
        df2 = df2.drop(columns=["symbol"])

    df2["datetime"] = pd.to_datetime(df2["datetime"], errors="coerce")
    df2 = df2.dropna(subset=["datetime"]).sort_values("datetime")


    df2["date"] = df2["datetime"].dt.strftime(DATE_FMT)
    df2["year"] = df2["datetime"].dt.year
    df2["month"] = df2["datetime"].dt.month

    out = {}
    for (y, m), g in df2.groupby(["year", "month"], sort=True):
        cols = ["date"] + [c for c in g.columns if c not in ("datetime", "year", "month", "date")]
        out[(int(y), int(m))] = g[cols].reset_index(drop=True)

    return out


def write_csvs(groups, base_dir):
    base_dir.mkdir(parents=True, exist_ok=True)
    written = []

    for (y, m), g in groups.items():
        year_dir = base_dir / f"{y}"
        year_dir.mkdir(parents=True, exist_ok=True)

        fpath = year_dir / f"{m:02d}.csv"  
        g.to_csv(fpath, index=False)
        written.append(fpath)

    return written


def upload_folder_to_s3(local_base, bucket, prefix, profile, region):
    session_kwargs = {"profile_name": profile}
    if region:
        session_kwargs["region_name"] = region

    session = boto3.Session(**session_kwargs)
    s3 = session.client("s3")

    for root, _, files in os.walk(local_base):
        for fn in files:
            if not fn.lower().endswith(".csv"):
                continue

            full_path = Path(root) / fn
            rel_path = full_path.relative_to(local_base).as_posix()  
            parts = rel_path.split("/")  
            year = parts[0]
            fname = parts[-1]            
            month = fname.split(".")[0]  

            new_rel_path = f"year={year}/month={month}/{fname}"
            key = f"{prefix.strip('/')}/{new_rel_path}".lstrip("/")

            s3.upload_file(str(full_path), bucket, key)
            print(f"Subido: s3://{bucket}/{key}")


def main():
    tv = TradingViewData()
    df = tv.get_hist(
        symbol=SYMBOL,
        exchange=EXCHANGE,
        interval=INTERVAL,
        n_bars=N_BARS,
        extended_session=False)

    groups = split_by_year_month(df)
    files = write_csvs(groups, OUT_DIR)
    print(f"CSVs creados: {len(files)} (base: {OUT_DIR})")

    upload_folder_to_s3(
        local_base=OUT_DIR,
        bucket=S3_BUCKET,
        prefix=S3_PREFIX,
        profile=AWS_PROFILE,
        region=AWS_REGION)


if __name__ == "__main__":
    main()
