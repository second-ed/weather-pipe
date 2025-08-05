import argparse
import datetime as dt
import glob
import os

import polars as pl

from weather_pipe.domain.result import Err, Ok, Result, safe


@safe
def combine_parquets(load_path: str, save_dir: str) -> str:
    date_time = dt.datetime.now(tz=dt.timezone.utc)
    lf = pl.scan_parquet(load_path)
    os.makedirs(save_dir, exist_ok=True)
    lf.sink_parquet(
        f"{save_dir}/{os.path.basename(load_path.replace('*', date_time.strftime('%y%m%d_%H%M%S')))}",
    )
    return load_path


def delete_files(load_path: str) -> Result[bool, list]:
    fails = []
    files = glob.glob(load_path)

    for f in files:
        try:
            os.remove(f)
        except Exception as e:  # noqa: PERF203 BLE001
            fails.append((f, e))

    if fails:
        return Err(fails)
    return Ok(inner_value=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combine multiple parquets into one.")
    parser.add_argument("--load-path", type=str, help="Path to the individual parquet files")
    parser.add_argument("--save-dir", type=str, help="The directory to save the combined file to")
    args = parser.parse_args()
    print(args)  # noqa: T201
    print(combine_parquets(args.load_path, args.save_dir).bind(delete_files))  # noqa: T201
