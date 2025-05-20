import argparse
import glob
import os
from datetime import datetime

import polars as pl
from returns.result import Failure, Result, Success


def combine_parquets(load_path: str, save_dir: str) -> Result[bool, Exception]:
    try:
        date_time = datetime.now()
        lf = pl.scan_parquet(load_path)
        os.makedirs(save_dir, exist_ok=True)
        lf.sink_parquet(
            f"{save_dir}/{os.path.basename(load_path.replace('*', date_time.strftime('%y%m%d_%H%M%S')))}"
        )
        return Success(load_path)
    except Exception as e:
        return Failure(e)


def delete_files(load_path: str) -> Result[bool, list]:
    fails = []
    files = glob.glob(load_path)

    for f in files:
        try:
            os.remove(f)
        except Exception as e:
            fails.append((f, e))

    if fails:
        return Failure(fails)
    return Success(True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combine multiple parquets into one.")
    parser.add_argument("--load-path", type=str, help="Path to the individual parquet files")
    parser.add_argument("--save-dir", type=str, help="The directory to save the combined file to")
    args = parser.parse_args()

    print(combine_parquets(args.load_path, args.save_dir).bind(delete_files))
