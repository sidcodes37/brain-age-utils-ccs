# Create a new CSV that keeps only rows whose filepath points to an existing file under a local base directory.

import pandas as pd
from pathlib import Path

INPUT_CSV = "../../outputs/TUH-EEG_data_selective_16.csv"
OUTPUT_CSV = "../../outputs/current_files_270s.csv"
BASE_PATH = Path("")
MIN_DURATION = 270


def make_local_path(orig_fp: str) -> Path:
    s = str(orig_fp).strip()
    # remove all leading "../"
    while s.startswith("../") or s.startswith("./"):
        s = s[3:]
    return BASE_PATH.joinpath(s).resolve()


def main():
    df = pd.read_csv(INPUT_CSV)
    # Build local absolute paths
    df["_local_path"] = df["filepath"].apply(make_local_path)

    # Keep only rows where the file exists on disk
    mask = df["_local_path"].apply(lambda p: p.exists())
    df_keep = df.loc[mask].copy()

    # Replace filepath column with the full local path (string)
    df_keep["filepath"] = df_keep["_local_path"].astype(str)

    # Drop helper column and write CSV with the original column order
    df_keep = df_keep.drop(columns=["_local_path"])

    filtered_duration = df_keep[df_keep["duration"] > MIN_DURATION]
    filtered_duration.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(filtered_duration)} rows (duration > 270) to {OUTPUT_CSV}")

    df_keep.to_csv(OUTPUT_CSV, index=False)
    print(f"{len(df_keep)} rows written to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
