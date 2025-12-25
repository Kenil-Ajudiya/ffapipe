#!/usr/bin/env python3

"""
cand_filter.py

Usage: python cand_filter.py <path to the directory>
Example: python cand_filter.py /lustre_data/spotlight/data/TST_FFA_20250824_111336/FFAPipeData/state/J0901-4046_20250824_114802


Filters and groups GPFold FFA candidate CSVs across beams, detects harmonics, flags likely RFI.

-- Global level filtering

Kshitij 
Sept 2025
"""

import sys
from glob import glob
import re
import argparse
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from utilities import MultiColorFormatter

def configure_logger(name: str = "cand_filter", level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(MultiColorFormatter())
        logger.addHandler(stream_handler)
    return logger

def extract_cand_number(fname: str) -> int:
    """Extract candidate number from a filename like 'riptide_cand_0001.h5' -> 1"""
    m = re.search(r'(\d+)(?=\.h5$)', fname)
    return int(m.group(1).lstrip('0') or '0') if m else None

def read_summary_csv(csv_path: Path) -> pd.DataFrame:
    """Read summary.csv assuming space-separated columns with header in first row."""
    df = pd.read_csv(csv_path, sep=r"\s+", engine="python")
    return df

def harmonic_filter(df, period_tol, dm_tol, harmonic_max):
    """ Group harmonically related candidates and keep only the highest SNR from each group.
    Assumes df is sorted in descending order of period (longest period first)."""

    if df.empty or len(df) == 1:
        return df

    used = np.zeros(len(df), dtype=bool)
    groups = []

    periods = df['period'].values
    dms = df['dm'].values

    for i, p1 in enumerate(periods):
        if used[i]:
            continue

        group_indices = [i]
        used[i] = True

        for j in range(i + 1, len(periods)):
            if used[j]:
                continue

            dm_diff = abs(dms[i] - dms[j])
            if dm_diff > dm_tol:
                continue

            p2 = periods[j]

            # Check if harmonic (P/n)
            for n in range(2, harmonic_max + 1):
                if abs(p2 - p1 / n) <= period_tol:
                    group_indices.append(j)
                    used[j] = True
                    break

        groups.append(group_indices)

    # From each harmonic group, select highest-SNR candidate
    kept_rows = []
    for group in groups:
        best_idx = df.iloc[group]['snr'].idxmax()
        kept_rows.append(best_idx)

    filtered_df = df.loc[kept_rows].reset_index(drop=True)
    logging.getLogger("cand_filter").info(f"Harmonic filtering: kept {len(filtered_df)} out of {len(df)} candidates.")

    return filtered_df

def filter_cand(df,
                min_period=1.0,       # seconds
                min_dm=5.0,           # pc cm^-3
                min_snr=7.0,
                min_snr_final=10.0,          
                dm_spread_tol = 40.0,    #for RFI 
                period_tol= 0.02,    #period tol for grouping
                period_rel_tol=0.01, #tolerance for harmonics
                dm_tol=5.0,     #for harmonics
                harmonic_max=20):      # max harmonic to check

    logger = logging.getLogger("cand_filter")
    logger.info("Starting beam filtering.")

    # ---------- Input validation ----------
    if df is None or df.empty:
        logger.warning("Input DataFrame is empty. Nothing to filter.")
        return pd.DataFrame()

    initial_count = len(df)
    logger.info(f"Initial candidates: {initial_count}")

    # ---------- Sanity check ----------
    # Drop rows with NaN
    df = df.dropna(subset=['period', 'dm', 'snr']) # This is unnecessary: Kenil

    # Apply threshold cuts
    df = df[(df['period'] >= min_period) &
            (df['dm']    >= min_dm) &
            (df['snr']   >= min_snr)]

    after_sanity = len(df)
    logger.info(f"After sanity filters: {after_sanity} (removed {initial_count - after_sanity})")

    # ---------- Duplication ----------
    # Group candidates by period within tolerance
    df['period_group'] = np.round(df['period'] / period_tol) * period_tol

    kept_rows = []

    # For each period group
    for __, group in df.groupby('period_group', group_keys=False):
        # Compute DM spread in this group
        dm_spread = group['dm'].max() - group['dm'].min()

        # If DM spread is too large, drop the whole group (likely RFI)
        if dm_spread > dm_spread_tol:
            continue

        # Otherwise keep the highest-SNR candidate from this group
        best_idx = group['snr'].idxmax()
        kept_rows.append(best_idx)

    # Keep only selected rows
    filtered_df = df.loc[kept_rows].reset_index(drop=True)
    filtered_df = filtered_df.drop(columns=['period_group'])

    # sort by period
    filtered_df = filtered_df.sort_values(by='period', ascending=False).reset_index(drop=True)

    logger.info(f"Candidates after filtering : {len(filtered_df)} ")

    logger.info("Starting harmonic filtering.")
    har_fil_df = harmonic_filter(filtered_df, period_rel_tol, dm_tol, harmonic_max)

    # sort by snr
    df_out = har_fil_df.sort_values(by='snr', ascending=False).reset_index(drop=True)

    # Apply snr cuts
    df_out = df_out[(df_out['snr'] >= min_snr_final)]
    logger.info(f"Candidates after final snr filter : {len(df_out)} ")

    return df_out

def main():
    parser = argparse.ArgumentParser(prog='cand_filter', description="Filter and group FFA candidates across beams")
    parser.add_argument("root_path", type=Path, help="Root directory containing beam subdirectories")
    args = parser.parse_args()

    logger = configure_logger("cand_filter")

    root_path: Path = args.root_path
    if not root_path.is_dir():
        logger.error(f"{root_path} is not a directory.")
        sys.exit(1)

    # beam_pattern = re.compile(r'^BM(\d+)\.down_RFI_Mitigated_01$')
    # beam_dirs = sorted([d for d in root_path.iterdir() if d.is_dir() and beam_pattern.match(d.name)])
    beam_dirs = [Path(d) for d in glob(str(root_path / "BM*")) if Path(d).is_dir()]

    logger.info(f"Found {len(beam_dirs)} beam directories.")

    if not beam_dirs:
        logger.warning("No beam directories found.")
        sys.exit(0)

    combined_rows = []
    for beam_dir in beam_dirs:
        # beam_match = beam_pattern.match(beam_dir.name)
        # beam_num = int(beam_match.group(1))
        beam_num = int(beam_dir.name[2:].split('.')[0])  # Extract beam number from directory name
        csv_path = beam_dir / "candidates" / "summary.csv"

        if not csv_path.exists():
            logger.warning(f"Missing {csv_path}")
            continue

        df = read_summary_csv(csv_path)
        # print(f"Read {len(df)} candidates from beam {beam_num}")

        df.insert(0, "cand", df["fname"].apply(extract_cand_number)) #get candidate number
        df.insert(0, "beam", beam_num) #add beam number
        
        combined_rows.append(df)

    
    if not combined_rows:
        logger.warning("No candidates found in any beam directories.")
        sys.exit(0)

    # COMBINE
    combined_df = pd.concat(combined_rows, ignore_index=True)

    out_path = root_path / "combined_candidates.csv"
    combined_df.to_csv(out_path, index=False)
    logger.info(f"Combined {len(combined_df)} candidates from {len(combined_rows)} beams.")
    logger.info(f"Output saved to: {out_path}")

    # FILTERING - TEST
    filtered_df = filter_cand(combined_df) 

    out_path = root_path / "filtered_candidates.csv"
    filtered_df.to_csv(out_path, index=False)
    logger.info(f"Output saved to: {out_path}")

if __name__ == "__main__":
    main()
