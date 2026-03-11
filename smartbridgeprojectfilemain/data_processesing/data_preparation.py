#!/usr/bin/env python3
"""
Strategic Product Placement Analysis — Data Preparation Pipeline
Loads raw Product_Positioning.csv, cleans and engineers features for Tableau.
"""

from __future__ import annotations

import os
import sys

import pandas as pd

# Project root (parent of data_processing/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATASET_DIR = os.path.join(PROJECT_ROOT, "dataset")
RAW_CSV = os.path.join(DATASET_DIR, "Product_Positioning.csv")
CLEAN_CSV = os.path.join(DATASET_DIR, "clean_product_data.csv")


def load_raw_data(path: str) -> pd.DataFrame:
    """Load CSV with UTF-8; tolerate BOM if present."""
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Dataset not found: {path}")
    return pd.read_csv(path, encoding="utf-8-sig")


def display_dataset_info(df: pd.DataFrame) -> None:
    """Print shape, dtypes, head, and describe for key numeric columns."""
    print("=" * 60)
    print("DATASET INFO")
    print("=" * 60)
    print(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns\n")
    print("Column dtypes:")
    print(df.dtypes)
    print("\nFirst 5 rows:")
    print(df.head())
    print("\nMissing values per column:")
    print(df.isna().sum())
    print("\nDuplicate rows (before drop):", df.duplicated().sum())


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate rows; keep first occurrence."""
    before = len(df)
    df = df.drop_duplicates(keep="first").reset_index(drop=True)
    after = len(df)
    print(f"\nDuplicates removed: {before - after} (remaining: {after})")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values:
    - Numeric: median imputation
    - Categorical: mode imputation; if still missing, fill with 'Unknown'
    """
    df = df.copy()
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    categorical_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()

    for col in numeric_cols:
        if df[col].isna().any():
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            print(f"Filled NaN in '{col}' with median: {median_val}")

    for col in categorical_cols:
        if df[col].isna().any():
            mode_series = df[col].mode()
            fill_val = mode_series.iloc[0] if len(mode_series) else "Unknown"
            df[col] = df[col].fillna(fill_val)
            print(f"Filled NaN in '{col}' with: {fill_val}")

    return df


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns for Tableau-friendly names (no apostrophes/spaces)."""
    rename_map = {
        "Competitor's Price": "Competitor_Price",
        "Product ID": "Product_ID",
        "Product Position": "Product_Position",
        "Foot Traffic": "Foot_Traffic",
        "Consumer Demographics": "Consumer_Demographics",
        "Product Category": "Product_Category",
        "Sales Volume": "Sales_Volume",
    }
    return df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})


def convert_numeric_types(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure Price, Competitor_Price, Sales_Volume, Product_ID are numeric."""
    df = df.copy()
    int_cols = ["Product_ID", "Sales_Volume"]
    float_cols = ["Price", "Competitor_Price"]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Features for Tableau:
    - Price gap vs competitor
    - Promotion as 0/1
    - Seasonal as readable label
    - Foot traffic ordinal for sorting
    """
    df = df.copy()

    if "Price" in df.columns and "Competitor_Price" in df.columns:
        df["Price_vs_Competitor_Gap"] = (df["Price"] - df["Competitor_Price"]).round(2)
        df["Price_Higher_Than_Competitor"] = (df["Price_vs_Competitor_Gap"] > 0).map(
            {True: "Yes", False: "No"}
        )

    if "Promotion" in df.columns:
        df["Promotion_Flag"] = df["Promotion"].map({"Yes": 1, "No": 0}).fillna(0).astype(int)

    if "Seasonal" in df.columns:
        df["Season_Label"] = df["Seasonal"].map({"Yes": "Seasonal", "No": "Non-Seasonal"}).fillna("Unknown")

    if "Foot_Traffic" in df.columns:
        traffic_order = {"Low": 1, "Medium": 2, "High": 3}
        df["Foot_Traffic_Order"] = df["Foot_Traffic"].map(traffic_order).fillna(0).astype(int)

    return df


def validate_for_tableau(df: pd.DataFrame) -> None:
    """Confirm columns needed for requested visualizations exist."""
    required = [
        "Product_Category",
        "Price",
        "Competitor_Price",
        "Consumer_Demographics",
        "Product_Position",
        "Seasonal",
        "Foot_Traffic",
        "Sales_Volume",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print("Warning: missing columns for some Tableau charts:", missing)
    else:
        print("All Tableau-oriented columns present.")


def main() -> int:
    os.chdir(PROJECT_ROOT)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Loading: {RAW_CSV}\n")

    df = load_raw_data(RAW_CSV)
    display_dataset_info(df)

    df = remove_duplicates(df)
    df = normalize_column_names(df)
    df = convert_numeric_types(df)
    df = handle_missing_values(df)
    df = feature_engineering(df)

    validate_for_tableau(df)

    df.to_csv(CLEAN_CSV, index=False)
    print(f"\nCleaned dataset saved: {CLEAN_CSV}")
    print(f"Final shape: {df.shape}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
