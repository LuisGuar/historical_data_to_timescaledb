"""Load water meter readings from Excel into TimescaleDB."""
from __future__ import annotations

import os
from typing import Final

import pandas as pd
from sqlalchemy import create_engine

EXCEL_PATH: Final[str] = r"E:\Integration\Projects\Astellas\Historical Data\Astellas - Water meters .xlsx"
DB_NAME: Final[str] = os.environ.get("TIMESCALE_DB_NAME", "appdata")
DB_USER: Final[str] = os.environ.get("TIMESCALE_DB_USER", "postgres")
DB_PASS: Final[str] = os.environ.get("TIMESCALE_DB_PASS", "Gallarus1.")
DB_HOST: Final[str] = os.environ.get("TIMESCALE_DB_HOST", "astellas-pw.nucleus4x.net")
DB_PORT: Final[str] = os.environ.get("TIMESCALE_DB_PORT", "5432")
DATABASE_URL: Final[str] = os.environ.get(
    "TIMESCALE_URL",
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
)
TARGET_SCHEMA: Final[str] = os.environ.get("TIMESCALE_SCHEMA", "public")
TARGET_TABLE: Final[str] = os.environ.get("TIMESCALE_TABLE", "waltero_tqv")
SKIP_ROWS: Final[int] = 4
USE_COLS: Final[tuple[str, str]] = ("Date", "M1")
DAYFIRST: Final[bool] = True


def load_dataframe(path: str) -> pd.DataFrame:
    """Read and clean the Excel file, returning the rows ready for insert."""
    df = pd.read_excel(path, skiprows=SKIP_ROWS, usecols=list(USE_COLS))
    df.columns = df.columns.astype(str).str.strip()

    df["M1"] = pd.to_numeric(df["M1"], errors="coerce")

    date_series = pd.to_datetime(df["Date"], dayfirst=DAYFIRST, errors="coerce")
    missing_dates = date_series.isna()
    if missing_dates.any():
        numeric_dates = pd.to_numeric(df.loc[missing_dates, "Date"], errors="coerce")
        convertible = missing_dates & numeric_dates.notna()
        date_series.loc[convertible] = pd.to_datetime(
            numeric_dates.loc[convertible],
            unit="D",
            origin="1899-12-30",
        )
    df["Date"] = date_series

    df = df.dropna(subset=["Date", "M1"])

    df = df.rename(columns={"Date": "time", "M1": "value"})
    df["field_name"] = "totalValue"
    df["topic"] = "Astellas/Primary/Main_Incoming_Water"
    df["quality_code"] = 192

    df = df[["time", "field_name", "topic", "value", "quality_code"]]
    df = df.sort_values("time").reset_index(drop=True)

    return df


def insert_dataframe(df: pd.DataFrame) -> int:
    """Insert the DataFrame into TimescaleDB and return the number of rows."""
    engine = create_engine(DATABASE_URL)
    with engine.begin() as connection:
        df.to_sql(
            TARGET_TABLE,
            connection,
            schema=TARGET_SCHEMA,
            if_exists="append",
            index=False,
        )
    return len(df)


def main() -> None:
    if not os.path.isfile(EXCEL_PATH):
        raise FileNotFoundError(f"Excel file not found at: {EXCEL_PATH}")

    df = load_dataframe(EXCEL_PATH)
    if df.empty:
        print("No rows to insert after cleaning; nothing written.")
        return

    inserted = insert_dataframe(df)
    print(f"Inserted {inserted} rows into {TARGET_SCHEMA}.{TARGET_TABLE}.")


if __name__ == "__main__":
    main()
