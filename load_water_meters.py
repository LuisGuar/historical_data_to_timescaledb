"""Load water meter readings from Excel into TimescaleDB."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
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
FRIENDLY_NAME_ROW: Final[int] = 2  # 1-based row number for descriptive headers
DATE_COLUMN: Final[str] = "Date"
SOURCE_SHEET: Final[str] = "Totaliser Reading"
DAYFIRST: Final[bool] = True


@dataclass(frozen=True)
class MeterConfig:
    topic: str
    friendly_name: str
    sheet_column: str


METER_CONFIGS: Final[tuple[MeterConfig, ...]] = (
    MeterConfig("Astellas/Primary/Main_Incoming_Water", "Main incoming water", "M1"),
    MeterConfig("Astellas/Auxiliary/Utilities-Pre_Sand_Filter", "Utilities - pre-sand filter", "M2"),
    MeterConfig("Astellas/Auxiliary/Utilities-Post_Sand_Filter", "Utilities - post-sand filter", "M4"),
    MeterConfig("Astellas/Auxiliary/FK506-Incoming_Post_PRW_Take_Off", "FK506 - Incoming post PRW take off", "M6"),
    MeterConfig("Astellas/Primary/FK506-Incoming", "FK506 - Incoming", "M5"),
    MeterConfig("Astellas/Primary/FK506-Take_Off_QC_Admin", "FK506 - take off for QC admin", "M7"),
    MeterConfig("Astellas/Auxiliary/FK506-PRW_Take_Off", "FK506 - PRW take off", "M8"),
    MeterConfig("Astellas/Primary/WWTP-Final_Effluent", "WWTP - Final Effluent", "M16"),
    MeterConfig("Astellas/Auxiliary/WWTP-I_Line", "WWTP - I Line", "M - I Line"),
    MeterConfig("Astellas/Auxiliary/WWTP-E_Line", "WWTP - E Line", "M - E Line"),
    MeterConfig("Astellas/Auxiliary/Utilities-Boiler_House_Top_Up", "Utilities - Boiler House top-up", "M3"),
    MeterConfig("Astellas/Auxiliary/QC_Admin-DCW_To_DHW", "QC Admin - DCW to DHW", "M11"),
    MeterConfig("Astellas/Auxiliary/QC_Admin-Take_Off_For_QC_Admin", "QC Admin - take off for QC admin", "M9"),
    MeterConfig("Astellas/Auxiliary/QC_Admin-Canteen", "QC Admin - Canteen", "M13"),
    MeterConfig("Astellas/Primary/ARK-Ground_Floor", "ARK - Ground floor", "M15"),
)


def normalize_label(label: str) -> str:
    """Normalize header labels for robust comparisons."""
    return re.sub(r"\s+", " ", (label or "").strip()).casefold()


def load_sheet(path: str) -> tuple[pd.DataFrame, dict[str, str]]:
    """Load the shared worksheet that contains all meter columns."""
    df = pd.read_excel(
        path,
        sheet_name=SOURCE_SHEET,
        skiprows=SKIP_ROWS,
    )
    df.columns = df.columns.astype(str).str.strip()

    header_rows = pd.read_excel(
        path,
        sheet_name=SOURCE_SHEET,
        header=None,
        nrows=FRIENDLY_NAME_ROW,
    )
    friendly_row_index = max(0, FRIENDLY_NAME_ROW - 1)
    friendly_row = header_rows.iloc[friendly_row_index]

    friendly_lookup: dict[str, str] = {}
    for idx, column in enumerate(df.columns):
        header_value = friendly_row.iloc[idx] if idx < len(friendly_row) else ""
        friendly_lookup[column] = re.sub(r"\s+", " ", str(header_value or "").strip())

    return df, friendly_lookup


def load_dataframe(
    sheet_df: pd.DataFrame,
    friendly_lookup: dict[str, str],
    meter: MeterConfig,
) -> pd.DataFrame:
    """Prepare the DataFrame for a specific meter column."""
    if DATE_COLUMN not in sheet_df.columns:
        raise ValueError(f"Column '{DATE_COLUMN}' not found in sheet '{SOURCE_SHEET}'.")
    if meter.sheet_column not in sheet_df.columns:
        raise ValueError(f"Column '{meter.sheet_column}' not found in sheet '{SOURCE_SHEET}'.")

    friendly_value = friendly_lookup.get(meter.sheet_column, "")
    if normalize_label(friendly_value) != normalize_label(meter.friendly_name):
        raise ValueError(
            f"Column '{meter.sheet_column}' header mismatch on row {FRIENDLY_NAME_ROW}: "
            f"expected '{meter.friendly_name}', found '{friendly_value}'."
        )

    df = sheet_df[[DATE_COLUMN, meter.sheet_column]].copy()
    df = df.rename(columns={meter.sheet_column: "M1"})

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
    df["topic"] = meter.topic
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

    sheet_df, friendly_lookup = load_sheet(EXCEL_PATH)

    total_inserted = 0
    for meter in METER_CONFIGS:
        try:
            df = load_dataframe(sheet_df, friendly_lookup, meter)
        except ValueError as exc:
            print(f"{meter.friendly_name}: skipped (column not found) -> {exc}")
            continue

        if df.empty:
            print(f"{meter.topic}: skipped (no valid rows after cleaning).")
            continue

        inserted = insert_dataframe(df)
        total_inserted += inserted
        print(f"{meter.topic}: inserted {inserted} rows into {TARGET_SCHEMA}.{TARGET_TABLE}.")

    if total_inserted == 0:
        print("No rows inserted from any sheet; nothing written.")
    else:
        print(f"Total rows inserted: {total_inserted} into {TARGET_SCHEMA}.{TARGET_TABLE}.")


if __name__ == "__main__":
    main()
