# Historical Data to TimescaleDB

Python utilities for ingesting Astellas water meter readings from the shared Excel workbook into TimescaleDB. The loader reads each configured meter column, validates the friendly header text, cleans the data, and appends the results into the target hypertable.

## Project Structure

```
load_water_meters.py  # Main ETL script
```

## Requirements

- Python 3.10+
- Pandas
- SQLAlchemy
- psycopg2 (installed automatically when using `psycopg2-binary`)

Install dependencies with:

```bash
python -m venv .venv
. .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install pandas sqlalchemy psycopg2-binary
```

## Configuration

Set the following environment variables (or rely on the defaults baked into the script):

| Variable | Description | Default |
|----------|-------------|---------|
| `TIMESCALE_DB_NAME` | Database name | `appdata` |
| `TIMESCALE_DB_USER` | TimescaleDB user | `postgres` |
| `TIMESCALE_DB_PASS` | Password | `Gallarus1.` |
| `TIMESCALE_DB_HOST` | Hostname | `astellas-pw.nucleus4x.net` |
| `TIMESCALE_DB_PORT` | Port | `5432` |
| `TIMESCALE_URL` | Full SQLAlchemy URL (overrides all above) | constructed from the values above |
| `TIMESCALE_SCHEMA` | Target schema | `public` |
| `TIMESCALE_TABLE` | Target table | `waltero_tqv` |

Update the `EXCEL_PATH` constant in `load_water_meters.py` so it points to the local copy of *Astellas - Water meters .xlsx*.

## Running the Loader

```bash
python load_water_meters.py
```

The script will:

1. Validate the presence of the Excel workbook.
2. Read the configured sheet (`Totaliser Reading`), skipping the descriptive header rows.
3. Cross-check each configured meter column with the friendly header text.
4. Clean, sort, and normalize the data.
5. Insert rows into `TIMESCALE_SCHEMA.TIMESCALE_TABLE` and print a per-meter summary.

If any column headers are missing or mismatched, the script reports the issue and continues with the remaining meters.

## Development Workflow

1. Create a feature branch: `git checkout -b feature/<short-description>`.
2. Make changes, add tests or validations where possible.
3. Format/lint if tools are available.
4. Commit using descriptive messages.
5. Push and open a pull request on GitHub for review before merging to `main`.

## Security Notes

- Rotate Personal Access Tokens (PATs) frequently and avoid committing them.
- Consider moving sensitive defaults (like database passwords) into environment variables or a secrets manager before production use.

