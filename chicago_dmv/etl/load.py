import os
from pathlib import Path
import pandas as pd
from psycopg import connect, sql


def _parse_table_name(table_name: str) -> tuple[str, str]:
    """Parse schema-qualified table names, defaulting schema to public."""
    parts = [part.strip().strip('"') for part in table_name.split('.') if part.strip()]

    if len(parts) == 1:
        return "public", parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]

    raise ValueError(f"Invalid table name format: {table_name}")


def _get_database_url() -> str | None:
    database_url = os.environ.get("NEON_DATABASE_URL")
    if database_url:
        return database_url

    # Fallback for local runs when .env is present but not sourced in shell.
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return None

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        if key.strip() == "NEON_DATABASE_URL":
            return value.strip().strip('"').strip("'")

    return None

def load_data(df: pd.DataFrame, table_name: str):
    """
    Load transformed data into Neon (PostgreSQL) table using bulk insert.
    :param df: pandas dataframe, transformed data to be loaded
    :param table_name: str, schema-qualified target table in PostgreSQL
    :return: number of inserted rows or None if failed
    """
    
    # Mapping of columns per table to ensure schema alignment
    schema_mapping = {
        'crash': ['CRASH_UNIT_ID', 'CRASH_ID', 'PERSON_ID', 'VEHICLE_ID', 'NUM_UNITS', 'TOTAL_INJURIES'],
        'vehicle': ['CRASH_UNIT_ID', 'CRASH_ID', 'CRASH_DATE', 'VEHICLE_ID', 'VEHICLE_MAKE', 'VEHICLE_MODEL', 'VEHICLE_YEAR', 'VEHICLE_TYPE'],
        'person': ['PERSON_ID', 'CRASH_ID', 'CRASH_DATE', 'PERSON_TYPE', 'VEHICLE_ID', 'PERSON_SEX', 'PERSON_AGE']
    }
    source_aliases = {
        'CRASH_ID': ['CRASH_RECORD_ID'],
        'TOTAL_INJURIES': ['INJURIES_TOTAL'],
        'VEHICLE_MAKE': ['MAKE'],
        'VEHICLE_MODEL': ['MODEL'],
        'VEHICLE_TYPE': ['UNIT_TYPE'],
        'PERSON_AGE': ['AGE'],
    }

    database_url = _get_database_url()
    if not database_url:
        raise ValueError("Environment variable NEON_DATABASE_URL is not set.")

    table_key = table_name.split('.')[-1].strip().strip('"').lower()
    if table_key not in schema_mapping:
        raise ValueError(f"Postgre Data Table {table_name} does not exist in this pipeline.")

    schema_name, raw_table_name = _parse_table_name(table_name)
    selected_columns = schema_mapping[table_key]
    db_columns = [column.lower() for column in selected_columns]

    # Build an aligned DataFrame to match table schema with fallback aliases.
    aligned_data: dict[str, pd.Series] = {}
    for column in selected_columns:
        if column in df.columns:
            aligned_data[column] = df[column]
            continue

        alias_column = next((alias for alias in source_aliases.get(column, []) if alias in df.columns), None)
        if alias_column:
            aligned_data[column] = df[alias_column]
        else:
            aligned_data[column] = pd.Series([None] * len(df), index=df.index)

    aligned_df = pd.DataFrame(aligned_data)

    # Convert DataFrame to DB-ready tuples and replace NaN/NaT with None.
    clean_df = aligned_df.astype(object).where(pd.notna(aligned_df), None)
    data_to_insert = list(clean_df.itertuples(index=False, name=None))

    if not data_to_insert:
        print(f"No data to load into {table_name}.")
        return 0

    try:
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(schema_name, raw_table_name),
            sql.SQL(", ").join(sql.Identifier(column) for column in db_columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in selected_columns),
        )

        with connect(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, data_to_insert)
            conn.commit()

        print(f"Successful load of {len(data_to_insert)} records to {table_name}.")
        return len(data_to_insert)
    except Exception as e:
        print(f"Error during load execution: {e}")
        return None
