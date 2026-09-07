"""
generate_new_rows.py

Builds New_Rows.hyper -- a small data source that incremental_update.py reads
its brand-new rows from, instead of hardcoding them in Python. 
It is the new-rows counterpart to generate_updates.py.

The output is a single table, "public"."New_Rows", with EXACTLY the same
21-column shape as the "Extract" table in Finished_Merged.hyper. Matching the
shape lets incremental_update.py append the rows with one engine-side
`INSERT INTO ... SELECT * FROM ...`, so no row data ever crosses into Python.

Rather than re-declare all 21 columns by hand (and risk drifting from the real
schema), this script derives the table definition straight from the live
extract via catalog.get_table_definition(...) -- the same lookup the original
add-rows example used -- then creates an identically-shaped table under a new
name.

Usage (from the project root):
    python3 scripts/generate_new_rows.py
"""

import datetime
import os

from tableauhyperapi import (
    HyperProcess,
    Connection,
    Telemetry,
    CreateMode,
    SchemaName,
    TableName,
    TableDefinition,
    Inserter,
)

# --- Configuration -----------------------------------------------------------
# The extract we copy the schema from, and the payload file we produce. 
# We open the source directly (read-only) just to read its table definition; the 
# payload table is created as "public"."New_Rows" in the file this script owns.
SOURCE_FILE = "Finished_Merged.hyper"
NEWROWS_FILE = "New_Rows.hyper"
EXTRACT_TABLE = TableName("public", "Extract")
NEWROWS_TABLE = TableName("public", "New_Rows")

# Resolve paths relative to this file (see update_existing_rows.py for the why):
# this script lives in scripts/ and reads/writes .hyper files in the sibling
# data/ folder -- both the source extract and the payload we build live there.
HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(HERE), "data")
SOURCE_PATH = os.path.join(DATA_DIR, SOURCE_FILE)
NEWROWS_PATH = os.path.join(DATA_DIR, NEWROWS_FILE)

# -----------------------------------------------------------------------------
# What if the new rows come from Snowflake, not this hardcoded list?
# -----------------------------------------------------------------------------
#
# Option 1 -- official Python connector (simplest; fine for modest volumes):
#
#     import snowflake.connector                    # pip install snowflake-connector-python
#     sf = snowflake.connector.connect(
#         account=..., user=..., password=...,      # or key-pair / SSO / OAuth
#         warehouse=..., database=..., schema=...,
#     )
#     cur = sf.cursor()
#     cur.execute(                                  # list columns in the EXTRACT's order
#         'SELECT "Chain Id", "Match Id", /* ... */ "Turnover Origin" '
#         'FROM NEW_MATCH_CHAINS'                    # your Snowflake table/view
#     )
#     new_rows = cur.fetchall()                      # -> use in place of NEW_ROWS below
#
#   The Inserter step further down stays the same: `inserter.add_rows(new_rows)`.
#   Just make sure the SELECT returns columns in the extract's order and that the
#   types line up with new_def (DATE columns as datetime.date, etc.).
#
# Option 2 -- bulk via Parquet (better for large volumes; keeps rows out of
#   Python entirely): in Snowflake, `COPY INTO <stage> ... FILE_FORMAT=(TYPE=PARQUET)`
#   to unload the query, then let the Hyper engine read that file directly with
#   `COPY "public"."New_Rows" FROM 'new_rows.parquet' WITH (FORMAT parquet)` (see
#   the Hyper `COPY` / `external()` docs). incremental_update.py is untouched --
#   it still just attaches the resulting New_Rows.hyper.
#
# Either way, incremental_update.py's `INSERT INTO ... SELECT *` never changes;
# only where THIS file gets its rows does.
# -----------------------------------------------------------------------------

# --- The payload -------------------------------------------------------------
# Brand-new rows to append: a fictional future match that isn't in the extract
# yet (the real data ends 2025-09-23). Each row is in the extract's 21-column
# order (see datasource_metadata.json). Categorical values reuse members that
# already exist in the data so the rows look native; trailing columns that don't
# apply are None. To change what gets appended, edit this list and re-run -- no
# other code changes.
NEW_MATCH_DATE = datetime.date(2026, 7, 4)
NEW_ROWS = [
    # Chain Id, Match Id, Season, Competition, Match Date, Venue, Home Or Away,
    # Team, Opposition, Period, Period Seconds, Chain Start State,
    # Chain Start Zone, Chain Duration Seconds, Chain Phases, Chain End State,
    # Ruck Speed Seconds, Metres Gained, Set Piece Result, Kick Territory Metres,
    # Turnover Origin
    ["999999001_001", 999999001, 2026, "Rugby Championship", NEW_MATCH_DATE, "Stadium Australia", "Home", "Wallabies", "France", "H1", 120, "Kickoff", "Own 22", 18, 3, "Try", 2.5, 45, None, None, None],
    ["999999001_002", 999999001, 2026, "Rugby Championship", NEW_MATCH_DATE, "Stadium Australia", "Home", "Wallabies", "France", "H1", 240, "Lineout", "Opposition 22", 12, 4, "Penalty Kick", 3.1, 8, "Won Clean", None, None],
    ["999999001_003", 999999001, 2026, "Rugby Championship", NEW_MATCH_DATE, "Stadium Australia", "Home", "Wallabies", "France", "H2", 600, "Scrum", "Midfield", 22, 6, "Knock-On", 2.8, 30, "Won Contested", None, "Set-Piece"],
]


def main():
    # 1. Precondition: we copy the schema from the real extract, so it must exist.
    if not os.path.exists(SOURCE_PATH):
        raise SystemExit(f"Source file: {SOURCE_FILE} not found ")

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "generatenewrows") as hyper:
        # 2. Read the extract's table definition (read-only), so New_Rows can be
        #    given exactly the same 21 columns (names/types/order) as Extract --
        #    the guarantee that makes incremental_update.py's `SELECT *` line up.
        #    Done in its own connection so New_Rows is the sole database below and
        #    the unqualified "public" resolves cleanly (attaching a second
        #    database would remove that default-database context).
        with Connection(hyper.endpoint, SOURCE_PATH) as source_conn:
            source_def = source_conn.catalog.get_table_definition(EXTRACT_TABLE)
        new_def = TableDefinition(NEWROWS_TABLE, columns=list(source_def.columns))

        # 3. Open (create) the output file. CREATE_AND_REPLACE makes the script
        #    safely re-runnable: it starts the file fresh every time.
        with Connection(hyper.endpoint, NEWROWS_PATH, CreateMode.CREATE_AND_REPLACE) as connection:
            # 4. Create the identically-shaped table under the new name.
            connection.catalog.create_schema_if_not_exists(SchemaName("public"))
            connection.catalog.create_table(new_def)

            # 5. Bulk-insert the payload rows in one go.
            with Inserter(connection, new_def) as inserter:
                inserter.add_rows(NEW_ROWS)
                inserter.execute()

            # 6. Read the row count back as a sanity check on what we wrote.
            written = connection.execute_scalar_query(f"SELECT COUNT(*) FROM {NEWROWS_TABLE}")

    print(f"Wrote {written} new row(s) to {NEWROWS_FILE} (table {NEWROWS_TABLE}).")


if __name__ == "__main__":
    main()
