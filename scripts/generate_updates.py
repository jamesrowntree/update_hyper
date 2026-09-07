"""
generate_updates.py

Builds Updates.hyper -- the small data source that update_existing_rows.py
reads its update payload from, instead of hardcoding the values in Python.

Why a generator script? .hyper files are binary and there is no CSV-import
path in this repo, so every .hyper file here is produced by a script (see
generate_split_by_year.py and union_hyper_files.py). This is the one place that
defines a table *from scratch* with an explicit TableDefinition, rather than
deriving it from an existing file the way generate_new_rows.py does with
catalog.get_table_definition(...).

The output is a single table, "public"."Updates", with one row per update to
apply:

    "Chain Id"          -- the key, matches "Chain Id" in Finished_Merged.hyper
    "New Metres Gained" -- the absolute value to set "Metres Gained" to

update_existing_rows.py attaches this file and applies every row in one
engine-side `UPDATE ... FROM` join, so the update values never cross into
Python.

Usage (from the project root):
    python3 scripts/generate_updates.py
"""

import os

from tableauhyperapi import (
    HyperProcess,
    Connection,
    Telemetry,
    CreateMode,
    SchemaName,
    TableName,
    TableDefinition,
    SqlType,
    NOT_NULLABLE,
    Inserter,
)

# --- Configuration -----------------------------------------------------------
# The file we produce and the table inside it. Unqualified here ("public".
# "Updates") because this script owns the whole file; update_existing_rows.py
# attaches it under the "updates" alias when it reads it.
UPDATES_FILE = "Updates.hyper"          # display name (see UPDATES_PATH for the real location)
UPDATES_TABLE = TableName("public", "Updates")

# This script lives in scripts/; the payload file is written into the project's
# data/ folder (a sibling of scripts/), resolved relative to __file__ so it runs
# from any working directory, alongside the .hyper files update_existing_rows.py reads.
HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(HERE), "data")
UPDATES_PATH = os.path.join(DATA_DIR, UPDATES_FILE)

# --- Table shape -------------------------------------------------------------
# The schema of the payload table. Column types match the target table in
# datasource_metadata.json ("Chain Id" is TEXT, "Metres Gained" is BIG_INT).
# Both are NOT_NULLABLE: a null key would match nothing, and this example has
# no need to set a measure to NULL.
UPDATES_TABLE_DEF = TableDefinition(
    table_name=UPDATES_TABLE,
    columns=[
        TableDefinition.Column("Chain Id", SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column("New Metres Gained", SqlType.big_int(), NOT_NULLABLE),
    ],
)

# --- The payload -------------------------------------------------------------
# This is the data that used to be hardcoded inside update_existing_rows.py.
# Each pair is [Chain Id, New Metres Gained]. These are real Chain Ids from
# Finished_Merged.hyper (a Wallabies vs. England match on 2021-07-11), so the
# before/after in update_existing_rows.py is concrete and verifiable. To change
# which rows get updated, edit this list and re-run -- no other code changes.
UPDATE_ROWS = [
    ["110023590_001", 25],   # was 10
    ["110023590_002", 15],   # was 4
    ["110023590_003", 40],   # was 31
    ["110023590_004", 8],    # was 5
    ["110023590_005", 22],   # was 19
]


def main():
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "generateupdates") as hyper:
        # 1. Open (create) the output file. CREATE_AND_REPLACE makes the script
        #    safely re-runnable: it starts the file fresh every time.
        with Connection(hyper.endpoint, UPDATES_PATH, CreateMode.CREATE_AND_REPLACE) as connection:
            # 2. Create the schema and the empty table from the definition above.
            connection.catalog.create_schema_if_not_exists(SchemaName("public"))
            connection.catalog.create_table(UPDATES_TABLE_DEF)

            # 3. Bulk-insert the payload rows in one go.
            with Inserter(connection, UPDATES_TABLE_DEF) as inserter:
                inserter.add_rows(UPDATE_ROWS)
                inserter.execute()

            # 4. Read the row count back as a sanity check on what we wrote.
            written = connection.execute_scalar_query(f"SELECT COUNT(*) FROM {UPDATES_TABLE}")

    print(f"Wrote {written} update row(s) to {UPDATES_FILE} (table {UPDATES_TABLE}).")


if __name__ == "__main__":
    main()
