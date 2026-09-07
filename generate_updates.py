"""
generate_updates.py

Builds Updates.hyper -- the small data source that example_update_record.py
reads its update payload from, instead of hardcoding the values in Python.

Why a generator script? .hyper files are binary and there is no CSV-import
path in this repo, so every .hyper file here is produced by a script (see
split_by_year.py and union_hyper_files.py). This is the one place that
defines a table *from scratch* with an explicit TableDefinition, rather than
deriving it from an existing file the way example_add_new_rows.py does with
catalog.get_table_definition(...).

The output is a single table, "public"."Updates", with one row per update to
apply:

    "Chain Id"          -- the key, matches "Chain Id" in Finished_Merged.hyper
    "New Metres Gained" -- the absolute value to set "Metres Gained" to

example_update_record.py attaches this file and applies every row in one
engine-side `UPDATE ... FROM` join, so the update values never cross into
Python.

Usage:
    python3 generate_updates.py
"""

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

UPDATES_FILE = "Updates.hyper"
UPDATES_TABLE = TableName("public", "Updates")

# Column types match the target table in datasource_metadata.json:
# "Chain Id" is TEXT and "Metres Gained" is BIG_INT.
UPDATES_TABLE_DEF = TableDefinition(
    table_name=UPDATES_TABLE,
    columns=[
        TableDefinition.Column("Chain Id", SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column("New Metres Gained", SqlType.big_int(), NOT_NULLABLE),
    ],
)

# Each pair is [Chain Id, New Metres Gained]. These are real Chain Ids from
# Finished_Merged.hyper (a Wallabies vs. England match on 2021-07-11), so the
# before/after in example_update_record.py is concrete and verifiable. Add
# more real Chain Ids here to update more rows -- no code changes needed.
UPDATE_ROWS = [
    ["110023590_001", 25],   # was 10
    ["110023590_002", 15],   # was 4
    ["110023590_003", 40],   # was 31
    ["110023590_004", 8],    # was 5
    ["110023590_005", 22],   # was 19
]


def main():
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "generateupdates") as hyper:
        # CREATE_AND_REPLACE makes the script safely re-runnable: it starts
        # the file fresh every time.
        with Connection(hyper.endpoint, UPDATES_FILE, CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_schema_if_not_exists(SchemaName("public"))
            connection.catalog.create_table(UPDATES_TABLE_DEF)

            with Inserter(connection, UPDATES_TABLE_DEF) as inserter:
                inserter.add_rows(UPDATE_ROWS)
                inserter.execute()

            written = connection.execute_scalar_query(f"SELECT COUNT(*) FROM {UPDATES_TABLE}")

    print(f"Wrote {written} update row(s) to {UPDATES_FILE} (table {UPDATES_TABLE}).")


if __name__ == "__main__":
    main()
