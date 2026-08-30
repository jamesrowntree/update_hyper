"""
example_add_new_rows.py

Answers: "can you incrementally update an existing Hyper extract?" -- yes,
this is the other half (see also example_update_record.py). This
demonstrates appending brand-new rows into an EXISTING table with
Inserter, instead of rebuilding the whole file the way efficient_merge.py
does (attach_database + CREATE TABLE ... AS SELECT ... UNION ALL over every
yearly file).

The key fact that makes this possible: Inserter does not create a table --
it looks up an already-existing table's definition via
catalog.get_table_definition(...) and appends rows to it. If only a
handful of new rows have arrived (e.g. one new match), this is a much
cheaper operation than re-attaching and re-unioning every input file again.

This script never touches Finished_Merged.hyper. It copies it to
Example_Insert.hyper first and appends to the copy, so it's safe to re-run
and never disturbs the file that's actually published to Tableau Cloud.

Usage:
    python3 example_add_new_rows.py
"""

import datetime
import os
import shutil

from tableauhyperapi import HyperProcess, Connection, Telemetry, TableName, Inserter

SOURCE_FILE = "Finished_Merged.hyper"
EXAMPLE_FILE = "Example_Insert.hyper"
TABLE = TableName("public", "Extract")

# One new row, in the same column order as the table (see generate_metadata.py
# or datasource_metadata.json for the authoritative column list). Nullable
# columns that don't apply to this chain are set to None.
NEW_ROW = [
    "999999001_001",              # Chain Id
    999999001,                    # Match Id
    2026,                         # Season
    "Rugby Championship",         # Competition
    datetime.date.today(),        # Match Date
    "Stadium Australia",          # Venue
    "Home",                       # Home Or Away
    "Wallabies",                  # Team
    "France",                     # Opposition
    "1st Half",                   # Period
    120,                          # Period Seconds
    "Kickoff",                    # Chain Start State
    "Own 22",                     # Chain Start Zone
    18,                           # Chain Duration Seconds
    3,                            # Chain Phases
    "Try",                        # Chain End State
    2.5,                          # Ruck Speed Seconds
    45,                           # Metres Gained
    None,                         # Set Piece Result
    None,                         # Kick Territory Metres
    None,                         # Turnover Origin
]


def row_count(connection):
    return connection.execute_scalar_query(f"SELECT COUNT(*) FROM {TABLE}")


def main():
    if not os.path.exists(SOURCE_FILE):
        raise SystemExit(f"{SOURCE_FILE} not found -- run split_by_year.py then efficient_merge.py first.")

    shutil.copyfile(SOURCE_FILE, EXAMPLE_FILE)
    print(f"Copied {SOURCE_FILE} -> {EXAMPLE_FILE} (only the copy will be modified)")

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "exampleinsert") as hyper:
        # Default create_mode is CreateMode.NONE: opens the existing file
        # as-is. It is NOT recreated or wiped.
        with Connection(hyper.endpoint, EXAMPLE_FILE) as connection:
            before = row_count(connection)
            print(f"Row count before: {before}")

            # get_table_definition looks up the EXISTING table -- Inserter
            # never creates one. Reusing it guarantees the new row lines up
            # with the real column list/types with no manual duplication.
            table_def = connection.catalog.get_table_definition(TABLE)

            with Inserter(connection, table_def) as inserter:
                inserter.add_row(NEW_ROW)
                inserter.execute()

            after = row_count(connection)
            print(f"Row count after:  {after} (added {after - before})")

            inserted = connection.execute_list_query(
                f'SELECT "Chain Id", "Match Id", "Team", "Opposition", "Match Date", "Metres Gained" '
                f'FROM {TABLE} WHERE "Chain Id" = \'{NEW_ROW[0]}\''
            )
            print("Newly inserted row, read back from the file:")
            for row in inserted:
                print(f"    {row}")

    print(
        f"\nDone. Only {EXAMPLE_FILE} was modified -- {SOURCE_FILE} (and the live "
        f"Tableau Cloud data source it backs) is untouched. No attach_database, "
        f"no UNION ALL, no full rebuild -- just an append to the existing table."
    )


if __name__ == "__main__":
    main()
