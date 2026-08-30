"""
example_update_record.py

Answers the question: "can you incrementally update an existing Hyper extract?" -- yes.
This demonstrates finding a specific row (or a specific game's worth of
rows) in an existing .hyper file and changing one measure in place with a
plain SQL UPDATE, instead of rebuilding the table the way efficient_merge.py
does (attach_database + CREATE TABLE ... AS SELECT ... UNION ALL).

The key fact that makes this possible: opening a Connection against a
.hyper file uses CreateMode.NONE by default, which does NOT wipe the file --
it just opens it read-write. execute_command() runs any SQL statement, and
UPDATE ... SET ... WHERE ... is ordinary SQL, so it works exactly like the
CREATE TABLE AS statements already used elsewhere in this project.

This script never touches Finished_Merged.hyper. It copies it to
Example_Update.hyper first and edits the copy, so it's safe to re-run and
never disturbs the file that's actually published to Tableau Cloud.

Usage:
    python3 example_update_record.py
"""

import os
import shutil

from tableauhyperapi import HyperProcess, Connection, Telemetry, TableName

SOURCE_FILE = "Finished_Merged.hyper"
EXAMPLE_FILE = "Example_Update.hyper"
TABLE = TableName("public", "Extract")

# A real row, picked from the actual data, so this example is concrete and
# verifiable rather than using placeholder values.
EXAMPLE_CHAIN_ID = "110023590_001"
EXAMPLE_MATCH_ID = 110023590


def show_rows(connection, label, where_clause, max_shown=5):
    rows = connection.execute_list_query(
        f'SELECT "Chain Id", "Match Id", "Team", "Opposition", "Match Date", "Metres Gained" '
        f'FROM {TABLE} WHERE {where_clause} ORDER BY "Chain Id"'
    )
    print(f"{label} ({len(rows)} row(s)):")
    for row in rows[:max_shown]:
        print(f"    {row}")
    if len(rows) > max_shown:
        print(f"    ... and {len(rows) - max_shown} more")


def main():
    if not os.path.exists(SOURCE_FILE):
        raise SystemExit(f"{SOURCE_FILE} not found -- run split_by_year.py then efficient_merge.py first.")

    shutil.copyfile(SOURCE_FILE, EXAMPLE_FILE)
    print(f"Copied {SOURCE_FILE} -> {EXAMPLE_FILE} (only the copy will be modified)")

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "exampleupdate") as hyper:
        # Default create_mode is CreateMode.NONE: opens the existing file
        # as-is. It is NOT recreated or wiped.
        with Connection(hyper.endpoint, EXAMPLE_FILE) as connection:

            print("\n--- Variant A: update exactly one row (keyed on \"Chain Id\") ---")
            show_rows(connection, "Before", f'"Chain Id" = \'{EXAMPLE_CHAIN_ID}\'')

            affected = connection.execute_command(
                f'UPDATE {TABLE} SET "Metres Gained" = 25 '
                f'WHERE "Chain Id" = \'{EXAMPLE_CHAIN_ID}\''
            )
            print(f"UPDATE affected {affected} row(s) (expected: 1)")

            show_rows(connection, "After", f'"Chain Id" = \'{EXAMPLE_CHAIN_ID}\'')

            print('\n--- Variant B: update every chain belonging to one match (keyed on "Match Id") ---')
            show_rows(connection, "Before", f'"Match Id" = {EXAMPLE_MATCH_ID}')

            affected = connection.execute_command(
                f'UPDATE {TABLE} SET "Metres Gained" = "Metres Gained" + 5 '
                f'WHERE "Match Id" = {EXAMPLE_MATCH_ID}'
            )
            print(f"UPDATE affected {affected} row(s) (expected: >1 -- every chain in this match)")

            show_rows(connection, "After", f'"Match Id" = {EXAMPLE_MATCH_ID}')

    print(
        f"\nDone. Only {EXAMPLE_FILE} was modified -- {SOURCE_FILE} (and the live "
        f"Tableau Cloud data source it backs) is untouched."
    )


if __name__ == "__main__":
    main()
