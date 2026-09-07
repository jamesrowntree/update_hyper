"""
incremental_update.py

The incremental-append half of the extract-mutation examples (its sibling,
update_existing_rows.py, is the edit-in-place half). This demonstrates
appending brand-new rows to an EXISTING extract without rebuilding it the way
union_hyper_files.py does (attach_database + CREATE TABLE ... AS SELECT ...
UNION ALL over every yearly file).

The new rows live in their own data source: New_Rows.hyper (produced by 
generate_new_rows.py), a single table with the same 21-column shape as the 
extract's "Extract" table. This script attaches to that file and appends 
every row from it into the target hyper file, with one engine-side
`INSERT INTO ... SELECT * FROM ...` -- the same attach_database +
execute_command pattern update_existing_rows.py, union_hyper_files.py and
generate_split_by_year.py all use.

The key facts that make this possible:
  * Opening a Connection against a .hyper file uses CreateMode.NONE by default,
    which does NOT wipe the file -- it just opens it read-write.
  * catalog.attach_database() makes a second .hyper file visible to the same SQL
    engine, addressable as "alias"."schema"."table".
  * execute_command() runs any SQL statement (INSERT INTO ... SELECT ...)
    entirely inside Hyper.

HELP -- new data in Snowflake instead of a .hyper file? 
This append step is source-agnostic: it only ever reads the attached New_Rows.hyper, 
so it stays EXACTLY the same no matter where those rows came from. 
The Hyper engine cannot attach Snowflake directly, so the Snowflake pull happens 
in the generator, not here -- to repoint at Snowflake you change only generate_new_rows.py
(see the Snowflake block there). That separation is the whole point: this
file is the reusable "apply" step; the generator is the swappable "source" step.

This script never touches Finished_Merged.hyper. It copies it to
Example_Insert.hyper first and appends to the copy, so it's safe to re-run and
never disturbs the file that's actually published to Tableau Cloud.

Usage (from the project root):
    python3 scripts/generate_new_rows.py     # once, to create data/New_Rows.hyper
    python3 scripts/incremental_update.py
"""

import os
import shutil

from tableauhyperapi import HyperProcess, Connection, Telemetry, TableName

# --- Configuration -----------------------------------------------------------
# The three files this script touches (short display names used in the output
# below; the on-disk locations are resolved just after, as *_PATH):
#   SOURCE_FILE  - the real extract; read-only, never modified.
#   EXAMPLE_FILE - a disposable copy of SOURCE_FILE; this is what we append to.
#   NEWROWS_FILE - the externalised new-rows payload built by generate_new_rows.py.
SOURCE_FILE = "Finished_Merged.hyper"
EXAMPLE_FILE = "Example_Insert.hyper"
NEWROWS_FILE = "New_Rows.hyper"

# This script lives in scripts/; all .hyper files live in the project's data/
# folder (a sibling of scripts/). Resolve paths relative to this file so it runs
# from any working directory: the source extract, the disposable copy, and the
# new-rows payload all live in data/.
HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(HERE), "data")
SOURCE_PATH = os.path.join(DATA_DIR, SOURCE_FILE)
EXAMPLE_PATH = os.path.join(DATA_DIR, EXAMPLE_FILE)
NEWROWS_PATH = os.path.join(DATA_DIR, NEWROWS_FILE)

# Both files are attached under their own alias (see attach_database below) and
# referenced with fully-qualified "alias"."schema"."table" names, exactly like
# update_existing_rows.py, union_hyper_files.py and generate_split_by_year.py.
TABLE = TableName("target", "public", "Extract")            # the copy we append to
NEWROWS_TABLE = TableName("newrows", "public", "New_Rows")  # the payload we read


# --- Display helper ----------------------------------------------------------
# Pretty-prints the rows matching a WHERE clause, so we can show the newly
# appended rows read back from the file. It plays no part in the append itself.
def show_rows(connection, label, where_clause, max_shown=10):
    rows = connection.execute_list_query(
        f'SELECT "Chain Id", "Match Id", "Team", "Opposition", "Match Date", "Metres Gained" '
        f'FROM {TABLE} WHERE {where_clause} ORDER BY "Chain Id"'
    )
    print(f"{label} ({len(rows)} row(s)):")
    for row in rows[:max_shown]:
        print(f"    {row}")
    if len(rows) > max_shown:
        print(f"    ... and {len(rows) - max_shown} more")


def row_count(connection):
    return connection.execute_scalar_query(f"SELECT COUNT(*) FROM {TABLE}")


def main():
    # 1. Preconditions -- both the source extract and the new-rows payload must
    #    exist before we do anything. Fail early with an actionable message.
    if not os.path.exists(SOURCE_PATH):
        raise SystemExit(f"{SOURCE_FILE} not found -- run generate_split_by_year.py then union_hyper_files.py first.")
    if not os.path.exists(NEWROWS_PATH):
        raise SystemExit(f"{NEWROWS_FILE} not found -- run generate_new_rows.py first to create the new-rows source.")

    # 2. Work on a throwaway copy, so the real extract (and the live Tableau
    #    Cloud data source it backs) is never at risk and the script is re-runnable.
    shutil.copyfile(SOURCE_PATH, EXAMPLE_PATH)
    print(f"Copied {SOURCE_FILE} -> {EXAMPLE_FILE} (only the copy will be modified)")

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "incrementalupdate") as hyper:
        # 3. Open a bare connection and attach both files under their own alias, so
        #    a single SQL engine sees both. attach_database uses CreateMode.NONE:
        #    the files are opened as-is, NOT recreated or wiped. The target
        #    ("target"."public"."Extract") is the disposable copy; the new rows
        #    are "newrows"."public"."New_Rows".
        with Connection(hyper.endpoint) as connection:
            connection.catalog.attach_database(EXAMPLE_PATH, alias="target")
            connection.catalog.attach_database(NEWROWS_PATH, alias="newrows")

            # 4. Count the rows before, and note how many we expect to add.
            before = row_count(connection)
            expected = connection.execute_scalar_query(f"SELECT COUNT(*) FROM {NEWROWS_TABLE}")
            print(f"--- Appending new rows from {NEWROWS_FILE} ---")
            print(f"Row count before: {before}")

            # 5. Append every new row in one engine-side statement. New_Rows has
            #    the same column shape as Extract, so `SELECT *` lines up 1:1 and
            #    no row data is ever marshalled into Python -- Hyper reads the rows
            #    straight from the attached "newrows" table. (If those rows came
            #    from Snowflake, only generate_new_rows.py changes; this INSERT
            #    stays exactly as-is.)
            affected = connection.execute_command(
                f"INSERT INTO {TABLE} SELECT * FROM {NEWROWS_TABLE}"
            )
            print(f"INSERT added {affected} row(s) (expected: {expected}, one per row in {NEWROWS_FILE})")

            # 6. Count again to confirm, then read the appended rows back out of
            #    the file to prove they landed.
            after = row_count(connection)
            print(f"Row count after:  {after} (added {after - before})")
            show_rows(
                connection,
                "Newly appended rows, read back from the copy",
                f'"Chain Id" IN (SELECT "Chain Id" FROM {NEWROWS_TABLE})',
            )

    # 7. Report. The copy is the only thing that changed; appending different
    #    rows is a data-only change (edit generate_new_rows.py, or point it at
    #    Snowflake), not a change to this script.
    print(
        f"\nDone. Only {EXAMPLE_FILE} was modified -- {SOURCE_FILE} (and the live "
        f"Tableau Cloud data source it backs) is untouched. To change which rows "
        f"are appended, edit generate_new_rows.py and regenerate {NEWROWS_FILE} -- "
        f"no change to this script needed."
    )


if __name__ == "__main__":
    main()
