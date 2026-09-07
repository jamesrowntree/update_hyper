"""
incremental_update.py

Answers the question: "can you incrementally update an existing Hyper extract?" -- yes.
This demonstrates changing one measure in place on existing rows with a plain
SQL UPDATE, instead of rebuilding the table the way union_hyper_files.py does
(attach_database + CREATE TABLE ... AS SELECT ... UNION ALL).

The update payload is NOT hardcoded here. It lives in its own data source,
Updates.hyper (produced by generate_updates.py), which holds one row per
update to apply: a "Chain Id" key and the "New Metres Gained" value to set.
This script attaches that file and applies every row in a single engine-side
`UPDATE ... FROM` join -- the same attach_database + execute_command pattern
union_hyper_files.py and split_by_year.py use, so the update values never
cross into Python.

The key facts that make this possible:
  * Opening a Connection against a .hyper file uses CreateMode.NONE by
    default, which does NOT wipe the file -- it just opens it read-write.
  * catalog.attach_database() makes a second .hyper file visible to the same
    SQL engine, addressable as "alias"."schema"."table".
  * execute_command() runs any SQL statement, and UPDATE ... SET ... FROM ...
    WHERE ... is ordinary SQL, executed entirely inside Hyper.

This script never touches Finished_Merged.hyper. It copies it to
Example_Update.hyper first and edits the copy, so it's safe to re-run and
never disturbs the file that's actually published to Tableau Cloud.

Usage:
    python3 generate_updates.py        # once, to create Updates.hyper
    python3 incremental_update.py
"""

import os
import shutil

from tableauhyperapi import HyperProcess, Connection, Telemetry, TableName

# --- Configuration -----------------------------------------------------------
# The three files this script touches:
#   SOURCE_FILE  - the real extract; read-only, never modified.
#   EXAMPLE_FILE - a disposable copy of SOURCE_FILE; this is what we edit.
#   UPDATES_FILE - the externalised update payload built by generate_updates.py.
SOURCE_FILE = "Finished_Merged.hyper"
EXAMPLE_FILE = "Example_Update.hyper"
UPDATES_FILE = "Updates.hyper"

# Both files are attached under their own alias (see attach_database below) and
# referenced with fully-qualified "alias"."schema"."table" names, exactly like
# union_hyper_files.py and split_by_year.py.
TABLE = TableName("target", "public", "Extract")           # the copy we edit
UPDATES_TABLE = TableName("updates", "public", "Updates")  # the payload we read


# --- Display helper ----------------------------------------------------------
# Pretty-prints the rows matching a WHERE clause, purely so we can show the
# before/after state around the update. It plays no part in the update itself.
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


def main():
    # 1. Preconditions -- both the source extract and the update payload must
    #    exist before we do anything. Fail early with an actionable message.
    if not os.path.exists(SOURCE_FILE):
        raise SystemExit(f"{SOURCE_FILE} not found -- run split_by_year.py then union_hyper_files.py first.")
    if not os.path.exists(UPDATES_FILE):
        raise SystemExit(f"{UPDATES_FILE} not found -- run generate_updates.py first to create the update source.")

    # 2. Work on a throwaway copy, so the real extract (and the live Tableau
    #    Cloud data source it backs) is never at risk and the script is re-runnable.
    shutil.copyfile(SOURCE_FILE, EXAMPLE_FILE)
    print(f"Copied {SOURCE_FILE} -> {EXAMPLE_FILE} (only the copy will be modified)")

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "incrementalupdate") as hyper:
        # 3. Open a bare connection and attach both files under their own alias, so
        #    a single SQL engine sees both. attach_database uses CreateMode.NONE:
        #    the files are opened as-is, NOT recreated or wiped. The target
        #    ("target"."public"."Extract") is the disposable copy; the update
        #    source is "updates"."public"."Updates".
        with Connection(hyper.endpoint) as connection:
            connection.catalog.attach_database(EXAMPLE_FILE, alias="target")
            connection.catalog.attach_database(UPDATES_FILE, alias="updates")

            # The rows we're about to touch are exactly the keys in the update
            # source, so filter Extract to those Chain Ids for the before/after.
            affected_filter = f'"Chain Id" IN (SELECT "Chain Id" FROM {UPDATES_TABLE})'

            # 4. Snapshot the rows we're about to change (before state).
            print("--- Applying updates from", UPDATES_FILE, "---")
            show_rows(connection, "Before", affected_filter)

            # 5. Apply every update in one engine-side statement. `expected` is
            #    how many updates the payload holds; `affected` is how many rows
            #    the join actually changed. No update value ever crosses into
            #    Python -- Hyper reads the new value straight from the attached
            #    "updates" table.
            expected = connection.execute_scalar_query(f"SELECT COUNT(*) FROM {UPDATES_TABLE}")
            affected = connection.execute_command(
                f'UPDATE {TABLE} AS e '
                f'SET "Metres Gained" = u."New Metres Gained" '
                f'FROM {UPDATES_TABLE} AS u '
                f'WHERE e."Chain Id" = u."Chain Id"'
            )
            print(f"UPDATE affected {affected} row(s) (expected: {expected}, one per row in {UPDATES_FILE})")
            # A shortfall means some Chain Id in the payload matched no row in the
            # extract (usually a typo'd key) -- worth flagging, not worth failing on.
            if affected < expected:
                print(f"WARNING: applied {affected} of {expected} updates -- "
                      f"{expected - affected} key(s) in {UPDATES_FILE} matched no row in {TABLE}")

            # 6. Snapshot the same rows again (after state) to confirm the change.
            show_rows(connection, "After", affected_filter)

    # 7. Report. The copy is the only thing that changed; editing which rows get
    #    updated is a data-only change (edit generate_updates.py), not a code change.
    print(
        f"\nDone. Only {EXAMPLE_FILE} was modified -- {SOURCE_FILE} (and the live "
        f"Tableau Cloud data source it backs) is untouched. To change which rows "
        f"are updated, edit generate_updates.py and regenerate {UPDATES_FILE} -- "
        f"no change to this script needed."
    )


if __name__ == "__main__":
    main()
