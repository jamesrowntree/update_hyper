"""
union_hyper_files.py

A version of the "recommended solution" for unioning multiple Hyper files,
adapted from Adrian Vogelsgesang's gist:
https://gist.github.com/vogelsgesang/e83260fd3e1429aefed99ad30a27f196#file-efficient_merge-py
(referenced from the Tableau engineering blog post
"Using the Hyper API to union Hyper files":
https://www.tableau.com/blog/using-hyper-api-union-hyper-files)

Merges Start_2020.hyper, Start_2021.hyper, ... Start_2025.hyper (produced
by split_by_year.py) back into a single Finished_Merged.hyper file containing
every row from every year.

Why this is the fastest approach
---------------------------------
The naive way to merge N Hyper files is to open each one, read every row
through the Hyper API into Python, and re-insert each row into a new file
with an Inserter. That approach pays the cost of crossing the Python/C++
API boundary and serializing/deserializing every single row, N times over.

This script instead avoids ever bringing a row into Python:

  1. It starts a single Hyper process and, within one Connection, attaches
     every input .hyper file as its own "database" under a unique alias
     (input0, input1, ...), plus a fresh output database.
  2. It builds one SQL statement:

         CREATE TABLE "output"."public"."Extract" AS
         SELECT * FROM "input0"."public"."Extract"
         UNION ALL
         SELECT * FROM "input1"."public"."Extract"
         UNION ALL
         ...

  3. It executes that single statement with `connection.execute_command`.

Hyper's query engine is a native, columnar, vectorized execution engine.
Because every input table is attached directly into the same engine
instance, `CREATE TABLE ... AS SELECT ... UNION ALL ...` is executed
entirely inside Hyper: reading columns, concatenating them, and writing the
result to the output file, without any row ever being marshalled across
the API into Python. This turns an O(rows) Python loop into a single
engine-internal bulk operation, which is dramatically faster and is why
the Tableau blog post calls this the "recommended" way to combine files.

Requirement: every input file must have the same table structure (schema
name, table name, column names, column types) for `UNION ALL` to line the
columns up correctly. split_by_year.py guarantees this because every
output file is derived from the same source table.
"""

from glob import glob
from time import time
import os

from tableauhyperapi import (
    HyperProcess,
    Connection,
    Telemetry,
    SchemaName,
    TableName,
)

INPUT_GLOB = "Start_*.hyper"
SCHEMA_NAME = "public"
TABLE = "Extract"
OUTPUT_FILE = "Finished_Merged.hyper"


def main():
    input_files = sorted(glob(INPUT_GLOB))
    if not input_files:
        raise SystemExit(f"No input files found matching {INPUT_GLOB!r}")

    # Delete the output file so the script can be safely rerun.
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    start_time = time()
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "unionfiles_efficient") as hyper:
        with Connection(hyper.endpoint) as connection:
            catalog = connection.catalog

            # Attach every input file under a unique alias in one shot.
            for i, file in enumerate(input_files):
                catalog.attach_database(file, alias=f"input{i}")
            print(f"{time() - start_time:6.2f}s  attached {len(input_files)} input file(s): {input_files}")

            # Prepare the output database.
            catalog.create_database(OUTPUT_FILE)
            catalog.attach_database(OUTPUT_FILE, alias="output")
            catalog.create_schema_if_not_exists(SchemaName("output", SCHEMA_NAME))
            print(f"{time() - start_time:6.2f}s  prepared output database {OUTPUT_FILE!r}")

            # Build the single CREATE TABLE ... AS ... UNION ALL ... statement.
            output_table = TableName("output", SCHEMA_NAME, TABLE)
            union_query = " UNION ALL\n".join(
                f'SELECT * FROM {TableName(f"input{i}", SCHEMA_NAME, TABLE)}'
                for i in range(len(input_files))
            )
            create_table_sql = f"CREATE TABLE {output_table} AS\n{union_query}"

            connection.execute_command(create_table_sql)
            print(f"{time() - start_time:6.2f}s  merged all inputs into {output_table}")

            row_count = connection.execute_scalar_query(f"SELECT COUNT(*) FROM {output_table}")
            print(f"{time() - start_time:6.2f}s  done -- {row_count} total rows in {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
