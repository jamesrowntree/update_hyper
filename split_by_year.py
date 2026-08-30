"""
split_by_year.py

Splits Start.hyper into one .hyper file per calendar year found in the
"Match Date" column of the "public"."Extract" table:

    Start.hyper  ->  Start_2020.hyper, Start_2021.hyper, ... Start_2025.hyper

Every output file has an *identical* table structure to the source file
(same schema name, table name, column names, column types, nullability),
so the files can later be recombined losslessly with union_hyper_files.py.

Performance note
-----------------
This script uses the same technique as the "recommended solution" for
merging Hyper files described at
https://www.tableau.com/blog/using-hyper-api-union-hyper-files and
https://gist.github.com/vogelsgesang/e83260fd3e1429aefed99ad30a27f196
-- just run in reverse. Instead of reading rows into Python and
re-inserting them (slow, row-by-row, crosses the API boundary many
times), it:

  1. Attaches the source .hyper file to a single Hyper process.
  2. Creates one output database per year and attaches it too.
  3. Issues a single `CREATE TABLE ... AS SELECT * FROM source WHERE ...`
     statement per year.

The filtering and copying happen entirely inside the Hyper engine's
native, columnar, vectorized execution -- no row ever crosses into
Python. This is purported to be the fastest possible way to partition 
a Hyper file.
"""

from time import time
import os

from tableauhyperapi import (
    HyperProcess,
    Connection,
    Telemetry,
    SchemaName,
    TableName,
)

SOURCE_FILE = "Start.hyper"
SOURCE_SCHEMA = "public"
SOURCE_TABLE = "Extract"
DATE_COLUMN = '"Match Date"'
OUTPUT_PATTERN = "Start_{year}.hyper"


def main():
    start_time = time()

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "splitbyyear") as hyper:
        with Connection(hyper.endpoint) as connection:
            catalog = connection.catalog

            # Attach the source file under an alias so it can be referenced
            # in SQL as "source"."public"."Extract".
            catalog.attach_database(SOURCE_FILE, alias="source")
            source_table = TableName("source", SOURCE_SCHEMA, SOURCE_TABLE)
            print(f"{time() - start_time:6.2f}s  attached {SOURCE_FILE} as \"source\"")

            # Discover the distinct years present in the data. This is a
            # single aggregate query executed natively by Hyper.
            years = [
                row[0]
                for row in connection.execute_list_query(
                    f'SELECT DISTINCT EXTRACT(YEAR FROM {DATE_COLUMN}) AS yr '
                    f'FROM {source_table} '
                    f'ORDER BY yr'
                )
            ]
            print(f"{time() - start_time:6.2f}s  found {len(years)} year(s): {years}")

            for year in years:
                output_file = OUTPUT_PATTERN.format(year=year)
                if os.path.exists(output_file):
                    os.remove(output_file)

                alias = f"year_{year}"
                catalog.create_database(output_file)
                catalog.attach_database(output_file, alias=alias)
                catalog.create_schema_if_not_exists(SchemaName(alias, SOURCE_SCHEMA))

                out_table = TableName(alias, SOURCE_SCHEMA, SOURCE_TABLE)
                create_sql = (
                    f'CREATE TABLE {out_table} AS\n'
                    f'SELECT * FROM {source_table} '
                    f'WHERE EXTRACT(YEAR FROM {DATE_COLUMN}) = {year}'
                )
                connection.execute_command(create_sql)

                row_count = connection.execute_scalar_query(
                    f'SELECT COUNT(*) FROM {out_table}'
                )
                catalog.detach_database(alias)

                print(
                    f"{time() - start_time:6.2f}s  wrote {output_file} "
                    f"({row_count} rows)"
                )

            catalog.detach_database("source")

    print(f"{time() - start_time:6.2f}s  done")


if __name__ == "__main__":
    main()
