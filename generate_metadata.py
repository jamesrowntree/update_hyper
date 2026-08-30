"""
generate_metadata.py

Inspects Finished_Merged.hyper (produced by union_hyper_files.py) and writes a
metadata file, datasource_metadata.json, describing it: a suggested name,
description, tags, certification status, and per-column descriptions.

publish_to_tableau_cloud.py reads this file and applies the datasource-level
fields (name, description, tags, certification) to the published data source.

Column descriptions below are inferred from column names and general sports
possession-chain analytics conventions (this looks like rugby union/league
event data: "Ruck", "Chain", "Set Piece", "Turnover" are rugby terms) -- they
are NOT sourced from authoritative documentation for this dataset. Review and
correct COLUMN_DESCRIPTIONS below before treating them as ground truth; this
disclaimer is also carried into the output JSON so it isn't lost downstream.

Rerun this script any time Finished_Merged.hyper changes -- the row count,
date range, and year list are always re-derived from the actual file, so
they can't drift out of sync the way a hand-maintained metadata file would.
"""

import json
import os
from datetime import datetime, timezone

from tableauhyperapi import HyperProcess, Connection, Telemetry, TableName, Nullability

HYPER_FILE = "Finished_Merged.hyper"
SCHEMA_NAME = "public"
TABLE = "Extract"
OUTPUT_METADATA_FILE = "datasource_metadata.json"

DATASOURCE_NAME = "Finished Merged"
TAGS = ["hyper-union", "merged", "yearly-split-rejoin"]
CERTIFIED = False
CERTIFICATION_NOTE = ""

COLUMN_DESCRIPTIONS = {
    "Chain Id": (
        "Unique identifier for a possession chain -- a continuous sequence of "
        "play by one team from gaining possession until it changes hands or "
        "the ball becomes dead."
    ),
    "Match Id": "Unique identifier for the match this chain occurred in.",
    "Season": "The season (year) the match belongs to, as recorded in the source system.",
    "Competition": "Name of the competition or league the match was part of.",
    "Match Date": (
        "Calendar date the match was played. This is the column split_by_year.py "
        "partitions on to produce one file per year."
    ),
    "Venue": "Name of the stadium or ground where the match was played.",
    "Home Or Away": "Whether the team recorded on this row was playing at home or away.",
    "Team": "The team in possession during this chain.",
    "Opposition": "The opposing team.",
    "Period": "The period of the match the chain occurred in (e.g. first half, second half).",
    "Period Seconds": "Elapsed time, in seconds, into the period when the chain started.",
    "Chain Start State": "How the chain began, e.g. from a set piece, turnover, or restart.",
    "Chain Start Zone": "The area of the field where the chain started.",
    "Chain Duration Seconds": "Total duration of the chain, in seconds.",
    "Chain Phases": "Number of phases (individual plays/tackles) within the chain.",
    "Chain End State": "How the chain ended, e.g. try scored, turnover, kick.",
    "Ruck Speed Seconds": (
        "Average time, in seconds, taken to recycle the ball at the ruck during the chain."
    ),
    "Metres Gained": "Net metres gained by the team in possession during the chain.",
    "Set Piece Result": (
        "Outcome of the set piece (e.g. scrum, lineout) that started or ended the "
        "chain, where applicable."
    ),
    "Kick Territory Metres": "Net territory gained from kicks during the chain, in metres.",
    "Turnover Origin": "How possession was lost, when the chain ended in a turnover.",
}

COLUMN_DESCRIPTION_DISCLAIMER = (
    "Column descriptions were inferred from column names and general rugby "
    "possession-chain analytics conventions by whoever wrote generate_metadata.py -- "
    "they are not sourced from authoritative documentation for this dataset. "
    "Review and correct COLUMN_DESCRIPTIONS in generate_metadata.py before relying on them."
)


def main():
    if not os.path.exists(HYPER_FILE):
        raise SystemExit(
            f"{HYPER_FILE} not found. Run split_by_year.py then union_hyper_files.py first."
        )

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "generatemetadata") as hyper:
        with Connection(hyper.endpoint, HYPER_FILE) as connection:
            table_name = TableName(SCHEMA_NAME, TABLE)
            table_def = connection.catalog.get_table_definition(table_name)

            columns = []
            unknown_columns = []
            for col in table_def.columns:
                name = col.name.unescaped
                description = COLUMN_DESCRIPTIONS.get(name)
                if description is None:
                    description = "No description available."
                    unknown_columns.append(name)
                columns.append(
                    {
                        "name": name,
                        "type": str(col.type),
                        "nullable": col.nullability == Nullability.NULLABLE,
                        "description": description,
                    }
                )

            row_count = connection.execute_scalar_query(f"SELECT COUNT(*) FROM {table_name}")
            date_min, date_max = connection.execute_list_query(
                f'SELECT MIN("Match Date"), MAX("Match Date") FROM {table_name}'
            )[0]
            years = [
                row[0]
                for row in connection.execute_list_query(
                    f'SELECT DISTINCT EXTRACT(YEAR FROM "Match Date") AS yr '
                    f'FROM {table_name} ORDER BY yr'
                )
            ]

    if unknown_columns:
        print(
            "Warning: no hand-written description for these columns "
            f"(schema drift?): {unknown_columns}"
        )

    description = (
        f"Union of {len(years)} yearly Hyper extracts ({years[0]}-{years[-1]}) that were "
        f"split out of Start.hyper by split_by_year.py and rejoined by union_hyper_files.py "
        f"using Hyper's native attach_database + CREATE TABLE ... AS SELECT ... UNION ALL "
        f"(no row ever passed through Python). Contains {row_count} rows spanning "
        f"{date_min} to {date_max}."
    )

    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_file": HYPER_FILE,
        "datasource": {
            "name": DATASOURCE_NAME,
            "description": description,
            "tags": TAGS,
            "certified": CERTIFIED,
            "certification_note": CERTIFICATION_NOTE,
        },
        "stats": {
            "row_count": row_count,
            "match_date_min": str(date_min),
            "match_date_max": str(date_max),
            "years_included": years,
        },
        "columns": columns,
        "column_description_disclaimer": COLUMN_DESCRIPTION_DISCLAIMER,
    }

    with open(OUTPUT_METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    print(f"Wrote metadata for {len(columns)} column(s) to {OUTPUT_METADATA_FILE}")
    print(f"  {row_count} rows, {date_min} to {date_max}, years: {years}")


if __name__ == "__main__":
    main()
