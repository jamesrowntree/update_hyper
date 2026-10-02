"""
publish_metadata.py

Applies a metadata JSON file (as produced by generate_metadata.py) to an
EXISTING Tableau Cloud data source -- description, certification, tags,
column descriptions, and calculated fields -- without touching its extract
data. No local .hyper file is read or uploaded.

Why this exists
----------------
clientside_publish_hyper.py always publishes a .hyper file; --metadata is
only ever applied alongside that data refresh. This script is for the case
where only the *metadata* changed -- tidied up a field description, added a
calculated field, want to tag/certify the data source -- and there's no
reason to re-run the hyper pipeline or re-upload an extract just to do it.

How "no data" is achieved
--------------------------
Tableau's REST API has no endpoint that edits a published data source's .tds
model directly (see the "Dead end: Tableau Catalog / Metadata API" note in
readme_Union.html) -- the only way to change column descriptions or
calculated fields is to download the .tdsx, patch the .tds XML, and
republish it. This script does exactly that, but -- unlike
clientside_publish_hyper.py's refresh path -- it NEVER swaps in a new
.hyper: the extract bytes already inside the downloaded .tdsx are repacked
unchanged, so the republish hands the data source its own existing extract
back, not a fresh one. Description/certification are set on the
DatasourceItem before this republish (the same "set before publish, not
update()" finding documented in clientside_publish_hyper.py); tags go
through the separate /tags endpoint, same as there.

The data source must already exist -- there's no bare extract here to
bootstrap a new one from. Publish it at least once with
clientside_publish_hyper.py first.

Configuration (TABLEAU_* env vars), signing in, logging setup, project/
data-source lookups, and .tds-patching are shared with the other scripts in
scripts/ -- see tableau_auth.py, tableau_logging.py, tableau_lookup.py, and
tds_model.py; see .env.example for the env vars themselves. Every run writes
a full, timestamped record to a per-run log file under logs/ (e.g.
logs/metadata_20260914-093015.log); --silent only silences the console,
never the log.

Never commit .env, paste its contents into chat, or share it: it holds a
Tableau Personal Access Token that can overwrite content on your site.

Usage (run from the project root):
    python3 scripts/publish_metadata.py --target=<name> --metadata=<file>.json [--dry-run] [--silent]

    --target is required -- the name of the data source ALREADY PUBLISHED on
    Tableau Cloud. There is no default and no bootstrap path.

    --metadata is required -- this script has nothing to do without one.

    Examples:

    python3 scripts/publish_metadata.py --target="Finished Merged" --metadata=data/datasource_metadata.json
        Applies description, certification, tags, column descriptions, and
        calculated fields from the metadata JSON to the already-published
        "Finished Merged" data source. Its extract data is untouched.

    python3 scripts/publish_metadata.py --target="Finished Merged" --metadata=data/datasource_metadata.json --dry-run
        Prints what would be applied -- without making any network call to
        Tableau Cloud.
"""

import argparse
import logging
import os

import tableauserverclient as TSC

from tableau_auth import load_config, connect
from tableau_logging import setup_logging, make_section
from tableau_lookup import find_project, find_datasource
from tds_model import (
    load_metadata,
    patch_tds,
    download_tdsx_bytes,
    read_tds_from_tdsx,
    rebuild_tdsx,
    summary_line,
    apply_tags,
)

# All human-readable output goes through this logger, never print(), so the
# same messages reach a timestamped log file (always) and stdout (unless
# --silent). See setup_logging().
logger = logging.getLogger("publish_metadata")
section = make_section(logger)

TARGET_USAGE = "--target=<existing-datasource-name>"
METADATA_USAGE = "--metadata=<path-to-file>.json"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Apply a metadata JSON file to an existing Tableau Cloud data source, without touching its extract data."
    )
    parser.add_argument(
        "--target",
        metavar="NAME",
        help=(
            "Name of the data source to update, e.g. --target='Finished Merged'. "
            "Must already be published -- this script never creates one. Required."
        ),
    )
    parser.add_argument(
        "--metadata",
        metavar="FILE.json",
        help=(
            "Path to a metadata JSON file, as produced by generate_metadata.py: "
            "description, tags, certification, column descriptions, and any "
            "calculated fields listed under a 'calculations' block. Required."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print what would be applied -- description, certification, tags, "
            "and which column descriptions/calculated fields would be patched "
            "onto the model -- without making any network call to Tableau Cloud."
        ),
    )
    parser.add_argument(
        "-s", "--s", "--silent",
        dest="silent",
        action="store_true",
        help=(
            "Suppress all stdout output. The run still writes a full, "
            "timestamped record to a log file under logs/ -- --silent only "
            "silences the console, never the log."
        ),
    )
    return parser.parse_args()


@section("dry-run report")
def print_dry_run(config, target, ds_meta, columns_metadata, calculations):
    """
    dry-run report
    --------------
    Reports the plan derived entirely from local config/metadata -- no
    TSC.Server is constructed, so this makes zero network calls. Because it
    never talks to the server, it can't confirm the data source exists; it
    just reports what would be applied to it.
    """
    logger.info("-- DRY RUN: no network call will be made, nothing will be published --")
    logger.info(
        "Target site: %s  site=%r  project=%r",
        config["server_url"], config["site_content_url"], config["project_name"],
    )
    logger.info(
        "Would update existing data source %r -- metadata only, extract data untouched:",
        target,
    )
    logger.info("  description: %s", ds_meta.get("description") or "(none)")
    certified = bool(ds_meta.get("certified", False))
    note = f" -- {ds_meta['certification_note']}" if ds_meta.get("certification_note") else ""
    logger.info("  certified: %s%s", certified, note)
    tags = sorted(set(ds_meta.get("tags", [])))
    logger.info("  tags: %s", tags if tags else "(none)")

    describable = [
        col for col in columns_metadata
        if (col.get("description") or "").strip()
        and col["description"].strip() != "No description available."
    ]
    logger.info(
        "  column descriptions: %d of %d column(s) would be applied (via a download/patch/republish of the .tds)",
        len(describable), len(columns_metadata),
    )
    for col in describable:
        logger.info("    %r: %s", col["name"], col["description"])

    if calculations:
        logger.info("  calculated fields: %d would be added or updated on the model:", len(calculations))
        for calc in calculations:
            logger.info("    %r = %s", calc["name"], calc["formula"])
    else:
        logger.info("  calculated fields: none in metadata (any already on the data source are preserved)")


@section("publish metadata only")
def publish_metadata_only(server, existing_item, ds_meta, columns_metadata, calculations):
    """
    publish metadata only
    -----------------------
    Download the data source's current .tdsx, patch its .tds (column
    descriptions + calculated fields), and republish it with the SAME
    extract bytes it already had -- no local .hyper file is ever read, so
    the data is byte-for-byte unchanged. Description/certification are set
    on the DatasourceItem beforehand, since only the publish-time request
    applies them (tableauserverclient's update() silently drops
    description -- see clientside_publish_hyper.py).

    Returns (published_item, summary).
    """
    existing_item.description = ds_meta.get("description")
    existing_item.certified = bool(ds_meta.get("certified", False))
    if ds_meta.get("certification_note"):
        existing_item.certification_note = ds_meta["certification_note"]

    tdsx_bytes = download_tdsx_bytes(server, existing_item)
    tds_member, tds_bytes = read_tds_from_tdsx(tdsx_bytes)
    new_tds_bytes, summary = patch_tds(tds_bytes, columns_metadata, calculations)
    logger.debug("Patched .tds in place: %s", summary)

    # new_hyper_bytes is deliberately omitted: the extract member already in
    # the downloaded .tdsx is repacked as-is, so no data changes.
    rebuilt = rebuild_tdsx(tdsx_bytes, tds_member, new_tds_bytes)
    logger.debug("Republishing rebuilt .tdsx with Overwrite (extract bytes unchanged)...")
    published = server.datasources.publish(existing_item, rebuilt, TSC.Server.PublishMode.Overwrite)
    return published, summary


@section("run")
def _run(args):
    """
    run
    ---
    Validate args, load config/metadata, then either print the dry-run plan
    or sign in and apply the metadata to the existing data source.
    """
    if not args.target:
        raise SystemExit(
            "Missing required argument: --target (no data source name was specified).\n"
            f"Usage:   {TARGET_USAGE}\n"
            "Example: python3 scripts/publish_metadata.py --target='Finished Merged' --metadata=data/datasource_metadata.json"
        )
    if not args.metadata:
        raise SystemExit(
            "Missing required argument: --metadata (nothing to apply without one).\n"
            f"Usage:   {METADATA_USAGE}\n"
            "Run generate_metadata.py first if you don't have one yet."
        )

    with section("load configuration"):
        config = load_config()
    metadata = load_metadata(args.metadata)
    ds_meta = metadata["datasource"]
    columns_metadata = metadata.get("columns", [])
    calculations = metadata.get("calculations", [])

    if args.dry_run:
        print_dry_run(config, args.target, ds_meta, columns_metadata, calculations)
        return

    logger.debug("Signing in to %s (site %r)...", config["server_url"], config["site_content_url"])
    with section("sign in"), connect(config) as server:
        logger.debug("Signed in; server API version %s", server.version)
        project = find_project(server, config["project_name"])
        existing = find_datasource(server, project, args.target)
        if existing is None:
            raise SystemExit(
                f"Data source {args.target!r} does not exist in project "
                f"{config['project_name']!r}. This script only updates metadata "
                "on a data source that's already published -- publish it at "
                "least once with clientside_publish_hyper.py first."
            )
        # get_by_id gives a fully-populated item so description/certification
        # set below don't start from a half-empty object.
        existing = server.datasources.get_by_id(existing.id)

        with section("apply metadata to existing data source"):
            logger.info(
                "Applying metadata to %r (id %s) in project %r -- extract data will not change...",
                args.target, existing.id, config["project_name"],
            )
            published, summary = publish_metadata_only(server, existing, ds_meta, columns_metadata, calculations)
            logger.info("Published. Data source ID: %s", published.id)
            logger.info(
                "URL: %s/#/site/%s/datasources/%s",
                config["server_url"], config["site_content_url"], published.id,
            )

        applied = apply_tags(server, published, ds_meta)
        if applied:
            logger.info("Applied tags: %s", applied)
        logger.info(summary_line(summary))

    logger.info("Done.")


def main():
    args = parse_args()
    log_path = setup_logging(logger, "metadata", args.silent)
    # Name the script at the very top of every log (and console) so a log
    # file is unmistakably attributable: logs/ also holds clientside_*.log
    # and serverside_*.log from the other two scripts.
    logger.info("Script: %s", os.path.basename(__file__))
    logger.info("Logging this run to %s", log_path)
    logger.debug(
        "Args: target=%r metadata=%r dry_run=%s silent=%s",
        args.target, args.metadata, args.dry_run, args.silent,
    )
    try:
        _run(args)
    except SystemExit as exc:
        # Usage/validation failures raise SystemExit with a message. Record it,
        # then exit with status 1 WITHOUT re-raising the string -- re-raising it
        # would make Python re-print the message to stderr, duplicating the
        # console line and, worse, breaking --silent's no-stdout guarantee.
        if exc.code not in (0, None):
            logger.error("%s", exc.code)
            raise SystemExit(1) from exc
        raise
    except Exception:
        logger.exception("Unhandled error -- aborting")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
