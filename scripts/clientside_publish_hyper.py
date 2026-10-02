"""
clientside_publish_hyper.py

Publishes a .hyper file to Tableau Cloud as a published data source, and,
if a metadata JSON file (as produced by generate_metadata.py) is given via
--metadata, applies it: name, description, tags, certification, and
per-column field descriptions.

Column descriptions and calculated fields are applied by editing the data
source's own .tds XML directly and republishing it.

When the data source already exists on Cloud, its current model is downloaded
and edited in place -- so any calculated fields, folders, or aliases added in
Tableau are preserved rather than clobbered -- and only its extract data is
swapped for the local .hyper. A brand-new data source is bootstrapped from the
.hyper, then the model metadata is patched onto it.

Configuration (TABLEAU_* env vars), signing in, logging setup, project/
data-source lookups, and .tds-patching are shared with the other scripts in
scripts/ -- see tableau_auth.py, tableau_logging.py, tableau_lookup.py, and
tds_model.py. Configuration itself is read from environment variables -- see
.env.example for the full list and what each one means. Copy .env.example to
.env, fill in your real values; load_config() loads it automatically via
python-dotenv.

Never commit .env, paste its contents into chat, or share it: it holds a
Tableau Personal Access Token that can publish/overwrite content on your site.

Every run -- silent or not, dry-run or not -- writes a full, timestamped
record to a per-run log file under logs/ (e.g. logs/clientside_20260914-093015.log).
The log always captures everything; --silent only silences the console.

Usage:
    python3 clientside_publish_hyper.py --source=<file>.hyper [--target=<name>] [--metadata=<file>.json] [--dry-run] [--silent]

    --source is required -- there is no default .hyper file. Omitting it is
    an error that prints this exact usage line.

    --silent (-s) suppresses all stdout output. The run still writes its
    complete, timestamped log file under logs/ -- nothing is lost, the console
    is just quiet. Useful for cron/scheduled runs.

    --target is optional -- it's the name the data source will have on
    Tableau Cloud. If omitted, it defaults to the --source filename with
    its extension stripped and underscores replaced with spaces, e.g.
    Finished_Merged.hyper -> "Finished Merged".

    --metadata is optional -- omit it to publish with no description,
    certification, tags, or column descriptions applied. Passing --metadata
    with a file that doesn't exist is an error (it means you asked for
    metadata to be applied but nothing can be read).

    Examples (run from the project root):

    python3 scripts/clientside_publish_hyper.py --source=data/Finished_Merged.hyper
        Publishes data/Finished_Merged.hyper as data source "Finished Merged"
        (derived from the filename) with no metadata applied.

    python3 scripts/clientside_publish_hyper.py --source=data/Finished_Merged.hyper --target="Rugby Chains"
        Publishes the same file, but names the data source "Rugby Chains"
        instead of the derived default.

    python3 scripts/clientside_publish_hyper.py --source=data/Finished_Merged.hyper --metadata=data/datasource_metadata.json
        Publishes data/Finished_Merged.hyper and applies name, description, tags,
        certification, and column descriptions from data/datasource_metadata.json
        (the file generate_metadata.py produces).

    python3 scripts/clientside_publish_hyper.py --source=data/Finished_Merged.hyper --metadata=data/datasource_metadata.json --dry-run
        Prints what would be published/updated -- target site and project,
        datasource name, description, certification, tags, and which column
        descriptions would be applied -- without making any network call to
        Tableau Cloud (nothing is published or overwritten).
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
# same messages can be sent to a timestamped log file (always) and to stdout
# (unless --silent). See setup_logging().
logger = logging.getLogger("clientside_publish_hyper")
section = make_section(logger)

SOURCE_USAGE = "--source=<path-to-file>.hyper"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Publish a .hyper file to Tableau Cloud and (optionally) apply a metadata JSON file to it."
    )
    parser.add_argument(
        "--source",
        metavar="FILE.hyper",
        help=(
            "Path to the .hyper file to publish, e.g. --source=Finished_Merged.hyper. "
            "Required."
        ),
    )
    parser.add_argument(
        "--target",
        metavar="NAME",
        help=(
            "Name to give the published data source on Tableau Cloud, e.g. --target='Finished Merged'. "
            "Optional -- if omitted, defaults to the --source filename with"
            "its extension stripped and underscores replaced with spaces"
            ", e.g. Finished_Merged.hyper "'-> "Finished Merged".'
        ),
    )
    parser.add_argument(
        "--metadata",
        metavar="FILE.json",
        help=(
            "Path to a metadata JSON file, as produced by generate_metadata.py, to "
            "apply to the published data source (e.g. --metadata=datasource_metadata.json): "
            "name, description, tags, certification, column descriptions, and any "
            "calculated fields listed under a 'calculations' block. "
            "Optional -- omit it to publish with none of that applied."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print what would be published -- target site/project, datasource "
            "name, description, certification, tags, and which column "
            "descriptions would be applied -- without making any network "
            "call to Tableau Cloud."
        ),
    )
    parser.add_argument(
        "-s",
        "--s",
        "--silent",
        dest="silent",
        action="store_true",
        help=(
            "Suppress all stdout output. The run still writes a full, "
            "timestamped record to a log file under logs/ -- --silent only "
            "silences the console, never the log."
        ),
    )
    return parser.parse_args()


@section("resolve target name")
def resolve_target(hyper_file, target_arg):
    """
    resolve target name
    --------------------
    The data source's name on Tableau Cloud: --target if given, otherwise
    derived from --source itself (extension stripped, underscores -> spaces)
    so there's always a sensible name without a hardcoded default tied to
    one specific file.
    """
    if target_arg:
        return target_arg, "given via --target"
    derived = os.path.splitext(os.path.basename(hyper_file))[0].replace("_", " ")
    return derived, "derived from --source (no --target given)"


def _print_published(config, published):
    logger.info("Published. Data source ID: %s", published.id)
    logger.info("URL: %s/#/site/%s/datasources/%s", config["server_url"], config["site_content_url"], published.id)


@section("publish preserving model")
def publish_preserving_model(server, existing_item, hyper_file, columns_metadata, calculations):
    """
    publish preserving model
    -------------------------
    Refresh an existing data source's data WITHOUT discarding its model.

    Downloads the data source's current .tdsx, patches the embedded .tds
    (column descriptions + calculated fields), swaps in the fresh local .hyper
    extract, and republishes once with Overwrite. Existing calculated fields,
    folders and aliases survive because the model is edited in place, never
    regenerated from the bare .hyper.

    NB: the extract swap assumes the local .hyper's schema matches the
    connection ("public"."Extract", same columns) -- true for a data refresh.
    A column added/removed would need the .tds's <metadata-record>s
    regenerated, which this does not do.

    Returns (published_item, summary).
    """
    tdsx_bytes = download_tdsx_bytes(server, existing_item)
    tds_member, tds_bytes = read_tds_from_tdsx(tdsx_bytes)
    new_tds_bytes, summary = patch_tds(tds_bytes, columns_metadata, calculations)
    logger.debug("Patched .tds in place: %s", summary)

    with open(hyper_file, "rb") as f:
        fresh_hyper = f.read()
    logger.debug("Read fresh extract %s (%d bytes) to swap in", hyper_file, len(fresh_hyper))

    rebuilt = rebuild_tdsx(tdsx_bytes, tds_member, new_tds_bytes, new_hyper_bytes=fresh_hyper)
    logger.debug("Republishing rebuilt .tdsx with Overwrite...")
    published = server.datasources.publish(existing_item, rebuilt, TSC.Server.PublishMode.Overwrite)
    return published, summary


@section("apply model metadata after publish")
def apply_model_metadata_after_publish(server, datasource_item, columns_metadata, calculations):
    """
    apply model metadata after publish
    -----------------------------------
    Bootstrap case only: a brand-new data source was just published from the
    bare .hyper (which generates a fresh model), so there is nothing to
    preserve. Download that just-published .tdsx, patch its .tds with column
    descriptions and calculated fields, and republish. The extract is already
    current, so only the .tds is swapped.

    A failure here never aborts the run -- name/description/tags/certification
    were already applied by the initial publish.
    """
    if not columns_metadata and not calculations:
        return
    try:
        tdsx_bytes = download_tdsx_bytes(server, datasource_item)
        tds_member, tds_bytes = read_tds_from_tdsx(tdsx_bytes)
    except Exception as e:
        logger.warning("Skipping column descriptions/calculations -- could not read the published datasource: %s", e)
        return

    new_tds_bytes, summary = patch_tds(tds_bytes, columns_metadata, calculations)
    if not any(summary.values()):
        logger.info("Skipping republish -- nothing in the metadata matched the model.")
        return

    rebuilt = rebuild_tdsx(tdsx_bytes, tds_member, new_tds_bytes)
    try:
        server.datasources.publish(datasource_item, rebuilt, TSC.Server.PublishMode.Overwrite)
    except Exception as e:
        logger.warning("Could not republish with model metadata: %s", e)
        return
    logger.info(summary_line(summary))


@section("dry-run report")
def print_dry_run(config, hyper_file, target, target_origin, ds_meta, columns_metadata, calculations, skip_metadata):
    """
    dry-run report
    --------------
    Reports the same plan main() would otherwise execute, derived entirely
    from local config/metadata -- no TSC.Server is constructed, so this
    makes zero network calls (even TSC.Server(..., use_server_version=True)
    itself would ping the server, which is why this returns before that
    line rather than short-circuiting inside a `with connect(config)`
    block). Because it makes no network call, it can't know whether the data
    source already exists -- it reports what would be applied either way.
    """
    logger.info("-- DRY RUN: no network call will be made, nothing will be published --")
    logger.info(f"Target site: {config['server_url']}  site={config['site_content_url']!r}  project={config['project_name']!r}")
    logger.info(f"Would publish {hyper_file!r} as data source {target!r} ({target_origin}) (PublishMode.Overwrite):")
    logger.info(
        "  model: if the data source already exists, its current .tds is edited in "
        "place (existing calculated fields, folders and aliases preserved) and only "
        "the extract data is swapped; if it's new, it's bootstrapped from the .hyper."
    )

    if skip_metadata:
        logger.info("  metadata: none applied (no --metadata given) -- no description, certification, tags, column descriptions, or calculations")
        return

    logger.info(f"  description: {ds_meta.get('description') or '(none)'}")
    certified = bool(ds_meta.get("certified", False))
    note = f" -- {ds_meta['certification_note']}" if ds_meta.get("certification_note") else ""
    logger.info(f"  certified: {certified}{note}")
    tags = sorted(set(ds_meta.get("tags", [])))
    logger.info(f"  tags: {tags if tags else '(none)'}")

    describable = [
        col for col in columns_metadata
        if (col.get("description") or "").strip()
        and col["description"].strip() != "No description available."
    ]
    logger.info(
        f"  column descriptions: {len(describable)} of {len(columns_metadata)} "
        "column(s) would be applied (via a download/patch/republish of the .tds)"
    )
    for col in describable:
        logger.info(f"    {col['name']!r}: {col['description']}")

    if calculations:
        logger.info(f"  calculated fields: {len(calculations)} would be added or updated on the model:")
        for calc in calculations:
            logger.info(f"    {calc['name']!r} = {calc['formula']}")
    else:
        logger.info("  calculated fields: none in metadata (any already on the data source are preserved)")


@section("run")
def _run(args):
    """
    run
    ---
    Top-level orchestration: validate args, load config/metadata, then either
    print the dry-run plan or sign in and publish. Each step below is its own
    named section, so the log names whichever one is running (and whichever
    one fails).
    """
    if not args.source:
        raise SystemExit(
            "Missing required argument: --source (no .hyper file to publish was specified).\n"
            f"Usage:   {SOURCE_USAGE}\n"
            "Example: python3 scripts/clientside_publish_hyper.py --source=data/Finished_Merged.hyper"
        )
    hyper_file = args.source
    with section("load configuration"):
        config = load_config()

    if not os.path.exists(hyper_file):
        raise SystemExit(f"{hyper_file} not found. Run generate_split_by_year.py then union_hyper_files.py first.")

    target, target_origin = resolve_target(hyper_file, args.target)

    if args.metadata:
        metadata = load_metadata(args.metadata)
        ds_meta = metadata["datasource"]
        columns_metadata = metadata.get("columns", [])
        calculations = metadata.get("calculations", [])
    else:
        ds_meta = {}
        columns_metadata = []
        calculations = []

    if args.dry_run:
        print_dry_run(config, hyper_file, target, target_origin, ds_meta, columns_metadata, calculations, skip_metadata=not args.metadata)
        return

    # sign in
    # -------
    logger.debug("Signing in to %s (site %r)...", config["server_url"], config["site_content_url"])
    with section("sign in"), connect(config) as server:
        logger.debug("Signed in; server API version %s", server.version)
        project = find_project(server, config["project_name"])
        existing = find_datasource(server, project, target)
        logger.debug(
            "Data source %r %s in project %r",
            target,
            "already exists (preserve path)" if existing else "does not exist yet (bootstrap path)",
            config["project_name"],
        )

        if existing is None:
            # publish new data source (bootstrap)
            # -----------------------------------
            # No data source by this name yet, so there's no model to preserve.
            # Publish the bare .hyper (Tableau generates a fresh model), then
            # patch descriptions/calculations onto it.
            #
            # Name, description, and certification must be set on the DatasourceItem
            # BEFORE publish(): the update-after-publish request does not include
            # description at all (verified against tableauserverclient's request
            # builder), so setting it after the fact would silently do nothing.
            with section("publish new data source"):
                new_datasource = TSC.DatasourceItem(project_id=project.id, name=target)
                new_datasource.description = ds_meta.get("description")
                new_datasource.certified = bool(ds_meta.get("certified", False))
                if ds_meta.get("certification_note"):
                    new_datasource.certification_note = ds_meta["certification_note"]

                logger.info(
                    "Publishing %s to project %r as %r (%s) [new data source]...",
                    hyper_file, config["project_name"], target, target_origin,
                )
                published_ds = server.datasources.publish(
                    new_datasource, hyper_file, TSC.Server.PublishMode.Overwrite
                )
                _print_published(config, published_ds)

            if not args.metadata:
                logger.info("No --metadata given: no description, certification, tags, column descriptions, or calculations applied.")
            else:
                # Tags are NOT part of the publish payload -- they require a
                # separate call against the /tags endpoint.
                applied = apply_tags(server, published_ds, ds_meta)
                if applied:
                    logger.info("Applied tags: %s", applied)
                apply_model_metadata_after_publish(server, published_ds, columns_metadata, calculations)
        else:
            # refresh existing data source (preserve model)
            # ---------------------------------------------
            # The data source already exists, so keep its model (including any
            # calculated fields authored in Tableau) and only refresh its
            # extract, editing the .tds in place. get_by_id gives a
            # fully-populated item so a metadata-less republish won't blank the
            # existing description/certification.
            with section("refresh existing data source"):
                existing = server.datasources.get_by_id(existing.id)
                if args.metadata:
                    existing.description = ds_meta.get("description")
                    existing.certified = bool(ds_meta.get("certified", False))
                    if ds_meta.get("certification_note"):
                        existing.certification_note = ds_meta["certification_note"]

                logger.info(
                    "Refreshing existing data source %r (id %s) in project %r "
                    "-- preserving its model, swapping in %s...",
                    target, existing.id, config["project_name"], hyper_file,
                )
                published_ds, summary = publish_preserving_model(
                    server, existing, hyper_file, columns_metadata, calculations
                )
                _print_published(config, published_ds)

            if args.metadata:
                applied = apply_tags(server, published_ds, ds_meta)
                if applied:
                    logger.info("Applied tags: %s", applied)
            logger.info(summary_line(summary))

    logger.info("Done.")


def main():
    args = parse_args()
    log_path = setup_logging(logger, "clientside", args.silent)
    # Name the script at the very top of every log (and console) so a log
    # file is unmistakably attributable: logs/ also holds serverside_*.log
    # from serverside_publish_hyper.py and metadata_*.log from
    # publish_metadata.py.
    logger.info("Script: %s", os.path.basename(__file__))
    # First line so a reader (and anyone tailing the file) knows where the full
    # record lives. In --silent mode this reaches only the log, not the console.
    logger.info("Logging this run to %s", log_path)
    logger.debug(
        "Args: source=%r target=%r metadata=%r dry_run=%s silent=%s",
        args.source, args.target, args.metadata, args.dry_run, args.silent,
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
