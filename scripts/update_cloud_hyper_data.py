"""
update_cloud_hyper_data.py

Grow a published Tableau Cloud data source by pushing individual .hyper
"pieces" to it and letting Cloud union them SERVER-SIDE, instead of unioning
everything locally (union_hyper_files.py) and republishing one large extract
(publish_to_tableau_cloud.py).

Why this exists
---------------
A single publish to Tableau Cloud is bounded in size. When the full,
locally-unioned extract (Finished_Merged.hyper) gets large, publishing it in
one shot can hit that ceiling. This script sidesteps that by using the REST
"Update Data in Hyper" API (tableauserverclient's
`datasources.update_hyper_data`): it uploads each piece file on its own and
issues an `insert` (append) or `replace` (clear-then-load) action, so the
server assembles the full data set from many small uploads. The *aggregate*
data source can be far larger than any single upload.

Two modes
---------
  --mode append   Insert one or more pieces into the EXISTING data source,
                  leaving its current rows in place. Use for incremental
                  loads (e.g. a new season's file). Re-sending the same piece
                  duplicates its rows -- send each piece once.

  --mode reload   Rebuild the whole data source from the given pieces: the
                  first piece is loaded with `replace` (which clears the
                  target table first), every remaining piece with `insert`.
                  If the target data source doesn't exist yet, it is created
                  by publishing the first piece, then the rest are inserted.

Model is preserved
-------------------
`update_hyper_data` changes DATA only, so calculated fields, column
descriptions, folders and aliases already on the data source survive
untouched -- no .tds download/patch/rebuild is needed (unlike
publish_to_tableau_cloud.py). The one exception is the create path (a brand-new
data source published from a bare piece has only the fields in the extract).

The size ceiling still applies PER PIECE
----------------------------------------
Each uploaded payload is bounded by the Cloud setting
`api.server.update_uploaded_file.max_size_in_mb` (default 100 MB, fixed on
Cloud -- chunked upload does NOT let a single update exceed it). So every
piece file must be <= that limit even though the whole data source may exceed
it. This script runs a PRE-FLIGHT SIZE CHECK and refuses to start if any piece
is too big, telling you to split it finer (e.g. by month) rather than
half-loading the target.

Target: a SEPARATE test data source by default
-----------------------------------------------
To avoid touching the live "Finished Merged" data source while validating this
approach, --datasource-name defaults to DEFAULT_TARGET (a distinct name).
Point it at the real data source only once you're happy.

Source vs target table names
----------------------------
The piece files carry table "public"."Extract" (union_hyper_files.py's
SCHEMA_NAME/TABLE). A data source published from such a .hyper stores its
extract as "Extract"."Extract" on Cloud (see data/Finished_Merged.tds's
<connection schema='Extract' tablename='Extract'>), which is why the target
defaults differ from the source. If an update fails complaining about an
unknown table, flip --target-schema (e.g. to public); both are configurable.

Configuration (TABLEAU_* env vars) and the .env handling are shared with
publish_to_tableau_cloud.py -- see .env.example. Every run writes a full,
timestamped record to a per-run log file under logs/ (e.g.
logs/update_20260914-093015.log); --silent only silences the console, never
the log.

Never commit .env, paste its contents into chat, or share it: it holds a
Tableau Personal Access Token that can overwrite content on your site.

Usage (run from the project root):
    # Build the whole test data source from all yearly pieces server-side
    # (creates it if it doesn't exist):
    python3 scripts/update_cloud_hyper_data.py --mode reload

    # Append just the new season's piece to it:
    python3 scripts/update_cloud_hyper_data.py --mode append --files data/Start_2026.hyper

    # See the plan + per-piece size report without calling Cloud:
    python3 scripts/update_cloud_hyper_data.py --mode reload --dry-run

    # Push to a specific data source (e.g. the real one, once validated):
    python3 scripts/update_cloud_hyper_data.py --mode reload --datasource-name "Finished Merged"
"""

import argparse
import functools
import logging
import os
import sys
import uuid
from datetime import datetime
from glob import glob

import tableauserverclient as TSC

# Reuse the publish script's .env loading and lookups rather than duplicating
# them -- both live here in scripts/ and are import-safe (their real work is
# guarded behind `if __name__ == "__main__"`). find_project/find_datasource
# log to publish's own logger; only their return values matter here.
from publish_to_tableau_cloud import find_project, find_datasource, load_config

# All human-readable output goes through this logger, never print(), so the
# same messages reach a timestamped log file (always) and stdout (unless
# --silent). See setup_logging().
logger = logging.getLogger("update_cloud_hyper_data")

# Default target data source name -- deliberately NOT "Finished Merged" so this
# leaves the live data source alone while you validate the server-side union.
DEFAULT_TARGET = "Finished Merged (API Union Test)"

# Per-payload upload ceiling. The Cloud default for
# api.server.update_uploaded_file.max_size_in_mb; fixed on Cloud.
DEFAULT_MAX_PAYLOAD_MB = 100.0

# Table names inside the piece files (match union_hyper_files.py) and the
# defaults for the table inside the published extract on Cloud.
DEFAULT_SOURCE_SCHEMA = "public"
DEFAULT_SOURCE_TABLE = "Extract"
DEFAULT_TARGET_SCHEMA = "Extract"
DEFAULT_TARGET_TABLE = "Extract"

# Every .hyper file lives in the project's data/ folder, a sibling of this
# scripts/ folder. Resolve it relative to this file (not the cwd) so the script
# runs correctly from anywhere.
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
DEFAULT_GLOB = os.path.join(DATA_DIR, "Start_*.hyper")


class section:
    """
    Section marker
    --------------
    Names a block of work so the log tells you which stage is running and, if
    something breaks, exactly which stage broke. Mirrors the helper of the same
    name in publish_to_tableau_cloud.py. Usable as a decorator or a context
    manager:

        with section("insert Start_2026.hyper"):
            ...

    On entry it logs "Section: <name>" (DEBUG -- recorded in the log file
    without cluttering the console). If the block raises an ordinary Exception
    it logs "Error in section '<name>': ..." once (the innermost section wins)
    then lets it propagate; SystemExit passes through untouched.
    """

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        logger.debug("Section: %s", self.name)
        return self

    def __exit__(self, exc_type, exc, tb):
        if isinstance(exc, Exception) and not getattr(exc, "_section_logged", False):
            logger.error("Error in section %r: %s", self.name, exc)
            try:
                exc._section_logged = True
            except Exception:
                pass  # a few exception types forbid attribute assignment
        return False  # never suppress the exception

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with section(self.name):
                return func(*args, **kwargs)
        return wrapper


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Push .hyper pieces to a Tableau Cloud data source and union them "
            "server-side via the REST 'Update Data in Hyper' API."
        )
    )
    parser.add_argument(
        "--mode",
        choices=["append", "reload"],
        required=True,
        help=(
            "append = insert the given pieces into the existing data source "
            "(keeps current rows); reload = replace the data source's data with "
            "the union of all given pieces (first piece via 'replace', rest via "
            "'insert'; creates the data source if it doesn't exist)."
        ),
    )
    parser.add_argument(
        "--files",
        metavar="PATH_OR_GLOB",
        nargs="+",
        help=(
            "One or more .hyper piece files (paths and/or globs). Defaults to "
            f"{DEFAULT_GLOB!r} (the yearly files generate_split_by_year.py "
            "produces)."
        ),
    )
    parser.add_argument(
        "--datasource-name",
        metavar="NAME",
        default=DEFAULT_TARGET,
        help=(
            "Name of the target data source on Cloud, within the project set by "
            f"TABLEAU_PROJECT_NAME in .env. Defaults to {DEFAULT_TARGET!r} so the "
            "live data source is left alone -- override to target the real one."
        ),
    )
    parser.add_argument(
        "--id",
        metavar="LUID",
        help=(
            "Target the data source by LUID instead of by name (skips the "
            "project lookup). Must already exist; cannot be combined with "
            "--create-new."
        ),
    )
    parser.add_argument(
        "--source-schema", metavar="SCHEMA", default=DEFAULT_SOURCE_SCHEMA,
        help=f"Schema of the table inside the piece files. Default {DEFAULT_SOURCE_SCHEMA!r}.",
    )
    parser.add_argument(
        "--source-table", metavar="TABLE", default=DEFAULT_SOURCE_TABLE,
        help=f"Table name inside the piece files. Default {DEFAULT_SOURCE_TABLE!r}.",
    )
    parser.add_argument(
        "--target-schema", metavar="SCHEMA", default=DEFAULT_TARGET_SCHEMA,
        help=(
            "Schema of the target table inside the published extract on Cloud. "
            f"Default {DEFAULT_TARGET_SCHEMA!r} (try 'public' if updates fail with "
            "an unknown-table error)."
        ),
    )
    parser.add_argument(
        "--target-table", metavar="TABLE", default=DEFAULT_TARGET_TABLE,
        help=f"Target table name inside the published extract. Default {DEFAULT_TARGET_TABLE!r}.",
    )
    parser.add_argument(
        "--max-payload-mb",
        metavar="MB",
        type=float,
        default=DEFAULT_MAX_PAYLOAD_MB,
        help=(
            "Per-piece payload ceiling for the pre-flight size check, in MB. "
            f"Default {DEFAULT_MAX_PAYLOAD_MB:g} (the Cloud default for "
            "api.server.update_uploaded_file.max_size_in_mb, fixed on Cloud). "
            "Any piece at or above this aborts the run before anything uploads."
        ),
    )
    parser.add_argument(
        "--create-new",
        action="store_true",
        help=(
            "Force the create path: publish the first piece as a fresh data "
            "source (Overwrite -- replaces model + data if it already exists), "
            "then insert the rest. Reload auto-creates when the target is "
            "missing, so you only need this to rebuild from scratch on purpose."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print the plan and the per-piece size report, then exit without "
            "making any network call to Tableau Cloud."
        ),
    )
    parser.add_argument(
        "-s", "--s", "--silent",
        dest="silent",
        action="store_true",
        help=(
            "Suppress all stdout output. The run still writes a full, "
            "timestamped record to a log file under logs/."
        ),
    )
    return parser.parse_args()


def setup_logging(silent):
    """
    Wire up logging so every run leaves a full, timestamped trail on disk,
    regardless of --silent or --dry-run. Mirrors publish_to_tableau_cloud.py:
    a DEBUG file handler under logs/ captures everything; an INFO stdout handler
    is added only when not silent. Returns the log file path.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"update_{datetime.now():%Y%m%d-%H%M%S}.log")

    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()  # idempotent if ever called more than once

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    logger.addHandler(file_handler)

    if not silent:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(console_handler)

    # Name the script at the very top of every log (and console) so a log file
    # is unmistakably attributable: logs/ also holds publish_*.log from the
    # publish script, and this header -- plus the update_/publish_ filename
    # prefix -- keeps the two apart at a glance.
    logger.info("Script: %s", os.path.basename(__file__))

    return log_path


@section("resolve piece files")
def resolve_files(files_args):
    """
    resolve piece files
    -------------------
    Expand --files (paths and/or globs; defaults to DEFAULT_GLOB) into a sorted,
    de-duplicated list of existing .hyper files. Fails with guidance if nothing
    matches or a listed path is missing.
    """
    patterns = files_args if files_args else [DEFAULT_GLOB]

    resolved = []
    for pattern in patterns:
        matches = glob(pattern)
        if matches:
            resolved.extend(matches)
        elif os.path.exists(pattern):
            resolved.append(pattern)
        elif not files_args:
            # The default glob simply matched nothing.
            raise SystemExit(
                f"No piece files found matching {pattern!r}. "
                "Run generate_split_by_year.py first, or pass --files."
            )
        else:
            raise SystemExit(
                f"--files entry {pattern!r} matched no files and is not an "
                "existing path."
            )

    files = sorted({os.path.abspath(f) for f in resolved})
    non_hyper = [f for f in files if not f.endswith(".hyper")]
    if non_hyper:
        raise SystemExit(
            "Only .hyper piece files can be pushed; these are not .hyper: "
            + ", ".join(os.path.basename(f) for f in non_hyper)
        )
    if not files:
        raise SystemExit("No .hyper piece files resolved from --files.")
    return files


@section("pre-flight size check")
def check_payload_sizes(files, max_payload_mb):
    """
    pre-flight size check
    ---------------------
    Each uploaded payload IS the piece file, so its on-disk size is an exact
    proxy for the update payload size. Log a per-piece size table, then ABORT
    before any upload if any piece is at/over max_payload_mb (so a doomed run
    never half-loads the target). Warn on pieces in the 90-100% band.
    """
    max_bytes = max_payload_mb * 1024 * 1024
    warn_bytes = 0.9 * max_bytes

    logger.info("Per-piece payload sizes (ceiling %.1f MB):", max_payload_mb)
    too_big, near_limit = [], []
    for f in files:
        size = os.path.getsize(f)
        size_mb = size / (1024 * 1024)
        flag = ""
        if size >= max_bytes:
            flag = "  <-- OVER LIMIT"
            too_big.append((f, size_mb))
        elif size >= warn_bytes:
            flag = "  <-- near limit"
            near_limit.append((f, size_mb))
        logger.info("  %-40s %8.2f MB%s", os.path.basename(f), size_mb, flag)

    for f, size_mb in near_limit:
        logger.warning(
            "%s is %.2f MB, within 10%% of the %.1f MB per-piece ceiling -- "
            "consider splitting it finer.",
            os.path.basename(f), size_mb, max_payload_mb,
        )

    if too_big:
        offenders = "\n".join(
            f"    {os.path.basename(f)}: {size_mb:.2f} MB" for f, size_mb in too_big
        )
        raise SystemExit(
            f"{len(too_big)} piece(s) are at/over the {max_payload_mb:.1f} MB "
            "per-update payload ceiling on Tableau Cloud "
            "(api.server.update_uploaded_file.max_size_in_mb, which chunked "
            "upload cannot exceed):\n"
            f"{offenders}\n"
            "Aborting before any upload so the target isn't half-loaded. Split "
            "the offending piece(s) finer (e.g. by month) so each stays under "
            "the ceiling, then rerun. (Only override --max-payload-mb if your "
            "Cloud instance is configured for a larger limit.)"
        )


def _update_one(server, item, action_type, payload_file, args):
    """
    Run one Update-Data-in-Hyper action against `item`, uploading `payload_file`
    as the payload, and block until its background job finishes. A fresh
    request_id per call lets a retry be de-duplicated by the server.
    """
    action = {
        "action": action_type,
        "source-schema": args.source_schema,
        "source-table": args.source_table,
        "target-schema": args.target_schema,
        "target-table": args.target_table,
    }
    name = os.path.basename(payload_file)
    with section(f"{action_type} {name}"):
        logger.info(
            "%s %s -> %r.%r ...",
            action_type, name, args.target_schema, args.target_table,
        )
        request_id = str(uuid.uuid4())
        try:
            job = server.datasources.update_hyper_data(
                item, request_id=request_id, actions=[action], payload=str(payload_file)
            )
            logger.debug("update_hyper_data job %s (request_id %s); waiting...",
                         getattr(job, "id", "?"), request_id)
            server.jobs.wait_for_job(job)
        except Exception as e:
            logger.error("%s of %s failed: %s", action_type, name, e)
            logger.error(
                "If this mentions an unknown/missing table, the published "
                "extract's internal table name differs from the target "
                "(%r.%r). Try rerunning with --target-schema public.",
                args.target_schema, args.target_table,
            )
            raise
        logger.info("  done (job %s)", getattr(job, "id", "?"))


@section("dry-run report")
def print_dry_run(config, args, files):
    """
    dry-run report
    --------------
    Report the plan (target, mode, per-piece action) from local state only --
    no TSC.Server is constructed, so this makes zero network calls and can't
    know whether the data source already exists; it says so where it matters.
    Piece sizes were already reported by the pre-flight check.
    """
    logger.info("-- DRY RUN: no network call will be made, nothing will be published or updated --")
    logger.info(
        "Target site: %s  site=%r  project=%r",
        config["server_url"], config["site_content_url"], config["project_name"],
    )
    target = f"id={args.id!r}" if args.id else f"{args.datasource_name!r}"
    logger.info("Target data source: %s", target)
    logger.info("Mode: %s   pieces: %d", args.mode, len(files))
    for i, f in enumerate(files):
        if args.create_new and i == 0:
            act = "publish as NEW data source (create/overwrite model + data)"
        elif args.mode == "reload" and i == 0:
            act = "replace (clear target table, then load) -- or create+load if the data source doesn't exist yet"
        else:
            act = "insert (append rows)"
        logger.info("  %-40s -> %s", os.path.basename(f), act)


@section("run")
def _run(args):
    """
    run
    ---
    Validate/resolve inputs, run the pre-flight size check, then either print
    the dry-run plan or sign in and push each piece.
    """
    files = resolve_files(args.files)

    # Pre-flight size check runs first -- it's the defining guardrail, needs no
    # config or network, and aborts a too-big run before anything uploads.
    check_payload_sizes(files, args.max_payload_mb)

    config = load_config()

    if args.id and args.create_new:
        raise SystemExit(
            "--create-new cannot be combined with --id: a new data source is "
            "created under a name in TABLEAU_PROJECT_NAME, not an existing LUID. "
            "Use --datasource-name."
        )

    if args.dry_run:
        print_dry_run(config, args, files)
        return

    tableau_auth = TSC.PersonalAccessTokenAuth(
        config["token_name"], config["token_secret"], site_id=config["site_content_url"]
    )
    server = TSC.Server(config["server_url"], use_server_version=True)

    logger.debug("Signing in to %s (site %r)...", config["server_url"], config["site_content_url"])
    with section("sign in"), server.auth.sign_in(tableau_auth):
        logger.debug("Signed in; server API version %s", server.version)

        # Resolve the target data source (by LUID or by name within the project).
        project = None
        if args.id:
            try:
                existing = server.datasources.get_by_id(args.id)
            except Exception as e:
                raise SystemExit(f"No data source with id {args.id!r} on this site: {e}")
        else:
            project = find_project(server, config["project_name"])
            existing = find_datasource(server, project, args.datasource_name)

        create = args.create_new or (existing is None and args.mode == "reload")

        if args.mode == "append" and existing is None:
            raise SystemExit(
                f"Data source {args.datasource_name!r} does not exist, so there is "
                "nothing to append to. Run --mode reload first (it creates the "
                "data source), then use --mode append for later increments."
            )

        if create:
            # Publish the first piece to create (or Overwrite) the data source,
            # then insert the rest. Overwrite creates it if absent.
            first, rest = files[0], files[1:]
            with section("create data source from first piece"):
                logger.info(
                    "Creating data source %r in project %r from %s (Overwrite)...",
                    args.datasource_name, config["project_name"], os.path.basename(first),
                )
                new_ds = TSC.DatasourceItem(project_id=project.id, name=args.datasource_name)
                target_item = server.datasources.publish(
                    new_ds, first, TSC.Server.PublishMode.Overwrite
                )
                logger.info("  created data source id %s", target_item.id)
            for f in rest:
                _update_one(server, target_item, "insert", f, args)
        elif args.mode == "reload":
            # Existing data source: replace with the first piece (clears the
            # table), insert the rest. The model is preserved (data-only update).
            target_item = existing
            _update_one(server, target_item, "replace", files[0], args)
            for f in files[1:]:
                _update_one(server, target_item, "insert", f, args)
        else:  # append into an existing data source
            target_item = existing
            for f in files:
                _update_one(server, target_item, "insert", f, args)

        logger.info(
            "URL: %s/#/site/%s/datasources/%s",
            config["server_url"], config["site_content_url"], target_item.id,
        )

    logger.info("Done -- pushed %d piece(s) to %r.", len(files), args.datasource_name)


def main():
    args = parse_args()
    log_path = setup_logging(args.silent)
    logger.info("Logging this run to %s", log_path)
    logger.debug(
        "Args: mode=%s files=%r datasource=%r id=%r target=%r.%r source=%r.%r "
        "max_payload_mb=%s create_new=%s dry_run=%s silent=%s",
        args.mode, args.files, args.datasource_name, args.id,
        args.target_schema, args.target_table, args.source_schema, args.source_table,
        args.max_payload_mb, args.create_new, args.dry_run, args.silent,
    )
    try:
        _run(args)
    except SystemExit as exc:
        # Usage/validation failures raise SystemExit with a message. Record it,
        # then exit 1 WITHOUT re-raising the string (that would re-print to
        # stderr, duplicating the console line and breaking --silent).
        if exc.code not in (0, None):
            logger.error("%s", exc.code)
            raise SystemExit(1) from exc
        raise
    except Exception:
        logger.exception("Unhandled error -- aborting")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
