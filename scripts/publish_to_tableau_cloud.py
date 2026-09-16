"""
publish_to_tableau_cloud.py

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

Configuration is read from environment variables -- see .env.example for
the full list and what each one means. Copy .env.example to .env, fill in
your real values, and this script loads it automatically via python-dotenv.

Never commit .env, paste its contents into chat, or share it: it holds a
Tableau Personal Access Token that can publish/overwrite content on your site.

Every run -- silent or not, dry-run or not -- writes a full, timestamped
record to a per-run log file under logs/ (e.g. logs/publish_20260914-093015.log).
The log always captures everything; --silent only silences the console.

Usage:
    python3 publish_to_tableau_cloud.py --source=<file>.hyper [--target=<name>] [--metadata=<file>.json] [--dry-run] [--silent]

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

    python3 scripts/publish_to_tableau_cloud.py --source=data/Finished_Merged.hyper
        Publishes data/Finished_Merged.hyper as data source "Finished Merged"
        (derived from the filename) with no metadata applied.

    python3 scripts/publish_to_tableau_cloud.py --source=data/Finished_Merged.hyper --target="Rugby Chains"
        Publishes the same file, but names the data source "Rugby Chains"
        instead of the derived default.

    python3 scripts/publish_to_tableau_cloud.py --source=data/Finished_Merged.hyper --metadata=data/datasource_metadata.json
        Publishes data/Finished_Merged.hyper and applies name, description, tags,
        certification, and column descriptions from data/datasource_metadata.json
        (the file generate_metadata.py produces).

    python3 scripts/publish_to_tableau_cloud.py --source=data/Finished_Merged.hyper --metadata=data/datasource_metadata.json --dry-run
        Prints what would be published/updated -- target site and project,
        datasource name, description, certification, tags, and which column
        descriptions would be applied -- without making any network call to
        Tableau Cloud (nothing is published or overwritten).
"""

import argparse
import functools
import io
import json
import logging
import os
import sys
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime

import tableauserverclient as TSC
from dotenv import find_dotenv, load_dotenv

# All human-readable output goes through this logger, never print(), so the
# same messages can be sent to a timestamped log file (always) and to stdout
# (unless --silent). See setup_logging().
logger = logging.getLogger("publish_to_tableau_cloud")


class section:
    """
    Section marker
    --------------
    Names a block of work as a "section" so the log tells you, at every step,
    which stage of the publish is running and -- if something breaks -- exactly
    which stage broke. Usable two ways, with identical behaviour:

        @section("inject calculations")        # decorate a whole function
        def _inject_calculations(...): ...

        with section("publish new data source"):   # wrap an inline block
            ...

    On entry it logs "Section: <name>" (DEBUG, so the log file records the
    order sections actually ran in without cluttering the console). If the
    block raises an *unexpected* exception, it logs
    "Error in section '<name>': ..." naming the section that failed, then lets
    the exception propagate. SystemExit (our usage/validation errors) is
    deliberately left alone so those still flow to main() and print their own
    guidance untouched.

    Each section name matches the header on the first line of the decorated
    function's docstring. The raised exception is tagged so only the innermost
    section reports it -- outer sections it passes through on the way up stay
    quiet rather than logging the same failure again.
    """

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        logger.debug("Section: %s", self.name)
        return self

    def __exit__(self, exc_type, exc, tb):
        # Only annotate ordinary errors; SystemExit/KeyboardInterrupt are not
        # Exception subclasses, so they pass through unlogged here.
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

TABLEAU_USER_NS = "http://www.tableausoftware.com/xml/user"
ET.register_namespace("user", TABLEAU_USER_NS)

LOCAL_TYPE_TO_TABLEAU = {
    "string": ("string", "dimension", "nominal"),
    "integer": ("integer", "measure", "quantitative"),
    "real": ("real", "measure", "quantitative"),
    "date": ("date", "dimension", "ordinal"),
    "datetime": ("datetime", "dimension", "ordinal"),
    "boolean": ("boolean", "dimension", "nominal"),
}

SOURCE_USAGE = "--source=<path-to-file>.hyper"
METADATA_USAGE = "--metadata=<path-to-file>.json"

REQUIRED_ENV_VARS = [
    "TABLEAU_SERVER_URL",
    "TABLEAU_SITE_CONTENT_URL",
    "TABLEAU_PROJECT_NAME",
    "TABLEAU_TOKEN_NAME",
    "TABLEAU_TOKEN_SECRET",
]


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


def setup_logging(silent):
    """
    Wire up logging so every run leaves a full, timestamped trail on disk,
    regardless of --silent or --dry-run.

    A file handler (DEBUG) writes everything -- including the fine-grained
    steps that never reached the console -- to a fresh per-run file under
    logs/. A stdout handler (INFO, message-only to match the previous
    print() output) is added ONLY when not silent, so --silent leaves the
    console completely quiet while losing nothing from the log.

    Returns the path of the log file created for this run.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"publish_{datetime.now():%Y%m%d-%H%M%S}.log")

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
    # is unmistakably attributable: logs/ also holds update_*.log from
    # update_cloud_hyper_data.py, and this header -- plus the publish_/update_
    # filename prefix -- keeps the two apart at a glance.
    logger.info("Script: %s", os.path.basename(__file__))

    return log_path


@section("load configuration")
def load_config():
    """
    load configuration
    -------------------
    Read the Tableau connection settings from environment variables (populated
    from .env), failing early with a fix-it message if any are missing.
    """
    # Found explicitly (rather than leaving load_dotenv() to look it up
    # internally) so the missing-var error below can tell you exactly
    # whether a .env was found at all, or found but incomplete -- the two
    # have different fixes.
    dotenv_path = find_dotenv()
    load_dotenv(dotenv_path)
    missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        if dotenv_path:
            fix = (
                f"Found a .env file at {dotenv_path!r}, but it doesn't set the "
                "variable(s) above (or sets them to an empty value). Open it, "
                "fill those in, then rerun this script."
            )
        else:
            fix = (
                "No .env file was found (checked this script's folder and its "
                "parent folders). Create one from the template and fill in your "
                "real values, then rerun this script:\n"
                "    cp .env.example .env            (macOS/Linux)\n"
                "    Copy-Item .env.example .env     (Windows PowerShell)"
            )
        raise SystemExit(
            "Missing required environment variable(s): " + ", ".join(missing) + "\n" + fix
        )
    return {
        "server_url": os.environ["TABLEAU_SERVER_URL"],
        "site_content_url": os.environ["TABLEAU_SITE_CONTENT_URL"],
        "project_name": os.environ["TABLEAU_PROJECT_NAME"],
        "token_name": os.environ["TABLEAU_TOKEN_NAME"],
        "token_secret": os.environ["TABLEAU_TOKEN_SECRET"],
    }


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


@section("load metadata")
def load_metadata(metadata_file):
    """
    load metadata
    -------------
    Read and parse the metadata JSON file (--metadata) that supplies the
    name, description, tags, certification, column descriptions and calcs.
    """
    if not os.path.exists(metadata_file):
        raise SystemExit(
            f"Metadata file {metadata_file!r} not found, but --metadata asked for it "
            "to be applied.\n"
            "Run generate_metadata.py to generate one, point --metadata at an "
            f"existing metadata JSON file ({METADATA_USAGE}), or drop --metadata "
            "entirely to publish without applying any metadata."
        )
    with open(metadata_file) as f:
        return json.load(f)


@section("find project")
def find_project(server, project_name):
    """
    find project
    ------------
    Locate the Cloud project (by TABLEAU_PROJECT_NAME) the data source will
    live in; the project must already exist.
    """
    for project in TSC.Pager(server.projects):
        if project.name == project_name:
            logger.debug("Found project %r (id %s)", project_name, project.id)
            return project
    raise SystemExit(
        f"Project {project_name!r} not found on this site. "
        f"Check TABLEAU_PROJECT_NAME in .env -- the project must already exist."
    )


@section("find data source")
def find_datasource(server, project, name):
    """
    find data source
    ----------------
    Return the DatasourceItem named `name` in `project`, or None if no such
    published data source exists yet. None means the bootstrap case (publish
    the bare .hyper); a match means the preserve case (edit the model in place).
    """
    for ds in TSC.Pager(server.datasources):
        if ds.project_id == project.id and ds.name == name:
            return ds
    return None


def _set_column_desc(column_element, text):
    for child in list(column_element):
        if child.tag == "desc":
            column_element.remove(child)
    desc = ET.SubElement(column_element, "desc")
    formatted_text = ET.SubElement(desc, "formatted-text")
    run = ET.SubElement(formatted_text, "run")
    run.text = text


@section("patch column descriptions")
def _patch_tds_column_descriptions(tds_bytes, columns_metadata):
    """
    patch column descriptions
    --------------------------
    Adds/updates a <desc> on the .tds's top-level <column> element for each
    field named in columns_metadata, creating the <column> element itself
    when the field doesn't have one yet (true for any field that was never
    renamed/edited in Tableau -- it only exists as a <metadata-record>, which
    is not something Tableau's Fields tab reads a description from).

    Returns (new_xml_bytes, updated_count, created_count).
    """
    root = ET.fromstring(tds_bytes)

    local_type_by_field = {}
    for metadata_record in root.iter("metadata-record"):
        if metadata_record.get("class") != "column":
            continue
        local_name_el = metadata_record.find("local-name")
        local_type_el = metadata_record.find("local-type")
        if local_name_el is None or local_type_el is None:
            continue
        local_type_by_field[local_name_el.text] = local_type_el.text

    existing_columns = {c.get("name"): c for c in root.findall("column")}

    connection_el = root.find("connection")
    insert_index = list(root).index(connection_el) + 1 if connection_el is not None else 0

    updated, created = 0, 0
    for col in columns_metadata:
        name = f"[{col['name']}]"
        text = (col.get("description") or "").strip()
        if not text or text == "No description available.":
            continue

        if name in existing_columns:
            _set_column_desc(existing_columns[name], text)
            updated += 1
        else:
            local_type = local_type_by_field.get(name, "string")
            datatype, role, type_ = LOCAL_TYPE_TO_TABLEAU.get(local_type, ("string", "dimension", "nominal"))
            new_col = ET.Element("column", {"datatype": datatype, "name": name, "role": role, "type": type_})
            _set_column_desc(new_col, text)
            root.insert(insert_index, new_col)
            insert_index += 1
            created += 1

    return ET.tostring(root, encoding="utf-8", xml_declaration=True), updated, created


@section("inject calculations")
def _inject_calculations(tds_bytes, calculations):
    """
    inject calculations
    --------------------
    Adds (or updates) a calculated field on the .tds for each entry in
    `calculations`, as a top-level <column> carrying a
    <calculation class='tableau' formula='...'/> child -- the exact shape
    Tableau writes when you author a calculated field in Desktop.

    Matching is by caption (the display name), falling back to the internal
    name: if a <column> with that caption/name already exists, its formula is
    updated in place rather than duplicated. Calculated fields NOT named here
    are left untouched -- that's how hand-authored calcs on Cloud survive a
    republish.

    Returns (new_xml_bytes, added_count, updated_count).
    """
    if not calculations:
        return tds_bytes, 0, 0

    root = ET.fromstring(tds_bytes)
    # NB: an empty ElementTree Element is falsy, so these lookups use explicit
    # `is None` checks below rather than `a or b`.
    by_caption = {c.get("caption"): c for c in root.findall("column") if c.get("caption")}
    by_name = {c.get("name"): c for c in root.findall("column")}

    connection_el = root.find("connection")
    insert_index = list(root).index(connection_el) + 1 if connection_el is not None else 0

    added, updated = 0, 0
    for calc in calculations:
        caption = calc["name"]
        formula = calc["formula"]

        existing = by_caption.get(caption)
        if existing is None:
            existing = by_name.get(f"[{caption}]")

        if existing is not None:
            calc_el = existing.find("calculation")
            if calc_el is None:
                calc_el = ET.SubElement(existing, "calculation", {"class": "tableau"})
            calc_el.set("formula", formula)
            updated += 1
            continue

        col = ET.Element("column", {
            "caption": caption,
            "datatype": calc.get("datatype", "real"),
            "name": f"[{caption}]",
            "role": calc.get("role", "measure"),
            "type": calc.get("type", "quantitative"),
        })
        ET.SubElement(col, "calculation", {"class": "tableau", "formula": formula})
        description = (calc.get("description") or "").strip()
        if description:
            _set_column_desc(col, description)
        root.insert(insert_index, col)
        insert_index += 1
        added += 1

    return ET.tostring(root, encoding="utf-8", xml_declaration=True), added, updated


@section("patch model")
def _patch_tds(tds_bytes, columns_metadata, calculations):
    """
    patch model
    -----------
    Apply column descriptions, then inject/update calculated fields, on the
    .tds XML. Returns (new_tds_bytes, summary) where summary holds the four
    counts (descriptions updated/created, calculations added/updated).
    """
    tds_bytes, desc_updated, desc_created = _patch_tds_column_descriptions(tds_bytes, columns_metadata)
    tds_bytes, calc_added, calc_updated = _inject_calculations(tds_bytes, calculations)
    return tds_bytes, {
        "desc_updated": desc_updated,
        "desc_created": desc_created,
        "calc_added": calc_added,
        "calc_updated": calc_updated,
    }


def _summary_line(summary):
    return (
        f"Model updated -- descriptions: {summary['desc_updated']} updated, "
        f"{summary['desc_created']} created; calculated fields: "
        f"{summary['calc_added']} added, {summary['calc_updated']} updated."
    )


@section("download data source")
def _download_tdsx_bytes(server, datasource_item):
    """
    download data source
    ---------------------
    Download a published data source as a .tdsx and return its raw bytes.
    download() appends its own extension, so the real path is its return
    value; the temp file is always cleaned up.
    """
    tdsx_path = None
    try:
        logger.debug("Downloading current .tdsx for data source id %s...", datasource_item.id)
        tdsx_path = server.datasources.download(
            datasource_item.id, filepath=f"{datasource_item.id}.download", include_extract=True
        )
        with open(tdsx_path, "rb") as f:
            data = f.read()
        logger.debug("Downloaded .tdsx (%d bytes)", len(data))
        return data
    finally:
        if tdsx_path and os.path.exists(tdsx_path):
            os.remove(tdsx_path)


@section("read tds from tdsx")
def _read_tds_from_tdsx(tdsx_bytes):
    """
    read tds from tdsx
    ------------------
    Return (tds_member_name, tds_bytes) for the .tds inside a .tdsx zip.
    """
    with zipfile.ZipFile(io.BytesIO(tdsx_bytes)) as zf:
        tds_names = [n for n in zf.namelist() if n.endswith(".tds")]
        if not tds_names:
            raise ValueError("no .tds file found inside the downloaded .tdsx")
        tds_bytes = zf.read(tds_names[0])
        logger.debug("Read .tds member %r (%d bytes) from .tdsx", tds_names[0], len(tds_bytes))
        return tds_names[0], tds_bytes


@section("rebuild tdsx")
def _rebuild_tdsx(original_tdsx_bytes, tds_member, new_tds_bytes, new_hyper_bytes=None):
    """
    rebuild tdsx
    ------------
    Copy every member of the original .tdsx verbatim, replacing the .tds with
    `new_tds_bytes` and -- if `new_hyper_bytes` is given -- the embedded .hyper
    extract with it. The extract keeps its original member path so the .tds
    connection still resolves to it. Returns a BytesIO positioned at 0.
    """
    out = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(original_tdsx_bytes)) as zin, \
            zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
        for info in zin.infolist():
            payload = zin.read(info.filename)
            if info.filename == tds_member:
                payload = new_tds_bytes
            elif new_hyper_bytes is not None and info.filename.endswith(".hyper"):
                payload = new_hyper_bytes
            zout.writestr(info, payload)
    out.seek(0)
    return out


@section("apply tags")
def _apply_tags(server, datasource_item, ds_meta):
    """
    apply tags
    ----------
    Push the metadata's tags onto the published data source (a separate
    /tags API call -- tags are not part of the publish payload).
    """
    tags = set(ds_meta.get("tags", []))
    if tags:
        datasource_item.tags = tags
        server.datasources.update_tags(datasource_item)
        logger.info("Applied tags: %s", sorted(tags))


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
    tdsx_bytes = _download_tdsx_bytes(server, existing_item)
    tds_member, tds_bytes = _read_tds_from_tdsx(tdsx_bytes)
    new_tds_bytes, summary = _patch_tds(tds_bytes, columns_metadata, calculations)
    logger.debug("Patched .tds in place: %s", summary)

    with open(hyper_file, "rb") as f:
        fresh_hyper = f.read()
    logger.debug("Read fresh extract %s (%d bytes) to swap in", hyper_file, len(fresh_hyper))

    rebuilt = _rebuild_tdsx(tdsx_bytes, tds_member, new_tds_bytes, new_hyper_bytes=fresh_hyper)
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
        tdsx_bytes = _download_tdsx_bytes(server, datasource_item)
        tds_member, tds_bytes = _read_tds_from_tdsx(tdsx_bytes)
    except Exception as e:
        logger.warning("Skipping column descriptions/calculations -- could not read the published datasource: %s", e)
        return

    new_tds_bytes, summary = _patch_tds(tds_bytes, columns_metadata, calculations)
    if not any(summary.values()):
        logger.info("Skipping republish -- nothing in the metadata matched the model.")
        return

    rebuilt = _rebuild_tdsx(tdsx_bytes, tds_member, new_tds_bytes)
    try:
        server.datasources.publish(datasource_item, rebuilt, TSC.Server.PublishMode.Overwrite)
    except Exception as e:
        logger.warning("Could not republish with model metadata: %s", e)
        return
    logger.info(_summary_line(summary))


@section("dry-run report")
def print_dry_run(config, hyper_file, target, target_origin, ds_meta, columns_metadata, calculations, skip_metadata):
    """
    dry-run report
    --------------
    Reports the same plan main() would otherwise execute, derived entirely
    from local config/metadata -- no TSC.Server is constructed, so this
    makes zero network calls (even TSC.Server(..., use_server_version=True)
    itself would ping the server, which is why this returns before that
    line rather than short-circuiting inside a `with server.auth.sign_in`
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
            "Example: python3 scripts/publish_to_tableau_cloud.py --source=data/Finished_Merged.hyper"
        )
    hyper_file = args.source
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

    tableau_auth = TSC.PersonalAccessTokenAuth(
        config["token_name"], config["token_secret"], site_id=config["site_content_url"]
    )
    server = TSC.Server(config["server_url"], use_server_version=True)

    # sign in
    # -------
    logger.debug("Signing in to %s (site %r)...", config["server_url"], config["site_content_url"])
    with section("sign in"), server.auth.sign_in(tableau_auth):
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
                _apply_tags(server, published_ds, ds_meta)
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
                _apply_tags(server, published_ds, ds_meta)
            logger.info(_summary_line(summary))

    logger.info("Done.")


def main():
    args = parse_args()
    log_path = setup_logging(args.silent)
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
