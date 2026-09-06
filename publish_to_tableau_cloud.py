"""
publish_to_tableau_cloud.py

Publishes a .hyper file to Tableau Cloud as a published data source, and,
if a metadata JSON file (as produced by generate_metadata.py) is given via
--metadata, applies it: name, description, tags, certification, and
per-column field descriptions.

Column descriptions are applied by editing the datasource's own .tds XML
directly and republishing it.

Configuration is read from environment variables -- see .env.example for
the full list and what each one means. Copy .env.example to .env, fill in
your real values, and this script loads it automatically via python-dotenv.

Never commit .env, paste its contents into chat, or share it: it holds a
Tableau Personal Access Token that can publish/overwrite content on your site.

Usage:
    python3 publish_to_tableau_cloud.py --source=<file>.hyper [--target=<name>] [--metadata=<file>.json] [--dry-run]

    --source is required -- there is no default .hyper file. Omitting it is
    an error that prints this exact usage line.

    --target is optional -- it's the name the data source will have on
    Tableau Cloud. If omitted, it defaults to the --source filename with
    its extension stripped and underscores replaced with spaces, e.g.
    Finished_Merged.hyper -> "Finished Merged".

    --metadata is optional -- omit it to publish with no description,
    certification, tags, or column descriptions applied. Passing --metadata
    with a file that doesn't exist is an error (it means you asked for
    metadata to be applied but nothing can be read).
    
    Examples:

    python3 publish_to_tableau_cloud.py --source=Finished_Merged.hyper
        Publishes Finished_Merged.hyper as data source "Finished Merged"
        (derived from the filename) with no metadata applied.

    python3 publish_to_tableau_cloud.py --source=Finished_Merged.hyper --target="Rugby Chains"
        Publishes the same file, but names the data source "Rugby Chains"
        instead of the derived default.

    python3 publish_to_tableau_cloud.py --source=Finished_Merged.hyper --metadata=datasource_metadata.json
        Publishes Finished_Merged.hyper and applies name, description, tags,
        certification, and column descriptions from datasource_metadata.json
        (the file generate_metadata.py produces).

    python3 publish_to_tableau_cloud.py --source=Finished_Merged.hyper --metadata=datasource_metadata.json --dry-run
        Prints what would be published/updated -- target site and project,
        datasource name, description, certification, tags, and which column
        descriptions would be applied -- without making any network call to
        Tableau Cloud (nothing is published or overwritten).
"""

import argparse
import io
import json
import os
import xml.etree.ElementTree as ET
import zipfile

import tableauserverclient as TSC
from dotenv import find_dotenv, load_dotenv

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
            "name, description, tags, certification, and column descriptions. "
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
    return parser.parse_args()


def load_config():
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


def resolve_target(hyper_file, target_arg):
    """
    The data source's name on Tableau Cloud: --target if given, otherwise
    derived from --source itself (extension stripped, underscores -> spaces)
    so there's always a sensible name without a hardcoded default tied to
    one specific file.
    """
    if target_arg:
        return target_arg, "given via --target"
    derived = os.path.splitext(os.path.basename(hyper_file))[0].replace("_", " ")
    return derived, "derived from --source (no --target given)"


def load_metadata(metadata_file):
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


def find_project(server, project_name):
    for project in TSC.Pager(server.projects):
        if project.name == project_name:
            return project
    raise SystemExit(
        f"Project {project_name!r} not found on this site. "
        f"Check TABLEAU_PROJECT_NAME in .env -- the project must already exist."
    )


def _set_column_desc(column_element, text):
    for child in list(column_element):
        if child.tag == "desc":
            column_element.remove(child)
    desc = ET.SubElement(column_element, "desc")
    formatted_text = ET.SubElement(desc, "formatted-text")
    run = ET.SubElement(formatted_text, "run")
    run.text = text


def _patch_tds_column_descriptions(tds_bytes, columns_metadata):
    """
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


def apply_column_descriptions(server, datasource_item, columns_metadata):
    """
    Sets per-column field descriptions by downloading the just-published
    datasource as a .tdsx, patching <column>/<desc> elements directly into
    its embedded .tds XML, and republishing with Overwrite.


    A failure here never aborts the publish -- name/description/tags/
    certification have already been applied by this point.
    """
    if not columns_metadata:
        return

    tdsx_path = None
    try:
        # download() appends its own extension to whatever path is passed in,
        # so the actual saved path must be read from its return value.
        tdsx_path = server.datasources.download(
            datasource_item.id, filepath=f"{datasource_item.name}.download", include_extract=True
        )
        with open(tdsx_path, "rb") as f:
            original_tdsx = f.read()
    except Exception as e:
        print(f"Skipping column descriptions -- could not download the published datasource: {e}")
        return
    finally:
        if tdsx_path and os.path.exists(tdsx_path):
            os.remove(tdsx_path)

    with zipfile.ZipFile(io.BytesIO(original_tdsx)) as zf:
        tds_names = [n for n in zf.namelist() if n.endswith(".tds")]
        if not tds_names:
            print("Skipping column descriptions -- no .tds file found inside the downloaded .tdsx.")
            return
        tds_path = tds_names[0]
        tds_bytes = zf.read(tds_path)

    new_tds_bytes, updated, created = _patch_tds_column_descriptions(tds_bytes, columns_metadata)
    if updated == 0 and created == 0:
        print("Skipping republish -- no column descriptions matched any field.")
        return

    out = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(original_tdsx)) as zin, zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
        for info in zin.infolist():
            payload = zin.read(info.filename)
            if info.filename == tds_path:
                payload = new_tds_bytes
            zout.writestr(info, payload)
    out.seek(0)

    try:
        server.datasources.publish(datasource_item, out, TSC.Server.PublishMode.Overwrite)
    except Exception as e:
        print(f"Could not republish with column descriptions: {e}")
        return

    print(f"Republished with column descriptions: {updated} updated, {created} newly created.")


def print_dry_run(config, hyper_file, target, target_origin, ds_meta, columns_metadata, skip_metadata):
    """
    Reports the same plan main() would otherwise execute, derived entirely
    from local config/metadata -- no TSC.Server is constructed, so this
    makes zero network calls (even TSC.Server(..., use_server_version=True)
    itself would ping the server, which is why this returns before that
    line rather than short-circuiting inside a `with server.auth.sign_in`
    block).
    """
    print("-- DRY RUN: no network call will be made, nothing will be published --")
    print(f"Target site: {config['server_url']}  site={config['site_content_url']!r}  project={config['project_name']!r}")
    print(f"Would publish {hyper_file!r} as data source {target!r} ({target_origin}) (PublishMode.Overwrite):")

    if skip_metadata:
        print("  metadata: none applied (no --metadata given) -- no description, certification, tags, or column descriptions")
        return

    print(f"  description: {ds_meta.get('description') or '(none)'}")
    certified = bool(ds_meta.get("certified", False))
    note = f" -- {ds_meta['certification_note']}" if ds_meta.get("certification_note") else ""
    print(f"  certified: {certified}{note}")
    tags = sorted(set(ds_meta.get("tags", [])))
    print(f"  tags: {tags if tags else '(none)'}")

    describable = [
        col for col in columns_metadata
        if (col.get("description") or "").strip()
        and col["description"].strip() != "No description available."
    ]
    print(
        f"  column descriptions: {len(describable)} of {len(columns_metadata)} "
        "column(s) would be applied (via a follow-up download/patch/republish of the .tds)"
    )
    for col in describable:
        print(f"    {col['name']!r}: {col['description']}")


def main():
    args = parse_args()

    if not args.source:
        raise SystemExit(
            "Missing required argument: --source (no .hyper file to publish was specified).\n"
            f"Usage:   {SOURCE_USAGE}\n"
            "Example: python3 publish_to_tableau_cloud.py --source=Finished_Merged.hyper"
        )
    hyper_file = args.source
    config = load_config()

    if not os.path.exists(hyper_file):
        raise SystemExit(f"{hyper_file} not found. Run split_by_year.py then union_hyper_files.py first.")

    target, target_origin = resolve_target(hyper_file, args.target)

    if args.metadata:
        metadata = load_metadata(args.metadata)
        ds_meta = metadata["datasource"]
        columns_metadata = metadata.get("columns", [])
    else:
        ds_meta = {}
        columns_metadata = []

    if args.dry_run:
        print_dry_run(config, hyper_file, target, target_origin, ds_meta, columns_metadata, skip_metadata=not args.metadata)
        return

    tableau_auth = TSC.PersonalAccessTokenAuth(
        config["token_name"], config["token_secret"], site_id=config["site_content_url"]
    )
    server = TSC.Server(config["server_url"], use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        project = find_project(server, config["project_name"])

        # Name, description, and certification must be set on the DatasourceItem
        # BEFORE publish(): the update-after-publish request does not include
        # description at all (verified against tableauserverclient's request
        # builder), so setting it after the fact would silently do nothing.
        new_datasource = TSC.DatasourceItem(project_id=project.id, name=target)
        new_datasource.description = ds_meta.get("description")
        new_datasource.certified = bool(ds_meta.get("certified", False))
        if ds_meta.get("certification_note"):
            new_datasource.certification_note = ds_meta["certification_note"]

        print(
            f"Publishing {hyper_file} to project {config['project_name']!r} "
            f"as {target!r} ({target_origin})..."
        )
        published_ds = server.datasources.publish(
            new_datasource, hyper_file, TSC.Server.PublishMode.Overwrite
        )
        print(f"Published. Data source ID: {published_ds.id}")
        print(f"URL: {config['server_url']}/#/site/{config['site_content_url']}/datasources/{published_ds.id}")

        if not args.metadata:
            print("No --metadata given: no description, certification, tags, or column descriptions applied.")
        else:
            # Tags are NOT part of the publish payload either -- they require a
            # separate call against the /tags endpoint.
            tags = set(ds_meta.get("tags", []))
            if tags:
                published_ds.tags = tags
                server.datasources.update_tags(published_ds)
                print(f"Applied tags: {sorted(tags)}")

            apply_column_descriptions(server, published_ds, columns_metadata)

    print("Done.")


if __name__ == "__main__":
    main()
