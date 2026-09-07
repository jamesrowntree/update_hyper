"""
download_metadata.py

Downloads an existing published data source from Tableau Cloud and pulls out
its .tds -- the Tableau data-source model: fields, roles, descriptions, and
any calculated fields defined on it. This is READ-ONLY: it never publishes,
overwrites, or changes anything on the site, so it is always safe to run
against the live data source.

It is the "pull" counterpart to publish_to_tableau_cloud.py's "push". Use it
to see what is actually on Cloud right now -- including calculated fields,
folders, or aliases that someone added in Tableau Desktop or web authoring,
which your local .hyper + metadata JSON know nothing about. Because publish
uses PublishMode.Overwrite, those browser-side additions are exactly what a
blind republish would wipe out; download them first to inspect (or preserve)
them.

It reuses publish_to_tableau_cloud.py's .env loading and project lookup, so it
reads the same TABLEAU_* variables (see .env.example). By default it downloads
the model only (include_extract=False) -- fast and small, since you want the
XML, not the extract data. Pass --with-extract to fetch the whole .tdsx.

Never commit .env, paste its contents into chat, or share it: it holds a
Tableau Personal Access Token that can read (and, via the publish script,
overwrite) content on your site.

Usage (run from the project root):
    python3 scripts/download_metadata.py --name "Finished Merged"
        Downloads the "Finished Merged" data source from the project named by
        TABLEAU_PROJECT_NAME and writes its model to data/Finished_Merged.tds,
        then prints a summary (fields, descriptions, calculated fields).

    python3 scripts/download_metadata.py --name "Finished Merged" --output=data/live_model.tds
        Same, but writes the .tds to an explicitly chosen path.

    python3 scripts/download_metadata.py --id <datasource-luid>
        Looks the data source up by its LUID instead of by name (no project
        lookup needed). Output defaults to data/<data source name>.tds.

    python3 scripts/download_metadata.py --name "Finished Merged" --with-extract
        Downloads the full .tdsx (model + extract data) before pulling the
        .tds out of it, instead of the model only.

    Either --name (the data source's name within TABLEAU_PROJECT_NAME) or --id
    (its LUID) is required. If --name matches nothing, the available data
    sources in that project are listed so you can pick one.
"""

import argparse
import os
import tempfile
import xml.etree.ElementTree as ET
import zipfile

import tableauserverclient as TSC

# Reuse the publish script's .env loading and project lookup rather than
# duplicating them -- both live here in scripts/ and are import-safe (their
# real work is guarded behind `if __name__ == "__main__"`).
from publish_to_tableau_cloud import find_project, load_config


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download a published data source's .tds model from Tableau Cloud (read-only)."
    )
    parser.add_argument(
        "--name",
        metavar="NAME",
        help=(
            "Name of the data source to download, within the project set by "
            "TABLEAU_PROJECT_NAME in .env, e.g. --name='Finished Merged'. "
            "Required unless --id is given."
        ),
    )
    parser.add_argument(
        "--id",
        metavar="LUID",
        help=(
            "LUID of the data source to download. An alternative to --name that "
            "skips the project lookup. Takes precedence over --name if both are given."
        ),
    )
    parser.add_argument(
        "--output",
        metavar="FILE.tds",
        help=(
            "Path to write the extracted .tds to. Defaults to data/<name>.tds "
            "(the data source name with spaces replaced by underscores)."
        ),
    )
    parser.add_argument(
        "--with-extract",
        action="store_true",
        help=(
            "Download the full .tdsx (model + extract data) before extracting "
            "the .tds. Off by default -- the model alone is far smaller and is "
            "all you need to read the XML."
        ),
    )
    return parser.parse_args()


def resolve_datasource(server, project, name, ds_id):
    """
    Returns the DatasourceItem to download. By --id (exact) when given,
    otherwise by --name within the given project. Lists what's available if a
    name matches nothing, rather than failing with a bare "not found".
    """
    if ds_id:
        try:
            return server.datasources.get_by_id(ds_id)
        except Exception as e:
            raise SystemExit(f"No data source with id {ds_id!r} found on this site: {e}")

    in_project = [ds for ds in TSC.Pager(server.datasources) if ds.project_id == project.id]
    matches = [ds for ds in in_project if ds.name == name]
    if not matches:
        available = sorted(ds.name for ds in in_project)
        raise SystemExit(
            f"No data source named {name!r} in project {project.name!r}.\n"
            f"Available in that project: {available or '(none)'}"
        )
    return matches[0]


def extract_tds_bytes(download_path):
    """
    Returns (tds_member_name, tds_bytes) for a download that may be either a
    .tdsx (a zip containing the .tds + the extract) or a bare .tds. Detects the
    zip by content, not extension, since download() picks the extension itself.
    """
    if zipfile.is_zipfile(download_path):
        with zipfile.ZipFile(download_path) as zf:
            tds_names = [n for n in zf.namelist() if n.endswith(".tds")]
            if not tds_names:
                raise SystemExit(f"No .tds file found inside {download_path!r}.")
            return tds_names[0], zf.read(tds_names[0])
    with open(download_path, "rb") as f:
        return os.path.basename(download_path), f.read()


def summarize_tds(tds_bytes):
    """
    Prints what the model actually contains -- most importantly the calculated
    fields (with their formulas), which are the thing a republish from the raw
    .hyper would silently drop.
    """
    root = ET.fromstring(tds_bytes)

    # Physical columns come from the connection metadata (<metadata-record
    # class='column'>); calculated and edited fields are top-level <column>s.
    physical = [
        rec.findtext("local-name")
        for rec in root.iter("metadata-record")
        if rec.get("class") == "column" and rec.findtext("local-name")
    ]

    calculations, descriptions = [], []
    for col in root.findall("column"):
        label = col.get("caption") or col.get("name")
        calc = col.find("calculation")
        if calc is not None and calc.get("formula") is not None:
            calculations.append((label, calc.get("formula")))
        run = col.find("./desc/formatted-text/run")
        if run is not None and (run.text or "").strip():
            descriptions.append(label)

    print(f"  physical columns: {len(physical)}")
    print(f"  field descriptions: {len(descriptions)}")
    if calculations:
        print(f"  calculated fields: {len(calculations)}")
        for label, formula in calculations:
            print(f"    {label}: {formula}")
    else:
        print("  calculated fields: none")


def main():
    args = parse_args()
    if not args.name and not args.id:
        raise SystemExit(
            "Nothing to download: pass --name <data source name> or --id <luid>.\n"
            "Example: python3 scripts/download_metadata.py --name 'Finished Merged'"
        )

    config = load_config()

    tableau_auth = TSC.PersonalAccessTokenAuth(
        config["token_name"], config["token_secret"], site_id=config["site_content_url"]
    )
    server = TSC.Server(config["server_url"], use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        if args.id:
            datasource = resolve_datasource(server, None, None, args.id)
        else:
            project = find_project(server, config["project_name"])
            datasource = resolve_datasource(server, project, args.name, None)

        print(
            f"Found data source {datasource.name!r} (id {datasource.id}) on "
            f"{config['server_url']} site={config['site_content_url']!r}."
        )

        # download() appends its own extension to the path it's given, so the
        # actual saved path must be read back from its return value. Kept in a
        # temp dir and removed afterwards so the repo is never littered.
        tmp_base = os.path.join(tempfile.gettempdir(), f"{datasource.id}.download")
        downloaded = server.datasources.download(
            datasource.id, filepath=tmp_base, include_extract=args.with_extract
        )
        try:
            tds_name, tds_bytes = extract_tds_bytes(downloaded)
        finally:
            if downloaded and os.path.exists(downloaded):
                os.remove(downloaded)

    output = args.output or os.path.join("data", f"{datasource.name.replace(' ', '_')}.tds")
    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
    with open(output, "wb") as f:
        f.write(tds_bytes)

    print(f"Wrote {tds_name} -> {output} ({len(tds_bytes)} bytes)")
    summarize_tds(tds_bytes)


if __name__ == "__main__":
    main()
