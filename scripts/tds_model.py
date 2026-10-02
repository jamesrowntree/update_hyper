"""
tds_model.py

Shared .tds model utilities: reading a metadata JSON file, downloading a
published data source's .tdsx, patching its .tds XML (column descriptions +
calculated fields), rebuilding the .tdsx, and applying tags -- the pieces
clientside_publish_hyper.py and publish_metadata.py both need to apply a
metadata JSON to a data source's model.

No logging of its own (see tableau_auth.py for why) -- callers log around
these calls with their own logger. apply_tags() returns the tags it applied
(or None) rather than logging, for the same reason.
"""

import io
import json
import os
import xml.etree.ElementTree as ET
import zipfile

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


def load_metadata(metadata_file):
    """
    Read and parse the metadata JSON file (--metadata) that supplies the
    name, description, tags, certification, column descriptions and calcs.
    """
    if not os.path.exists(metadata_file):
        raise SystemExit(
            f"Metadata file {metadata_file!r} not found, but --metadata asked for it "
            "to be applied.\n"
            "Run generate_metadata.py to generate one, point --metadata at an "
            "existing metadata JSON file (--metadata=<path-to-file>.json), or drop "
            "--metadata entirely to publish without applying any metadata."
        )
    with open(metadata_file) as f:
        return json.load(f)


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


def _inject_calculations(tds_bytes, calculations):
    """
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


def patch_tds(tds_bytes, columns_metadata, calculations):
    """
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


def summary_line(summary):
    return (
        f"Model updated -- descriptions: {summary['desc_updated']} updated, "
        f"{summary['desc_created']} created; calculated fields: "
        f"{summary['calc_added']} added, {summary['calc_updated']} updated."
    )


def download_tdsx_bytes(server, datasource_item):
    """
    Download a published data source as a .tdsx and return its raw bytes.
    download() appends its own extension, so the real path is its return
    value; the temp file is always cleaned up.
    """
    tdsx_path = None
    try:
        tdsx_path = server.datasources.download(
            datasource_item.id, filepath=f"{datasource_item.id}.download", include_extract=True
        )
        with open(tdsx_path, "rb") as f:
            return f.read()
    finally:
        if tdsx_path and os.path.exists(tdsx_path):
            os.remove(tdsx_path)


def read_tds_from_tdsx(tdsx_bytes):
    """
    Return (tds_member_name, tds_bytes) for the .tds inside a .tdsx zip.
    """
    with zipfile.ZipFile(io.BytesIO(tdsx_bytes)) as zf:
        tds_names = [n for n in zf.namelist() if n.endswith(".tds")]
        if not tds_names:
            raise ValueError("no .tds file found inside the downloaded .tdsx")
        return tds_names[0], zf.read(tds_names[0])


def rebuild_tdsx(original_tdsx_bytes, tds_member, new_tds_bytes, new_hyper_bytes=None):
    """
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


def apply_tags(server, datasource_item, ds_meta):
    """
    Push the metadata's tags onto the data source (a separate /tags API call
    -- tags are not part of the publish payload). Returns the sorted tags
    applied, or None if there were none, so the caller can log the result
    with its own logger.
    """
    tags = set(ds_meta.get("tags", []))
    if not tags:
        return None
    datasource_item.tags = tags
    server.datasources.update_tags(datasource_item)
    return sorted(tags)
