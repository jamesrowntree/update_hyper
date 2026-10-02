"""
tableau_lookup.py

Shared Tableau Cloud project/data-source lookups, used by every script that
needs to find a project (by TABLEAU_PROJECT_NAME) or a published data
source within it. Pure read-only Pager calls against an already-signed-in
server -- see tableau_auth.py for signing in.
"""

import tableauserverclient as TSC


def find_project(server, project_name):
    """
    Locate the Cloud project (by TABLEAU_PROJECT_NAME) the data source will
    live in; the project must already exist.
    """
    for project in TSC.Pager(server.projects):
        if project.name == project_name:
            return project
    raise SystemExit(
        f"Project {project_name!r} not found on this site. "
        f"Check TABLEAU_PROJECT_NAME in .env -- the project must already exist."
    )


def find_datasource(server, project, name):
    """
    Return the DatasourceItem named `name` in `project`, or None if no such
    published data source exists yet. None means the bootstrap case (publish
    the bare .hyper); a match means the preserve case (edit the model in place).
    """
    for ds in TSC.Pager(server.datasources):
        if ds.project_id == project.id and ds.name == name:
            return ds
    return None
