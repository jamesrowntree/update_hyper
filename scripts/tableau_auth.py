"""
tableau_auth.py

Shared Tableau Cloud connection helper. Every script in this project that
talks to Tableau Cloud -- clientside_publish_hyper.py,
serverside_publish_hyper.py, publish_metadata.py, download_metadata.py --
calls load_config() then connect(config) rather than duplicating the
PersonalAccessTokenAuth / TSC.Server / sign_in boilerplate.

This module does no logging of its own -- no logger, no section markers.
It's a plain, dependency-light leaf that every script's own logger wraps
around (each script logs "Signing in to ..." itself, around its own
`with connect(config) as server:` block), not a script with user-facing
output of its own. Import it, don't run it.

Configuration is read from environment variables -- see .env.example for
the full list and what each one means. Copy .env.example to .env, fill in
your real values; load_config() loads it automatically via python-dotenv.

Never commit .env, paste its contents into chat, or share it: it holds a
Tableau Personal Access Token that can publish/overwrite content on your site.
"""

import os
from contextlib import contextmanager

import tableauserverclient as TSC
from dotenv import find_dotenv, load_dotenv

REQUIRED_ENV_VARS = [
    "TABLEAU_SERVER_URL",
    "TABLEAU_SITE_CONTENT_URL",
    "TABLEAU_PROJECT_NAME",
    "TABLEAU_TOKEN_NAME",
    "TABLEAU_TOKEN_SECRET",
]


def load_config():
    """
    Read the Tableau connection settings from environment variables
    (populated from .env), failing early with a fix-it message if any are
    missing.
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


@contextmanager
def connect(config):
    """
    Build a TSC.Server for config['server_url'] and sign in with a
    PersonalAccessTokenAuth built from config's token_name/token_secret/
    site_content_url. Yields the signed-in server; signs out automatically
    on exit (via server.auth.sign_in's own context manager).
    """
    tableau_auth = TSC.PersonalAccessTokenAuth(
        config["token_name"], config["token_secret"], site_id=config["site_content_url"]
    )
    server = TSC.Server(config["server_url"], use_server_version=True)
    with server.auth.sign_in(tableau_auth):
        yield server
