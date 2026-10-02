"""
tableau_logging.py

Shared per-run logging setup for every script in this project. Each script
still owns its own module-level logger (logging.getLogger("<script
name>")) so "Script: ..." and the log filename say what actually ran --
this module just builds the handlers (a DEBUG file under logs/, plus an
INFO stdout handler unless --silent) and the `section` marker, so neither
has to be redefined per script.

Usage, in each script:

    logger = logging.getLogger("clientside_publish_hyper")
    section = make_section(logger)
    ...
    log_path = setup_logging(logger, "clientside", args.silent)
"""

import functools
import logging
import os
import sys
from datetime import datetime


def setup_logging(logger, prefix, silent):
    """
    Wire up logging so every run leaves a full, timestamped trail on disk,
    regardless of --silent or --dry-run.

    A file handler (DEBUG) writes everything -- including the fine-grained
    steps that never reached the console -- to a fresh per-run file under
    logs/<prefix>_<timestamp>.log. A stdout handler (INFO, message-only to
    match the previous print() output) is added ONLY when not silent, so
    --silent leaves the console completely quiet while losing nothing from
    the log.

    Returns the path of the log file created for this run.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{prefix}_{datetime.now():%Y%m%d-%H%M%S}.log")

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

    return log_path


def make_section(logger):
    """
    make_section
    ------------
    Returns a `section` class bound to `logger`, usable two ways, with
    identical behaviour:

        @section("inject calculations")        # decorate a whole function
        def inject_calculations(...): ...

        with section("publish new data source"):   # wrap an inline block
            ...

    On entry it logs "Section: <name>" (DEBUG, so the log file records the
    order sections actually ran in without cluttering the console). If the
    block raises an *unexpected* exception, it logs
    "Error in section '<name>': ..." naming the section that failed, then
    lets the exception propagate. SystemExit (usage/validation errors) is
    deliberately left alone so those still flow to main() and print their
    own guidance untouched.

    Bound to `logger` (rather than a single shared `section` class with a
    module-global logger) so each script's sections log to THAT script's
    own log file and console, not some other module's.
    """

    class section:
        def __init__(self, name):
            self.name = name

        def __enter__(self):
            logger.debug("Section: %s", self.name)
            return self

        def __exit__(self, exc_type, exc, tb):
            # Only annotate ordinary errors; SystemExit/KeyboardInterrupt are
            # not Exception subclasses, so they pass through unlogged here.
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

    return section
