#!/usr/bin/env python

"""Upload data to Sumo from FMU."""

import warnings
import os
import argparse
import logging
from pathlib import Path

from ert_shared.plugins.plugin_manager import hook_implementation  # type: ignore
from res.job_queue import ErtScript  # type: ignore

from fmu.sumo import uploader

logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)

# This documentation is for sumo_uploader as an ERT workflow
DESCRIPTION = """
SUMO_UPLOAD will upload files to Sumo. The typical use case is as add-on to 
post-processing workflows which aggregate data across an ensemble and stores the
results outside the realization folders.

SUMO_UPLOAD is implemented both as FORWARD_JOB and WORKFLOW_JOB and can be called from 
both contexts when running ERT.
"""

EXAMPLES = """
In an existing workflow e.g. ``ert/bin/workflows/MY_WORKFLOW`` with the contents::
  MY_JOB <arguments>
  SUMO_UPLOAD <CASEPATH> <CASEPATH>/MyIteration/share/results/tables/*.csv <SUMO_ENV>
...where ``MY_JOB`` typically refers to a post-processing job creating data.
...and where <CASEPATH> typically refers to <SCRATCH>/<USER>/<CASE>

<SUMO_ENV> is typically set in the config as it is used also by forward jobs.
It must refer to a valid Sumo environment. Normally this should be set to "prod".
"""  # noqa


def main() -> None:
    """Entry point from command line (e.g. ERT FORWARD_JOB)."""

    # Note for further development:
    # Using subscript/csv_merge.py as inspiration for turning this into something
    # runable also in ERT workflows. It is likely that some overhead is carried from
    # the example which may not apply to sumo_upload. The separate main function may
    # be one such carried feature which in the end does nothing (csv_merge applies
    # a dedicated argument parser for ERT workflow usage, which may be the reason for
    # this choice of architecture.)

    parser = get_parser()
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Legacy? Still needed?
    args.casepath = os.path.expandvars(args.casepath)
    args.searchpath = os.path.expandvars(args.searchpath)

    check_arguments(args)

    sumo_upload_main(
        casepath=args.casepath,
        searchpath=args.searchpath,
        env=args.env,
        metadata_path=args.metadata_path,
        threads=args.threads,
    )


def sumo_upload_main(
    casepath: str, searchpath: str, env: str, metadata_path: str, threads: int
) -> None:
    """A "main" function that can be used both from command line and from ERT workflow"""

    logger.debug("Running fmu_uploader_main()")

    # establish the connection to Sumo
    sumo_connection = uploader.SumoConnection(env=env)
    logger.info("Connection to Sumo established")

    # initiate the case on disk object
    logger.info("Case-relative metadata path is %s", metadata_path)
    case_metadata_path = Path(casepath) / Path(metadata_path)
    logger.info("case_metadata_path is %s", case_metadata_path)

    # Catch-all to ensure FMU workflow keeps running even if something happens.
    # This should be a temporary solution to be re-evaluated in the future.

    try:
        e = uploader.CaseOnDisk(
            case_metadata_path=case_metadata_path, sumo_connection=sumo_connection
        )
    except Exception as err:
        logger.info(
            "Encountered a problem connecting to Sumo. " "The error was: " f"{err}"
        )
        return

    # add files to the case on disk object
    logger.info("Adding files. Search path is %s", searchpath)
    e.add_files(searchpath)
    logger.info("%s files has been added", str(len(e.files)))

    if len(e.files) == 0:
        logger.debug("There are 0 (zero) files.")
        warnings.warn("No files found - aborting ")
        return

    # upload the indexed files
    logger.info("Starting upload")
    e.upload(threads=threads, register_case=False)
    logger.info("Upload done")


class SumoUpload(ErtScript):
    """A class with a run() function that can be registered as an ERT plugin.

    This is used for the ERT workflow context."""

    # pylint: disable=too-few-public-methods
    def run(self, *args):
        # pylint: disable=no-self-use
        """Parse with a simplified command line parser, for ERT only,
        call sumo_upload_main()"""
        logger.debug("Calling run() on SumoUpload")
        parser = get_parser()
        args = parser.parse_args(args)
        check_arguments(args)
        sumo_upload_main(
            casepath=args.casepath,
            searchpath=args.searchpath,
            env=args.env,
            metadata_path=args.metadata_path,
            threads=args.threads,
        )


def get_parser() -> argparse.ArgumentParser:
    """Construct parser object for sumo_upload."""

    parser = argparse.ArgumentParser()
    parser.add_argument("casepath", type=str, help="Absolute path to case root")
    parser.add_argument(
        "searchpath", type=str, help="Absolute search path for files to upload"
    )
    parser.add_argument("env", type=str, help="Sumo environment to use.")
    parser.add_argument(
        "--threads", type=int, help="Set number of threads to use.", default=2
    )
    parser.add_argument(
        "--metadata_path",
        type=str,
        help="Case-relative path to case metadata",
        default="share/metadata/fmu_case.yml",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--debug", action="store_true", help="Debug output, more verbose than --verbose"
    )

    return parser


def check_arguments(args) -> None:
    """Do sanity check of the input arguments."""

    logger.debug("Running check_arguments()")
    logger.debug("Arguments are: %s", str(vars(args)))

    if args.env not in ["dev", "test", "prod", "localhost"]:
        warnings.warn(f"Non-standard environment: {args.env}")

    if not Path(args.casepath).is_absolute():
        if args.casepath.startswith("<") and args.casepath.endswith(">"):
            ValueError("ERT variable is not defined: %s", args.casepath)
        raise ValueError("Provided casepath must be an absolute path to the case root")

    if not Path(args.casepath).exists():
        raise ValueError("Provided case path does not exist")

    logger.debug("check_arguments() has ended")


@hook_implementation
def legacy_ertscript_workflow(config):
    """Hook the SumoUpload class into ERT with the name SUMO_UPLOAD,
    and inject documentation"""
    workflow = config.add_workflow(SumoUpload, "SUMO_UPLOAD")
    workflow.parser = get_parser
    workflow.description = DESCRIPTION
    workflow.examples = EXAMPLES
    workflow.category = "export"
