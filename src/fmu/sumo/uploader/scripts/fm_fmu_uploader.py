#!/usr/bin/env python

"""This script uploads data to Sumo from an FMU workflow."""

import warnings
import os
import argparse
import logging
from pathlib import Path

from fmu.sumo import uploader


logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)


def main():
    args = parse_arguments()
    logger.setLevel(level=args.loglevel)

    logger.debug("Arguments are: %s", str(vars(args)))

    # establish the connection to Sumo
    sumo_connection = uploader.SumoConnection(env=args.env)
    logger.debug("Connection to Sumo established")

    # initiate the case on disk object
    logger.debug("Case-relative metadata path is %s", args.metadata_path)
    case_metadata_path = Path(args.casepath) / Path(args.metadata_path)
    logger.debug("case_metadata_path is %s", case_metadata_path)
    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata_path, sumo_connection=sumo_connection
    )

    # add files to the case on disk object
    logger.debug("Adding files. Search path is %s", args.searchpath)
    e.add_files(args.searchpath)
    logger.debug("%s files has been added", str(len(e.files)))

    if len(e.files) == 0:
        logger.debug("%s No files - aborting")
        warnings.warn("No files found - aborting ")
        return

    # upload the indexed files
    logger.debug("Starting upload")
    e.upload(
        threads=args.threads, register_case=False
    )  # registration should have been done by HOOK workflow
    logger.debug("upload done")


def parse_arguments():
    """

    Parse the arguments

    Returns:
        args: argparse.ArgumentParser() object

    """

    parser = argparse.ArgumentParser()
    parser.add_argument("casepath", type=str, help="Absolute path to case root")
    parser.add_argument(
        "searchpath", type=str, help="Case-relative search path for files to upload"
    )
    parser.add_argument(
        "env", type=str, help="Which environment to use.", default="prod"
    )
    parser.add_argument(
        "--threads", type=int, help="Set number of threads to use.", default=2
    )
    parser.add_argument(
        "--metadata_path",
        type=str,
        help="Case-relative path to case metadata",
        default="share/metadata/fmu_case.yml",
    )
    parser.add_argument(
        "--loglevel", type=str, help="Verbosity for the logger", default="DEBUG"
    )
    args = parser.parse_args()

    args.casepath = os.path.expandvars(args.casepath)
    args.searchpath = os.path.expandvars(args.searchpath)

    if args.env not in ["dev", "test", "prod", "exp", "preview"]:
        logger.error("env arg was %s", args.env)
        raise ValueError(
            f"Illegal environment: {args.env}. Valid environments: dev, test, prod, exp, preview"
        )

    if not Path(args.casepath).is_absolute():
        logger.error("casepath arg was %s", args.casepath)
        if args.casepath.startswith("<") and args.casepath.endswith(">"):
            ValueError("ERT variable is not defined: %s", args.casepath)
        raise ValueError("Provided casepath must be an absolute path to the case root")

    if not Path(args.casepath).exists():
        logger.error("casepath arg was %s", args.casepath)
        raise ValueError("Provided case path does not exist")

    return args


if __name__ == "__main__":
    main()
