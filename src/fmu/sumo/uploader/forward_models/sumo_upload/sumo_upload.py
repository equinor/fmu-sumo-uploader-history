#!/usr/bin/env python

"""Upload data to Sumo from FMU."""

import warnings
import logging
from pathlib import Path

try:
    from ert import ErtScript  # type: ignore
except ModuleNotFoundError:
    from res.job_queue import ErtScript  # type: ignore

from sumo.wrapper import SumoClient
from fmu.sumo import uploader
from fmu.sumo.uploader._logger import get_uploader_logger

logger = get_uploader_logger()


def run(
    casepath: str,
    searchpath: str,
    env: str,
    metadata_path: str,
    threads: int,
    config_path: str = "fmuconfig/output/global_variables.yml",
    parameters_path: str = "parameters.txt",
    sumo_mode: str = "copy",
    verbosity: int = logging.INFO,
):
    """A "main" function that can be used both from command line and from ERT workflow"""

    logger.setLevel(verbosity)
    logger.debug("Running fmu_uploader_main()")

    # Catch-all to ensure FMU workflow keeps running even if something happens.
    # This should be a temporary solution to be re-evaluated in the future.

    try:
        # establish the connection to Sumo
        sumoclient = SumoClient(env=env)
        logger.info("Connection to Sumo established, env=%s", env)

        # initiate the case on disk object
        logger.info("Case-relative metadata path is %s", metadata_path)
        case_metadata_path = Path(casepath) / Path(metadata_path)
        logger.info("case_metadata_path is %s", case_metadata_path)

        logger.info("Sumo mode: %s", sumo_mode)

        e = uploader.CaseOnDisk(
            case_metadata_path,
            sumoclient,
            verbosity,
            sumo_mode,
            config_path,
            parameters_path,
        )
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
        e.upload(threads=threads)
        logger.info("Upload done")
    except Exception as err:
        err = err.with_traceback(None)
        logger.warning(f"Problem related to Sumo upload: {err} {type(err)}")
        _sumo_logger = sumoclient.getLogger("fmu-sumo-uploader")
        _sumo_logger.propagate = False
        _sumo_logger.warning(
            "Problem related to Sumo upload for case: %s; %s %s",
            case_metadata_path,
            err,
            type(err),
            extra={"objectUuid": e.fmu_case_uuid},
        )
        return


class SumoUpload(ErtScript):
    """A class with a run() function that can be registered as an ERT plugin.

    This is used for the ERT workflow context."""

    # pylint: disable=too-few-public-methods
    def run(self, *args):
        # pylint: disable=no-self-use
        """Parse with a simplified command line parser, for ERT only,
        call sumo_upload_main()"""

        logger.setLevel(logging.WARNING)

        logger.debug("Calling run() on SumoUpload")
        parser = _get_parser()
        args = parser.parse_args(args)
        _check_arguments(args)
        sumo_upload_main(
            casepath=args.casepath,
            searchpath=args.searchpath,
            env=args.env,
            metadata_path=args.metadata_path,
            threads=args.threads,
            config_path=args.config_path,
            parameters_path=args.parameters_path,
            sumo_mode=args.sumo_mode,
            verbosity=logging.WARNING,
        )



        


