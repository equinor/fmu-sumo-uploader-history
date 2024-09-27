import os
import argparse

from fmu.sumo.uploader._logger import get_uploader_logger


description = """SUMO_UPLOAD will upload files to Sumo. The typical use case
is as add-on to post-processing workflows which aggregate data across an
ensemble and stores the results outside the realization folders.

SUMO_UPLOAD depends on the current case being registered in Sumo (done through
the ``WF_CREATE_CASE_METADATA`` workflow job) and on data being exported
with ``fmu-dataio``.

``fmu-dataio`` must be used to produce metadata for each file
to be uploaded to Sumo.

The ``WF_CREATE_CASE_METADATA`` workflow job must run *before* all SUMO_UPLOAD
instances to ensure the case is registered in Sumo before data are uploaded.

SUMO_UPLOAD is implemented both as FORWARD_JOB and WORKFLOW_JOB and
can be called from both contexts when running ERT.

It is recommended to upload files immediately after they are produced,
rather than lumping all SUMO_UPLOADs at the end of the ERT config
file.

"""

examples = """``<SUMO_ENV>`` must be defined. It is typically defined
in the ERT config, and normally set to ``prod``.

``<SUMO_CASEPATH>`` must be defined. It is typically defined in the ERT config,
and normally set to ``<SCRATCH>/<USER>/<CASE_DIR>``
e.g. ``/scratch/myfield/myuser/mycase/``

Note! Filenames produced by FMU workflows use "--" as separator. Avoid
this string in searchpaths, as it will cause following text to be
parsed as a comment.

FORWARD_MODEL example::

  FORWARD_MODEL XX -- Some other job that makes data
  FORWARD_MODEL SUMO_UPLOAD(<SEARCHPATH>="share/results/maps/*.gri")
  FORWARD_MODEL SUMO_UPLOAD(<SEARCHPATH>="share/results/polygons/*.csv")

WORKFLOW_JOB example::

  <MY_JOB> -- The workflow job that creates data
  SUMO_UPLOAD <SUMO_CASEPATH> "<SUMO_CASEPATH>/share/observations/maps/*.gri"  <SUMO_ENV>

"""

category = "other"

logger = get_uploader_logger()


def _get_parser() -> argparse.ArgumentParser:
    """Construct parser object for sumo_upload."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "casepath", type=str, help="Absolute path to case root"
    )
    parser.add_argument(
        "searchpath",
        type=str,
        help="path relative to runpath for files to upload",
    )
    parser.add_argument("env", type=str, help="Sumo environment to use.")
    parser.add_argument(
        "--config_path",
        type=str,
        help="path to global variables relative to runpath",
        default="fmuconfig/output/global_variables.yml",
    )
    parser.add_argument(
        "--sumo_mode",
        type=str,
        help="copy or move files to cloud storage",
        default="copy",
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
        "--parameters_path",
        type=str,
        help="path to parameters.txt",
        default="parameters.txt",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose output"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Debug output, more verbose than --verbose",
    )

    return parser


def _check_arguments(args) -> None:
    """Do sanity check of the input arguments."""

    logger.debug("Running check_arguments()")
    logger.debug("Arguments are: %s", str(vars(args)))

    if args.env not in ["preview", "dev", "test", "prod", "localhost"]:
        warnings.warn(f"Non-standard environment: {args.env}")

    if not Path(args.casepath).is_absolute():
        if args.casepath.startswith("<") and args.casepath.endswith(">"):
            ValueError("ERT variable is not defined: %s", args.casepath)
        raise ValueError(
            "Provided casepath must be an absolute path to the case root"
        )

    if not Path(args.casepath).exists():
        raise ValueError("Provided case path does not exist")

    logger.debug("check_arguments() has ended")


def main() -> None:
    """Entry point from command line (e.g. ERT FORWARD_JOB)."""

    parser = _get_parser()
    args = parser.parse_args()

    logger.setLevel(logging.INFO)

    if args.verbose:
        logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Legacy? Still needed?
    args.casepath = os.path.expandvars(args.casepath)
    args.searchpath = os.path.expandvars(args.searchpath)

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
        verbosity=logging.INFO,
    )


# May not be necessary.    
main_entry_point = main
