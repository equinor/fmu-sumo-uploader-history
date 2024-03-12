import os
import sys
import pytest
import time
from pathlib import Path
import logging
import subprocess
import json
import yaml

from fmu.sumo import uploader

if not sys.platform.startswith("darwin"):
    import openvds

# Run the tests from the root dir
TEST_DIR = Path(__file__).parent / "../"
os.chdir(TEST_DIR)

ENV = "dev"

logger = logging.getLogger(__name__)
logger.setLevel(level="DEBUG")


class SumoConnection:
    def __init__(self, env, token=None):
        self.env = env
        self._connection = None
        self.token = token

    @property
    def connection(self):
        if self._connection is None:
            self._connection = uploader.SumoConnection(
                env=self.env, token=self.token)
        return self._connection


def _remove_cached_case_id():
    """The sumo uploader caches case uuid on disk, but we should remove this
    file between tests"""
    try:
        os.remove("tests/data/test_case_080/sumo_parent_id.yml")
    except FileNotFoundError:
        pass


def _update_metadata_file_with_unique_uuid(metadata_file, unique_case_uuid):
    """Updates an existing sumo metadata file with unique case uuid.
    (To be able to run tests in parallell towards Sumo server,
    unique case uuids must be used.)
    """

    # Read the sumo metadata file given as input
    with open(metadata_file) as f:
        parsed_yaml = yaml.safe_load(f)

    # Update case uuid with the given unique uuid
    parsed_yaml["fmu"]["case"]["uuid"] = str(unique_case_uuid)

    # Update the metadata file using the unique uuid
    with open(metadata_file, "w") as f:
        yaml.dump(parsed_yaml, f)


### TESTS ###


def test_initialization(token):
    """Assert that the CaseOnDisk object can be initialized"""
    sumo_connection = SumoConnection(env=ENV, token=token).connection

    case = uploader.CaseOnDisk(
        case_metadata_path="tests/data/test_case_080/case.yml",
        sumo_connection=sumo_connection,
    )


def test_pre_teardown(token):
    """Run teardown first to remove remnants from other test runs
    and prepare for running test suite again."""

    _remove_cached_case_id()


def test_upload_without_registration(token, unique_uuid):
    """Assert that attempting to upload to a non-existing/un-registered case gives warning."""
    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    _remove_cached_case_id()

    case_file = "tests/data/test_case_080/case.yml"
    _update_metadata_file_with_unique_uuid(case_file, unique_uuid)

    case = uploader.CaseOnDisk(
        case_metadata_path=case_file,
        sumo_connection=sumo_connection,
        verbosity="DEBUG",
    )

    # On purpose NOT calling case.register before adding file here
    child_binary_file = "tests/data/test_case_080/surface.bin"
    child_metadata_file = "tests/data/test_case_080/.surface.bin.yml"
    _update_metadata_file_with_unique_uuid(child_metadata_file, unique_uuid)
    case.add_files(child_binary_file)
    with pytest.warns(UserWarning, match="Case is not registered"):
        case.upload(threads=1)


def test_case(token):
    """Assert that after uploading case to Sumo, the case is there and is the only one."""
    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    _remove_cached_case_id()

    logger.debug("initialize CaseOnDisk")

    case_file = "tests/data/test_case_080/case.yml"
    e = uploader.CaseOnDisk(
        case_metadata_path=case_file,
        sumo_connection=sumo_connection,
    )

    # Assert that this case is not there in the first place
    logger.debug("Asserting that the test case is not already there")
    query = f"class:case AND fmu.case.uuid:{e.fmu_case_uuid}"
    search_results = sumo_connection.api.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    logger.debug("search results: %s", str(search_results))
    if not search_results:
        raise ValueError("No search results returned")
    hits = search_results.get("hits").get("hits")
    assert len(hits) == 0

    # Register the case
    e.register()
    time.sleep(1)

    # assert that the case is there now
    search_results = sumo_connection.api.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    hits = search_results.get("hits").get("hits")
    logger.debug(search_results.get("hits"))
    assert len(hits) == 1

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumo_connection.api.delete(path=path)

def test_case_with_restricted_child(token, unique_uuid):
    """Assert that uploading a child with 'classification: restricted' works. 
    Assumes that the identity running this test have enough rights for that. """
    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    _remove_cached_case_id()

    logger.debug("initialize CaseOnDisk")

    case_file = "tests/data/test_case_080/case.yml"
    _update_metadata_file_with_unique_uuid(case_file, unique_uuid)
    e = uploader.CaseOnDisk(
        case_metadata_path=case_file,
        sumo_connection=sumo_connection,
    )

    # Register the case
    e.register()
    time.sleep(1)

    child_binary_file = "tests/data/test_case_080/surface_restricted.bin"
    child_metadata_file = "tests/data/test_case_080/.surface_restricted.bin.yml"
    _update_metadata_file_with_unique_uuid(child_metadata_file, unique_uuid)
    e.add_files(child_binary_file)
    e.upload()
    time.sleep(1)

    query = f"{e.fmu_case_uuid}"
    search_results = sumo_connection.api.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    total = search_results.get("hits").get("total").get("value")
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumo_connection.api.delete(path=path)


def test_one_file(token, unique_uuid):
    """Upload one file to Sumo. Assert that it is there."""

    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    _remove_cached_case_id()

    logger.debug("initialize CaseOnDisk")
    case_file = "tests/data/test_case_080/case.yml"
    _update_metadata_file_with_unique_uuid(case_file, unique_uuid)
    e = uploader.CaseOnDisk(
        case_metadata_path=case_file,
        sumo_connection=sumo_connection,
    )
    e.register()
    time.sleep(1)

    child_binary_file = "tests/data/test_case_080/surface.bin"
    child_metadata_file = "tests/data/test_case_080/.surface.bin.yml"
    _update_metadata_file_with_unique_uuid(child_metadata_file, unique_uuid)
    e.add_files(child_binary_file)
    e.upload()
    time.sleep(1)

    query = f"{e.fmu_case_uuid}"
    search_results = sumo_connection.api.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    total = search_results.get("hits").get("total").get("value")
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumo_connection.api.delete(path=path)


def test_missing_metadata(token, unique_uuid):
    """
    Try to upload files where one does not have metadata. Assert that warning is given
    and that upload commences with the other files. Check that the children are present.
    """
    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    _remove_cached_case_id()

    case_file = "tests/data/test_case_080/case.yml"
    _update_metadata_file_with_unique_uuid(case_file, unique_uuid)
    e = uploader.CaseOnDisk(
        case_metadata_path=case_file,
        sumo_connection=sumo_connection,
    )
    e.register()

    # Add a valid child
    child_binary_file = "tests/data/test_case_080/surface.bin"
    child_metadata_file = "tests/data/test_case_080/.surface.bin.yml"
    _update_metadata_file_with_unique_uuid(child_metadata_file, unique_uuid)
    e.add_files(child_binary_file)

    # Assert that expected warning is given when the binary file
    # do not have a companion metadata file
    with pytest.warns(
        UserWarning
    ) as warnings_record:  # testdata contains one file with missing metadata
        e.add_files("tests/data/test_case_080/surface_no_metadata.bin")
        for _ in warnings_record:
            assert len(warnings_record) == 1, warnings_record
            assert (
                warnings_record[0]
                .message.args[0]
                .endswith("No metadata, skipping file.")
            )

    e.upload()
    time.sleep(1)

    # Assert parent and valid child is on Sumo
    query = f"{e.fmu_case_uuid}"
    search_results = sumo_connection.api.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    total = search_results.get("hits").get("total").get("value")
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumo_connection.api.delete(path=path)


def test_wrong_metadata(token, unique_uuid):
    """
    Try to upload files where one does have metadata with error. Assert that warning is given
    and that upload commences with the other files. Check that the children are present.
    """
    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    _remove_cached_case_id()

    case_file = "tests/data/test_case_080/case.yml"
    _update_metadata_file_with_unique_uuid(case_file, unique_uuid)
    e = uploader.CaseOnDisk(
        case_metadata_path=case_file,
        sumo_connection=sumo_connection,
    )
    e.register()

    # Add a valid child
    child_binary_file = "tests/data/test_case_080/surface.bin"
    child_metadata_file = "tests/data/test_case_080/.surface.bin.yml"
    _update_metadata_file_with_unique_uuid(child_metadata_file, unique_uuid)
    e.add_files(child_binary_file)

    # Add a child with problem in its metadata file
    problem_binary_file = "tests/data/test_case_080/surface_error.bin"
    problem_metadata_file = "tests/data/test_case_080/.surface_error.bin.yml"
    _update_metadata_file_with_unique_uuid(problem_metadata_file, unique_uuid)
    e.add_files(problem_binary_file)

    e.upload()
    time.sleep(1)

    # Assert parent and valid child are on Sumo
    query = f"{e.fmu_case_uuid}"
    search_results = sumo_connection.api.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    total = search_results.get("hits").get("total").get("value")
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumo_connection.api.delete(path=path)


def _get_segy_path(segy_command):
    """Find the path to the OpenVDS SEGYImport or SEGYExport executables.
    Supply either 'SEGYImport' or 'SEGYExport' as parameter"""
    if sys.platform.startswith("win"):
        segy_command = segy_command + ".exe"
    python_path = os.path.dirname(sys.executable)
    logger.info(python_path)
    # The SEGYImport folder location is not fixed
    locations = [
        os.path.join(python_path, "bin"),
        os.path.join(python_path, "..", "bin"),
        os.path.join(python_path, "..", "shims"),
        "/home/vscode/.local/bin",
        "/usr/local/bin",
    ]
    path_to_executable = None
    for loc in locations:
        path = os.path.join(loc, segy_command)
        if os.path.isfile(path):
            path_to_executable = path
            break
    if path_to_executable is None:
        logger.error("Could not find OpenVDS executables folder location")
    logger.info("Path to OpenVDS executable: " + path_to_executable)
    return path_to_executable


@pytest.mark.skipif(
    sys.platform.startswith("darwin"), reason="do not run OpenVDS SEGYImport on mac os"
)
def test_openvds_available():
    """Test that OpenVDS is installed and can be successfully called"""
    path_to_SEGYImport = _get_segy_path("SEGYImport")
    check_SEGYImport_version = subprocess.run(
        [path_to_SEGYImport, "--version"], capture_output=True, text=True
    )
    assert check_SEGYImport_version.returncode == 0
    assert "SEGYImport" in check_SEGYImport_version.stdout


@pytest.mark.skipif(
    sys.platform.startswith("darwin"), reason="do not run OpenVDS SEGYImport on mac os"
)
def test_seismic_openvds_file(token, unique_uuid):
    """Upload seimic in OpenVDS format to Sumo. Assert that it is there."""
    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    case_file = "tests/data/test_case_080/case_segy.yml"
    _update_metadata_file_with_unique_uuid(case_file, unique_uuid)
    e = uploader.CaseOnDisk(
        case_metadata_path=case_file,
        sumo_connection=sumo_connection,
    )
    e.register()
    time.sleep(1)

    child_binary_file = "tests/data/test_case_080/seismic.segy"
    child_metadata_file = "tests/data/test_case_080/.seismic.segy.yml"
    _update_metadata_file_with_unique_uuid(child_metadata_file, unique_uuid)
    segy_filepath = child_binary_file
    e.add_files(segy_filepath)
    e.upload()
    time.sleep(1)

    # Read the parent object from Sumo
    query = f"_sumo.parent_object:{e.fmu_case_uuid}"
    search_results = sumo_connection.api.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    total = search_results.get("hits").get("total").get("value")
    assert total == 1

    # Verify some of the metadata values
    assert (
        search_results.get("hits")
        .get("hits")[0]
        .get("_source")
        .get("data")
        .get("format")
        == "openvds"
    )
    assert (
        search_results.get("hits")
        .get("hits")[0]
        .get("_source")
        .get("file")
        .get("checksum_md5")
        == ""
    )

    # Get SAS token to read seismic directly from az blob store
    child_id = search_results.get("hits").get("hits")[0].get("_id")
    method = f"/objects('{child_id}')/blob/authuri"
    token_results = sumo_connection.api.get(method).content
    # Sumo server have had 2 different ways of returning the SAS token,
    # and this code should be able to work with both
    try:
        url = (
            "azureSAS:"
            + json.loads(token_results.decode("utf-8")).get("baseuri")[6:]
            + child_id
        )
        url_conn = "Suffix=?" + \
            json.loads(token_results.decode("utf-8")).get("auth")
    except:
        token_results = token_results.decode("utf-8")
        url = "azureSAS" + token_results.split("?")[0][5:] + "/"
        url_conn = "Suffix=?" + token_results.split("?")[1]

    # Export from az blob store to a segy file on local disk
    # Openvds 3.4.0 workarounds:
    #     SEGYExport fails on 3 out of 4 attempts, hence retry loop
    #     SEGYExport does not work on ubuntu, hence the platform check
    export_succeeded = False
    export_retries = 0
    if not sys.platform.startswith("linux"):
        while not export_succeeded and export_retries < 40:
            print("SEGYExport retry", export_retries)
            exported_filepath = "exported.segy"
            if os.path.exists(exported_filepath):
                os.remove(exported_filepath)
            path_to_SEGYExport = _get_segy_path("SEGYExport")
            cmdstr = [
                path_to_SEGYExport,
                "--url",
                url,
                "--connection",
                url_conn,
                "exported.segy",
            ]
            cmd_result = subprocess.run(
                cmdstr, capture_output=True, text=True, shell=False)

            if cmd_result.returncode == 0:
                assert os.path.isfile(exported_filepath)
                assert os.stat(exported_filepath).st_size == os.stat(segy_filepath).st_size
                if os.path.exists(exported_filepath):
                    os.remove(exported_filepath)
                print("SEGYExport succeeded on retry", export_retries)
                export_succeeded = True
            else:
                time.sleep(16)

            export_retries+=1

        assert export_succeeded

    # Use OpenVDS Python API to read directly from az cloud storage
    handle = openvds.open(url, url_conn)
    layout = openvds.getLayout(handle)
    channel_count = layout.getChannelCount()
    assert channel_count == 3
    assert layout.getChannelName(0) == "Amplitude"

    # Delete this case
    path = f"/objects('{e.fmu_case_uuid}')"
    sumo_connection.api.delete(path=path)
    # Sumo/Azure removes the container which takes some time
    time.sleep(30)

    # OpenVDS reads should fail after deletion
    with pytest.raises(RuntimeError, match="Error on downloading*"):
        handle = openvds.open(url, url_conn)


def test_teardown(token):
    """Teardown all testdata between every test"""

    _remove_cached_case_id()

    # Set all the metadata files back to same case uuid as before, to avoid
    # git reporting changes.
    test_dir = "tests/data/test_case_080/"
    files = os.listdir(test_dir)
    for f in files:
        if f.endswith(".yml"):
            dest_file = test_dir + os.path.sep + f
            _update_metadata_file_with_unique_uuid(
                dest_file, "11111111-1111-1111-1111-111111111111"
            )
