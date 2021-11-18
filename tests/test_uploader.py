import os
import sys
import pytest
import time
from pathlib import Path
import logging

from fmu.sumo import uploader

# run the tests from the root dir
TEST_DIR = Path(__file__).parent / "../"
os.chdir(TEST_DIR)

ENV = "localhost"

logger = logging.getLogger(__name__)
logger.setLevel(level="DEBUG")

class SumoConnection:
    def __init__(self, env):
        self.env = env
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = uploader.SumoConnection(env=self.env)
        return self._connection


def test_initialisation():
    sumo_connection = SumoConnection(env=ENV).connection
    e = uploader.CaseOnDisk(case_metadata_path="tests/data/test_case_080/case.yml",
                                sumo_connection=sumo_connection)


def test_upload_without_registration():
    sumo_connection = uploader.SumoConnection(env=ENV)
    e = uploader.CaseOnDisk(case_metadata_path="tests/data/test_case_080/case.yml",
                                sumo_connection=sumo_connection)
    with pytest.raises(IOError):
        # assert that uploading withouth registering fails
        e.upload(threads=1)


def test_case():
    """
        Upload case to Sumo. Assert that the case is there and that only one
        case with this ID exists.
    """
    sumo_connection = uploader.SumoConnection(env=ENV)
    logger.debug("initialize CaseOnDisk")
    e = uploader.CaseOnDisk(case_metadata_path="tests/data/test_case_080/case.yml",
                                sumo_connection=sumo_connection)

    query = f'fmu.case.uuid:{e.fmu_case_uuid}'

    # assert that it is not there in the first place
    logger.debug("Asserting that the test case is not already there")
    search_results = sumo_connection.api.searchroot(query)
    logger.debug("search results: %s", str(search_results))
    if not search_results:
        raise ValueError("No search results returned")
    hits = search_results.get('hits').get('hits')
    assert len(hits) == 0

    # register it
    e.register()

    # assert that it is there now
    time.sleep(3)  # wait 3 seconds
    search_results = sumo_connection.api.searchroot(query)
    hits = search_results.get('hits').get('hits')
    logger.debug(search_results.get('hits'))
    assert len(hits) == 1


def test_one_file():
    """
        Upload one file to Sumo. Assert that it is there.
    """
    sumo_connection = uploader.SumoConnection(env=ENV)
    e = uploader.CaseOnDisk(case_metadata_path="tests/data/test_case_080/case.yml",
                                sumo_connection=sumo_connection)
    e.register()
    e.add_files('tests/data/test_case_080/surface.bin')

    # Assert children is on Sumo

    e.upload()
    time.sleep(4)
    search_results = sumo_connection.api.search(query=f'{e.fmu_case_uuid}')
    total = search_results.get('hits').get('total').get('value')
    assert total == 2


def test_missing_metadata():
    """
        Try to upload files where one does not have metadata. Assert that warning is given
        and that upload commences with the other files. Check that the children are present.
    """
    sumo_connection = uploader.SumoConnection(env=ENV)
    e = uploader.CaseOnDisk(case_metadata_path="tests/data/test_case_080/case.yml",
                                sumo_connection=sumo_connection)

    # Assert that expected warning was given
    with pytest.warns(UserWarning) as warnings_record:  # testdata contains one file with missing metadata
        e.add_files('tests/data/test_case_080/surface_no_metadata.bin')
        for _ in warnings_record:
            assert len(warnings_record) == 1, warnings_record
            assert warnings_record[0].message.args[0].endswith("No metadata, skipping file.")

    # Assert children is on Sumo
    search_results = sumo_connection.api.search(query=f'{e.fmu_case_uuid}')
    total = search_results.get('hits').get('total').get('value')
    assert total == 2


def test_wrong_metadata():
    """
        Try to upload files where one does have metadata with error. Assert that warning is given
        and that upload commences with the other files. Check that the children are present.
    """
    sumo_connection = uploader.SumoConnection(env=ENV)
    e = uploader.CaseOnDisk(case_metadata_path="tests/data/test_case_080/case.yml",
                                sumo_connection=sumo_connection)

    # Assert that expected warning was given
    e.add_files('tests/data/test_case_080/surface_error.bin')

    e.upload()
    time.sleep(4)
    # Assert children is on Sumo
    search_results = sumo_connection.api.search(query=f'{e.fmu_case_uuid}')
    total = search_results.get('hits').get('total').get('value')
    assert total == 2


def test_seismic_file():
    """
        Upload one seimic file to Sumo. Assert that it is there.
    """
    sumo_connection = uploader.SumoConnection(env=ENV)
    e = uploader.CaseOnDisk(case_metadata_path="tests/data/test_case_080/case.yml",
                                sumo_connection=sumo_connection)
    e.register()
    e.add_files('tests/data/test_case_080/seismic.segy')

    # Assert children is on Sumo

    e.upload()
    time.sleep(4)
    search_results = sumo_connection.api.search(query=f'{e.fmu_case_uuid}')
    total = search_results.get('hits').get('total').get('value')
    assert total == 3

    

def test_teardown():
    """
    Teardown all testdata
    """
    sumo_connection = uploader.SumoConnection(env=ENV)
    e = uploader.CaseOnDisk(case_metadata_path="tests/data/test_case_080/case.yml",
                                sumo_connection=sumo_connection)

    # This uploads ensemble metadata to Sumo
    e.register()

    sumo_connection.api.delete_object(e.sumo_parent_id)

    time.sleep(4)
    # Assert children is not on Sumo
    search_results = sumo_connection.api.search(query=f'{e.fmu_case_uuid}')
    total = search_results.get('hits').get('total').get('value')
    assert total == 0
