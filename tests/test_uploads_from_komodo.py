""" Verifies SUMO uploads that has been run by Github Actions in the komodo-releases repo. 
    https://github.com/equinor/komodo-releases/blob/main/.github/workflows/run_drogon.yml
"""
import os
import sys
from datetime import datetime, timedelta
import pytest
from pathlib import Path
import logging
from random import seed
from random import randint
from fmu.sumo.explorer import Explorer

if not sys.platform.startswith("darwin"):
    import openvds

# Run the tests from the root dir
TEST_DIR = Path(__file__).parent / "../"
os.chdir(TEST_DIR)

ENV = "dev"

logger = logging.getLogger(__name__)
logger.setLevel(level="DEBUG")


@pytest.fixture(name="explorer")
def fixture_explorer(token: str) -> Explorer:
    """Returns an explorer to every test which have 'explorer: Explorer' as argument"""
    return Explorer("dev", token=token)


def _get_creation_date(metadata):
    """Returns the date this case was created, based on the case metadata"""
    for item in metadata["tracklog"]:
        if item["event"] == "created":
            return item["datetime"]


def _get_newest_case(explorer):
    """Returns latest case that was uploaded by f_scout_ci"""
    cases = explorer.cases
    cases.users
    cases = cases.filter(user="f_scout_ci")
    assert len(cases) > 0
    newest_date = "2000-01-01T00:00:00.000Z"
    newest_case = cases[0]
    for case in cases:
        case_created_at = _get_creation_date(case.metadata)
        if case_created_at > newest_date:
            newest_date = case_created_at
            newest_case = case

    print("Newest case: ", newest_case.uuid, " ", newest_case.name)
    return newest_case


def test_case_surfaces(explorer: Explorer):
    """Test surfaces from the latest case uploaded from komodo-releases"""
    case = _get_newest_case(explorer)
    assert case
    iter_count = 0
    real_count = 0
    for surf in case.surfaces:
        assert surf.uuid
        assert surf.name
        assert surf.tagname
        if surf.iteration is not None:
            iter_count += 1
        if surf.realization is not None:
            real_count += 1
    assert iter_count > 53
    assert real_count > 53
    # No need to test every blob element every time
    seed()
    random_index = randint(0, len(case.surfaces) - 1)
    reg = case.surfaces[random_index].to_regular_surface()
    mean = reg.values.mean()
    assert mean


def test_case_tables(explorer: Explorer):
    """Test tables from the latest case uploaded from komodo-releases"""
    case = _get_newest_case(explorer)
    assert case
    iter_count = 0
    real_count = 0
    tagname_count = 0
    # SIM2SUMO uploads:
    drogon_rft = False
    drogon_satfunc = False
    drogon_summary = False
    for tbl in case.tables:
        assert tbl.uuid
        assert tbl.name
        if tbl.iteration is not None:
            iter_count += 1
        if tbl.realization is not None:
            real_count += 1
        if tbl.tagname is not None:
            tagname_count += 1
        if tbl.name == "DROGON" and tbl.tagname == "rft":
            drogon_rft = True
        if tbl.name == "DROGON" and tbl.tagname == "satfunc":
            drogon_satfunc = True
        if tbl.name == "DROGON" and tbl.tagname == "summary":
            drogon_summary = True
    assert iter_count > 6
    assert real_count > 6
    assert tagname_count > 6
    assert drogon_rft
    assert drogon_satfunc
    assert drogon_summary
    # No need to test every blob element every time
    seed()
    random_index = randint(0, len(case.tables) - 1)
    arrow = case.tables[random_index].to_arrow()
    arrow.validate() 


def test_case_polygons(explorer: Explorer):
    """Test polygons from the latest case uploaded from komodo-releases"""
    case = _get_newest_case(explorer)
    assert case
    iter_count = 0
    real_count = 0
    tagname_count = 0
    for poly in case.polygons:
        assert poly.uuid
        assert poly.name
        if poly.iteration is not None:
            iter_count += 1
        if poly.realization is not None:
            real_count += 1
        if poly.tagname is not None:
            tagname_count += 1
    assert iter_count > 5
    assert real_count > 5
    assert tagname_count > 5
    # No need to test every blob element every time
    seed()
    random_index = randint(0, len(case.polygons) - 1)
    case.polygons[random_index].to_pandas()
    

def test_case_dictionaries(explorer: Explorer):
    """Test dictionaries from the latest case uploaded from komodo-releases"""
    case = _get_newest_case(explorer)
    assert case
    iter_count = 0
    real_count = 0
    tagname_count = 0
    name_parameter_found = False
    for dct in case.dictionaries:
        assert dct.uuid
        assert dct.name
        if dct.iteration is not None:
            iter_count += 1
        if dct.realization is not None:
            real_count += 1
        if dct.tagname is not None:
            tagname_count += 1
        if dct.name == "parameters":
            name_parameter_found = True
    assert iter_count > 0
    assert real_count > 0
    assert tagname_count >= 0
    assert name_parameter_found
    # No need to test every blob element every time
    seed()
    random_index = randint(0, len(case.dictionaries) - 1)
    obj = case.dictionaries[random_index]
    obj._blob


@pytest.mark.skipif(
    sys.platform.startswith("darwin"),
    reason="do not run OpenVDS on mac os",
)
def test_case_seismic(explorer: Explorer):
    """Test seismic cubes in the latest case uploaded from komodo-releases"""
    case = _get_newest_case(explorer)
    assert case
    assert len(case.cubes) > 9
    iter_count = 0
    real_count = 0
    for cube in case.cubes:
        assert cube.uuid
        assert cube.name
        assert cube.tagname
        if cube.iteration is not None:
            iter_count += 1
        if cube.realization is not None:
            real_count += 1
    assert iter_count > 9
    assert real_count > 9

    # No need to test every blob element every time
    seed()
    random_index = randint(0, len(case.cubes) - 1)
    cube = case.cubes[random_index]
    handle = cube.openvds_handle
    layout = openvds.getLayout(handle)
    channel_count = layout.getChannelCount()
    assert channel_count > 0


def test_case(explorer: Explorer):
    """Test the latest case uploaded from komodo-releases by f_scout_ci"""
    case = _get_newest_case(explorer)
    assert case
    number_of_children = (
        len(case.cubes)
        + len(case.polygons)
        + len(case.surfaces)
        + len(case.dictionaries)
        + len(case.tables)
    )
    assert number_of_children > 110
    assert len(case.surfaces) > 86
    assert len(case.cubes) > 9
    assert len(case.tables) > 6
    assert len(case.polygons) > 5
    assert len(case.dictionaries) > 0

    case_timestamp = _get_creation_date(case.metadata)
    yesterday = datetime.now() - timedelta(days=1)
    print("Timestamp of latest case: ", case_timestamp)
    # Assuming that komodo-releases repo Github Actions runs at least every 24 hours:
    assert case_timestamp > yesterday.strftime("%Y-%m-%dT%H:%M"), f"The newest case is too old: {case_timestamp}: check komodo-releases repo Github Actions runs"
