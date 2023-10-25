# fmu-sumo-uploader
Upload data from FMU to Sumo

### Concepts
`SumoConnection`: The SumoConnection object represents the connection to Sumo, and will handle authentication etc when initiated. This object uses the Sumo python wrapper under the hood.

`CaseOnDisk`: The CaseOnDisk object represents an ensemble of reservoir model realisations. The object relates to the case metadata. Individual files belonging to the case are represented as FileOnDisk objects.

`FileOnDisk`: The FileOnDisk object represents a single file in an FMU case, stored on the local disk.

`CaseOnJob`: Similar to CaseOnDisk, but does not refer to files on disk. Instead uses in-memory structures.

`FileOnJob`: Similar to FileOnDisk, but uses in-memory structures.

### workflow for uploading during ERT runs

HOOK (presim) workflow registering the case:
```python
from fmu.sumo import uploader

# Establish connection to Sumo
connection = sumo.SumoConnection()

# Initiate the case object
case = sumo.CaseOnDisk(
    case_metadata_path="/path/to/case_metadata.yml",
    sumo_connection=sumo_connection
    )

# Register the case on Sumo
# This uploads case metadata to Sumo
case.register()
```

FORWARD_JOB uploading data (can be repeated multiple times during a workflow):
```python
from fmu.sumo import uploader

# Establish connection to Sumo
connection = sumo.SumoConnection()

# Initiate the case object
case = sumo.CaseOnDisk(
    case_metadata_path="/path/to/case_metadata",
    sumo_connection=sumo_connection
    )

# Add file-objects to the case
case.add_files("/globable/path/to/files/*.gri")

# Upload case data objects (files)
case.upload()

```
