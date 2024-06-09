#! python
"""Check for bug 164 in ids.server.

There is a `bug`__ in class `LockManager` that may cause that the
`mainStorage.lock()` method of the storage plugin may be called
multiple times for the same dataset, which may result in an
`OverlappingFileLockException` exception to be thrown (depending on
the implementation of `mainStorage.lock()`).

This script triggers the bug and thus checks whether your ids.server
is affected.  The bug is not visible from outside, so this script
can't tell you the result.  But if the bug is triggered, you will find
a trace in your server.log.

The script uploads a new dataset to an investigation.  It is assumed
that the investigation is unique by its name and has no dataset with
the name "test-upload-ids-bug164".  It is furthermore assumed that
your ids.server uses twolevel storage, is not readOnly, that the
storage unit in your ids.server is "dataset", and
delayDatasetWritesSeconds is set to 60 (the default).

Please be aware that if your ids.server is susceptible to the bug,
running the script will crash its FSM thread, so you'll need to
restart ids.server.

.. __: https://github.com/icatproject/ids.server/issues/164
"""

import datetime
import io
import logging
import time

import icat
import icat.config
from icat.query import Query

log = logging.getLogger(__name__)

file_content = """Lorem ipsum dolor sit amet, consectetur adipisici elit, sed eiusmod tempor incidunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi consequat. Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
""".encode('ascii')

class SlowFile:
    """A file object ... that ... reads ... its ... content ... slooooowly.
    """
    def __init__(self, content):
        self.buffer = io.BytesIO(content)
        self.size = len(content)

    def read(self, size=-1):
        if size < 0:
            size = self.size
        size = min(size, 10)
        self.size -= size
        time.sleep(2)
        return self.buffer.read(size)

def get_dataset_type(client):
    """Pick a random DatasetType.
    """
    return client.assertedSearch("0,1 DatasetType")[0]

def get_datafile_format(client):
    """Pick a random DatafileFormat.
    """
    return client.assertedSearch("0,1 DatafileFormat")[0]

def get_investigation(client, name):
    """Get an investigation by name.
    """
    query = Query(client, "Investigation", conditions={"name": "= '%s'" % name})
    return client.assertedSearch(query)[0]

def main(client, investigation):
    """We create a new dataset and upload two datafiles to it.  

    The second upload takes significant time.  The bug is triggered by
    the fact that ids.server starts the WRITE deferred operation from
    the first upload while the second one is still ongoing.  Thus,
    ids.server acquires two shared locks on the same dataset in
    different threads.
    """
    dataset = client.new("Dataset")
    dataset.investigation = investigation
    dataset.type = get_dataset_type(client)
    dataset.name = "test-upload-ids-bug164"
    dataset.complete = False
    dataset.description = "Testing ids.server Issue #164"
    dataset.create()
    df_format = get_datafile_format(client)
    modTime = datetime.datetime.now(tz=datetime.timezone.utc)
    df1 = client.new("Datafile", name="Lorem-1.txt",
                     dataset=dataset, datafileFormat=df_format)
    df1.datafileCreateTime = df1.datafileModTime = modTime
    client.putData(io.BytesIO(file_content), df1)
    df2 = client.new("Datafile", name="Lorem-2.txt",
                     dataset=dataset, datafileFormat=df_format)
    df2.datafileCreateTime = df2.datafileModTime = modTime
    client.putData(SlowFile(file_content), df2)

if __name__ == "__main__":
    config = icat.config.Config(ids="mandatory")
    config.add_variable('investigation', ("--investigation",),
                        dict(help="investigation to upload test data to"))
    client, conf = config.getconfig()
    client.login(conf.auth, conf.credentials)
    inv = get_investigation(client, conf.investigation)
    main(client, inv)
