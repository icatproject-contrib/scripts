#! python
"""Verify the size attributes in Datasets and Investigations.

This script checks all Datasets and Investigations and verifies the
values of the size attributes Dataset.fileSize and
Investigation.fileSize respectively.

The script needs to be run by an ICAT user having read access to all
Investigations, Datasets, and Datafiles.

The script assumes that the `Pull Request #233: ICAT schema extension
with columns for investigation and dataset sizes`__ has been merged in
the icat.server the script talks to.

The script requires python-icat >= 0.18.0.

.. __: https://github.com/icatproject/icat.server/pull/233
"""

from distutils.version import StrictVersion as Version
import logging
import icat
import icat.config
from icat.query import Query

log = logging.getLogger(__name__)

def check_dataset(inv, ds):
    client = ds.client
    ds_name = "Dataset(%s / %s / %s)" % (inv.name, inv.visitId, ds.name)
    log.debug("Considering %s", ds_name)
    fileCount_query = Query(client, "Datafile", conditions={
        "dataset.id": "= %d" % ds.id
    }, aggregate="COUNT")
    fileCount = client.assertedSearch(fileCount_query)[0]
    if not fileCount:
        log.debug("%s has no datafiles", ds_name)
        fileSize = 0
        if ds.fileCount:
            log.warn("%s: fileCount is wrong: %d versus %d",
                     ds_name, ds.fileCount, 0)
        if ds.fileSize:
            log.warn("%s: fileSize is wrong: %d versus %d",
                     ds_name, ds.fileSize, 0)
    else:
        fileSize_query = Query(client, "Datafile", conditions={
            "dataset.id": "= %d" % ds.id
        }, attributes="fileSize", aggregate="SUM")
        fileSize = client.assertedSearch(fileSize_query)[0]
        if ds.fileCount is None:
            log.warn("%s: fileCount is not set", ds_name)
        elif ds.fileCount != fileCount:
            log.warn("%s: fileCount is wrong: %d versus %d",
                     ds_name, ds.fileCount, fileCount)
        if ds.fileSize is None:
            log.warn("%s: fileSize is not set", ds_name)
        elif ds.fileSize != fileSize:
            log.warn("%s: fileSize is wrong: %d versus %d",
                     ds_name, ds.fileSize, fileSize)
    return fileCount, fileSize

def check_investigation(inv):
    client = inv.client
    inv_name = "Investigation(%s / %s)" % (inv.name, inv.visitId)
    log.debug("Check sizes in %s", inv_name)
    fileCount = 0
    fileSize = 0
    ds_select = Query(client, "Dataset", conditions={
        "investigation.id": "= %d" % inv.id
    })
    for ds in client.searchChunked(ds_select):
        ds_c, ds_s = check_dataset(inv, ds)
        fileCount += ds_c
        fileSize += ds_s
    if not fileCount:
        log.debug("%s has no datafiles", inv_name)
        if inv.fileCount:
            log.warn("%s: fileCount is wrong: %d versus %d",
                     inv_name, inv.fileCount, 0)
        if inv.fileSize:
            log.warn("%s: fileSize is wrong: %d versus %d",
                     inv_name, inv.fileSize, 0)
    else:
        if inv.fileCount is None:
            log.warn("%s: fileCount is not set", inv_name)
        elif inv.fileCount != fileCount:
            log.warn("%s: fileCount is wrong: %d versus %d",
                     inv_name, inv.fileCount, fileCount)
        if inv.fileSize is None:
            log.warn("%s: fileSize is not set", inv_name)
        elif inv.fileSize != fileSize:
            log.warn("%s: fileSize is wrong: %d versus %d",
                     inv_name, inv.fileSize, fileSize)

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    # suds is somewhat too chatty
    logging.getLogger('suds').setLevel(logging.WARN)

    config = icat.config.Config(ids=False)
    client, conf = config.getconfig()
    client.login(conf.auth, conf.credentials)

    if Version(icat.__version__) < '0.18':
        raise RuntimeError("Your python-icat version %s is too old, "
                           "need 0.18.0 or newer" % icat.__version__)

    if not ('fileSize' in client.typemap['investigation'].InstAttr and
            'fileCount' in client.typemap['investigation'].InstAttr and
            'fileSize' in client.typemap['dataset'].InstAttr and
            'fileCount' in client.typemap['dataset'].InstAttr):
        raise RuntimeError("This ICAT server does not support the size "
                           "attributes in Datasets and Investigations")

    for inv in client.searchChunked(Query(client, "Investigation")):
        check_investigation(inv)

if __name__ == "__main__":
    main()
