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

The script requires python-icat >= 0.17.0.

.. __: https://github.com/icatproject/icat.server/pull/233
"""

from distutils.version import StrictVersion as Version
import logging
import icat
import icat.config
from icat.query import Query

log = logging.getLogger(__name__)

def check_investigation(inv):
    client = inv.client
    inv_name = "Investigation(%s / %s)" % (inv.name, inv.visitId)
    log.debug("Check sizes in %s", inv_name)
    inv_fileCount = 0
    inv_fileSize = 0
    ds_select = Query(client, "Dataset", conditions={
        "investigation.id": "= %d" % inv.id
    })
    for ds in client.searchChunked(ds_select):
        ds_name = "Dataset(%s / %s / %s)" % (inv.name, inv.visitId, ds.name)
        log.debug("Considering %s", ds_name)
        fileCount_query = Query(client, "Datafile", conditions={
            "dataset.id": "= %d" % ds.id
        }, aggregate="COUNT")
        ds_fileCount = client.assertedSearch(fileCount_query)[0]
        if not ds_fileCount:
            log.debug("%s has no datafiles", ds_name)
            if ds.fileCount:
                log.warn("%s: fileCount is wrong: %d versus %d",
                         ds_name, ds.fileCount, 0)
            if ds.fileSize:
                log.warn("%s: fileSize is wrong: %d versus %d",
                         ds_name, ds.fileSize, 0)
        else:
            fileSize_query = Query(client, "Datafile", conditions={
                "dataset.id": "= %d" % ds.id
            }, attribute="fileSize", aggregate="SUM")
            ds_fileSize = client.assertedSearch(fileSize_query)[0]
            inv_fileCount += ds_fileCount
            inv_fileSize += ds_fileSize
            if ds.fileCount is None:
                log.warn("%s: fileCount is not set", ds_name)
            elif ds.fileCount != ds_fileCount:
                log.warn("%s: fileCount is wrong: %d versus %d",
                         ds_name, ds.fileCount, ds_fileCount)
            if ds.fileSize is None:
                log.warn("%s: fileSize is not set", ds_name)
            elif ds.fileSize != ds_fileSize:
                log.warn("%s: fileSize is wrong: %d versus %d",
                         ds_name, ds.fileSize, ds_fileSize)
    if not inv_fileCount:
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
        elif inv.fileCount != inv_fileCount:
            log.warn("%s: fileCount is wrong: %d versus %d",
                     inv_name, inv.fileCount, inv_fileCount)
        if inv.fileSize is None:
            log.warn("%s: fileSize is not set", inv_name)
        elif inv.fileSize != inv_fileSize:
            log.warn("%s: fileSize is wrong: %d versus %d",
                     inv_name, inv.fileSize, inv_fileSize)

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    # suds is somewhat too chatty
    logging.getLogger('suds').setLevel(logging.WARN)

    config = icat.config.Config(ids=False)
    client, conf = config.getconfig()
    client.login(conf.auth, conf.credentials)

    if Version(icat.__version__) < '0.17':
        raise RuntimeError("Your python-icat version %s is too old, "
                           "need 0.17.0 or newer" % icat.__version__)

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
