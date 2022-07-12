#! python
"""Verify the size attributes in Datasets and Investigations.

This script checks all Datasets and Investigations and verifies the
values of the fileCount and fileSize attributes respectively.

The script needs to be run by an ICAT user having read access to all
Investigations, Datasets, and Datafiles.  If you use the --fix-values
command line switch, this user must furthermore have update permission
for Investigations and Datasets.

Note that the size attributes are expected to be in the next major
release of icat.server 5.0, but are not yet present in any release
version so far.  The script is based on the implementation of `Pull
Request #256: ICAT schema extensions for ICAT Server 5.0 release`__ as
of 2021-11-26.  The final implementation in a future release version
may differ.

The script requires python-icat >= 0.18.0.

.. __: https://github.com/icatproject/icat.server/pull/256
"""

from distutils.version import StrictVersion as Version
import logging
import icat
import icat.config
from icat.query import Query

log = logging.getLogger(__name__)

def check_dataset(inv, ds, fixvalues=False):
    client = ds.client
    ds_name = "Dataset(%s / %s / %s)" % (inv.name, inv.visitId, ds.name)
    log.debug("Considering %s", ds_name)
    need_fix = False
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
            need_fix = True
        if ds.fileSize:
            log.warn("%s: fileSize is wrong: %d versus %d",
                     ds_name, ds.fileSize, 0)
            need_fix = True
    else:
        fileSize_query = Query(client, "Datafile", conditions={
            "dataset.id": "= %d" % ds.id
        }, attributes="fileSize", aggregate="SUM")
        fileSize = client.assertedSearch(fileSize_query)[0]
        if ds.fileCount is None:
            log.warn("%s: fileCount is not set", ds_name)
            need_fix = True
        elif ds.fileCount != fileCount:
            log.warn("%s: fileCount is wrong: %d versus %d",
                     ds_name, ds.fileCount, fileCount)
            need_fix = True
        if ds.fileSize is None:
            log.warn("%s: fileSize is not set", ds_name)
            need_fix = True
        elif ds.fileSize != fileSize:
            log.warn("%s: fileSize is wrong: %d versus %d",
                     ds_name, ds.fileSize, fileSize)
            need_fix = True
    if fixvalues and need_fix:
        ds.fileCount = fileCount
        ds.fileSize = fileSize
        ds.update()
    return fileCount, fileSize

def check_investigation(inv, fixvalues=False):
    client = inv.client
    inv_name = "Investigation(%s / %s)" % (inv.name, inv.visitId)
    log.debug("Check sizes in %s", inv_name)
    fileCount = 0
    fileSize = 0
    need_fix = False
    ds_select = Query(client, "Dataset", conditions={
        "investigation.id": "= %d" % inv.id
    })
    if fixvalues:
        ds_select.addIncludes("1")
    for ds in client.searchChunked(ds_select):
        ds_c, ds_s = check_dataset(inv, ds, fixvalues)
        fileCount += ds_c
        fileSize += ds_s
    if not fileCount:
        log.debug("%s has no datafiles", inv_name)
        if inv.fileCount:
            log.warn("%s: fileCount is wrong: %d versus %d",
                     inv_name, inv.fileCount, 0)
            need_fix = True
        if inv.fileSize:
            log.warn("%s: fileSize is wrong: %d versus %d",
                     inv_name, inv.fileSize, 0)
            need_fix = True
    else:
        if inv.fileCount is None:
            log.warn("%s: fileCount is not set", inv_name)
            need_fix = True
        elif inv.fileCount != fileCount:
            log.warn("%s: fileCount is wrong: %d versus %d",
                     inv_name, inv.fileCount, fileCount)
            need_fix = True
        if inv.fileSize is None:
            log.warn("%s: fileSize is not set", inv_name)
            need_fix = True
        elif inv.fileSize != fileSize:
            log.warn("%s: fileSize is wrong: %d versus %d",
                     inv_name, inv.fileSize, fileSize)
            need_fix = True
    if fixvalues and need_fix:
        inv.fileCount = fileCount
        inv.fileSize = fileSize
        inv.update()

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    # suds is somewhat too chatty
    logging.getLogger('suds').setLevel(logging.WARN)

    config = icat.config.Config(ids=False)
    config.add_variable('fixvalues', ("--fix-values",),
                        dict(help="fix wrong fileCount and fileSize values"),
                        type=icat.config.flag, default=False)
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

    inv_query = Query(client, "Investigation")
    if conf.fixvalues:
        inv_query.addIncludes("1")
    for inv in client.searchChunked(inv_query):
        check_investigation(inv, conf.fixvalues)

if __name__ == "__main__":
    main()
