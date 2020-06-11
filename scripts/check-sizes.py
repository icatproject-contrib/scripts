#! python
"""Verify the size attributes in Datasets and Investigations.

This script checks all Datasets and Investigations and verifies the
values of the size attributes Dataset.datasetSize and
Investigation.investigationSize respectively.

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

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
# suds is somewhat too chatty
logging.getLogger('suds').setLevel(logging.WARN)

config = icat.config.Config(ids=False)
client, conf = config.getconfig()
client.login(conf.auth, conf.credentials)

log = logging.getLogger(__name__)

if Version(icat.__version__) < '0.17':
    raise RuntimeError("Your python-icat version %s is too old, "
                       "need 0.17.0 or newer" % icat.__version__)

if not ('investigationSize' in client.typemap['investigation'].InstAttr and
        'datasetSize' in client.typemap['dataset'].InstAttr):
    raise RuntimeError("This ICAT server does not support the size "
                       "attributes in Datasets and Investigations")

inv_select = Query(client, "Investigation")
for inv in client.searchChunked(inv_select):
    inv_name = "Investigation(%s / %s)" % (inv.name, inv.visitId)
    log.debug("Check sizes in %s", inv_name)
    inv_size = 0
    ds_select = Query(client, "Dataset", conditions={
        "investigation.id": "= %d" % inv.id
    })
    for ds in client.searchChunked(ds_select):
        ds_name = "Dataset(%s / %s / %s)" % (inv.name, inv.visitId, ds.name)
        log.debug("Considering %s", ds_name)
        ds_file_count_query = Query(client, "Datafile", conditions={
            "dataset.id": "= %d" % ds.id
        }, aggregate="COUNT")
        if not client.assertedSearch(ds_file_count_query)[0]:
            log.debug("%s has no datafiles", ds_name)
            if ds.datasetSize:
                log.warn("%s: datasetSize is wrong: %d versus %d",
                         ds_name, ds.datasetSize, 0)
        else:
            ds_size_query = Query(client, "Datafile", conditions={
                "dataset.id": "= %d" % ds.id
            }, attribute="fileSize", aggregate="SUM")
            ds_size = client.assertedSearch(ds_size_query)[0]
            inv_size += ds_size
            if ds.datasetSize is None:
                log.warn("%s: datasetSize is not set", ds_name)
            elif ds.datasetSize != ds_size:
                log.warn("%s: datasetSize is wrong: %d versus %d",
                         ds_name, ds.datasetSize, ds_size)
    if inv.investigationSize is None:
        log.warn("%s: investigationSize is not set", inv_name)
    elif inv.investigationSize != inv_size:
        log.warn("%s: investigationSize is wrong: %d versus %d",
                 inv_name, inv.investigationSize, inv_size)
