#! /usr/bin/python3
"""Test size attributes.

`PR #233`_ adds the size attributes `Dataset.datasetSize` and
`Investigation.investigationSize` to icat.server, along with database
triggers to keep them up to date.  This script intends to test that
the triggers work as expected and to assess the performance.  The
script may also used with the current release version of icat.server
not having the attributes and triggers in order to measure a reference
timing.

The script requires an investigation to be present in ICAT to create
datasets and datafiles.  The user running the script must have
permission to create datasets and datafiles in that investigation.

The script only tests creating datasets and datafiles and updating the
fileSize attribute of datafiles.  Deleting datafiles or datasets,
moving datafiles between datasets, or moving datasets between
investigations is not tested.

The tests consider four test cases:

1. Create datafiles having a positive fileSize.

2. Create datasets with datafiles (in one call using cascading) having
   a positive fileSize.

3. Create datasets with datafiles having the fileSize not set and then
   update the fileSize to a positive value in a second step.

4. Given datafiles with a positive fileSize, update the fileSize to a
   different positive value.

The script requires python-icat >= 0.17.0.

.. _PR #233: https://github.com/icatproject/icat.server/pull/233
"""

import logging
import re
from timeit import default_timer as timer
import icat
import icat.config
from icat.query import Query

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

config = icat.config.Config(ids=False)
config.add_variable('investigation', ("investigation",), 
                    dict(help="name and optionally visit id "
                         "(separated by a colon) of the investigation"))
client, conf = config.getconfig()
client.login(conf.auth, conf.credentials)

have_size_attrs = (
    'investigationSize' in client.typemap['investigation'].InstAttr and
    'datasetSize' in client.typemap['dataset'].InstAttr
)


class Time(float):
    """Convenience: human readable time intervals.
    """
    second = 1
    minute = 60*second
    hour = 60*minute
    day = 24*hour
    millisecond = (1/1000)*second
    units = { 'ms':millisecond, 's':second, 'min':minute, 'h':hour, 'd':day, }

    def __new__(cls, value):
        if isinstance(value, str):
            m = re.match(r'^(\d+(?:\.\d+)?)\s*(ms|s|min|h|d)$', value)
            if not m:
                raise ValueError("Invalid time string '%s'" % value)
            v = float(m.group(1)) * cls.units[m.group(2)]
            return super(Time, cls).__new__(cls, v)
        else:
            v = float(value)
            if v < 0:
                raise ValueError("Invalid time value %f" % v)
            return super(Time, cls).__new__(cls, v)

    def __str__(self):
        for u in ['d', 'h', 'min', 's']:
            if self >= self.units[u]:
                return "%.3f %s" % (self / self.units[u], u)
        else:
            return "%.3f ms" % (self / self.units['ms'])

class TestBase:
    Name = None
    Case_No = None
    Description = None
    Num_Datasets = 100
    Num_Datafiles = 1000
    FileSize = 997

    @staticmethod
    def _get_investigation(invid):
        l = invid.split(':')
        query = Query(client, "Investigation")
        if len(l) == 1:
            # No colon, invid == name
            query.addConditions({
                "name": "='%s'" % l[0],
            })
        elif len(l) == 2:
            # one colon, invid == name:visitId
            query.addConditions({
                "name": "= '%s'" % l[0],
                "visitId": "= '%s'" % l[1],
            })
        else:
            # too many colons
            raise ValueError("Invalid investigation identifier '%s'" % invid)
        return client.assertedSearch(query)[0]

    def __init__(self, invid):
        self.investigation = self._get_investigation(invid)
        self.inv_name = "Investigation(%s)" % invid
        self.inv_size = (self.investigation.investigationSize
                         if have_size_attrs
                         else 0)
        query = Query(client, "DatasetType", conditions={"name": "= 'other'"})
        self.ds_type = client.assertedSearch(query)[0]

    def run_dataset(self, ds_name):
        """Run the test for one individual dataset.

        The test may either create the dataset or manipulate an
        existing dataset.  Must be implemented in subclasses.  Must
        return a tuple with two elements: the dataset and the time in
        seconds elapsed to run the test.
        """
        raise NotImplementedError()

    def run(self):
        min_time = None
        max_time = None
        total_time = 0
        log.info("%s: %s", self.Name, self.Description)
        for ds_count in range(self.Num_Datasets):
            client.autoRefresh()
            ds_name = "test_%d_%04d" % (self.Case_No, ds_count)
            dataset, elapsed = self.run_dataset(ds_name)
            if min_time is None or elapsed < min_time:
                min_time = elapsed
            if max_time is None or elapsed > max_time:
                max_time = elapsed
            total_time += elapsed
            if have_size_attrs:
                ds_size = self.Num_Datafiles * self.FileSize
                dataset.get("Dataset")
                if dataset.datasetSize != ds_size:
                    log.warn("%s: datasetSize is wrong: %d versus %d",
                             dataset.name, dataset.datasetSize, ds_size)
        if have_size_attrs:
            ds_size = self.Num_Datafiles * self.FileSize
            self.inv_size += self.Num_Datasets * ds_size
            self.investigation.get("Investigation")
            if self.investigation.investigationSize != self.inv_size:
                log.warn("%s: investigationSize is wrong: %d versus %d",
                         self.inv_name,
                         self.investigation.investigationSize,
                         self.inv_size)
        avg_time = total_time / self.Num_Datasets
        log.info("%s: done %d datasets having %d datafiles each",
                 self.Name, self.Num_Datasets, self.Num_Datafiles)
        log.info("%s: min/max/avg time per dataset: %s / %s / %s",
                 self.Name, Time(min_time), Time(max_time), Time(avg_time))

class TestCase1(TestBase):
    Name = "Test case 1"
    Case_No = 1
    Description = "create datafiles having a positive fileSize"

    def run_dataset(self, ds_name):
        dataset = client.new("dataset",
                             investigation=self.investigation,
                             type=self.ds_type,
                             name=ds_name,
                             complete=False)
        dataset.create()
        dataset.get("Dataset")
        start_time = timer()
        for df_count in range(self.Num_Datafiles):
            datafile = client.new("datafile",
                                  dataset=dataset,
                                  name="df_%04d" % df_count,
                                  fileSize=self.FileSize)
            datafile.create()
        end_time = timer()
        elapsed = end_time - start_time
        return dataset, elapsed

class TestCase2(TestBase):
    Name = "Test case 2"
    Case_No = 2
    Description = "create datasets with datafiles having a positive fileSize"

    def run_dataset(self, ds_name):
        start_time = timer()
        dataset = client.new("dataset",
                             investigation=self.investigation,
                             type=self.ds_type,
                             name=ds_name,
                             complete=False)
        for df_count in range(self.Num_Datafiles):
            datafile = client.new("datafile",
                                  name="df_%04d" % df_count,
                                  fileSize=self.FileSize)
            dataset.datafiles.append(datafile)
        dataset.create()
        end_time = timer()
        elapsed = end_time - start_time
        return dataset, elapsed

class TestCase3(TestBase):
    Name = "Test case 3"
    Case_No = 3
    Description = "update datafile.fileSize from not set to a positive value"

    def run_dataset(self, ds_name):
        dataset = client.new("dataset",
                             investigation=self.investigation,
                             type=self.ds_type,
                             name=ds_name,
                             complete=False)
        for df_count in range(self.Num_Datafiles):
            datafile = client.new("datafile", name="df_%04d" % df_count)
            dataset.datafiles.append(datafile)
        dataset.create()
        dataset.get("Dataset")
        query = Query(client, "Datafile", conditions={
            "dataset.id": "= %d" % dataset.id
        }, includes="1")
        datafiles = client.search(query)
        assert len(datafiles) == self.Num_Datafiles
        start_time = timer()
        for datafile in datafiles:
            datafile.fileSize = self.FileSize
            datafile.update()
        end_time = timer()
        elapsed = end_time - start_time
        return dataset, elapsed

class TestCase4(TestBase):
    Name = "Test case 4"
    # Hack: the Case_No is only used to build the dataset name.  By
    # setting this to 1, we essentially reuse the datasets from test
    # case 1.
    Case_No = 1
    Description = "update datafile.fileSize to a different positive value"
    FileSize = 9973

    def run_dataset(self, ds_name):
        query = Query(client, "Dataset", conditions={
            "investigation.id": "= %d" % self.investigation.id,
            "name": "= '%s'" % ds_name
        })
        dataset = client.assertedSearch(query)[0]
        if have_size_attrs:
            self.inv_size -= dataset.datasetSize
        query = Query(client, "Datafile", conditions={
            "dataset.id": "= %d" % dataset.id
        }, includes="1")
        datafiles = client.search(query)
        assert len(datafiles) == self.Num_Datafiles
        start_time = timer()
        for datafile in datafiles:
            datafile.fileSize = self.FileSize
            datafile.update()
        end_time = timer()
        elapsed = end_time - start_time
        return dataset, elapsed

TestCase1(conf.investigation).run()
TestCase2(conf.investigation).run()
TestCase3(conf.investigation).run()
TestCase4(conf.investigation).run()
