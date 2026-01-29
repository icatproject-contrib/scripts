#! python
"""Check Samples and help upgrading icat.server to 7.0

This script is supposed to run various checks and maintenance tasks on
Samples in an ICAT server.  It is mostly aimed at assisting an upgrade
of icat.server to version 7.0 which requires the Sample.pid attribute
to be populated with unique non-null values.

The script implements the following subcommands:

stats
    Display some statistics and provide an indication whether there
    are any obstacles for the upgrade to icat.server 7.0.

lsdup
    List all non-unique Sample.pid values along with the corresponding
    samples.

setpids
    Populate the pid attribute for all samples having it not set.  The
    values are of the form "<prefix>:<id>" which is guaranteed to be
    unique unless there are any existing samples using the same
    prefix.  These values are considered to be placeholders that may
    be replaced by something more sensible later on.  The default
    prefix is "_local", but this can be changed on the command line.

dedup
    Deduplicate existing pid values in samples, e.g. change them to be
    unique.  This is done by appending a suffix: the value "<pid>"
    will be changed to "<pid>/dedup-<nnn>" with some incremental
    number <nnn>.

For the subcommands that set new pid values (setpids and dedup), the
script checks whether there are any existing pid values that could
potentially conflict with the new values to be set before making any
changes.  In this case, the change will not be applied unless forced.

The script needs to be run by a user having read access to all
samples.  While this sounds like stating the obvious, it is important
to mention here, because obviously, the script can not point on issues
if it is not allowed to see them.  And the script has no way to detect
whether there are more samples than it is allowed to see.  So you
won't get any sort of a warning if the script can't see all the
samples.  Furthermore, for the setpids and dedup subcommands, the user
running the scripts needs the corresponding update permissions.

"""

import logging
import math
import re
import icat
import icat.config
from icat.query import Query

logging.NOTICE = logging.INFO + 5
logging.addLevelName(logging.NOTICE, "NOTICE")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ============================= helper ===============================

def searchChunkedNoSkip(client, query, chunksize=100):
    """A variant of client.searchChunked() that does not skip.

    To be used in cases where the body of the loop modifies the result
    set in a way that the treated objects do not match the search
    criterion any more.
    """
    query = query.copy()
    query.setLimit((0, chunksize))
    while True:
        items = client.search(query)
        for item in items:
            yield item
        if len(items) < chunksize:
            break

def get_pid_prefixes(client):
    prefixes = set()
    query = Query(client, "Sample", conditions={
        "pid": "LIKE '%:%'"
    }, attributes=["pid"], aggregate="DISTINCT")
    for pid in client.searchChunked(query):
        p, _ = pid.split(':', maxsplit=1)
        prefixes.add(p)
    return prefixes

def get_samples_by_pid(client, pid):
    query = Query(client, "Sample", conditions={
        "pid": "= '%s'" % pid
    }, order=["id"], includes="1")
    return client.searchChunked(query)

def get_max_sample_id(client):
    query = Query(client, "Sample",
                  attributes=["id"], order=[("id", "DESC")], limit=(0, 1))
    try:
        return client.assertedSearch(query)[0]
    except icat.SearchAssertionError as exc:
        if exc.num == 0:
            return 1
        else:
            raise

def find_potential_upgrade_conflicts(client, prefix):
    auto_pid_re = re.compile(r"%s:\d+" % prefix)
    query = Query(client, "Sample", conditions={
        "pid": "LIKE '%s:%%'" % prefix
    })
    for sample in client.searchChunked(query):
        pid = str(sample.pid)
        if not auto_pid_re.fullmatch(pid):
            continue
        p, i = pid.split(':')
        if int(i) != sample.id:
            yield sample.id, pid

def find_potential_dedup_conflicts(client, pid):
    auto_pid_re = re.compile(r"%s/dedup-\d+" % pid)
    query = Query(client, "Sample", conditions={
        "pid": "LIKE '%s/%%'" % pid
    })
    for sample in client.searchChunked(query):
        if not auto_pid_re.fullmatch(str(sample.pid)):
            continue
        yield sample.id, sample.pid

def find_duplicate_pids(client):
    pid_query = Query(client, "Sample", conditions={
        "pid": "IS NOT NULL"
    }, attributes=["pid"], order=["pid"], aggregate="DISTINCT")
    for pid in client.searchChunked(pid_query):
        count_query = Query(client, "Sample", conditions={
            "pid": "= '%s'" % pid
        }, aggregate="COUNT")
        if client.assertedSearch(count_query)[0] == 1:
            continue
        yield pid

def sample_attr_string(sample):
    attrs = []
    attrs.append("id:%d" % sample.id)
    attrs.append("name:'%s'" % sample.name)
    attrs.append("investigation.name:'%s'" % sample.investigation.name)
    attrs.append("investigation.visitId:'%s'" % sample.investigation.visitId)
    if sample.type:
        attrs.append("type.name:'%s'" % sample.type.name)
    return ", ".join(attrs)

# ============================= stats ================================
# The stats subcommand: provide some statistics and predict whether
# there are any obstacles for the schema upgrade.

def cmd_stats(client, conf):
    have_warning = False

    query = Query(client, "Sample", aggregate="COUNT")
    num_samples = client.assertedSearch(query)[0]
    logger.info("number of samples: %d", num_samples)

    query = Query(client, "Sample", conditions={
        "pid": "IS NOT NULL"
    }, aggregate="COUNT")
    num_samples_pid = client.assertedSearch(query)[0]
    logger.info("number of samples having pid set: %d", num_samples_pid)
    assert num_samples_pid <= num_samples

    query = Query(client, "Sample", conditions={
        "pid": "IS NOT NULL"
    }, attributes=["pid"], aggregate="COUNT:DISTINCT")
    num_pid_values = client.assertedSearch(query)[0]
    logger.info("number of distinct pid values: %d", num_pid_values)
    assert num_pid_values <= num_samples_pid
    if num_pid_values < num_samples_pid:
        logger.warning("there are duplicate pid values")
        have_warning = True

    query = Query(client, "Sample", conditions={
        "pid": "IS NULL"
    }, aggregate="COUNT")
    num_samples_nopid = client.assertedSearch(query)[0]
    logger.info("number of samples having no pid set: %d", num_samples_nopid)
    assert num_samples_nopid <= num_samples
    assert num_samples_pid + num_samples_nopid == num_samples

    prefixes = get_pid_prefixes(client)
    if prefixes:
        prefix_list = ",".join(("'%s'" % p for p in sorted(prefixes)))
        logger.info("prefixes in use in sample pids: %s", prefix_list)
        if num_samples_nopid > 0 and '_local' in prefixes:
            for id, pid in find_potential_upgrade_conflicts(client, '_local'):
                logger.warning("potentially conflicting pid value '%s' "
                               "in Sample %d", pid, id)
                have_warning = True
    else:
        logger.info("no prefixes in use in sample pids")

    if have_warning:
        logger.warning("there were warnings that need to be fixed "
                       "before upgrading to icat.server 7.0!")
    else:
        logger.info("no warnings, upgrading to icat.server 7.0 should succeed")

def cfg_stats(subcmd):
    help_string = "provide statistics and predict obstacles for schema upgrade"
    sub_cfg = subcmd.add_subconfig("stats",
                                   dict(help=help_string),
                                   func=cmd_stats)

# ============================= lsdup ================================
# The lsdup subcommand: show duplicates, e.g. different samples having
# the same pid value.

def cmd_lsdup(client, conf):
    num_dup_pid = 0
    for pid in find_duplicate_pids(client):
        num_dup_pid += 1
        dup_list = ""
        for sample in get_samples_by_pid(client, pid):
            dup_list += "\n\t%s" % sample_attr_string(sample)
        logger.warning("duplicate pid '%s': %s", pid, dup_list)
    if num_dup_pid:
        logger.warning("%d duplicate pids found", num_dup_pid)
    else:
        logger.info("no duplicate pids found")

def cfg_lsdup(subcmd):
    help_string = "show duplicates, e.g. samples having the same pid attributes"
    sub_cfg = subcmd.add_subconfig("lsdup",
                                   dict(help=help_string),
                                   func=cmd_lsdup)

# ============================ setpids ===============================
# The setpids subcommand: populate the pid attribute for all samples
# having it not set.

def cmd_setpids(client, conf):
    have_warning = False
    for id, pid in find_potential_upgrade_conflicts(client, conf.prefix):
        logger.warning("potentially conflicting pid value '%s' "
                       "in Sample %d", pid, id)
        have_warning = True
    if have_warning:
        if conf.force:
            logger.warning("potential conflicts detected, "
                           "proceeding anyway with force")
        else:
            logger.warning("potential conflicts detected, "
                           "won't proceed without force")
            return
    num_digits = max(math.ceil(math.log10(get_max_sample_id(client)))+1, 3)
    query = Query(client, "Sample", conditions={
        "pid": "IS NULL"
    }, includes="1")
    count = 0
    for sample in searchChunkedNoSkip(client, query):
        sample.pid = "%s:%0*d" % (conf.prefix, num_digits, sample.id)
        sample.update()
        count += 1
    logger.info("%d pid attributes set", count)

def cfg_setpids(subcmd):
    help_string = "populate the pid attribute for all samples having it not set"
    sub_cfg = subcmd.add_subconfig("setpids",
                                   dict(help=help_string),
                                   func=cmd_setpids)
    sub_cfg.add_variable('prefix', ("--prefix",),
                         dict(help="prefix to use in the dummy pid values"),
                         default="_local")
    sub_cfg.add_variable('force', ("--force",),
                         dict(help="do it even if there is the risk of "
                              "creating new conflicts"),
                         default=False, type=icat.config.flag)

# ============================= dedup ================================
# The dedup subcommand: deduplicate pid values.

def cmd_dedup(client, conf):
    num_dedup_pid = 0
    for pid in list(find_duplicate_pids(client)):
        have_warning = False
        for id, pid2 in find_potential_dedup_conflicts(client, pid):
            logger.warning("potentially conflicting pid value '%s' "
                           "in Sample %d", pid2, id)
            have_warning = True
        if have_warning:
            if conf.force:
                logger.warning("potential conflicts detected, "
                               "proceeding with dedup '%s' anyway with force",
                               pid)
            else:
                logger.warning("potential conflicts detected, "
                               "won't proceed with dedup '%s' without force",
                               pid)
                continue
        count = 0
        query = Query(client, "Sample", conditions={
            "pid": "= '%s'" % pid
        }, order=["id"], includes="1")
        for sample in searchChunkedNoSkip(client, query):
            sample.pid = "%s/dedup-%03d" % (pid, count)
            sample.update()
            count += 1
        num_dedup_pid += 1
    logger.info("%d pid values deduplicated", num_dedup_pid)

def cfg_dedup(subcmd):
    help_string = "deduplicate pid values"
    sub_cfg = subcmd.add_subconfig("dedup",
                                   dict(help=help_string),
                                   func=cmd_dedup)
    sub_cfg.add_variable('force', ("--force",),
                         dict(help="do it even if there is the risk of "
                              "creating new conflicts"),
                         default=False, type=icat.config.flag)

# ============================== main ================================

if __name__ == '__main__':
    logger.log(logging.NOTICE,
               "this scripts needs to be run by a user "
               "having read access to all samples!")
    config = icat.config.Config(ids=False)
    subcmd = config.add_subcommands()
    cfg_stats(subcmd)
    cfg_lsdup(subcmd)
    cfg_setpids(subcmd)
    cfg_dedup(subcmd)
    client, conf = config.getconfig()
    client.login(conf.auth, conf.credentials)
    conf.subcmd.func(client, conf)
