#! python
"""Check Samples and help upgrading icat.server to 7.0

This script is supposed to run various checks and maintenance tasks on
Samples in an ICAT server.  It is mostly aimed at assisting an upgrade
of icat.server to version 7.0 which requires the Sample.pid attribute
to be populated with unique non-null values.
"""

import logging
import re
import icat
import icat.config
from icat.query import Query

logging.NOTICE = logging.INFO + 5
logging.addLevelName(logging.NOTICE, "NOTICE")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ============================= helper ===============================

def get_pid_prefixes(client):
    prefixes = set()
    query = Query(client, "Sample", conditions={
        "pid": "LIKE '%:%'"
    }, attributes=["pid"], aggregate="DISTINCT")
    for pid in client.searchChunked(query):
        p, _ = pid.split(':', maxsplit=1)
        prefixes.add(p)
    return prefixes

def find_potential_upgrade_conflicts(client, prefix):
    auto_pid_re = re.compile("%s:\d+" % prefix)
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
        query = Query(client, "Sample", conditions={
            "pid": "= '%s'" % pid
        }, order=["id"], includes=["investigation", "type"])
        dup_list = ""
        for sample in client.searchChunked(query):
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

# ============================== main ================================

if __name__ == '__main__':
    logger.log(logging.NOTICE,
               "this scripts needs to be run by a user "
               "having read access to all samples!")
    config = icat.config.Config(ids=False)
    subcmd = config.add_subcommands()
    cfg_stats(subcmd)
    cfg_lsdup(subcmd)
    client, conf = config.getconfig()
    client.login(conf.auth, conf.credentials)
    conf.subcmd.func(client, conf)
