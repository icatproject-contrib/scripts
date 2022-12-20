#! /usr/bin/python3
"""Populate the ICAT Technique table from PaNET.

This script reads the PaNET ontology as an OWL file and populates the
ICAT Techniques table, creating one entry for each term in PaNET
(excluding the generic root PaNET00001).  The pid attribute is set to
"PaNET:PaNETxxxxx", assuming it is well known that the "PaNET" name
space stands for http://purl.org/pan-science/PaNET/.

Beside the standard python-icat command line arguments, this script
takes one positional argument: the name of an PaNET OWL file.  You can
generate such a file from the `PaNET build`_ environment by Paul
Millar.

This script requires `python-icat`_ and `rdflib`_.

.. _PaNET build: https://gitlab.desy.de/paul.millar/panet-build
.. _python-icat: https://github.com/icatproject/python-icat
.. _rdflib: https://github.com/RDFLib/rdflib
"""

import logging

import rdflib
from rdflib.namespace import RDFS

import icat
import icat.config
from icat.query import Query

PaNET = rdflib.Namespace("http://purl.org/pan-science/PaNET/")
OBO = rdflib.Namespace("http://purl.obolibrary.org/obo/")

logging.basicConfig(level=logging.INFO)
# Suds logging is somewhat too chatty
logging.getLogger('suds.client').setLevel(logging.CRITICAL)
logging.getLogger('suds').setLevel(logging.ERROR)
log = logging.getLogger(__name__)

config = icat.config.Config(ids=False)
config.add_variable('panet', ("panet",), 
                    dict(metavar="PaNET.owl", help="PaNET OWL file"))
client, conf = config.getconfig()
client.login(conf.auth, conf.credentials)

g = rdflib.Graph()
g.parse(conf.panet, format='application/rdf+xml')

def get_technique_names(g):
    """Get PaNET techniques with their labels.
    """
    query = """
    SELECT ?p ?n
    WHERE {
        ?p rdfs:subClassOf panet:PaNET00001.
        ?p rdfs:label ?n.
    }
    """
    qres = g.query(query, initNs={'panet': PaNET})
    for p, n in qres:
        if not p.startswith(str(PaNET)):
            continue
        yield "PaNET:%s" % str(p)[len(str(PaNET)):], str(n)

def get_technique_descriptions(g):
    """Get PaNET techniques with their descriptions.

    Note: this generator yields only the PaNET terms having a
    description defined.  This is only a subset.
    """
    query = """
    SELECT ?p ?d
    WHERE {
        ?p rdfs:subClassOf panet:PaNET00001.
        ?p obo:IAO_0000115 ?d.
    }
    """
    qres = g.query(query, initNs={'panet': PaNET, 'obo': OBO})
    for p, d in qres:
        if not p.startswith(str(PaNET)):
            continue
        yield "PaNET:%s" % str(p)[len(str(PaNET)):], str(d)

# Note: unfortunately, my SPARQL skills are limited.  I didn't found a
# way to query names and descriptions from the graph in a single query
# such that I get all the PaNET terms in the result, including those
# that do not have a description.  Therefore I do two sweeps: first
# only query names and PIDs that will return all the PaNET terms and
# in a second sweep query for the descriptions that will only return
# those that have one.  All PaNET will be created in ICAT in the first
# sweep.  The second sweep will then update the description attribute
# of some of the entries created before.

for p, n in get_technique_names(g):
    try:
        technique = client.new("technique", pid=p, name=n)
        technique.create()
        log.info("Technique %s created", p)
    except icat.ICATObjectExistsError:
        technique = client.searchMatching(technique)
        if technique.pid != p or technique.name != n:
            log.error("Conflict in Technique name / pid: "
                      "(%s / %s) versus (%s / %s)",
                      n, p, technique.name, technique.pid)

for p, d in get_technique_descriptions(g):
    # We assume all techniques to be already created in the last loop.
    try:
        query = Query(client, "Technique", conditions={"pid": "= '%s'" % p})
        technique = client.assertedSearch(query)[0]
    except icat.SearchResultError as e:
        log.error(e)
    else:
        if technique.description != d:
            technique.description = d
            technique.update()
