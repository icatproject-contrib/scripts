"""A collection of helper scripts for ICAT

This package intends to provide an unsorted collection of helper
scripts that may be useful in the administration of an `ICAT`_
service.

.. _ICAT: https://icatproject.org/
"""

from distutils.core import setup

doclines = __doc__.strip().split("\n")

setup(
    name = "contrib-scripts",
    description = doclines[0],
    long_description = "\n".join(doclines[2:]),
    url = "https://github.com/icatproject-contrib/scripts",
    license = "Apache-2.0",
    scripts = [
        "scripts/check-sizes.py",
        "scripts/test-schema-sizes-triggers.py",
    ]
)

