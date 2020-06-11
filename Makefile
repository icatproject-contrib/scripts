PYTHON   = python3
BUILDLIB = $(CURDIR)/build/lib


build:
	$(PYTHON) setup.py build

sdist:
	$(PYTHON) setup.py sdist

clean:
	rm -rf build

distclean: clean
	rm -f MANIFEST
	rm -rf dist


.PHONY: build sdist clean distclean
