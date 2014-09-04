#!/usr/bin/env python
from jinja2 import TemplateError
def assertion( actual, asserted, name ):
    """A Jinja2 custom filter.
    """
    # raises assertion error:
    try:
        assert( actual == asserted )
    except:
        raise TemplateError, "Assertion Error: " + name + " " + str(actual) + " != " + str(asserted)
    else:
        pass

