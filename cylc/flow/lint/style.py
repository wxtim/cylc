"""Style Checks to live here"""

from cylc.flow.lint.common import STYLE_GUIDE, Check


class S001(Check):
    """Use multiple spaces, not tabs."""
    def __init__(self):
        super.__init__(
            short = self.__doc__,
            url=STYLE_GUIDE + 'tab-characters'
        )
        self.check = re.compile(r'^\t').findall


CHECKS = {
    'S001': S001,
}

