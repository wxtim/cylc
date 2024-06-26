"""Common functionality for Cylc Lint.
"""

from dataclasses import dataclass


STYLE_GUIDE = (
    'https://cylc.github.io/cylc-doc/stable/html/workflow-design-guide/'
    'style-guide.html#'
)


@dataclass
class Check:
    """Template class for Cylc lint check objects.

    Args:
        short: Description to return on the command line.
        rst: Fuller description for documentation.
        url: URL for documentation.
        kwargs: If true will ... TODO?
        evaluate commented lines: Will not skip lines starting with "#"
        purpose: A short string describing the type of check.
    """
    short: str = None
    rst: str = None
    url: str = ''
    kwargs: bool = False
    evaluate_commented_lines: bool = False
    purpose: str = None

    def check(line):
        raise NotImplementedError('This should be overridden')

    @classmethod
    def get_summary(cls, rst: bool = False):
        """Summary for use in printing either on CLI or to RST."""
        if rst:
            if cls.rst:
                return cls.rst
            return cls.short
        return self.short.replace('``', '')

    @classmethod
    def get_index_str(cls, index: str) -> str:
        """Printable purpose string - mask useless numbers for auto-generated
        upgrades."""
        # TODO - reimplement
        # if cks.get('is_dep', None):
        #     return 'U998'
        # elif meta.get('is_obs', None):
        #     return 'U999'
        # else:
        return f'{index}'

