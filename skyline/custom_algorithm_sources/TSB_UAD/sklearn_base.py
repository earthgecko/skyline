"""Utility function copied over from sklearn/base.py
"""

from __future__ import division
from __future__ import print_function

import six
import numpy as np


def _pprint(params, offset=0, printer=repr):
    # noinspection PyPep8
    """Pretty print the dictionary 'params'
    See http://scikit-learn.org/stable/modules/generated/sklearn.base.BaseEstimator.html
    and sklearn/base.py for more information.
    :param params: The dictionary to pretty print
    :type params: dict
    :param offset: The offset in characters to add at the begin of each line.
    :type offset: int
    :param printer: The function to convert entries to strings, typically
        the builtin str or repr
    :type printer: callable
    :return: None
    """

    # Do a multi-line justified repr:
    options = np.get_printoptions()
    np.set_printoptions(precision=5, threshold=64, edgeitems=2)
    params_list = list()
    this_line_length = offset
    line_sep = ',\n' + (1 + offset // 2) * ' '
    for i, (k, v) in enumerate(sorted(six.iteritems(params))):
        if type(v) is float:
            # use str for representing floating point numbers
            # this way we get consistent representation across
            # architectures and versions.
            this_repr = '%s=%s' % (k, str(v))
        else:
            # use repr of the rest
            this_repr = '%s=%s' % (k, printer(v))
        if len(this_repr) > 500:
            this_repr = this_repr[:300] + '...' + this_repr[-100:]
        if i > 0:
            if this_line_length + len(this_repr) >= 75 or '\n' in this_repr:
                params_list.append(line_sep)
                this_line_length = len(line_sep)
            else:
                params_list.append(', ')
                this_line_length += 2
        params_list.append(this_repr)
        this_line_length += len(this_repr)

    np.set_printoptions(**options)
    lines = ''.join(params_list)
    # Strip trailing space to avoid nightmare in doctests
    lines = '\n'.join(l.rstrip(' ') for l in lines.split('\n'))
    return lines