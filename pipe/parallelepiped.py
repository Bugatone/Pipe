__all__ = ['Parallelepiped']

from itertools import chain
from multiprocessing.dummy import Pool


def identity(iterable):
    return iterable


# `map` and `imap` take a function of type x -> y and return a function of type Iterable(x) -> Iterable(y).
# Conversely, `demap` turns a function Iterable(x) -> Iterable(y) to a function x -> Iterable(y).
def demap(mapping):
    def demapped(x):
        return mapping([x])

    return demapped


def map_conjugate(mapper, mapping):
    def conjugated(iterable):
        return chain.from_iterable(mapper(func=demap(mapping),
                                          iterable=iterable))

    return conjugated


class Parallelepiped(object):
    """
    A pipeable parallelization tool.

    `Parallelpiped() | a | b` is a Pipe-compliant object that is equivalent to
    `a | b` but distributes the computation over the inputs.
    """
    def __init__(self, pool=None, function=None, ordered=True):
        """
        :param None|Pool pool: if not specified, a new Pool is created.
        :param function: leave empty
        :param bool ordered: if False, the LIFO order is not guaranteed, but
        there may be performance benefits.
        """
        if pool is None:
            pool = Pool()
        self.pool = pool
        if function is None:
            function = identity
        self.function = function
        self.ordered = ordered

    def __or__(self, rhs):
        """
        Add another segment of pipe to be multi-processed.

        :param pipe.Pipe rhs:
        :rtype: Parallelepiped
        """
        return Parallelepiped(pool=self.pool,
                              function=lambda iterable: (self.function(iterable) | rhs),
                              ordered=self.ordered)

    def __ror__(self, lhs):
        """
        Apply the pipe segment to a given iterable.

        :param collections.Iterable lhs:
        :rtype: collections.Iterable
        """
        mapper = (self.pool.imap
                  if self.ordered else
                  self.pool.imap_unordered)

        return map_conjugate(mapper, self.function)(lhs)
