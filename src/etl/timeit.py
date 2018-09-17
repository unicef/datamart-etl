# -*- coding: utf-8 -*-
from __future__ import absolute_import

import contextlib
import cProfile
import io
import pstats
import time


@contextlib.contextmanager
def profiled():
    pr = cProfile.Profile()
    pr.enable()
    s = io.StringIO()
    yield s
    pr.disable()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats()
    # uncomment this to see who's calling what
    # ps.print_callers()
    # return s.getvalue()


class Timer(object):
    """
    >>> with Timer() as t:
    ...     time.sleep(1)
    >>> t.humanized
    '00:00:01'
    >>> t.elapsed
    1
    >>> t.echo() # doctest: +ELLIPSIS
    'Process ended in 00:00:01. Running from ... to ... for 1 seconds'

    """

    def __init__(self,
                 message="Process ended in {humanized}. Running from {started_at} to {ended_at} for {elapsed} seconds"):
        self.__start = time.time()
        self.__end = self.__start
        self.__elapsed = 0
        self.__last_step = self.__start
        self.message = message
        self.bookmarks = {}

    def __enter__(self):
        self.__start = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__end = time.time()
        self.__elapsed = int(self.__end - self.__start)
        # if exc_type:
        #     raise Exception from exc_val

    @property
    def startedAt(self):
        return time.strftime("%H:%M:%S", time.gmtime(self.__start))

    @property
    def completeAt(self):
        return time.strftime("%H:%M:%S", time.gmtime(self.__end))

    @property
    def elapsed(self):
        return self.__elapsed

    @property
    def partial(self):
        return int(time.time() - self.__start)

    @property
    def step(self):
        step = time.time() - self.__last_step
        self.__last_step = time.time()
        return step

    def fmt(self, v):
        h = int(v / (60 * 60))
        m = int((v % (60 * 60)) / 60)
        s = int(v % 60)
        return "{0:>02}:{1:>02}:{2:>02}".format(h, m, s)

    @property
    def humanized(self):
        return self.fmt(self.__elapsed)

    def bookmark(self, name):
        self.bookmarks[name] = self.partial

    def echo(self, **kwargs):
        return self.message.format(humanized=self.humanized,
                                   start=self.__start,
                                   end=self.__end,
                                   elapsed=self.__elapsed,
                                   started_at=time.strftime("%H:%M:%S", time.gmtime(self.__start)),
                                   ended_at=time.strftime("%H:%M:%S", time.gmtime(self.__end)),
                                   **kwargs
                                   )
