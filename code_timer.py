
import time
import csv
import sys
import os
import threading
import collections
import json
import functools
import inspect


def interleave(*args):
    remaining = map(iter, args)
    while remaining:
        new_remaining = []
        for stream in remaining:
            try:
                yield stream.next()
                new_remaining.append(stream)
            except StopIteration:
                pass
        remaining = new_remaining

class _Guard(object):
    def __init__(self, timer, name, print_this = False, print_below = False):
        self._timer = timer
        self._name = name
        self._print_this = print_this
        self._print_below = print_below

    def __enter__(self):
        self._timer.start(self._name, self._print_below)

    def __exit__(self, exc_type, exc_value, traceback):
        self._timer.end(self._name, self._print_this, self._print_below)

class DummyTimer(object):
    def __init__(self, *args, **kwargs):
        pass
    @classmethod
    def __enter__(cls):
        return cls
    @classmethod
    def __exit__(cls, exc_type, exc_value, traceback):
        pass
    @classmethod
    def record_misc_data(cls, name, data):
        pass
    @classmethod
    def start(cls, name, print_below = False):
        pass
    @classmethod
    def end(cls, name, print_this = False, print_below = False):
        pass
    @classmethod
    def record(cls, name, print_this = False):
        return _Guard(cls, name)
    @classmethod
    def write(cls):
        pass


class CodeTimer(object):
    _file_lock = threading.Lock()

    def __init__(self, filename = None, do_prints = False, print_hierarchy = False, min_seconds_for_log = None, data_func = None):
        self.times = {}
        self._intervals = {}
        self._filename = filename
        self._fields = []
        self._prints_below = collections.Counter()
        self.misc_fields = {}
        self._orig_do_prints = do_prints
        self._do_prints = do_prints
        self._print_hierarchy = print_hierarchy
        self._data_func = data_func
        if self._data_func is None:
            self._data_func = time.time
        self.graph = collections.defaultdict(list)
        self.start('top-level')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.write()


    def _check_print_below(self):
        if sum(self._prints_below.values()) > 0:
            self._do_prints = True
        else:
            self._do_prints = self._orig_do_prints

    def record_misc_data(self, name, data):
        if name not in self.misc_fields:
            self._fields.append(name)
        self.misc_fields[name] = data
        if self._do_prints:
            print 'misc data', name, data
        for field_name in reversed(self._fields):
            if field_name not in self._intervals:
                continue
            self.graph[field_name].append(name)
            break

    def start(self, name, print_below = False):
        if print_below:
            self._prints_below[name] += 1

        self._check_print_below()

        #for parent_name in self._intervals:
        for field_name in reversed(self._fields):
            if field_name not in self._intervals:
                continue
            self.graph[field_name].append(name)
            break
        if name not in self.times:
            self._fields.append(name)
        self._intervals[name] = self._data_func()
        if self._do_prints:
            print 'starting', name

    def end(self, name, print_this = False, print_below = False):
        self.times[name] = self.times.get(name, 0) + (self._data_func() - self._intervals[name])
        del self._intervals[name]
        if self._do_prints or print_this:
            print 'ending', name, self.times[name]

        if print_below:
            self._prints_below[name] -= 1

        self._check_print_below()

    def record(self, name, print_this = False, print_below = False):
        return _Guard(self, name, print_this, print_below)

    def print_all(self):
        for field in self._fields:
            t = self.times.get(field)
            if t is None:
                continue
            print field, t

    def write(self):
        self.end('top-level')
        if self._print_hierarchy:
            for field in self._fields:
                if self.graph[field]:
                    print field, ':'
                    print '\t', ', '.join(self.graph[field])
                    print
        if not self._filename:
            return
        if os.path.exists(self._filename):
            add_header = False
            mode = 'ab'
        else:
            add_header = True
            mode = 'wb'

        row = {}
        row.update(self.times)
        row.update(self.misc_fields)

        fields = self._fields

        added_fields = [f + ' __' for f in fields]
        row.update({f: f[:-3] for f in added_fields})
        fields = list(interleave(added_fields, fields))

        with CodeTimer._file_lock:
            with open(self._filename, mode) as f:
                w = csv.DictWriter(f, fields)
                if add_header:
                    w.writeheader()
                w.writerow(row)


class RepeatingCodeTimer(object):
    _file_lock = threading.Lock()
    def __init__(self, filename = None, do_prints = False, print_hierarchy = False, min_seconds_for_log = None):
        self._filename = filename
        self._do_prints = do_prints
        self._events = []
        self._starts = collections.defaultdict(list)
        self._min_seconds_for_log = min_seconds_for_log

        self.start('top-level')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.write()

    def record(self, name, print_this = False):
        return _Guard(self, name, print_this)

    def record_misc_data(self, name, data):
        self._events.append((name, data))
        if self._do_prints:
            print name, data

    def start(self, name, print_below = False):
        count = len(self._starts[name])
        event = ['START %s_%d' % (name, count), time.time()]
        self._starts[name].append(event)
        self._events.append(event)
        if self._do_prints:
            print self._events[-1][0]

    def end(self, name, print_this = False, print_below = False):
        start_event = self._starts[name][-1]
        start_time = start_event[1]
        del self._starts[name][-1]
        count = len(self._starts[name])
        diff = time.time() - start_time
        start_event[1] = diff
        self._events.append(('END %s_%d' % (name, count), diff))
        if self._do_prints or print_this:
            print self._events[-1][0], self._events[-1][1]

    def write(self):
        self.end('top-level')
        end_event = self._events[-1]
        total_time = end_event[1]
        if self._min_seconds_for_log is not None and total_time < self._min_seconds_for_log:
            return

        if not self._filename:
            return
        if os.path.exists(self._filename):
            mode = 'ab'
        else:
            mode = 'wb'

        with RepeatingCodeTimer._file_lock:
            with open(self._filename, mode) as f:
                row = []
                writer = csv.writer(f)
                for event_name, event_value in self._events:
                    row.append(event_name)
                    if (event_name.startswith('END') or event_name.startswith('START')) and isinstance(event_value, float):
                        row.append('%.3f' % event_value)
                    elif event_value:
                        row.append(event_value)
                    else:
                        row.append('')
                writer.writerow(row)


def _get_default_values(argspec):

    if not argspec.defaults:
        return {}
    result = {
        argspec.args[len(argspec.args) - len(argspec.defaults) + i]: default
        for i, default in
        enumerate(argspec.defaults)
    }
    return result

def record_times(func):
    """This is a decorator for methods or functions. One of the keyword arguments should be a timer, otherwise the first argument should be self, and self._timer should a CodeTimer instance"""
    argspec = inspect.getargspec(func)
    default_values = _get_default_values(argspec)
    @functools.wraps(func)
    def func_wrapper(*args, **kwargs):
        name = func.func_name
        timer = None
        if 'timer' in kwargs:
            timer = kwargs['timer']
        else:
            if 'timer' in default_values:
                timer = default_values['timer']
        if not timer:
            raise Exception('no timer!')
        with timer.record(name):
            return func(*args, **kwargs)

    @functools.wraps(func)
    def method_wrapper(self, *args, **kwargs):
        timer = self._timer
        name = self.__class__.__name__ + '.' + func.func_name
        with timer.record(name):
            return func(self, *args, **kwargs)

    if 'timer' in argspec.args:
        return func_wrapper

    return method_wrapper
