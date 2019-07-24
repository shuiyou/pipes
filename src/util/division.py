from __future__ import unicode_literals

import glob
import json
import os
import weakref

LATEST_YEAR = 2019
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def gb2260_files_dict():
    file_path = os.path.join(base_dir, "util", "GB2260", "*.json")
    files = sorted(glob.glob(file_path), reverse=True)
    file_dict = {}
    for gb in files:
        file_dict[int(gb[-9:-5])] = gb
    return file_dict


def read_data():
    files_dict = gb2260_files_dict()
    result = {}
    for k, v in files_dict.items():
        store = {}
        with open(v) as f:
            value = json.load(f, encoding='utf-8')
            for d_v in value:
                store[int(d_v['code'])] = d_v['name']
        result[k] = store
    return result


data = read_data()
LATEST_YEAR = 2019


class Division(object):
    """The administrative division."""

    _identity_map = dict(
        (year, weakref.WeakValueDictionary()) for year in data)

    def __init__(self, code, name, year=None):
        self.code = str(code)
        self.name = str(name)
        self.year = year

    def __repr__(self):
        if self.year is None:
            return 'gb2260.get(%r)' % self.code
        else:
            return 'gb2260.get(%r, %r)' % (self.code, self.year)

    def __str__(self):
        name = 'GB2260' if self.year is None else 'GB2260-%d' % self.year
        humanize_name = '/'.join(x.name for x in self.stack())
        return '<%s %s %s>' % (name, self.code, humanize_name)

    def __hash__(self):
        return hash((self.__class__, self.code, self.year))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.code == other.code and self.year == other.year

    @classmethod
    def get(cls, code, year=None):
        """Gets an administrative division by its code.

        :param code: The division code.
        :param year: The year of revision.
        :returns: A :class:`gb2260.Division` object.
        """
        key = int(code)
        if year and year not in data:
            raise ValueError('year must be in %r' % list(data))

        cache = cls._identity_map[year]
        store = data[year]

        if key in cache:
            return cache[key]
        if key in store:
            instance = cls(code, store[key], year)
            cache[key] = instance
            return instance

        raise ValueError('%r is not valid division code' % code)

    @classmethod
    def search(cls, code):
        """Searches administrative division by its code in all revision.

        :param code: The division code.
        :returns: A :class:`gb2260.Division` object or ``None``.
        """
        # sorts from latest to oldest, and ``None`` means latest
        key = int(code)
        pairs = sorted(
            data.items(), reverse=True,
            key=lambda pair: make_year_key(pair[0]))
        for year, store in pairs:
            if key in store:
                return cls.get(key, year=year)

    @property
    def province(self):
        return self.get(self.code[:2] + '0000', self.year)

    @property
    def is_province(self):
        return self.province == self

    @property
    def prefecture(self):
        if self.is_province:
            return
        return self.get(self.code[:4] + '00', self.year)

    @property
    def is_prefecture(self):
        return self.prefecture == self

    @property
    def county(self):
        if self.is_province or self.is_prefecture:
            return
        return self

    @property
    def is_county(self):
        return self.county is not None

    def stack(self):
        yield self.province
        if self.is_prefecture or self.is_county:
            yield self.prefecture
        if self.is_county:
            yield self


def make_year_key(year):
    """A key generator for sorting years."""
    if year is None:
        return (LATEST_YEAR, 12)
    year = str(year)
    if len(year) == 4:
        return (int(year), 12)
    if len(year) == 6:
        return (int(year[:4]), int(year[4:]))
    raise ValueError('invalid year %s' % year)
