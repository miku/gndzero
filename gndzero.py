#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function

from colorama import Fore, Back, Style
from luigi.task import flatten
import collections
import datetime
import itertools
import luigi
import os
import pandas as pd
import random
import re
import slugify
import sqlite3
import string
import subprocess
import sys
import tempfile
import urllib

import config

tempfile.tempdir = config.TEMPDIR
HOME = config.HOME


#
# various utils, maybe put them into some other file
#
def dim(text):
    return Back.WHITE + Fore.BLACK + text + Fore.RESET + Back.RESET

def green(text):
    return Fore.GREEN + text + Fore.RESET

def red(text):
    return Fore.RED + text + Fore.RESET

def yellow(text):
    return Fore.YELLOW + text + Fore.RESET

def cyan(text):
    return Fore.CYAN + text + Fore.RESET

def magenta(text):
    return Fore.MAGENTA + text + Fore.RESET


def convert(name):
    """
    Convert CamelCase to underscore, http://stackoverflow.com/a/1176023/89391.
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1-\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1-\2', s1).lower()


def which(program):
    """
    Return `None` if no executable can be found.
    """
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None


def shellout(template, **kwargs):
    """
    Takes a shell command template and executes it. The template must use
    the new string format mini language. `kwargs` must consist of any defined
    placeholders, only `output` is optional.
    Raises RuntimeError on nonzero exit codes.

    Simple template:

        wc -l < {input} > {output}

    Quoted curly braces:

        ps ax|awk '{{print $1}}' > {output}

    Usage with luigi:

        ...
        tmp = shellout('wc -l < {input} > {output}', input=self.input().fn)
        luigi.File(tmp).move(self.output.fn())
        ....

    """
    kwargs.setdefault('output', random_tmp_path())
    stopover = kwargs.get('output')
    command = template.format(**kwargs)
    print(cyan(command), file=sys.stderr)
    code = subprocess.call([command], shell=True)
    if not code == 0:
        raise RuntimeError('%s exitcode: %s' % (command, code))
    return stopover


def random_string(length=16):
    """
    Return a random string (upper and lowercase letters) of length `length`,
    defaults to 16.
    """
    return ''.join(random.choice(string.letters) for _ in range(length))


def random_tmp_path():
    """
    Return a random path, that is located under the system's tmp dir. This
    is just a path, nothing gets touched or created.
    """
    return os.path.join(tempfile.gettempdir(), 'gndzero-%s' % random_string())


def split(iterable, n):
    """
    Generalized `pairwise`. Split an iterable after every `n` items.
    """
    i = iter(iterable)
    piece = tuple(itertools.islice(i, n))
    while piece:
        yield piece
        piece = tuple(itertools.islice(i, n))


class dbopen(object):
    """
    Simple context manager for sqlite3 databases. Commits everything at exit.

        with dbopen('/tmp/test.db') as cursor:
            query = cursor.execute('SELECT * FROM items')
            result = query.fetchall()
            ...
    """
    def __init__(self, path):
        self.path = path
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.path)
        self.conn.text_factory = str
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_class, exc, traceback):
        self.conn.commit()
        self.conn.close()


class DefaultTask(luigi.Task):
    """
    A default class for projects. Expects a TAG (e.g. SOURCE_ID) on the class,
    that gets turned into a instance attribute by the 
    `luigi.Task` __metaclass__.
    """
    TAG = NotImplemented

    def parameter_set(self):
        """
        Return the parameters names as set.
        """
        params = set()
        for k, v in self.__class__.__dict__.iteritems():
            if isinstance(v, luigi.Parameter):
                params.add(k)
        return params

    def fingerprint(self, default='artefact'):
        """
        The fingerprint of a task is a string consisting of the names
        and values of the parametes.
        """
        parts = ['%s-%s' % (p, slugify.slugify(unicode(getattr(self, p)))) 
                 for p in self.parameter_set()]
        fingerprint = '-'.join(parts)
        if len(fingerprint) == 0:
            fingerprint = default
        return fingerprint


    def path(self, filename=None, ext='tsv'):
        """ 
        Autogenerate a path based on some category (those are only
        conventions), the tag (source id) and the name of the class and a given
        extension.
        """
        if self.TAG == NotImplemented:
            raise ValueError('(Base)class must set TAG (source id).')

        klassname = convert(self.__class__.__name__)

        if filename is None:
            filename = '%s.%s' % (self.fingerprint(), ext)
        return os.path.join(HOME, str(self.TAG), klassname, filename)


class GNDTask(DefaultTask):
    TAG = 'gndzero'

    def latest(self):
        """ Adjust this, if updates should be done regularly. """
        return datetime.date(2013, 11, 8)


class Executable(luigi.Task):
    """ Checks, whether an external executable is available. """

    name = luigi.Parameter()
    msg = luigi.Parameter(default='')

    def run(self):
        """ Just complain explicitly about missing program."""
        if not which(self.name):
            raise Exception('%s required. %s' % (self.name, self.msg))

    def complete(self):
        return which(self.name) is not None


class VIAFDump(GNDTask):
    """ Download a VIAF Dump. """

    def requires(self):
        return Executable(name='wget')

    def run(self):
        url = "http://viaf.org/viaf/data/viaf-20131014-links.txt.gz"
        output = shellout("""wget --retry-connrefused {url} -O {output}""", url=url)
        luigi.File(output).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.txt.gz'.format(
                                                date=self.latest())))


class GNDDump(GNDTask):
    """ Download GND task. """

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Executable(name='wget')

    def run(self):
        server = "datendienst.dnb.de"
        path = "/cgi-bin/mabit.pl"
        params = urllib.urlencode({
            'cmd': 'fetch',
            'userID':'opendata',
            'pass': 'opendata',
            'mabheft': 'GND.rdf.gz'
        })
        url = "http://{server}{path}?{params}".format(server=server, path=path,
                                                      params=params)
        output = shellout("""wget --retry-connrefused "{url}" -O {output}""", url=url)
        luigi.File(output).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.rdf.gz'.format(
                                                date=self.latest())))


class GNDExtract(GNDTask):
    """ Extract the archive. """

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GNDDump(date=self.date)

    def run(self):
        output = shellout("gunzip -c {input} > {output}", input=self.input().fn)
        luigi.File(output).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.rdf'.format(
                                                date=self.latest())))


class SqliteDB(GNDTask):
    """ Turn the dump into a (id, content) sqlite3 db.
    This artefact will be used by the cache server.
    """

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GNDExtract(date=self.date)

    def run(self):
        stopover = random_tmp_path()
        pattern = re.compile("""rdf:about="http://d-nb.info/gnd/([0-9X-]+)">""")

        with dbopen(stopover) as cursor:
            cursor.execute("""CREATE TABLE gnd 
                              (id text  PRIMARY KEY, content blob)""")
            cursor.execute("""CREATE INDEX IF NOT EXISTS
                              idx_gnd_id ON gnd (id)""")

            with self.input().open() as handle:
                groups = itertools.groupby(handle, key=str.isspace)
                for i, (k, lines) in enumerate(groups):
                    if k:
                        continue
                    lines = map(string.strip, list(lines))
                    match = pattern.search(lines[0])
                    if match:
                        row = (match.group(1), '\n'.join(lines))
                        cursor.execute("INSERT INTO gnd VALUES (?, ?)", row)

        luigi.File(path=stopover).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.db'.format(
                                                date=self.latest())))


class SameAs(GNDTask):
    """
    Extract owl:sameAs relationships from extracted dump.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GNDExtract(date=self.date)

    def run(self):
        """ Example link to VIAF:
        <owl:sameAs rdf:resource="http://viaf.org/viaf/22508163" /> """

        link_pattern = re.compile(
            """<owl:sameAs rdf:resource="([^"]+)" />""", 24)
        id_pattern = re.compile(
            """rdf:about="http://d-nb.info/gnd/([0-9X-]+)">""")

        with self.input().open() as handle:
            with self.output().open('w') as output:
                groups = itertools.groupby(handle, key=str.isspace)
                for i, (k, lines) in enumerate(groups):
                    if k:
                        continue
                    lines = map(string.strip, list(lines))

                    match = id_pattern.search(lines[0])
                    if match:
                        row = (match.group(1), '\n'.join(lines))

                    matches = re.finditer(link_pattern, '\n'.join(lines))
                    for match in matches:
                        output.write('%s\t%s\n' % (row[0], match.group(1)))

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.tsv'.format(
                                                date=self.latest())))


class Successor(GNDTask):
    """
    Store all outbound edges for a GND in a two column table.
    This takes (toooo) long: 495m12.706s with a single process.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return SqliteDB(date=self.date)

    def run(self):
        pattern = re.compile("""http://d-nb.info/gnd/([0-9X-]+)""")

        # fetch all gnds
        idset = set()
        with dbopen(self.input().fn) as cursor:
            cursor.execute("SELECT id FROM gnd")
            rows = cursor.fetchall()
            for row in rows:
                idset.add(row[0])

        total, done = len(idset), 0
        with dbopen(self.input().fn) as cursor:
            with self.output().open('w') as output:
                for batch in split(idset, 1000):
                    print('{done}/{total}'.format(done=done, total=total))
                    cursor.execute("""SELECT id, content FROM gnd
                                      WHERE id IN (%s) """ % (
                            ','.join([ "'%s'" % id for id in tuple(batch)])))
                    rows = cursor.fetchall()
                    for row in rows:
                        id, content = row
                        for match in pattern.finditer(content, 24):
                            output.write('%s\t%s\n' % (id, match.group(1)))
                        done += 1

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.tsv'.format(
                                                date=self.latest())))


class SuccessorDB(GNDTask):
    """
    Store the successor relationships in an sqlite3 database.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Successor(date=self.date)

    def run(self):
        stopover = random_tmp_path()

        with self.input().open() as handle:
            with dbopen(stopover) as cursor:
                cursor.execute("""CREATE TABLE IF NOT EXISTS successor (id text,
                    successor text, PRIMARY KEY (id, successor))""")

                for line in handle:
                    id, successor = line.strip().split()
                    if id == successor:
                        continue
                    cursor.execute("INSERT INTO successor VALUES (?, ?)",
                                   (id, successor))

                cursor.execute("""CREATE INDEX IF NOT EXISTS
                                  idx_successor_id ON successor (id)""")
                cursor.execute("""CREATE INDEX IF NOT EXISTS
                                  idx_successor_successor
                                  ON successor (successor)""")

        luigi.File(stopover).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.db'.format(
                                                date=self.latest())))


class Reach(GNDTask):
    """
    Compute the reach (convex hull) of all GNDs. Dump a two column file with
    id and size of the hull. Do all in memory (about 4-5G required).

    Takes too long, too: 121m17.535s
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Successor(date=self.date)

    def run(self):
        lookup = collections.defaultdict(set)
        with self.input().open() as handle:
            for line in handle:
                id, successor = line.strip().split()
                lookup[id].add(successor)

        with self.output().open('w') as output:
            for i, id in enumerate(lookup.iterkeys()):
                queue, hull = set([id]), set()
                while True:
                    if len(queue) == 0:
                        break
                    current = queue.pop()
                    hull.add(current)
                    for successor in lookup.get(current, []):
                        if not successor in hull:
                            queue.add(successor)
                output.write('%s\t%s\n' % (id, len(hull)))

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.tsv'.format(
                                                date=self.latest())))


class TranslationMap(GNDTask):
    """
    Translate the GND to sequential ids to be used with matrix
    calculations, e.g. pagerank.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return SqliteDB(date=self.date)

    def run(self):
        sequential_id = 0
        with dbopen(self.input().fn) as cursor:
            cursor.execute("SELECT DISTINCT id FROM gnd")
            rows = cursor.fetchall()
            with self.output().open('w') as output:
                for row in rows:
                    output.write('%s\t%s\n' % (row[0], sequential_id))
                    sequential_id += 1

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.tsv'.format(
                                                date=self.latest())))


class TranslatedSuccessor(GNDTask):
    """
    Translate the successor list into the integer domain.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'data': Successor(date=self.date),
            'map': TranslationMap(date=self.date),
        }

    def run(self):
        mapping = {}
        with self.input().get('map').open() as handle:
            for line in handle:
                id, intid = line.strip().split()
                mapping[id] = intid

        misses = set()
        with self.input().get('data').open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    id, successor = line.strip().split()
                    if id == successor:
                        continue
                    try:
                        output.write('%s\t%s\n' % (mapping[id],
                                                   mapping[successor]))
                    except KeyError as err:
                        # TODO: these IDs are defined, but are not caught by the
                        # extraction regex
                        misses.add(id)
                        print('missed', id)

        print(len(misses))

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.tsv'.format(
                                                date=self.latest())))


class TranslatedSuccessorCompact(GNDTask):
    """
    One node per line.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return TranslatedSuccessor(date=self.date)


    def run(self):
        graph = collections.defaultdict(set)
        with self.input().open() as handle:
            for line in handle:
                id, successor = line.strip().split()
                graph[id].add(successor)

        with self.output().open('w') as output:
            for node, outbound in graph.iteritems():
                value = '\t'.join([node] + list(outbound))
                output.write('%s\n' % value)

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.tsv'.format(
                                                date=self.latest())))


class PageRank(GNDTask):
    """
    Use external program to compute pagerank fast.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'data': TranslatedSuccessorCompact(date=self.date),
            'pagerank': Executable(name='pagerank',
                msg='See: https://github.com/miku/gopagerank')
        }

    def run(self):
        temp = shellout("pagerank {input} > {output}",
                        input=self.input().get('data').fn)
        luigi.File(temp).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.tsv'.format(
                                                date=self.latest())))


class TranslatePageRank(GNDTask):
    """
    Convert pagerank id's back to GNDs.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'map': TranslationMap(date=self.date),
            'pagerank': PageRank(date=self.date)
        }

    def run(self):
        mapping = {}
        with self.input().get('map').open() as handle:
            for line in handle:
                id, intid = line.strip().split()
                mapping[intid] = id

        with self.input().get('pagerank').open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    intid, pagerank = line.strip().split()
                    output.write('%s\t%s\n' % (mapping[intid], pagerank))

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.tsv'.format(
                                                date=self.latest())))


class PreferredNameFile(GNDTask):
    """
    Extract all preferred names add create a single file
    with id, preferred name.

    Well, 661m26.249s.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return SqliteDB(date=self.date)

    def run(self):
        # fetch all gnds
        idset = set()
        with dbopen(self.input().fn) as cursor:
            cursor.execute("SELECT id FROM gnd")
            rows = cursor.fetchall()
            for row in rows:
                idset.add(row[0])

        pattern = re.compile("<(gnd:preferred[^>]*)>(.*?)</gnd:preferred")
        with dbopen(self.input().fn) as cursor:
            with self.output().open('w') as output:
                for id in idset:
                    cursor.execute("SELECT content FROM gnd WHERE id = ?", (id,))
                    content = cursor.fetchone()[0]
                    match = pattern.search(content)
                    if match:
                        output.write('%s\t%s\t%s\n' % (id, match.group(2), match.group(1)))

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.tsv'.format(
                                                date=self.latest())))


class HumanReadablePageRank(GNDTask):
    """ Add the concept name to the PageRank list. """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return {
            'pagerank': TranslatePageRank(date=self.date),
            'names': PreferredNameFile(date=self.date)
        }

    def run(self):
        pagerank = pd.read_csv(self.input().get('pagerank').fn, sep='\t',
                               names=('id', 'pagerank'))
        names = pd.read_csv(self.input().get('names').fn, sep='\t',
                            names=('id', 'name', 'kind'))
        df = pagerank.merge(names)
        with self.output().open('w') as output:
            df = df.sort(columns=['pagerank'], ascending=False)
            df.to_csv(output, sep='\t', cols=('id', 'pagerank', 'name', 'kind'),
                      index=False, header=False)

    def output(self):
        return luigi.LocalTarget(path=self.path(filename='{date}.tsv'.format(
                                                date=self.latest())))


if __name__ == '__main__':
    luigi.run()
