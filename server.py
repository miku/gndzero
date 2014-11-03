#!/usr/bin/env python
# coding: utf-8

"""
A simple GND cache server. Not too robust, but ok for testing.

Simple ApacheBench:

$ ab -c 20 -n 50000 "http://localhost:5000/gnd/118514768"

This is ApacheBench, Version 2.3 <$Revision: 655654 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking localhost (be patient)
Completed 5000 requests
Completed 10000 requests
Completed 15000 requests
Completed 20000 requests
Completed 25000 requests
Completed 30000 requests
Completed 35000 requests
Completed 40000 requests
Completed 45000 requests
Completed 50000 requests
Finished 50000 requests


Server Software:        Werkzeug/0.9.4
Server Hostname:        localhost
Server Port:            5000

Document Path:          /gnd/118514768
Document Length:        15263 bytes

Concurrency Level:      20
Time taken for tests:   35.080 seconds
Complete requests:      50000
Failed requests:        0
Write errors:           0
Total transferred:      771000000 bytes
HTML transferred:       763150000 bytes
Requests per second:    1425.33 [#/sec] (mean)
Time per request:       14.032 [ms] (mean)
Time per request:       0.702 [ms] (mean, across all concurrent requests)
Transfer rate:          21463.48 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.0      0       1
Processing:     1   14  20.6     13     743
Waiting:        1   14  20.6     13     743
Total:          1   14  20.6     13     743

Percentage of the requests served within a certain time (ms)
  50%     13
  66%     13
  75%     13
  80%     13
  90%     14
  95%     14
  98%     15
  99%     17
 100%    743 (longest request)

"""

from flask import Flask, Response, url_for, request, jsonify, redirect, abort
from gndzero import dbopen, SqliteDB
import requests
import re

app = Flask(__name__)

# the current database, must already be in place
task = SqliteDB()
DB = '/tmp/test.db'

def wrap(s, rewrite=True, header=True):
    """
    Wrap the snippet in a proper header. Optionally rewrite GND URLs
    to point to the local installation.
    """
    rewrite = True if rewrite in (True, 'on', '1', 1, 'yes') else False
    HEADER = """<rdf:RDF xmlns:gnd="http://d-nb.info/standards/elementset/gnd#"
                     xmlns:dc="http://purl.org/dc/elements/1.1/"
                     xmlns:rda="http://rdvocab.info/"
                     xmlns:foaf="http://xmlns.com/foaf/0.1/"
                     xmlns:isbd="http://iflastandards.info/ns/isbd/elements/"
                     xmlns:dcterms="http://purl.org/dc/terms/"
                     xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
                     xmlns:marcRole="http://id.loc.gov/vocabulary/relators/"
                     xmlns:lib="http://purl.org/library/"
                     xmlns:umbel="http://umbel.org/umbel#"
                     xmlns:bibo="http://purl.org/ontology/bibo/"
                     xmlns:owl="http://www.w3.org/2002/07/owl#"
                     xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                     xmlns:skos="http://www.w3.org/2004/02/skos/core#">"""
    if rewrite:
        for match in re.finditer(r"http://d-nb.info/gnd/([0-9a-zA-Z-]+)", s):
            gnd = match.group(1)
            s = s.replace("http://d-nb.info/gnd/{gnd}".format(gnd=gnd),
                          url_for('cache', gnd=gnd, _external=True))

    if header:
        return "%s\n%s\n</rdf:RDF>" % (HEADER, s)
    else:
        return "%s\n" % (s)


@app.route("/cache", methods=["PUT"])
def create_cache():
    with dbopen(DB) as cursor:
        cursor.execute("""CREATE TABLE IF NOT EXISTS gnd
                          (id text PRIMARY KEY, content blob,
                          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        cursor.execute("""CREATE INDEX IF NOT EXISTS
                              idx_gnd_id ON gnd (id)""")
    return jsonify(cache="ok")


@app.route("/cache", methods=["DELETE"])
def drop_cache():
    with dbopen(DB) as cursor:
        cursor.execute("""DROP TABLE IF EXISTS gnd """)
        cursor.execute("""DROP INDEX IF EXISTS idx_gnd_id""")
    return jsonify(cache="dropped")


@app.route("/gnd/<gnd>", methods=["GET"])
def cache_bc(gnd):
    """ Backwards compatibility. """
    return redirect(url_for('cache', gnd=gnd))


@app.route("/cache/<gnd>", methods=["GET"])
def cache(gnd):
    """ http://d-nb.info/gnd/118514768/about/rdf """
    with dbopen(DB) as cursor:
        query = cursor.execute("""SELECT content FROM gnd
                                  WHERE id = ?""", (gnd,))
        result = query.fetchone()
        if not result:
            # download and store
            r = requests.get("http://d-nb.info/gnd/{gnd}/about/rdf".format(gnd=gnd))
            if r.status_code == 200:
                cursor.execute("""INSERT INTO gnd (id, content)
                                  VALUES (?, ?)""", (gnd, r.text))
            else:
                # pass on the d-nb.info status code
                abort(r.status_code)

        query = cursor.execute("""SELECT content FROM gnd
                                  WHERE id = ?""", (gnd,))
        result = query.fetchone()
        if not result:
            abort(404)
        wrapped = wrap(result[0], rewrite=request.args.get('rewrite', True),
                       header=True)
        return Response(response=wrapped, status=200, headers=None,
                        mimetype='text/xml',
                        content_type='text/xml; charset=utf-8',
                        direct_passthrough=False)


@app.route("/")
def index():
    example = url_for('cache', gnd='118514768')
    return "Hello GND! Example: <a href=%s>%s</a>" % (example, example)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7000, debug=True)
