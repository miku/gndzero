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

from flask import Flask, Response, url_for
from gndzero import dbopen, SqliteDB

app = Flask(__name__)

# the current database, must already be in place
task = SqliteDB()
DB = task.output().fn


def wrap(s):
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
    return "%s\n%s\n</rdf:RDF>" % (HEADER, s)

@app.route("/")
def index():
    example = url_for('default', gnd='118514768')
    return "Hello GND! Example: <a href=%s>%s</a>" % (example, example)

@app.route("/gnd/<gnd>")
def default(gnd):
    with dbopen(DB) as cursor:
        query = cursor.execute('SELECT content FROM gnd WHERE id = ?', (gnd,))
        result = query.fetchone()
        wrapped = wrap(result[0])
        return Response(response=wrapped, status=200, headers=None,
                        mimetype='text/xml',
                        content_type='text/xml; charset=utf-8',
                        direct_passthrough=False)

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
