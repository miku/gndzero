#!/usr/bin/env python
# coding: utf-8

from flask import Flask, Response, url_for
from gndzero import dbopen, SqliteDB

app = Flask(__name__)

# the current database, must be there already
task = SqliteDB()
DB = task.output().fn


@app.route("/")
def hello():
    return "Hello GND! Example: <a href=%s>%s</a>" % (
        url_for('default', gnd='118514768'),
        url_for('default', gnd='118514768'))

@app.route("/gnd/<gnd>")
def default(gnd):
    with dbopen(DB) as cursor:
        query = cursor.execute('SELECT content FROM gnd WHERE id = ?', (gnd,))
        result = query.fetchone()
        return Response(response=result[0], status=200, headers=None, 
                        mimetype='text/plain', content_type=None,
                        direct_passthrough=False)

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)

