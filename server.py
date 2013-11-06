#!/usr/bin/env python
# coding: utf-8

from flask import Flask, Response
from gndzero import dbopen
app = Flask(__name__)

DB = "/media/mtc/Data/var/data/gndzero/sqlite-db/date-2013-11-06.db"


@app.route("/")
def hello():
    return "Hello GND!"

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

