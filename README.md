README
======

The zeroth prototyp of a GND cache.

Get started
-----------

Clone the repo:

    $ git clone git@github.com:miku/gndzero.git
    $ cd gndzero


Create a virtualenv:

    $ mkvirtualenv gndzero
    ...

Install dependencies (you'll need some C compiler):

    $ pip install -r requirements.txt


Copy `config.sample.py` to `config.py` and adjust the values:

    # TEMPDIR and HOME must reside on the same device
    TEMPDIR = '/tmp'
    HOME = './data'

Now create the sqlite datebase. This will take a while (download,
extract, insert) - about 20-30 minutes:

    $ python gndzero.py SqliteDB --local-scheduler


The server
----------

Run the server (development):

    $ until python server.py; do echo "Re: (err: $?)" >&2; sleep 0.5; done

Examples:

* [http://localhost:5000/gnd/118514768](http://localhost:5000/gnd/118514768)
* [http://localhost:5000/gnd/121608557](http://localhost:5000/gnd/121608557)
* [http://localhost:5000/gnd/4000362-0](http://localhost:5000/gnd/4000362-0)

Compare:

* [http://d-nb.info/gnd/118514768](http://d-nb.info/gnd/118514768)
* [http://d-nb.info/gnd/121608557](http://d-nb.info/gnd/121608557)
* [http://d-nb.info/gnd/4000362-0](http://d-nb.info/gnd/4000362-0)

Format the output:

    $ curl -s "http://localhost:5000/gnd/4000362-0"|xmllint --format -

Run the ([gunicorn](http://gunicorn.org/)) server (production):

    $ gunicorn --workers 4 server:app

Notes
-----

The local sqlite3 database is about 12G in size and contains 10004751 rows.

----

The GNDs form a graph, if occurences of GNDs in the metadata are interpreted
as outbound links, hence you can compute a pagerank to find some often
referred to concepts. An example output can be found here: https://gist.github.com/miku/7427351

    $ time python gndzero.py HumanReadablePageRank --local-scheduler

The `pagerank` command line program can be found here: https://github.com/miku/gopagerank,
with credits due to [Thomas Dimson](https://github.com/cosbynator). The overall
preprocessing for this takes too long (almost a day), but this is only a prototype.
