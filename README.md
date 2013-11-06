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

Install dependencies:

    $ pip install -r requirements.txt


Copy `config.sample.py` to `config.py` and adjust the values:

    # TEMPDIR and HOME must reside on the same device
    TEMPDIR = '/tmp'
    HOME = './data'

Now create the sqlite datebase. This will take a while (download, extract, insert):

    $ python gndzero.py SqliteDB --local-scheduler


The server
----------

Run the server:

    $ until python server.py; do echo "Re: (err: $?)" >&2; sleep 0.5; done

Examples:

* http://localhost:5000/gnd/118514768
* http://localhost:5000/gnd/121608557
* http://localhost:5000/gnd/4000362-0

Notes
-----

The local sqlite3 database is about 12G in size and contains 10004751 rows.
