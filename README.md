README
======

The zeroth prototyp of a GND cache.

Get started
-----------

Clone the repo:



Create a virtualenv:

	$ mkvirtualenv gndzero
    ...

Install dependencies:

	$ pip install -r requirements.txt


Change the path to where the data should be stored in `gndzero.py`:

	tempfile.tempdir = '/tmp'
	HOME = '/var/data'

Note that both directies must be on the same device.

Now create the sqlite datebase. This will take a while (download, extract, insert):

	$ python gndzero.py SqliteDB


The server
----------

Adjust the `DB` variable in `server.py`:

	DB = /path/to/your/db

Run the server:

	$ until python server.py; do echo "Re: (err: $?)" >&2; sleep 0.5; done
