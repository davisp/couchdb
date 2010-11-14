#! /usr/bin/env python

import json
import os
import string
import subprocess as sp
import sys
import time

from couchdbkit.client import Server

DBNAME = "perf_test"
PATH = os.path.dirname(__file__)
with open(os.path.join(PATH, "megaview.js")) as handle:
    MEGAVIEW = "//%s\n%s" % (time.time(), handle.read())
with open(os.path.join(PATH, "simpleview.js")) as handle:
    SIMPLEVIEW = "//%s\n%s" % (time.time(), handle.read())

def load_docs(db, simple=10000, unsimple=10000):
    common = {
       'rd_schema_id' : 'something',
       'rd_ext_id' : 'something else',
       'rd_key' : 'some key',
    }

    docs = []
    for i in range(simple):
        doc = {
            '_id': str(i) + ('a' * 200),
            'foo': 'bar',
            'etc': 'cough'
        }
        doc.update(common)
        docs.append(doc)
    db.bulk_save(docs)

    docs = []
    for i in range(unsimple):
        if i % 2 == 0:
            doc = {
                'field1': 'a' * 50,
                'field2': 'b' * 50,
                'field3': 'c' * 50,
                'complex': {
                    'sub_field': 'sub_value',
                    'extras': range(50),
                },
                'simple_list': range(20),
                'another_complex': {},
                'rd_megaview_expandable': ['simple_list'],
            }
            ac = doc['another_complex']
            for l in string.ascii_lowercase:
                ac[l * 20] = [l * 20]
        else:
            doc = {
                'anotherfield1' : 'a' * 10,
                'anotherfield2' : 'b' * 10,
                'anotherfield3' : 'c' * 10,
                'anotherfield4' : 'd' * 10,
                'anotherfield5' : 'e' * 10,
                'anotherfield6' : 'f' * 10,
                'anotherfield7' : 'g' * 10,
            }
        doc['_id'] = str(i) + ('b' * 200)
        doc.update(common)
        docs.append(doc)
        if len(docs) >= 1000:
            db.bulk_save(docs)
            docs = []
    if docs:
        db.bulk_save(docs)

def run_view(db, name, view):
    ddoc = {"_id": "_design/%s" % name, "views": {"foo": {"map": view}}}
    db.save_doc(ddoc)
    db.res.get("/_design/%s/_view/foo" % name, limit=0)

def timeit(func, db, name, code):
    start = time.time()
    func(db, name, code)
    print "%s - %0.2f (s)" % (name, time.time() - start)

def main():
    server = Server("http://127.0.0.1:5984")

    try:
        name = sp.Popen(["stg", "top"], stdout=sp.PIPE).communicate()[0].strip()
    except:
        cmd = ["git", "log", "--pretty=one", "-1"]
        name = sp.Popen(cmd, stdout=sp.PIPE).communicate()[0]
        name = name.split()[0]

    print "> %s" % name

    for i in range(1):
        if DBNAME in server:
            server.delete_db(DBNAME)
        db = server.create_db(DBNAME)

        sys.stderr.write("Loading docs...")
        load_docs(db)
        sys.stderr.write("Done\n")

        sys.stderr.write("Running tests...")
        timeit(run_view, db, "mega", MEGAVIEW)
        timeit(run_view, db, "simple", SIMPLEVIEW)
        sys.stderr.write("Done.\n")

if __name__ == '__main__':
    main()

