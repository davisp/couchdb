#! /usr/bin/env python

import json
import string
import time

from couchdbkit.client import Server

DBNAME = "perf_test"

MEGAVIEW = "//" + str(time.time()) + """
// A doc may have rd_megaview_ignore_doc set - this excludes the doc
// completely from the megaview.
// A doc may also have rd_megaview_ignore_values set - this writes the
// 'rd.core.*' schemas (so the document IDs can still be located) but the
// values aren't written.

function(doc) {
  if (doc.rd_schema_id
    && !doc.rd_megaview_ignore_doc
    && doc.rd_schema_id.indexOf("ui") != 0) { // ui extensions should be ok here?
    // every row we emit for this doc uses an identical 'value'.
    var row_val = {'_rev': doc._rev,
                   'rd_key' : doc.rd_key,
                   'rd_ext' : doc.rd_ext_id,
                   'rd_schema_id' : doc.rd_schema_id,
                   'rd_source' : doc.rd_source,
                  }
    // first emit some core 'pseudo-schemas'.
    emit(['rd.core.content', 'key', doc.rd_key], row_val);
    emit(['rd.core.content', 'schema_id', doc.rd_schema_id], row_val);
    emit(['rd.core.content', 'key-schema_id', [doc.rd_key, doc.rd_schema_id]], row_val);
    emit(['rd.core.content', 'ext_id', doc.rd_ext_id], row_val);
    emit(['rd.core.content', 'ext_id-schema_id', [doc.rd_ext_id, doc.rd_schema_id]], row_val);
    // don't emit the revision from the source in the key.
    var src_val;
    if (doc.rd_source)
      src_val = doc.rd_source[0];
    else
      src_val = null;
      
    emit(['rd.core.content', 'source', src_val], row_val);
    emit(['rd.core.content', 'key-source', [doc.rd_key, src_val]], row_val);
    emit(['rd.core.content', 'ext_id-source', [doc.rd_ext_id, src_val]], row_val);

    if (doc.rd_schema_confidence)
      emit(['rd.core.content', 'rd_schema_confidence', doc.rd_schema_confidence],
           row_val);

    // If this schema doesn't want/need values indexed, bail out now.
    if (doc.rd_megaview_ignore_values)
      return

    var rd_megaview_expandable = doc.rd_megaview_expandable || [];
    for (var prop in doc) {
        //Skip text fields that are big (better served by full
        //text search), private props and raindrop-housekeeping
        //props.
        if ( prop.charAt(0) == "_"
             || prop.indexOf("rd_") == 0
             || prop.indexOf("raindrop") == 0) {
          continue;
        }

      var val;
      // don't emit long string values, but do still emit a row with NULL
      // so it can be found.
      if ((typeof doc[prop] == "string") && doc[prop].length > 140)
        val = null;
      else
        val = doc[prop];
      // If the doc has a special attribute rd_megaview_expandable and this
      // property is in it, then that attribute is an array that each
      // elt can be expanded - eg 'tags'. We can't do this unconditionally as
      // things like identity_ids don't make sense expanded. Note we may also
      // want to unpack real objects?
      var expand = false;
      for (var i=0; i<rd_megaview_expandable.length && !expand; i++) {
        if (prop==rd_megaview_expandable[i]) {
          expand = true;
        }
      }
      if (expand) {
        for (var i=0; i<doc[prop].length; i++)
          emit([doc.rd_schema_id, prop, val[i]], row_val);
      } else {
        emit([doc.rd_schema_id, prop, val], row_val);
      }
    }
  }
}
"""

SIMPLEVIEW = "//" + str(time.time()) + """
function(doc) {
    if (doc.foo) emit(doc.foo, doc.etc);
}
"""

def load_docs(db, simple=1000, unsimple=1000):
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
    v = db.res.get("/_design/%s/_view/foo" % name, limit=0)
    print v

def timeit(func, *args):
    start = time.time()
    func(*args)
    print "Elapsed time: %0.2f (s)" % (time.time()-start)

def main():
    server = Server("http://127.0.0.1:5984")
    if DBNAME in server:
        server.delete_db(DBNAME)
    db = server.create_db(DBNAME)

    load_docs(db)

    print "Running MegaView: %s" % time.strftime("%a, %d %b %Y %H:%M:%S")
    timeit(run_view, db, "mega", MEGAVIEW)

    print "Running SimpleView: %s" % time.strftime("%a, %d %b %Y %H:%M:%S")
    timeit(run_view, db, "simple", SIMPLEVIEW)

if __name__ == '__main__':
    main()

