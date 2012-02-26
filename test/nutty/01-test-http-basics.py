import t
import h

import couch


SRV = couch.Server()
DBNAME = "test_suite_db"

def test_01_version():
    resp = couch.Server().get("/")
    t.eq(resp.json["couchdb"], "Welcome")

def test_02_delete_missing_db():
    db = SRV[DBNAME]
    # COUCHDB-100 DELETE non-existent DB returns 404
    db.delete_db()
    db.delete_db()
    t.eq(db.server.resp.status, 404)

def test_03_put_existing_db():
    db = SRV[DBNAME]
    db.delete_db()
    db.create_db()
    try:
        db.create_db()
    except couch.ServerError, e:
        t.eq(e.resp.status, 412)

def test_04_create_returns_location():
    for name in ["test_suite_db", "test_suite_db%2Fwith_slashes"]:
        resp = SRV.delete("/%s" % name, check=False)
        resp = SRV.put("/%s" % name)
        loc = resp.getheader("Location")
        t.a(loc.startswith("http://") or loc.startswith("https://"))
        t.eq(resp.getheader("Location")[-len(name):], name)

def test_05_db_info_has_name():
    db = SRV[DBNAME]
    t.eq(db.info()["db_name"], "test_suite_db")

def test_06_db_in_all_dbs():
    db = SRV[DBNAME]
    t.isin("test_suite_db", db.server.all_dbs())

def test_07_db_has_no_docs():
    db = SRV[DBNAME]
    t.eq(db.info()["doc_count"], 0)

def test_08_save_doc():
    db = SRV[DBNAME]
    doc = {"_id": "0", "a": 1, "b": 1}
    resp = db.save(doc)
    t.eq(resp["ok"], True)
    t.isin("id", resp)
    t.isin("rev", resp)
    t.eq(doc["_id"], resp["id"])
    t.eq(doc["_rev"], resp["rev"])

def test_09_revs_info():
    db = SRV[DBNAME]
    doc = db.open("0", revs_info=True)
    t.eq(doc["_revs_info"][0]["status"], "available")

def test_10_check_doc_count():
    db = SRV[DBNAME]
    for i in range(1, 4):
        t.a(db.save({"_id": str(i), "a":(i+1), "b":(i+1)**2})["ok"])
    t.eq(db.info()["doc_count"], 4)

def test_11_COUCHDB_954():
    db = SRV[DBNAME]
    oldrev = db.save({"_id": "COUCHDB-954", "a": 1})["rev"]
    newrev = db.save({"_id": "COUCHDB-954", "_rev": oldrev})["rev"]
    result = db.open("COUCHDB-954", open_revs=[oldrev, newrev])
    t.eq(len(result), 2)
    t.isin("ok", result[0])
    t.isin("ok", result[1])

    result = db.open("COUCHDB-954", open_revs=[oldrev, newrev], latest=True)
    t.eq(len(result), 1)
    t.eq(result[0]["ok"]["_rev"], newrev)

    result = db.open("COUCHDB-954", open_revs=[oldrev], latest=True)
    t.eq(len(result), 1)
    t.eq(result[0]["ok"]["_rev"], newrev)

    result = db.delete({"_id": "COUCHDB-954", "_rev": newrev})
    t.ne(result["rev"], newrev)

