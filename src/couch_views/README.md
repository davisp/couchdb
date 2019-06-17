CouchDB Views
=====

This is the new application that builds and runs Map/reduce views against FoundationDB.
Currently only map indexes are supported and it will always return the full index.

Code layout:

* `couch_views` - Main entry point to query a view
* `couch_views_reader` - Reads from the index.
* `couch_views_indexer` - Queries the changes feed from the last sequence and updates the index
* `couch_views_fdb` - a wrapper around erlfdb
* `couch_views_encoding` - Emitted key encoding to keep CouchDB sorting rules
* `couch_views_worker_server` - checks for indexing jobs and spawns a worker to build it
* `couch_views_worker` - runs couch_views_indexer and builds index along with sending updates back to jobs
* `couch_views_jobs` - a wrapper around couch_jobs
