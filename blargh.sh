#!/bin/bash

DB="http://127.0.0.1:5984/blargh_test"

echo "Creating database."
curl -X DELETE $DB
curl -X PUT $DB

echo "Creating a document with history."
DDOC='{"_id": "_design/bing", "validate_doc_update": "function() {}"}'
DOC_R1='{"_id":"doc","foo":"bar"}'
DOC_R2='{"_id":"doc","foo":"baz","_rev":"1-4c6114c65e295552ab1019e2b046b10e"}'
DOC_R3='{"_id":"doc","foo":"bam","_rev":"2-cfcd6781f13994bde69a1c3320bfdadb"}'
DOC_R4='{"_id":"doc","foo":"bat","_rev":"3-cc2f3210d779aef595cd4738be0ef8ff"}'

curl -X PUT $DB/_design/bing -d "$DDOC"
curl -X PUT $DB/doc -d "$DOC_R1"
curl -X PUT $DB/doc -d "$DOC_R2"
curl -X PUT $DB/doc -d "$DOC_R3"
#curl -X PUT $DB/doc -d "$DOC_R4"

echo "Compacting database."
curl -X POST $DB/_compact -H "Content-Type: application/json"
sleep 1

echo "Triggering error?"

DOCS="{\"docs\": [$DOC_R4, $DOC_R3, $DOC_R2], \"new_edits\": false}"
curl -X POST $DB/_bulk_docs -H "Content-Type: application/json" -d "$DOCS"

curl $DB/doc?rev=2-cfcd6781f13994bde69a1c3320bfdadb
curl $DB/doc?rev=3-cc2f3210d779aef595cd4738be0ef8ff
