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
