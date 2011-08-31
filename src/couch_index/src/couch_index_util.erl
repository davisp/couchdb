-module(couch_index_util).

-export([load_doc/4, sort_lib/1, hexsig/1]).


load_doc(Db, Id, {Props}, Opts) ->
    DocId = couch_util:get_value(<<"_id">>, Props, Id),
    Rev = case couch_util:get_value(<<"_rev">>, Props, undefined) of
        Rev0 when is_binary(Rev0) -> couch_doc:parse_rev(Rev0);
        _ -> nil
    end,
    load_doc_int(Db, DocId, Rev, Opts);
load_doc(Db, Id, _Value, Opts) ->
    load_doc_int(Db, Id, nil, Opts).


load_doc_int(Db, Id, Rev, Opts) ->
    case (catch couch_httpd_db:couch_doc_open(Db, Id, Rev, Opts)) of
        #doc{} = Doc ->
            [{doc, couch_doc:to_json_obj(Doc, [])}];
        _Else ->
            [{doc, null}]
    end.


sort_lib({Lib}) ->
    sort_lib(Lib, []).
sort_lib([], LAcc) ->
    lists:keysort(1, LAcc);
sort_lib([{LName, {LObj}}|Rest], LAcc) ->
    LSorted = sort_lib(LObj, []), % descend into nested object
    sort_lib(Rest, [{LName, LSorted}|LAcc]);
sort_lib([{LName, LCode}|Rest], LAcc) ->
    sort_lib(Rest, [{LName, LCode}|LAcc]).


hexsig(Sig) ->
    couch_util:to_hex(binary_to_list(Sig)).
