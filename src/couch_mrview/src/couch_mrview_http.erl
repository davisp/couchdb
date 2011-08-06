% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_mrview_http).

-export([handle_view_req/3, handle_temp_view_req/2, handle_info_req/3]).

-include("couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").


handle_temp_view_req(#httpd{method='POST'}=Req, Db) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    ok = couch_db:check_is_admin(Db),
    {Body} = couch_httpd:json_body_obj(Req),
    DDoc = couch_mrview_util:temp_view_to_ddoc({Body}),
    Keys = couch_util:get_value(<<"keys">>, Body, []),
    design_doc_view(Req, Db, DDoc, <<"temp">>, Keys);
handle_temp_view_req(Req, _Db) ->
    couch_httpd:send_method_not_allowed(Req, "POST").


handle_view_req(#httpd{method='GET'}=Req, Db, DDoc) ->
    [_, _, _, _, ViewName] = Req#httpd.path_parts,
    design_doc_view(Req, Db, DDoc, ViewName, []);
handle_view_req(#httpd{method='POST'}=Req, Db, DDoc) ->
    [_, _, _, _, ViewName] = Req#httpd.path_parts,
    {Fields} = couch_httpd:json_body_obj(Req),
    Keys = couch_util:get_value(<<"keys">>, Fields),
    design_doc_view(Req, Db, DDoc, ViewName, Keys);
handle_view_req(Req, _Db, _DDoc) ->
    couch_httpd:send_method_not_allowed(Req, "GET,POST,HEAD").


handle_info_req(#httpd{method='GET'}=Req, Db, DDoc) ->
    [_, _, Name, _] = Req#httpd.path_parts,
    {ok, Info} = couch_mrview_util:get_info(Db, DDoc),
    couch_httpd:send_json(Req, 200, {[
        {name, Name},
        {view_index, {Info}}
    ]});
handle_info_req(Req, _Db, _DDoc) ->
    couch_httpd:send_method_not_allowed(Req, "GET").


design_doc_view(Req, Db, DDoc, ViewName, Keys) ->
    Args0 = parse_qs(Req),
    Args = Args0#mrargs{keys=Keys},
    ETag = couch_uuids:new(),
    couch_stats_collector:increment({httpd, view_reads}),
    couch_httpd:etag_respond(Req, ETag, fun() ->
        Hdrs = [{"ETag", ETag}],
        {ok, Resp} = couch_httpd:start_json_response(Req, 200, Hdrs),
        CB = fun view_callback/2,
        couch_mrview:query_view(Db, DDoc, ViewName, Args, CB, {nil, Resp}),
        couch_httpd:end_json_response(Resp)
    end).


view_callback({meta, Meta}, {nil, Resp}) ->
    % Map function starting
    Total = couch_util:get_value(total, Meta),
    Offset = couch_util:get_value(offset, Meta),
    Chunk = "{\"total_rows\":~p,\"offset\":~p,\"rows\":[\r\n",
    couch_httpd:send_chunk(Resp, io_lib:format(Chunk, [Total, Offset])),
    {ok, {"", Resp}};
view_callback({row, Row}, {nil, Resp}) ->
    % Reduce function starting
    couch_httpd:send_chunk(Resp, ["{\"rows\":[\r\n", row_to_json(Row)]),
    {ok, {",\r\n", Resp}};
view_callback({row, Row}, {Prepend, Resp}) ->
    % Adding another row
    couch_httpd:send_chunk(Resp, [Prepend, row_to_json(Row)]),
    {ok, {",\r\n", Resp}};
view_callback(complete, {nil, Resp}) ->
    % Nothing in view
    couch_httpd:send_chunk(Resp, "{\"rows\":[]}");
view_callback(complete, {_, Resp}) ->
    % Finish view output
    couch_httpd:send_chunk(Resp, "\r\n]}").


row_to_json(Row) ->
    Id = case couch_util:get_value(id, Row) of
        undefined -> [];
        Id0 -> [{id, Id0}]
    end,
    Key = couch_util:get_value(key, Row, null),
    Val = couch_util:get_value(val, Row, null),
    Doc = case couch_util:get_value(doc, Row) of
        undefined -> [];
        Doc0 -> [{doc, Doc0}]
    end,
    Obj = {Id ++ [{key, Key}, {value, Val}] ++ Doc},
    ?JSON_ENCODE(Obj).


parse_qs(Req) ->
    Args = #mrargs{},
    lists:foldl(fun({K, V}, Acc) ->
        parse_qs(K, V, Acc)
    end, Args, couch_httpd:qs(Req)).
    

parse_qs(Key, Val, Args) ->
    case Key of
        "" ->
            Args;
        "reduce" ->
            Args#mrargs{reduce=parse_boolean(Val)};
        "key" ->
            JsonKey = ?JSON_DECODE(Val),
            Args#mrargs{start_key=JsonKey, end_key=JsonKey};
        "keys" ->
            Args#mrargs{keys=?JSON_DECODE(Val)};
        "startkey" ->
            Args#mrargs{start_key=?JSON_DECODE(Val)};
        "start_key" ->
            Args#mrargs{start_key=?JSON_DECODE(Val)};
        "start_key_doc_id" ->
            Args#mrargs{start_key_docid=list_to_binary(Val)};
        "endkey" ->
            Args#mrargs{end_key=?JSON_DECODE(Val)};
        "end_key" ->
            Args#mrargs{end_key=?JSON_DECODE(Val)};
        "end_key_doc_id" ->
            Args#mrargs{end_key_docid=list_to_binary(Val)};
        "limit" ->
            Args#mrargs{limit=parse_pos_int(Val)};
        "count" ->
            throw({query_parse_error, <<"QS param `count` is not `limit`">>});
        "stale" when Val == "ok" ->
            Args#mrargs{stale=ok};
        "stale" when Val == "update_after" ->
            Args#mrargs{stale=update_after};
        "stale" ->
            throw({query_parse_error, <<"Invalid value for `stale`.">>});
        "descending" ->
            case parse_boolean(Val) of
                true -> Args#mrargs{direction=rev};
                _ -> Args#mrargs{direction=fwd}
            end;
        "skip" ->
            Args#mrargs{skip=parse_pos_int(Val)};
        "group" ->
            case parse_boolean(Val) of
                true -> Args#mrargs{group_level=exact};
                _ -> Args#mrargs{group_level=0}
            end;
        "group_level" ->
            Args#mrargs{group_level=parse_pos_int(Val)};
        "inclusive_end" ->
            Args#mrargs{inclusive_end=parse_boolean(Val)};
        "include_docs" ->
            Args#mrargs{include_docs=parse_boolean(Val)};
        "conflicts" ->
            Args#mrargs{conflicts=parse_boolean(Val)};
        "list" ->
            Args#mrargs{list=list_to_binary(Val)};
        "callback" ->
            Args#mrargs{callback=list_to_binary(Val)};
        _ ->
            BKey = list_to_binary(Key),
            BVal = list_to_binary(Val),
            Args#mrargs{extra=[{BKey, BVal} | Args#mrargs.extra]}
    end.


parse_boolean(Val) ->
    case string:to_lower(Val) of
    "true" -> true;
    "false" -> false;
    _ ->
        Msg = io_lib:format("Invalid boolean parameter: ~p", [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.


parse_int(Val) ->
    case (catch list_to_integer(Val)) of
    IntVal when is_integer(IntVal) ->
        IntVal;
    _ ->
        Msg = io_lib:format("Invalid value for integer: ~p", [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.


parse_pos_int(Val) ->
    case parse_int(Val) of
    IntVal when IntVal >= 0 ->
        IntVal;
    _ ->
        Fmt = "Invalid value for positive integer: ~p",
        Msg = io_lib:format(Fmt, [Val]),
        throw({query_parse_error, ?l2b(Msg)})
    end.