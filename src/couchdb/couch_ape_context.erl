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
-module(couch_ape_context).
-behaviour(gen_server).

-export([start_link/1, handle_request/3]).

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-include("couch_db.hrl").

-record(st, {db_name, ddoc_id, ddoc_rev, ctx}).

start_link({_DbName, _DDocId, _DDocRev, _DDoc}=Key) ->
    gen_server:start_link(?MODULE, [Key], []).

handle_request(Pid, Req, Db) ->
    JsonReq = couch_httpd_external:json_req_obj(Req, Db),
    {ok, ReqId} = gen_server:call(Pid, {init_req, JsonReq}),
    handle_request_async(Pid, ReqId, Db).

% gen_server callbacks

init([{DbName, DDocId, DDocRev, DDocJson}]) ->
    {ok, Ctx} = emonk:create_ctx(),
    {ok, _} = emonk:eval(Ctx, read_js()),
    Info = {[{dbname, DbName}, {ddoc, DDocJson}]},
    {ok, true} = do_call(Ctx, <<"init">>, [Info]),
    {ok, #st{
        db_name=DbName,
        ddoc_id=DDocId,
        ddoc_rev=DDocRev,
        ctx=Ctx
    }}.

terminate(Reason, _State) ->
    ?LOG_DEBUG("couch_ape_context shutting down: ~p", [Reason]),
    ok.

handle_call({init_req, JsonReq}, _From, State) ->
    ReqId = couch_uuids:new(),
    {ok, true} = do_call(State#st.ctx, <<"init_req">>, [ReqId, JsonReq]),
    {reply, {ok, ReqId}, State};
handle_call({next_req, ReqId}, _From, State) ->
    case do_call(State#st.ctx, <<"next_req">>, [ReqId]) of
        {ok, [Type, Args]} ->
            {reply, {ok, Type, Args}, State};
        {ok, null} ->
            {reply, {ok, empty_response}, State}
    end;
handle_call({respond, ReqId, Resp}, _From, State) ->
    {ok, true} = do_call(State#st.ctx, <<"respond">>, [ReqId, Resp]),
    {reply, ok, State};
handle_call({error, ReqId, Err}, _From, State) ->
    {ok, true} = do_call(State#st.ctx, <<"error">>, [ReqId, Err]),
    {reply, ok, State};
handle_call({start_view, ReqId, Info}, _From, State) ->
    {ok, true} = do_call(State#st.ctx, <<"start_view">>, [ReqId, Info]),
    {reply, ok, State};
handle_call({send_row, ReqId, Row}, _From, State) ->
    Resp = case do_call(State#st.ctx, <<"send_row">>, [ReqId, Row]) of
        {ok, true} -> ok;
        {ok, false} -> stop
    end,
    {reply, Resp, State};
handle_call({end_view, ReqId}, _From, State) ->
    {ok, true} = do_call(State#st.ctx, <<"end_view">>, [ReqId]),
    {reply, ok, State}.

handle_cast(close, State) ->
    {stop, normal, State};
handle_cast(Msg, State) ->
    ?LOG_ERROR("Ignoring unexpected cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    ?LOG_ERROR("Ignoring unexpected info message: ~p", [Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_request_async(Pid, ReqId, Db) ->
    case gen_server:call(Pid, {next_req, ReqId}) of
        {ok, <<"response">>, Resp} ->
            {ok, Resp};
        {ok, ReqType, Args} ->
            case handle_request_type(Pid, ReqId, Db, ReqType, Args) of
                {ok, done} ->
                    ok; % already handled
                {ok, Resp} ->
                    ok = gen_server:call(Pid, {respond, ReqId, Resp});
                {error, Err} ->
                    ok = gen_server:call(Pid, {error, ReqId, Err})
            end,
            handle_request_async(Pid, ReqId, Db);
        {ok, empty_response} ->
            {ok, {[{<<"code">>, 500}, {<<"body">>, <<"">>}]}};
        Error ->
            throw({ape_error, Error})
    end.

handle_request_type(_Pid, _ReqId, Db, <<"open_doc">>, [DocId]) ->
    Doc = couch_httpd_db:couch_doc_open(Db, DocId, nil, []),
    ErlJson = couch_doc:to_json_obj(Doc, []),
    {ok, ErlJson};
handle_request_type(_Pid, _ReqId, Db, <<"save_doc">>, [{DocProps}]) ->
    ErlJson = case proplists:get_value(<<"_id">>, DocProps) of
        undefined ->
            DocId = couch_uuids:new(),
            {[{<<"_id">>, DocId} | DocProps]};
        _DocId ->
            {DocProps}
    end,
    Doc = couch_doc:from_json_obj(ErlJson),
    {ok, Rev} = couch_db:update_doc(Db, Doc, []),
    {ok, {[{id, Doc#doc.id}, {rev, couch_doc:rev_to_str(Rev)}]}};
handle_request_type(Pid, ReqId, Db, <<"delete_doc">>, [{DocProps}]) ->
    DocProps2 = {[{<<"_deleted">>, true} | DocProps]},
    handle_request_type(Pid, ReqId, Db, <<"save_doc">>, [DocProps2]);
handle_request_type(Pid, ReqId, Db, <<"query_view">>, [null, {Args}]) ->
    Keys = proplists:get_value(<<"keys">>, Args),
    QueryArgs = parse_view_args(Args, Keys, map),
    all_docs(Pid, ReqId, Db, null, <<"_all_docs">>, QueryArgs),
    {ok, done};
handle_request_type(Pid, ReqId, Db, <<"query_view">>, [ViewInfo, {Args}]) ->
    [DDocId, ViewName] = ViewInfo,
    Stale = get_stale(Args),
    Reduce = get_reduce(Args),
    Keys = proplists:get_value(<<"keys">>, Args),
    Result = case couch_view:get_map_view(Db, DDocId, ViewName, Stale) of
    {ok, View, Group} ->
        QueryArgs = parse_view_args(Args, Keys, map),
        output_map_view(Pid, ReqId, Db, View, Group, QueryArgs, Keys);
    {not_found, _Reason} ->
        case couch_view:get_reduce_view(Db, DDocId, ViewName, Stale) of
        {ok, ReduceView, Group} ->
            case Reduce of
            false ->
                QueryArgs = parse_view_args(Args, Keys, map_red),
                MapView = couch_view:extract_map_view(ReduceView),
                output_map_view(Pid, ReqId, Db, MapView, Group,
                                                    QueryArgs, Keys);
            _ ->
                QueryArgs = parse_view_args(Args, Keys, reduce),
                output_reduce_view(Pid, ReqId, Db, ReduceView, Group,
                                                    QueryArgs, Keys)
            end;
        {not_found, Reason} ->
            throw({not_found, Reason})
        end
    end.


do_call(Ctx, FName, Args) ->
    handle(Ctx, emonk:call(Ctx, FName, Args)).

do_send(Ctx, Mesg) ->
    handle(Ctx, emonk:send(Ctx, get(emonk_ref), Mesg)).

handle(Ctx, Response) ->
    case Response of
        {ok, Resp} ->
            {ok, Resp};
        {message, Ref, LogMesg} ->
            ?LOG_INFO("Emonk Log :: ~p", [LogMesg]),
            handle(Ctx, emonk:send(Ctx, Ref, true))
    end.


read_js() ->
    FileName = couch_config:get(<<"ape_server">>, <<"source">>),
    case file:read_file(FileName) of
        {ok, Script} ->
            Script;
        {error, Reason} ->
            Fmt = "Failed to read file (~p): ~p",
            Mesg = ?l2b(io_lib:format(Fmt, [Reason, FileName])),
            ?LOG_ERROR(Mesg, []),
            throw({error, Reason})
    end.


get_stale(Args) ->
    case proplists:get_value(<<"stale">>, Args) of
        undefined -> nil;
        <<"ok">> -> ok
    end.

get_reduce(Args) ->
    case proplists:get_value(<<"reduce">>, Args) of
        undefined-> true;
        Bool when is_boolean(Bool) -> Bool
    end.


parse_view_args(Args, Keys, ViewType) ->
    IsMultiGet = is_list(Keys),
    ArgsInit = #view_query_args{
        view_type=ViewType,
        multi_get=IsMultiGet
    },
    QueryArgs = lists:foldl(fun(Other, ArgsAcc) ->
        {K, V} = Other,
        Key = list_to_atom(?b2l(K)),
        try
            couch_httpd_view:validate_view_query(Key, V, ArgsAcc)
        catch
            _:_ ->
                Fmt = "Ignoring invalid view arg: ~p = ~p", 
                io:format(Fmt, [K, V]),
                ArgsAcc
        end
    end, ArgsInit, Args),
    GroupLevel = QueryArgs#view_query_args.group_level,
    case {ViewType, GroupLevel, IsMultiGet} of
        {reduce, exact, true} ->
            QueryArgs;
        {reduce, _, false} ->
            QueryArgs;
        {reduce, _, _} ->
            Msg = <<"Multi-key fetches for reduce must include group=true">>,
            throw({query_parse_error, Msg});
        _ ->
            QueryArgs
    end.


all_docs(Pid, ReqId, Db, null, <<"_all_docs">>, Args) ->
    #view_query_args{
        limit = Limit,
        skip = SkipCount,
        stale = Stale,
        direction = Dir,
        group_level = GroupLevel,
        start_key = StartKey,
        start_docid = StartDocId,
        end_key = EndKey,
        end_docid = EndDocId,
        inclusive_end = Inclusive
    } = Args,
    {ok, Info} = couch_db:get_db_info(Db),
    TotalRowCount = proplists:get_value(doc_count, Info),
    StartId = case is_binary(StartKey) of
        true -> StartKey;
        _ -> StartDocId
    end,
    EndId = case is_binary(EndKey) of
        true -> EndKey;
        _ -> EndDocId
    end,
    FoldAccInit = {Limit, SkipCount, undefined, []},
    UpdateSeq = couch_db:get_update_seq(Db),
    StartResponse = fun(_Req, Etag, RowCount, Offset, _Acc, UpdateSeq) ->
        Obj = {[
            {etag, Etag},
            {row_count, RowCount},
            {offset, Offset},
            {update_seq, UpdateSeq}
        ]},
        R = gen_server:call(Pid, {start_view, ReqId, Obj}),
        {ok, nil, {Offset, nil}}
    end,
    SendRow = fun(_Resp, Db, Doc, IncludeDocs, {Offset, Acc}) ->
        RowObj = couch_httpd_view:view_row_obj(Db, Doc, IncludeDocs),
        Go = gen_server:call(Pid, {send_row, ReqId, RowObj}),
        {Go, {Offset, nil}}
    end,
    FoldlFun = couch_httpd_view:make_view_fold_fun(nil, Args, <<"">>, Db,
        UpdateSeq, TotalRowCount, #view_fold_helper_funs{
            reduce_count = fun couch_db:enum_docs_reduce_to_count/1,
            start_response = StartResponse,
            send_row = SendRow
        }),
    AdapterFun = fun(#full_doc_info{id=Id}=FullDocInfo, Offset, Acc) ->
        case couch_doc:to_doc_info(FullDocInfo) of
        #doc_info{revs=[#rev_info{deleted=false, rev=Rev}|_]} ->
            RevStr = couch_doc:rev_to_str(Rev),
            FoldlFun({{Id, Id}, {[{rev, RevStr}]}}, Offset, Acc);
        #doc_info{revs=[#rev_info{deleted=true}|_]} ->
            {ok, Acc}
        end
    end,
    couch_db:enum_docs(
        Db, AdapterFun, FoldAccInit, [{start_key, StartId}, {dir, Dir},
            {if Inclusive -> end_key; true -> end_key_gt end, EndId}]
    ),
    ok = gen_server:call(Pid, {end_view, ReqId}).


output_map_view(Pid, ReqId, Db, View, Group, Args, Keys) ->
    #view_query_args{
        limit = Limit,
        skip = Skip
    } = Args,
    {ok, NumRows} = couch_view:get_row_count(View),
    StartResponse = fun(Req, Etag, RowCount, Offset, RowAcc, UpdateSeq) ->
        Req2 = nil,
        RowAcc2 = nil,
        {ok, Req2, RowAcc2}
    end,
    SendRow = fun(Resp, Db, {{Key, DocId}, Value}, IncludeDocs, RowAcc) ->
        Go = nil,
        RowAcc2 = nil,
        {Go, RowAcc2}
    end,
    Helpers = #view_fold_helper_funs{
        start_response = StartResponse,
        send_row = SendRow,
        reduce_count=fun couch_view:reduce_to_count/1
    },
    FoldAccInit = {Limit, Skip, undefined, []},
    UpdateSeq = couch_db:get_update_seq(Db),
    Etag = nil,
    FoldFun = couch_httpd_view:make_view_fold_fun(
                    ReqId, Args, Etag, Db, UpdateSeq, NumRows, Helpers
    ),
    {ok, LastRed, FoldResult} = couch_view:fold(
                    View, FoldFun, FoldAccInit, make_key_options(Args)
    ),
    FinalCount = couch_view:reduce_to_count(LastRed),
    finish_view_fold(ReqId, NumRows, FinalCount, FoldResult).


output_reduce_view(Pid, ReqId, Db, View, Group, QueryArgs, Keys) ->
    ok.

finish_view_fold(ReqId, NumRows, FinalCount, FoldResult) ->
    ok.

make_key_options(Args) ->
    ok.
