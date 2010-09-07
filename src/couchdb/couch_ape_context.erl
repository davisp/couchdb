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
            case handle_request_type(Db, ReqType, Args) of
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

handle_request_type(Db, <<"open_doc">>, [DocId]) ->
    Doc = couch_httpd_db:couch_doc_open(Db, DocId, nil, []),
    ErlJson = couch_doc:to_json_obj(Doc, []),
    {ok, ErlJson};
handle_request_type(Db, <<"save_doc">>, [{DocProps}]) ->
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
handle_request_type(Db, <<"delete_doc">>, [{DocProps}]) ->
    DocProps2 = {[{<<"_deleted">>, true} | DocProps]},
    handle_request_type(Db, <<"save_doc">>, [DocProps2]).

do_call(Ctx, FName, Args) ->
    handle(Ctx, emonk:call(Ctx, FName, Args)).

do_send(Ctx, Mesg) ->
    handle(Ctx, emonk:send(Ctx, get(emonk_ref), Mesg)).

handle(Ctx, Response) ->
    %io:format("Response: ~p~n", [Response]),
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

