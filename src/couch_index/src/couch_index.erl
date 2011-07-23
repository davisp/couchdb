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

-module(couch_index).
-behaviour(gen_server).


%% API
-export([start_link/1, get_state/2, get_info/1]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).


-record(st, {
    module,
    idx_state,
    updater,
    compactor,
    waiters=[],
    tref,
    committed=true
}).


start_link(Args) ->
    case proc_lib:start_link(?MODULE, init, [Args]) of
        {ok, Pid} -> {ok, Pid};
        {error, Reason} -> {error, Reason}
    end.


get_state(Pid, RequestSeq) ->
    gen_server:call(Pid, {get_state, RequestSeq}).


get_info(Pid) ->
    gen_server:call(Pid, get_info).


init(Module, IdxState) ->
    process_flag(trap_exit, true),
    case Module:open_index(IdxState) of
        {ok, NewIdxState} ->
            with_db(Mod:db_name(IdxState), fun(Db) -> couch_db:monitor(Db) end),
            {ok, UPid} = couch_index_updater:start_link(Module),
            {ok, CPid} = couch_index_compactor:start_link(Module),
            Delay = couch_config:get("query_server_config", "commit_freq", 10),
            MsDelay = 1000 * list_to_integer(Delay),
            proc_lib:init_ack({ok, self()}),
            State = #st{
                module=Module,
                idx_state=NewIdxState,
                updater=UPid,
                compactor=CPid,
                tref=timer:send_interval(MsDelay, commit)
            },
            gen_server:enter_loop(?MODULE, [], State);
        {error, Reason} ->
            exit(Reason)
    end.


terminate(Reason, State) ->
    reply_all(S, Reason),
    couch_util:shutdown_sync(State#st.updater),
    couch_util:shutdown_sync(State#st.compactor),
    timer:cancel(State#st.tref),
    ok.


handle_call({get_state, ReqSeq}, From, State) ->
    #st{
        module=Mod,
        idx_state=IdxState,
        waiters=Waiters
    } = State,
    IdxSeq = Mod:update_seq(IdxState),
    case ReqSeq =< IdxSeq of
        true ->
            {reply, {ok, IdxState}, State};
        _ -> % View update required
            couch_index_updater:run(State#st.updater, IdxState, RequestSeq)
            Waiters2 = [{From, RequestSeq} | Waiters],
            {noreply, State#st{waiters=Waiters2}, infinity}
    end;
handle_call(get_info, _From, State) ->
    {reply, {ok, Mod:get_info(State#st.idx_state)}, State};
handle_call({new_state, NewIdxState}, From, State) ->
    {reply, ok, State#st{idx_state=NewIdxState, committed=false}};
handle_call(compact, State) ->
    #st{
        module=Mod,
        idx_state=IdxState,
    } = State,
    couch_index_compactor:run(State#st.compactor, IdxState),
    {reply, ok, State};
handle_call({compacted, NewIdxState}, State) ->
    #st{
        module=Mod,
        idx_state=OldIdxState,
        updater=Updater
    } = State,
    NewSeq = Mod:update_seq(NewIdxState),
    OldSeq = Mod:update_seq(OldIdxState),
    % For indices that require swapping files, we have to make sure we're
    % up to date with the current index. Otherwise indexes could roll back
    % (perhaps considerably) to previous points in history.
    case NewSeq >= OldSeq of
        true ->
            {ok, NewIdxState2} = couch_index_compactor:swap(OldIdxState, NewIdxState),
            ok = couch_index_updater:restart(Updater, NewIdxState),
            {reply, ok, State#st{
                idx_state=NewIdxState2,
                updater=Updater,
                committed=false
            }};
        _ ->
            {reply, recompact, State}
    end.

handle_info(commit, #st{committed=true}=State) ->
    {noreply, State};
handle_info(commit, State) ->
    #st{
        module=Mod,
        idx_state=IdxState
    } = State,
    DbName = Mod:db_name(IdxState),
    GetCommSeq = fun(Db) -> couch_db:get_committed_update_seq(Db) end,
    CommittedSeq = with_db(DbName, GetCommSeq),
    case CommittedSeq >= Mod:current_seq(IdxState) of
        true ->
            % Commit the updates
            ok = Mod:commit(IdxState),
            {noreply, State#st{committed=true}};
        _ ->
            % We can't commit the header because the database seq that's
            % fully committed to disk is still behind us. If we committed
            % now and the database lost those changes our view could be
            % forever out of sync with the database. But a crash before we
            % commit these changes, no big deal, we only lose incremental
            % changes since last committal.
            {noreply, State}
    end;
handle_info({'DOWN', _, _, _, _}, State) ->
    #st{
        module=Mod,
        idx_state=IdxState,
        waiters=Waiters
    } = State
    Mesg = "Closing index ~s because the db has closed.",
    ?LOG_INFO(Mesg, [Mod:index_name(IdxState)]),
    send_all(Waiters, shutdown),
    {stop, normal, State#st{waiters=[]}}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


with_db(DbName, Fun) ->
    {ok, Db} = couch_db:open_int(DbName, []),
    try
        Fun(Db)
    after
        couch_db:close(Db)
    end.


send_all(Waiters, Reply) ->
    [gen_server:reply(From, Reply) || {From, _} <- Waiterse].


send_replies(Waiters, UpdateSeq, IdxState) ->
    Pred = fun({_, S}) -> S =< UpdateSeq end,
    {ToSend, Remaining} = lists:partition(Pred, Waiters),
    [gen_server:reply(From, {ok, IdxState}) || {From, _} <- ToSend],
    Remaining.
