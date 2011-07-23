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

-module(couch_index_server).
-behaviour(gen_server).

-export([start_link/0, get_index/2]).

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-define(TABLE, couchdb_indexes_by_sig).

-record(idx, {
    sig,
    pid,
    dbname
}).

-record(st, {
    pids=gb_trees:empty(),
    waiters=gb_trees:empty()
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


get_index(Module, DbName, Sig, IdxState) ->
    case ets:lookup(?TABLE, Sig) of
        [Idx] ->
            {ok, Idx#idx.pid}
        [] ->
            gen_server:call(?MODULE, {get_index, {Module, DbName, Sig, State}})
    end.


init([]) ->
    process_flag(trap_exit, true),
    couch_config:register(fun config_change/2),
    couch_db_update_notifier:start_link(fun update_notify/2),
    ets:new(?TABLE, [protected, set, named_table, [{keypos, #idx.pid}]]),
    {ok, #st{}}.


terminate(_Reason, _Srv) ->
    Pids = [Pid || #idx{pid=Pid} <- ets:tab2list(?TABLE)],
    lists:map(fun couch_util:shutdown_sync/1, Pids),
    ok.


handle_call({get_index, {_, _, Sig, _}=Args}, From, State) ->
    % Check the ets table again in case it was
    % just inserted.
    case ets:lookup(?TABLE, Sig) of
        [#idx{pid=Pid}] ->
            {reply, {ok, Pid}, State};
        [] ->
            {ok, Waiters} = spawn_index(Args, From, State#st.waiters),
            {noreply, State#st{waiters=Waiters}}
    end;
handle_call({async_open, Response}, _From, State) ->
    {Resp, Pids} = case Response of
        {ok, Pid, Sig, DbName} ->
            link(Pid),
            ets:insert(?TABLE, #idx{sig=Sig, pid=Pid, dbname=DbName}),
            {{ok, Pid}, gb_trees:insert(Pid, Sig, State#st.pids)};
        {error, Reason, Sig} ->
            {{error, Reason}, State#st.pids}
    end,
    Waiters = gb_trees:lookup(Sig, State#st.waiters),
    [gen_server:reply(From, Resp) || From <- Waiters],
    {reply, ok, #st{
        pids=Pids,
        waiters=gb_trees:delete(Sig, State#st.waiters)
    }};
handle_call({reset_indexes, DbName}, _From, State) ->
    reset_indexes(State, DbName),
    {reply, ok, Server};


handle_cast({reset_indexes, DbName}, State) ->
    reset_indexes(State, DbName),
    {noreply, Server}.


handle_info({'EXIT', FromPid, Reason}, Pids) ->
    case gb_trees:lookup(FromPid, Pids) of
        {value, Sig} ->
            ets:delete(?TABLE, Sig);
        none when Reason == normal ->
            ok;
        none ->
            Mesg = "Unknown linked process ~p died: ~p",
            ?LOG_ERROR(Mesg, [FromPid, Reason]),
            exit(Reason)
    end,
    {noreply, Server}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


spawn_index({_, _, Sig, _}=Args, From, Waiters) ->
    case gb_trees:lookup(Sig, Waiters) of
        none ->
            spawn_link(fun() -> new_index(Args) end),
            {ok, gb_trees:insert(Sig, [From])};
        {value, WaiterList} ->
            {ok, gb_trees:update(Sig, [From | WaiterList])}
    end.


new_index({Module, DbName, Sig, IdxState}=Args) ->
    Mesg = "Opening index for ~s: ~s",
    ?LOG_DEBUG(Mesg, [DbName, Module:index_name(IdxState)]),
    case couch_index:start_link(Args) of
        {ok, NewPid} ->
            gen_server:call(?MODULE, {async_open, {ok, Pid, Sig, DbName}}),
            unlink(NewPid);
        Error ->
            gen_server:call(?MODULE, {async_open, {error, Error, Sig}})
    end.


do_reset_indexes(DbName) ->
    ?LOG_DEBUG("Resetting indexes for: ~s", [DbName]),
    Sigs = ets:match(?TABLE, #idx{dbname=DbName, sig='$1', _='_'}),
    lists:foreach(fun(Sig) ->
        [Idx] = ets:lookup(?TABLE, Sig),
        couch_index:delete(Idx#idx.pid),
        ets:delete(?TABLE, Sig)
    end, Sigs).


config_change("couchdb", "index_dir") ->
    exit(whereis(?MODULE), config_change).


update_notify({deleted, DbName}) ->
    gen_server:cast(?MODULE, {reset_indexes, DbName});
update_notify({created, DbName}) ->
    gen_server:cast(?MODULE, {reset_indexes, DbName});
update_notify(_) ->
    ok.

