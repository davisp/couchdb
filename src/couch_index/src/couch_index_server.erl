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

-module(couch_view).
-behaviour(gen_server).

-export([start_link/0]).
-export([get_index/3]).

-export([cleanup_index_files/1]).


-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).


-record(st, {root_dir, indexes, names}).
-record(idxname, {dbname, ddocid, type}).
-record(idx, {pid, name}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
    process_flag(trap_exit, true),
    couch_config:register(fun config_change/2),
    couch_db_update_notifier:start_link(fun db_update/1),
    RootDir = couch_config:get("couchdb", "index_dir"),
    ok = couch_file:init_delete_dir(RootDir),

    {ok, #st{
        root_dir = RootDir,
        indexes = ets:new(indexes, [set, #idx.pid]),
        names = ets:new(by_db, [bag, {keypos, #idx.name}])
    }}.


terminate(_Reason, State) ->
    Pids = ets:tab2list(State#st.indexes),
    [couch_util:shutdown_sync(P) || {P, _} <- Pids],
    ok.


handle_call({open, IdxName}, From, State) ->
    case ets:lookup(State#st.names, IdxName) of
        [] ->
            #idxname{dbname=DbName, ddocid=DDocId, type=Type} = IdxName,
            case couch_index:open(DbName, DDocId, Type) of
                {ok, Pid} ->
                    add_index(State, #idx{pid=Pid, name=IdxName}),
                    {reply, {ok, Pid}, State};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        [Idx] ->
            {reply, {ok, Idx#idx.pid}, State}
    end.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State) ->
    % Remove indexer.
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


config_change("couchdb", "index_dir") ->
    exit(whereis(?MODULE), config_change).


db_update({created, DbName}) ->
    gen_server:cast(?MODULE, {reset_indexes, DbName});
db_update({deleted, DbName}) ->
    gen_server:cast(?MODULE, {reset_indexes, DbName});
db_update(_) ->
    ok.


reset_indexes(State, DbName) ->
    Pattern = #idx{
        pid = '$1',
        name = #idxname{name=DbName, _='_'},
        _ = '_'
    },
    Pids = ets:match(State#st.by_db, Pattern),
    [couch_index:reset(P) || P <- Pids].


add_index(State, Idx) ->
    ets:insert(State#st.indexes, Idx),
    ets:insert(State#st.by_db, Idx).


del_index(State, Idx) ->
    ets:delete(State#st.indexes, Idx#idx.pid),
    ets:delete_object(State#st.names, Idx).

    