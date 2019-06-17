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

-module(couch_views_server).


-behaviour(gen_server).


-export([
    start_link/0
]).


-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-define(MAX_WORKERS, 100).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_) ->
    process_flag(trap_exit, true),
    couch_views_jobs:set_timeout(),
    State = #{
        workers => [],
        num_workers => num_workers()
    },
    {ok, spawn_workers(State)}.


terminate(_, _St) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info({'EXIT', Pid, Reason}, State) ->
    #{workers := Workers} = State,
    case Workers -- [Pid] of
        Workers ->
            LogMsg = "~p : unknown process ~p exited with ~p",
            couch_log:error(LogMsg, [?MODULE, Pid, Reason]),
            {stop, {unknown_pid_exit, Pid}, State};
        NewWorkers ->
            if Reason == normal -> ok; true ->
                LogMsg = "~p : indexer process ~p exited with ~p",
                couch_log:error(LogMsg, [?MODULE, Pid, Reason])
            end,
            {noreply, spawn_workers(State#{workers := NewWorkers})}
    end;

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


spawn_workers(State) ->
    #{
        workers := Workers,
        num_workers := NumWorkers
    } = State,
    case length(Workers) < NumWorkers of
        true ->
            Pid = couch_views_indexer:spawn_link(),
            spawn_workers(State#{workers := [Pid | Workers]});
        false ->
            State
    end.


num_workers() ->
    config:get_integer("couch_views", "max_workers", ?MAX_WORKERS).
