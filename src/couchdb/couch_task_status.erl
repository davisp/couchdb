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

-module(couch_task_status).
-behaviour(gen_server).

% This module allows is used to track the status of long running tasks.
% Long running tasks register (add_task/3) then update their status (update/1)
% and the task and status is added to tasks list. When the tracked task dies
% it will be automatically removed the tracking. To get the tasks list, use the
% all/0 function

-export([start_link/0, stop/0]).
-export([add_task/3, add_task/4]).
-export([update/1, update/2, set/2, set/3, remove/1]).
-export([set_update_frequency/1]).
-export([all/0]).

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-import(couch_util, [to_binary/1]).

-include("couch_db.hrl").

-record(task, {id, pid, type, task, status}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


stop() ->
    gen_server:cast(?MODULE, stop).


all() ->
    gen_server:call(?MODULE, all).


add_task(Type, TaskName, StatusText) ->
    add_task(self(), Type, TaskName, StatusText).

add_task(Id, Type, TaskName, StatusText) ->
    put(task_status_update, {{0, 0, 0}, 0}),
    Msg = {
        add_task,
        #task{
            id=Id,
            type=to_binary(Type),
            task=to_binary(TaskName),
            status=to_binary(StatusText)
        }
    },
    gen_server:call(?MODULE, Msg).


set_update_frequency(Msecs) ->
    put(task_status_update, {{0, 0, 0}, Msecs * 1000}).


update(StatusText) ->
    set(self(), "~s", [StatusText]).

update(Format, Data) ->
    set(self(), Format, Data).


set(Id, Data) ->
    set(Id, "~s", [Data]).

set(Id, Format, Data) ->
    {LastUpdateTime, Frequency} = get(task_status_update),
    case timer:now_diff(Now = now(), LastUpdateTime) >= Frequency of
    true ->
        put(task_status_update, {Now, Frequency}),
        Msg = ?l2b(io_lib:format(Format, Data)),
        gen_server:cast(?MODULE, {update_status, Id, Msg});
    false ->
        ok
    end.


remove(Id) ->
    gen_server:cast(?MODULE, {remove_task, Id}).


init([]) ->
    % read configuration settings and register for configuration changes
    ets:new(?MODULE, [ordered_set, protected, named_table, {keypos, #task.id}]),
    {ok, nil}.


terminate(_Reason,_State) ->
    ok.


handle_call({add_task, #task{}=Task0}, {From, _}, Server) ->
    Task = Task0#task{pid=From},
    case ets:lookup(?MODULE, Task#task.id) of
    [] ->
        true = ets:insert(?MODULE, Task),
        erlang:monitor(process, From),
        {reply, ok, Server};
    [_] ->
        {reply, {add_task_error, already_registered}, Server}
    end;
handle_call(all, _, Server) ->
    All = [
        [
            {id, to_binary(Task#task.id)},
            {pid, ?l2b(pid_to_list(Task#task.pid))},
            {type, Task#task.type},
            {task, Task#task.task},
            {status, Task#task.status}
        ]
        ||
        Task <- ets:tab2list(?MODULE)
    ],
    {reply, All, Server}.


handle_cast({update_status, Id, StatusText}, Server) ->
    case ets:lookup(?MODULE, Id) of
        [#task{id=Id}=Task] ->
            Fmt = "New task status for ~s: ~s",
            ?LOG_DEBUG(Fmt, [Task#task.task, StatusText]),
            true = ets:insert(?MODULE, Task#task{status=StatusText});
        [] ->
            Fmt = "Update to unknown task ~s: ~s",
            ?LOG_DEBUG(Fmt, [Id, StatusText])
    end,
    {noreply, Server};
handle_cast({remove_task, Id}, Server) ->
    ets:delete(?MODULE, Id),
    {noreply, Server};
handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, Server) ->
    lists:foreach(fun([Id]) ->
        ets:delete(?MODULE, Id)
    end, ets:match(?MODULE, #task{id='$1', pid=Pid, _='_'})),
    {noreply, Server}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

