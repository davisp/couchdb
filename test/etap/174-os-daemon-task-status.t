#!/usr/bin/env escript
%% -*- erlang -*-

% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License.  You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
% License for the specific language governing permissions and limitations under
% the License.

-record(daemon, {
    port,
    name,
    cmd,
    kill,
    status=running,
    cfg_patterns=[],
    errors=[],
    buf=[]
}).

daemon_name() ->
    "wheee".

daemon_cmd() ->
    test_util:build_file("test/etap/test_os_task_status").

main(_) ->
    test_util:init_code_path(),

    etap:plan(4),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.

test() ->
    couch_config:start_link(test_util:config_files()),
    {ok, _} = couch_task_status:start_link(),
    couch_os_daemons:start_link(),

    % Register daemon, wait for it to boot.
    couch_config:set("os_daemons", daemon_name(), daemon_cmd(), false),
    timer:sleep(500),

    {ok, [D1]} = couch_os_daemons:info([table]),
    etap:is(D1#daemon.status, running, "Daemon booted successfully."),

    % Daemon immediately creates a task status.
    [Task1] = couch_task_status:all(),
    InitStatus = proplists:get_value(status, Task1),
    etap:is(InitStatus, <<"status">>, "Task was registerd with status."),
    
    % Daemon sleeps a second and updates the status.
    timer:sleep(1000),
    [Task2] = couch_task_status:all(),
    NewStatus = proplists:get_value(status, Task2),
    etap:is(NewStatus, <<"new status">>, "Task status was updated."),
    
    % Daemon waits another second and then kills the task.
    timer:sleep(1000),
    etap:is(couch_task_status:all(), [], "Task was removed."),
    
    ok.

wait_for_start(0) ->
    throw({error, wait_for_start});
wait_for_start(N) ->
    case couch_os_daemons:info([table]) of
        {ok, []} ->
            timer:sleep(200),
            wait_for_start(N-1);
        _ ->
            timer:sleep(500)
    end.
