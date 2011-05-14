#!/usr/bin/env escript
%% -*- erlang -*-

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

main(_) ->
    test_util:init_code_path(),
    etap:plan(22),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.

loop() ->
    receive
    {add, From} ->
        Resp = couch_task_status:add_task("type", "task", "init"),
        From ! {ok, self(), Resp},
        loop();
    {add, Id, From} ->
        Resp = couch_task_status:add_task(Id, "type", "task", "init"),
        From ! {ok, self(), Resp},
        loop();
    {update, Status, From} ->
        Resp = couch_task_status:update(Status),
        From ! {ok, self(), Resp},
        loop();
    {set, Id, Status, From} ->
        Resp = couch_task_status:set(Id, Status),
        From ! {ok, self(), Resp},
        loop();
    {remove, Id, From} ->
        Resp = couch_task_status:remove(Id),
        From ! {ok, self(), Resp},
        loop();
    {update_frequency, Msecs, From} ->
        Resp = couch_task_status:set_update_frequency(Msecs),
        From ! {ok, self(), Resp},
        loop();
    {done, From} ->
        From ! {ok, self(), ok}
    end.

call(Pid, Command) ->
    Pid ! {Command, self()},
    wait(Pid).

call(Pid, Command, Arg) ->
    Pid ! {Command, Arg, self()},
    wait(Pid).

call(Pid, Command, Arg1, Arg2) ->
    Pid ! {Command, Arg1, Arg2, self()},
    wait(Pid).

wait(Pid) ->
    receive
        {ok, Pid, Msg} -> Msg
    after 1000 ->
        throw(timeout_error)
    end.

close(Pid) ->
    Before = length(couch_task_status:all()),
    call(Pid, done),
    wait_for_removal(Before, 3).

wait_for_removal(_, 0) ->
    throw(timeout_error);
wait_for_removal(N, Tries) ->
    Curr = length(couch_task_status:all()),
    case Curr < N of
        true ->
            Curr;
        _ ->
            timer:sleep(333),
            wait_for_removal(N, Tries-1)
    end.    

num_tasks() ->
    length(couch_task_status:all()).

status(TaskId) when is_pid(TaskId) ->
    status(list_to_binary(pid_to_list(TaskId)));
status(TaskId) when is_atom(TaskId) ->
    status(list_to_binary(atom_to_list(TaskId)));
status(TaskId) when is_binary(TaskId) ->
    [TaskInfo] = lists:foldl(fun(Props, Acc) ->
        case couch_util:get_value(id, Props) of
            TaskId -> [Props | Acc];
            _ -> Acc
        end
    end, [], couch_task_status:all()),
    couch_util:get_value(status, TaskInfo).


test() ->
    {ok, TaskStatusPid} = couch_task_status:start_link(),

    TaskUpdater = fun() -> loop() end,

    % create three updaters
    Pid1 = spawn(TaskUpdater),
    Pid2 = spawn(TaskUpdater),
    Pid3 = spawn(TaskUpdater),
    Pid4 = spawn(TaskUpdater),

    % First task, can only add once
    ok = call(Pid1, add),
    etap:is(num_tasks(), 1, "Started a task"),
    etap:is(status(Pid1), <<"init">>, "Task status was set to 'init'."),

    call(Pid1,update,"running"),
    etap:is(status(Pid1), <<"running">>, "Status updated to 'running'."),

    etap:is(
        call(Pid1, add),
        {add_task_error, already_registered},
        "Unable to register multiple tasks for a single Pid."
    ),

    
    % Second task
    call(Pid2,add),
    etap:is(num_tasks(), 2, "Started a second task."),
    etap:is(status(Pid2), <<"init">>, "Second task's status is 'init'."),

    call(Pid2, update, "running"),
    etap:is(status(Pid2), <<"running">>, "Second task's status is 'running'."),


    % Task with update limit.
    call(Pid3, add),
    etap:is(num_tasks(), 3, "Registered a third task."),
    etap:is(status(Pid3), <<"init">>, "Third tasks's status is 'init'."),

    call(Pid3, update, "running"),
    etap:is(status(Pid3), <<"running">>, "Third task's status is 'running'."),

    call(Pid3, update_frequency, 500),
    call(Pid3, update, "running2"),
    etap:is(status(Pid3), <<"running2">>, "Third task's status is 'running2'."),

    call(Pid3, update, "skip this update"),
    etap:is(status(Pid3), <<"running2">>, "Third task is still 'running2'."),

    call(Pid3, update_frequency, 0),
    call(Pid3, update, "running3"),
    etap:is(status(Pid3), <<"running3">>, "Status update after limit reset."),


    % Multiple tasks per process.
    call(Pid4, add, task_id_1),
    etap:is(num_tasks(), 4, "Registered a fourth task with id."),
    call(Pid4, add, task_id_2),
    etap:is(num_tasks(), 5, "Registered a fifth task with id."),
    call(Pid4, add, task_id_3),
    etap:is(num_tasks(), 6, "Registered a sixth task with id."),

    call(Pid4, set, task_id_2, <<"new status">>),
    etap:is(status(task_id_2), <<"new status">>, "Status updated by task id."),
    
    call(Pid4, remove, task_id_2),
    etap:is(num_tasks(), 5, "Removed a task by id."),
    
    % Process death clears out all remaining tasks.
    etap:is(close(Pid4), 3, "Removed both tasks left in Pid4"),
    
    
    % Closing tasks
    etap:is(close(Pid1), 2, "First task finished."),
    etap:is(close(Pid2), 1, "Second task finished."),
    etap:is(close(Pid3), 0, "Third task finished."),

    erlang:monitor(process, TaskStatusPid),
    couch_task_status:stop(),
    receive
        {'DOWN', _, _, TaskStatusPid, _} ->
            ok
    after
        1000 ->
            throw(timeout_error)
    end,

    ok.
