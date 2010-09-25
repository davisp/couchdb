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
-module(couch_os_daemons).
-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([info/0]).

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-include("couch_db.hrl").

-define(PORT_OPTIONS, [stream, {line, 1024}, binary, exit_status, hide]).
-define(TIMEOUT, 5000).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

info() ->
    gen_server:call(?MODULE, daemon_info).

init(_) ->
    process_flag(trap_exit, true),
    Fun = fun("os_daemons", _) -> gen_server:cast(?MODULE, config_change) end,
    ok = couch_config:register(Fun),
    Table = ets:new(?MODULE, [private, set, {keypos, 2}]),
    reload_daemons(Table),
    {ok, Table}.

terminate(_Reason, Table) ->
    [stop_port(D) || D <- ets:tab2list(Table)],
    ok.

handle_call(daemon_info, _From, Table) ->
    {reply, {ok, ets:tab2list(Table)}, Table};
handle_call(Msg, From, Table) ->
    ?LOG_ERROR("Unknown call message to ~p from ~p: ~p", [?MODULE, From, Msg]),
    {stop, error, Table}.

handle_cast(config_change, Table) ->
    reload_daemons(Table),
    {noreply, Table};
handle_cast(stop, Table) ->
    {stop, normal, Table};
handle_cast(Msg, Table) ->
    ?LOG_ERROR("Unknown cast message to ~p: ~p", [?MODULE, Msg]),
    {stop, error, Table}.

handle_info({'EXIT', Port, Reason}, Table) ->
    case ets:lookup(Table, Port) of
        [] ->
            ?LOG_INFO("Port ~p exited after stopping: ~p~n", [Port, Reason]);
        [#daemon{errors=stopping}] ->
            true = ets:delete(Table, Port);
        [#daemon{name=Name, errors=halted}] ->
            ?LOG_ERROR("Halted daemon process: ~p", [Name]);
        [D] ->
            ?LOG_ERROR("Invalid port state at exit: ~p", [D])
    end,
    {noreply, Table};
handle_info({Port, closed}, Table) ->
    handle_info({Port, {exit_status, closed}}, Table);
handle_info({Port, {exit_status, Status}}, Table) ->
    case ets:lookup(Table, Port) of
        [] ->
            ?LOG_ERROR("Unknown port ~p exiting ~p", [Port, Status]),
            {stop, {error, unknown_port_died, Status}, Table};
        [#daemon{errors=stopping}=D] ->
            % The configuration changed and this daemon is no
            % longer needed.
            ?LOG_DEBUG("Port ~p shut down.", [D#daemon.name]),
            true = ets:delete(Table, Port),
            {noreply, Table};
        [D] ->
            % Port died for unknown reason. Check to see if it's
            % died too many times or if we should boot it back up.
            case should_halt([now() | D#daemon.errors]) of
                {true, _} ->
                    % Halting the process. We won't try and reboot
                    % until the configuration changes.
                    Fmt = "Daemon ~p halted with exit_status ~p",
                    ?LOG_ERROR(Fmt, [D#daemon.name, Status]),
                    true = ets:insert(Table, D#daemon{errors=halted, buf=nil}),
                    {noreply, Table};
                {false, Errors} ->
                    % We're guessing it was a random error, this daemon
                    % has behaved so we'll give it another chance.
                    Fmt = "Daemon ~p is being rebooted after exit_status ~p",
                    ?LOG_INFO(Fmt, [D#daemon.name, Status]),
                    true = ets:delete(Table, Port),
                    {ok, Port2} = start_port(D#daemon.cmd),
                    true = ets:insert(Table, D#daemon{
                        port=Port2, kill=undefined, errors=Errors, buf=[]
                    }),
                    {noreply, Table}
            end;
        _Else ->
            throw(error)
    end;
handle_info({Port, {data, {noeol, Data}}}, Table) ->
    [#daemon{buf=Buf}=D] = ets:lookup(Table, Port),
    true = ets:insert(Table, D#daemon{buf=[Data | Buf]}),
    {noreply, Table};
handle_info({Port, {data, {eol, Data}}}, Table) ->
    [#daemon{buf=Buf}=D] = ets:lookup(Table, Port),
    Line = lists:reverse(Buf, Data),
    % The first line echoed back is the kill command
    % for when we go to get rid of the port. Lines after
    % that are considered part of the stdio API.
    case D#daemon.kill of
        undefined ->
            true = ets:insert(Table, D#daemon{kill=?b2l(Line), buf=[]});
        _Else ->
            case (catch ?JSON_DECODE(Line)) of
                {invalid_json, Rejected} ->
                    ?LOG_ERROR("Ignoring OS daemon request: ~p", [Rejected]);
                JSON ->
                    handle_port_message(D, JSON)
            end,
            true = ets:insert(Table, D#daemon{buf=[]})
    end,
    {noreply, Table};
handle_info({Port, Error}, Table) ->
    ?LOG_ERROR("Unexpectd message from port ~p: ~p", [Port, Error]),
    stop_port(Port),
    {noreply, Table};
handle_info(Msg, Table) ->
    ?LOG_ERROR("Unexpected info message to ~p: ~p", [?MODULE, Msg]),
    {stop, error, Table}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% Internal API

%
% Port management helpers
%

start_port(Command) ->
    PrivDir = couch_util:priv_dir(),
    Spawnkiller = filename:join(PrivDir, "couchspawnkillable"),
    Port = open_port({spawn, Spawnkiller ++ " " ++ Command}, ?PORT_OPTIONS),
    {ok, Port}.

stop_port(#daemon{port=Port, kill=undefined}=D) ->
    ?LOG_ERROR("Stopping daemon without a kill command: ~p", [D#daemon.name]),
    catch port_close(Port);
stop_port(#daemon{port=Port}=D) ->
    ?LOG_DEBUG("Stopping daemon: ~p", [D#daemon.name]),
    os:cmd(D#daemon.kill),
    catch port_close(Port).

handle_port_message(#daemon{port=Port}, [<<"get">>, Section]) ->
    KVs = couch_config:get(Section),
    Data = lists:map(fun({K, V}) -> {?l2b(K), ?l2b(V)} end, KVs),
    JsonData = ?JSON_ENCODE({Data}),
    port_command(Port, JsonData ++ "\n");
handle_port_message(#daemon{port=Port}, [<<"get">>, Section, Key]) ->
    Value = couch_config:get(Section, Key, null),
    port_command(Port, ?JSON_ENCODE(?l2b(Value)) ++ "\n");
handle_port_message(#daemon{name=Name}, [<<"log">>, Msg]) ->
    ?LOG_INFO("Daemon ~p :: ~p", [Name, Msg]);
handle_port_message(#daemon{name=Name}, Else) ->
    ?LOG_ERROR("Daemon ~p made invalid request: ~p", [Name, Else]).

%
% Daemon management helpers
%

reload_daemons(Table) ->
    Configured = lists:sort(couch_config:get("os_daemons")),
    MSpec = {daemon, '_', '$1', '$2', '_', '_', '_'},
    Running = lists:sort([
        {Name, Cmd} || [Name, Cmd] <- ets:match(Table, MSpec)
    ]),
    ok = stop_os_daemons(Table, find_to_stop(Configured, Running, [])),
    ok = boot_os_daemons(Table, find_to_boot(Configured, Running, [])),
    ok.

stop_os_daemons(_Table, []) ->
    ok;
stop_os_daemons(Table, [{Name, Cmd} | Rest]) ->
    [[Port]] = ets:match(Table, {daemon, '$1', Name, Cmd, '_', '_', '_'}),
    [D] = ets:lookup(Table, Port),
    case D#daemon.errors of
        halted ->
            ets:delete(Table, Port);
        _ ->
            stop_port(D),
            true = ets:insert(Table, D#daemon{errors=stopping, buf=nil})
    end,
    stop_os_daemons(Table, Rest).
    
boot_os_daemons(_Table, []) ->
    ok;
boot_os_daemons(Table, [{Name, Cmd} | Rest]) ->
    {ok, Port} = start_port(Cmd),
    true = ets:insert(Table, #daemon{port=Port, name=Name, cmd=Cmd}),
    boot_os_daemons(Table, Rest).
    
% Elements unique to the configured set need to be booted.
find_to_boot([], _Rest, Acc) ->
    % Nothing else configured.
    Acc;
find_to_boot([D | R1], [D | R2], Acc) ->
    % Elements are equal, daemon already running.
    find_to_boot(R1, R2, Acc);
find_to_boot([D1 | R1], [D2 | _]=A2, Acc) when D1 < D2 ->
    find_to_boot(R1, A2, [D1 | Acc]);
find_to_boot(A1, [_ | R2], Acc) ->
    find_to_boot(A1, R2, Acc);
find_to_boot(Rest, [], Acc) ->
    % No more candidates for already running. Boot all.
    Rest ++ Acc.

% Elements unique to the running set need to be killed.
find_to_stop([], Rest, Acc) ->
    % The rest haven't been found, so they must all
    % be ready to die.
    Rest ++ Acc;
find_to_stop([D | R1], [D | R2], Acc) ->
    % Elements are equal, daemon already running.
    find_to_stop(R1, R2, Acc);
find_to_stop([D1 | R1], [D2 | _]=A2, Acc) when D1 < D2 ->
    find_to_stop(R1, A2, Acc);
find_to_stop(A1, [D2 | R2], Acc) ->
    find_to_stop(A1, R2, [D2 | Acc]);
find_to_stop(_, [], Acc) ->
    % No more running daemons to worry about.
    Acc.

should_halt(Errors) ->
    RetryTimeCfg = couch_config:get("os_daemon_settings", "retry_time", "5"),
    RetryTime = list_to_integer(RetryTimeCfg),

    Now = now(),
    RecentErrors = lists:filter(fun(Time) ->
        timer:now_diff(Now, Time) =< RetryTime * 1000000
    end, Errors),

    RetryCfg = couch_config:get("os_daemon_settings", "max_retries", "3"),
    Retries = list_to_integer(RetryCfg),

    {length(RecentErrors) >= Retries, RecentErrors}.
