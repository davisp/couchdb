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
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).


-record(idx, {
    type,
    dbname,
    ddocid,
    sig,
    curr_seq,
    purge_seq,
    init_args,
    updater=nil,
    compactor=nil,
    commit_pending,
    state
}).


start_link(DbName) ->
    case proc_lib:start_link(?MODULE, init, [Opts]) of
        {ok, Pid} -> {ok, Pid};
        {error, Reason} -> {error, Reason}
    end.


init(InitArgs) ->
    process_flag(trap_exit, true),
    proc_lib:init_ack({ok, self()}),
    gen_server:enter_loop(?MODULE, [], nil).


terminate(_Reason, _State) ->
    ok.


handle_call(_Msg, _From, State) ->
    {reply, {ok, true}, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Msg, State) ->
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

