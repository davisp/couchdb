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

-module(couch_index_updater).
-behaviour(gen_server).


%% API
-export([start_link/1, run/2]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).


-record(st, {
    idx,
    mod,
    pid=nil
}).


start_link(Index, Module) ->
    gen_server:start_link(?MODULE, [{Index, Module}], []).


run(Pid, IdxState) ->
    gen_server:call(Pid, {update, IdxState}).


restart(Pid, IdxState) ->
    gen_server:call(Pid, {restart, IdxState}).


init({Index, Module}) ->
    process_flag(trap_exit, true),
    {ok, #st{idx=Index, mod=Module}}.


terminate(_Reason, State) ->
    couch_util:shutdown_sync(State#st.pid),
    ok.


handle_call({update, IdxState}, _From, #st{pid=Pid}=State) when is_pid(Pid) ->
    {reply, ok, State};
handle_call({update, IdxState}, _From, State) ->
    Pid = spawn_link(fun() -> update(State#st.mod, IdxState) end),
    {reply, ok, State#st{pid=Pid}}.


handle_cast(_Mesg, State) ->
    {stop, unknown_cast, State}.


handle_info({'EXIT', Pid, {updated, IdxState}}, #st{pid=Pid}=State) ->
    ok = gen_server:call(State#st.idx, {new_state, IdxState}),
handle_info(_Mesg, State) ->
    {stop, unknown_info, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


update(Mod, IdxState) ->
    % Need to pull in code from couch_view_updater.erl
    ok.



