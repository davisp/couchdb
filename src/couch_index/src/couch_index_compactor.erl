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

-module(couch_index_compactor).
-behaviour(gen_server).


%% API
-export([start_link/1, run/2, swap/3]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).


-record(st, {
    idx,
    mod,
    pid=nil
}).


start_link({Index, Module}) ->
    gen_server:start_link(?MODULE, [{Index, Module}], []).


run(Pid, IdxState) ->
    gen_server:call(Pid, {compact, IdxState}).


swap(Pid, OldIdxState, NewIdxState) ->
    gen_server:call(Pid, {swap, OldIdxState, NewIdxState}).


init({Index, Module}) ->
    process_flag(trap_exit, true),
    {ok, #st{idx=Index, mod=Module}}.


terminate(_Reason, State) ->
    couch_util:shutdown_sync(State#st.pid),
    ok.


handle_call({compact, _}, _From, #st{pid=Pid}=State) when is_pid(Pid) ->
    {reply, ok, State};
handle_call({compact, IdxState}, _From, State) ->
    Pid = spawn_link(fun() -> compact(State#st.mod, IdxState) end),
    {reply, ok, State#st{pid=Pid}};
handle_call({swap, OldIdxState, NewIdxState}, _From, State) ->
    #st{mod=Mod} = State,
    {ok, NewIdxState1} = Mod:compaction_swap(OldIdxState, NewIdxState),
    {reply, {ok, NewIdxState1}, State}.


handle_cast(_Mesg, State) ->
    {stop, unknown_cast, State}.


handle_info({'EXIT', Pid, {compacted, IdxState}}, #st{pid=Pid}=State) ->
    case gen_server:call(State#st.idx, {compacted, IdxState}) of
        recompact ->
            Fun = fun() -> compact(State#st.mod, IdxState, [recompact]) end,
            Pid2 = spawn_link(Fun),
            {noreply, State#st{pid=Pid2}};
        ok ->
            {noreply, State#st{pid=nil}}
    end;
handle_info(_Mesg, State) ->
    {stop, unknown_info, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


compact(Mod, IdxState) ->
    compact(Mod, IdxState, []).

compact(Mod, IdxState, Opts) ->
    {ok, NewIdxState} = Mod:compact(IdxState, Opts),
    exit({compacted, NewIdxState}).



