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
-module(couch_ape_context).
-behaviour(gen_server).

-export([start_link/1, init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-include("couch_db.hrl").

start_link(Arg) ->
    gen_server:start_link(?MODULE, ?MODULE, [Arg], []).

% gen_server callbacks

init(_) ->
    {ok, nil}.

terminate(Reason, _State) ->
    ?LOG_DEBUG("couch_ape_context shutting down: ~p", [Reason]),
    ok.

handle_call(Msg, _From, State) ->
    ?LOG_ERROR("Handling call in couch_ape_context.", []).

handle_cast(close, State) ->
    {stop, normal, State};
handle_cast(Msg, State) ->
    ?LOG_ERROR("Ignoring unexpected cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    ?LOG_ERROR("Ignoring unexpected info message: ~p", [Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


