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
-module(couch_ape_server).
-behaviour(gen_server).

-export([start_link/0, init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-export([get_context/4]).

-include("couch_db.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_context(DbName, DDocId, DDocRev, DDoc) ->
    gen_server:call(?MODULE, {get_ctx, {DbName, DDocId, DDocRev, DDoc}}).

% gen_server callbacks

init(_) ->
    process_flag(trap_exit, true),
    Contexts = ets:new(?MODULE, [private, set]),
    {ok, Contexts}.

terminate(Reason, _Contexts) ->
    ?LOG_DEBUG("couch_ape_server shutting down: ~p~n", [Reason]),
    ok.

handle_call({get_ctx, Key}, _From, Contexts) ->
    case ets:lookup(Contexts, Key) of
        [{Key, Pid}] ->
            {reply, {ok, Pid}, Contexts};
        [] ->
            {ok, Pid} = couch_ape_context:start_link(Key),
            true = ets:insert(Contexts, {Key, Pid}),
            {reply, {ok, Pid}, Contexts}
    end.

handle_cast(close, Contexts) ->
    {stop, normal, Contexts};
handle_cast(Msg, Contexts) ->
    ?LOG_ERROR("Ignoring unexpected cast message: ~p~n", [Msg]),
    {noreply, Contexts}.

handle_info({'EXIT', Pid, Status}, Contexts) ->
    case ets:match(Contexts, {'$1', Pid}) of
        [Key] ->
            ?LOG_INFO("couch_ape_context died: ~p => ~p", [Pid, Status]),
            true = ets:delete(Contexts, Key);
        [] ->
            Fmt = "Unknown pid dying under couch_ape_server: ~p => ~p",
            ?LOG_INFO(Fmt, [Pid, Status]),
            ok
    end,
    {noreply, Contexts}.

code_change(_OldVsn, Contexts, _Extra) ->
    {ok, Contexts}.


