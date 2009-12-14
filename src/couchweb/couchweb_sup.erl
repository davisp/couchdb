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

-module(couchweb_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    BindAddress = couch_config:get("httpd", "bind_address", any),
    Port = couch_config:get("httpd", "wm_port", "5983"),
    DispatchFile = filename:join(code:priv_dir("couchweb"), "dispatch.conf"),
    {ok, Dispatch} = file:consult(DispatchFile),

    WebConfig = [
        {ip, BindAddress},
        {backlog, 1000},
        {port, Port},
        {log_dir, "priv/log"},
        {dispatch, Dispatch}
    ],
    Web = {
        webmachine_mochiweb,
        {webmachine_mochiweb, start, [WebConfig]},
        permanent, 5000, worker, dynamic
    },
    Processes = [Web],
    {ok, {{one_for_one, 10, 10}, Processes}}.
