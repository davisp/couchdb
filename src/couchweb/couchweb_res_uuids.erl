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

-module(couchweb_res_uuids).
-export([
    init/1,
    content_types_provided/2,
    malformed_request/2,
    to_json/2,
    to_text/2
]).

-include_lib("couchdb/couch_db.hrl").
-include_lib("webmachine/webmachine.hrl").

init([]) ->
    {ok, undefined}.
    
content_types_provided(Wrq, Ctx) ->
    {[
        {"application/json", to_json},
        {"text/plain", to_text}
    ], Wrq, Ctx}.

malformed_request(Wrq, _) ->
    CountStr = wrq:get_qs_value("count", "1", Wrq),
    case (catch list_to_integer(CountStr)) of
        Count when is_integer(Count) andalso Count > 0 ->
            {false, Wrq, {count, Count}};
        _ ->
            {true, Wrq, undefined}
    end.

to_json(Wrq, {count, Count}) ->
    UUIDs = [couch_uuids:new() || _ <- lists:seq(1, Count)],
    {?JSON_ENCODE(UUIDs) ++ <<"\n">>, Wrq, undefined}.

to_text(Wrq, Ctx) ->
    to_json(Wrq, Ctx).
