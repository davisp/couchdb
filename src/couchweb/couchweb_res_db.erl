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
-module(couchweb_res_db).

-export([
    init/1,
    allowed_methods/2,
    content_types_provided/2,
    content_types_accepted/2,
    resource_exists/2,
    is_conflict/2,
    is_authorized/2,
    delete_resource/2,
    delete_completed/2,

    from_json/2,
    to_json/2,
    to_text/2
]).

-include_lib("couchdb/couch_db.hrl").
-include_lib("webmachine/webmachine.hrl").

-record(ctx, {
    db,
    user_ctx
}).

init([]) ->
    {ok, #ctx{}}.

allowed_methods(Req, Ctx) ->
    {['GET', 'HEAD', 'PUT', 'DELETE'], Req, Ctx}.

is_authorized(Req, Ctx) ->
    {true, Req, Ctx}.
    
content_types_provided(Req, Ctx) ->
    {[
        {"application/json", to_json},
        {"text/plain", to_text},
        {"text/html", to_text}
    ], Req, Ctx}.
    
content_types_accepted(Req, Ctx) ->
    {[
        {"application/json", from_json},
        {"application/octet-stream", from_json}
    ], Req, Ctx}.

resource_exists(Req, Ctx) ->
    DbName = wrq:path_info(dbname, Req),
    UserCtx =  #user_ctx{}, 
    case couch_db:open(?l2b(DbName),  [{user_ctx, UserCtx}]) of
        {ok, Db} ->
            {true, Req, Ctx#ctx{db=Db}};
        Error ->
            Req2 = couchweb_utils:error_response(Error, Req),
            {false, Req2, Ctx}
    end.

is_conflict(Req, Ctx) ->
    DbName = wrq:path_info(dbname, Req),
    UserCtx =  #user_ctx{}, 
    case couch_db:open(?l2b(DbName),  [{user_ctx, UserCtx}]) of
        {ok, Db} ->
            couch_db:close(Db),
            Req2 = couchweb_utils:error_response({error, database_exists}, Req),
            {true, Req2, Ctx};
        _ ->
            {false, Req, Ctx}
    end.

delete_resource(Req, Ctx) ->
    DbName = wrq:path_info(dbname, Req),
    UserCtx =  #user_ctx{},
    case couch_server:delete(?l2b(DbName), [{user_ctx, UserCtx}]) of
    ok ->
        Req2 = couchweb_utils:json_body({[{ok, true}]}, Req),
        {true, Req2, Ctx};
    Error ->
        Req2 = couchweb_utils:error_response(Error, Req),
        {false, Req2, Ctx}
    end.

delete_completed(Req, Ctx) ->
    % Needs to depend on whether full-commit was set.
    {false, Req, Ctx}.

from_json(Req, Ctx) ->
    DbName = wrq:path_info(dbname, Req),
    UserCtx =  #user_ctx{},
    case couch_server:create(?l2b(DbName), [{user_ctx, UserCtx}]) of
        {ok, Db} ->
            couch_db:close(Db),
            DocUrl = "/" ++ couch_util:url_encode(DbName),
            Req2 = couchweb_utils:json_body({[{ok, true}]}, Req),
            Req3 = wrq:set_resp_header("Location", DocUrl, Req2),
            {true, Req3, Ctx};
        Error ->
            Req2 = couchweb_utils:error_response(Req, Error),
            {false, Req2, Ctx}
    end.

to_json(Req, Ctx=#ctx{db=Db}) ->
    {ok, DbInfo} = couch_db:get_db_info(Db),
    {?JSON_ENCODE({DbInfo}) ++ <<"\n">>, Req, Ctx}.
    
to_text(Req, Ctx) ->
    to_json(Req, Ctx).
