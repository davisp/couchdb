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
-module(couchweb_res_utils).

-export([
    init/1,
    allowed_methods/2,
	resource_exists/2,
	last_modified/2,
	content_types_provided/2,
	moved_permanently/2,
	previously_existed/2,
	generate_etag/2,
	
	% Body generation
	contents/2
]).

-record(ctx, {
    root,
    body=undefined
}).

-include_lib("kernel/include/file.hrl").
-include_lib("webmachine/webmachine.hrl").

init(Config) ->
    {root, Root} = proplists:lookup(root, Config),
    {ok, #ctx{root=Root}}.

allowed_methods(Req, Ctx) ->
    {['HEAD', 'GET'], adjust_path(Req), Ctx}.

moved_permanently(Req, Ctx) ->
    case wrq:path(Req) of 
        "/_utils" -> {{true, "/_utils/"}, Req, Ctx};
        _ -> {false, Req, Ctx}
    end.

previously_existed(Req, Ctx) ->
    {true, Req, Ctx}.

resource_exists(Req, Ctx) ->
    Req2 = adjust_path(Req),
    case file_exists(Ctx, wrq:disp_path(Req2)) of
        {true, _} -> {true, Req2, Ctx};
        _ -> {fase, Req2, Ctx}
    end.

content_types_provided(Req, Ctx) ->
    CType = webmachine_util:guess_mime(wrq:disp_path(Req)),
    {[{CType, contents}], Req, Ctx}.

last_modified(Req, Ctx) ->
    case file_exists(Ctx, wrq:disp_path(Req)) of
        {true, Path} -> {filelib:last_modified(Path), Req, Ctx};
        _ -> {error, Req, Ctx}
    end.

generate_etag(Req, Ctx) ->
    case get_contents(Ctx, wrq:disp_path(Req)) of
        {undefined, Ctx2} -> {undefiend, Req, Ctx2};
        {Body, Ctx2} -> {hash_body(Body), Req, Ctx2}
    end.

% Body generation

contents(Req, Ctx) ->
    case get_contents(Ctx, wrq:disp_path(Req)) of 
        {undefined, Ctx2} -> {error, Req, Ctx2};
        {Body, Ctx2} -> {Body, Req, Ctx2}
    end.

% Utility functions

adjust_path(Req) ->
    Path = wrq:disp_path(Req),
    case lists:reverse(Path) of
        [] -> wrq:set_disp_path("index.html", Req);
        [$/ | _] -> wrq:set_disp_path(Path ++ "index.html", Req);
        _ -> Req
    end.

file_path(Ctx, "") ->
    Ctx#ctx.root;
file_path(Ctx, [$/ | Rest]) ->
    filename:join([Ctx#ctx.root, Rest]);
file_path(Ctx, Name) ->
    filename:join(Ctx#ctx.root, Name).

file_exists(Ctx, Name) ->
    NamePath = file_path(Ctx, Name),
    case filelib:is_regular(NamePath) of 
    	true -> {true, NamePath};
    	false -> false
    end.

hash_body(Body) ->
    mochihex:to_hex(binary_to_list(crypto:sha(Body))).

get_contents(Ctx, Path) ->
    case Ctx#ctx.body of
	    undefined ->
	        case file_exists(Ctx, Path) of 
		        {true, FullPath} ->
		            {ok, Value} = file:read_file(FullPath),
		            {Value, Ctx#ctx{body=Value}};
		        false ->
		            {undefined, Ctx}
	        end;
	    Body ->
	        {Body, Ctx}
    end.
