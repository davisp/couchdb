% Copyright (c) 2008-2009 Paul J. Davis <paul.joseph.davis@gmail.com>
% Copyright (c) 2008-2009 Enrico Thierbach <eno@open-lab.org>
%
% This file is part of EEP0018, which is released under the MIT license.

-module(eep0018).

-export([start_driver/1]).
-export([decode_json/1, encode_json/1]).

% Public API

encode_json(Term) ->
    erlang:port_control(drv_port(), 0, term_to_binary(Term)).

decode_json(Json) when is_list(Json) ->
    decode_json(list_to_binary(Json));
decode_json(Json) when is_binary(Json) ->
    % The null byte is important for bare literals. Without it
    % yajl will throw a fit because it doesn't think it's finished
    % parsing correctly.
    case erlang:port_control(drv_port(), 1, <<Json/binary, 0:8>>) of
        [] ->
            receive {json, Decoded} -> Decoded end;
        Error ->
            throw({invalid_json, binary_to_list(Error)})
    end.

start_driver(LibDir) ->
    case erl_ddll:load_driver(LibDir, "eep0018_drv") of
    ok ->
        ok;
    {error, already_loaded} ->
        ok;
    {error, Error} ->
        exit(erl_ddll:format_error(Error))
    end.

drv_port() ->
    case get(eep0018_drv_port) of
    undefined ->
        Port = open_port({spawn, "eep0018_drv"}, [binary]),
        put(eep0018_drv_port, Port),
        Port;
    Port ->
        Port
    end.
