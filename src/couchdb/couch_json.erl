
-module(ejson).

-export([decode/1, encode/1]).

decode(Data) when is_list(Data) ->
    decode(list_to_binary(Data));
decode(Data) when is_binary(Data) ->
    case (catch ejson_decode:value(Data)) of
        {error, Reason} ->
            {error, Reason};
        {_Rest, EJson} ->
            EJson
    end.
    
encode(Term) ->
    case (catch ejson_encode:value(Term)) of
        {error, Reason} ->
            {error, Reason};
        Else ->
            Else
    end.