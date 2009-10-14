
-module(couch_json).

-export([decode/1, encode/1]).

decode(Data) when is_list(Data) ->
    decode(list_to_binary(Data));
decode(Data) when is_binary(Data) ->
    case (catch couch_json_dec:value(Data)) of
        {error, Reason} ->
            throw({invalid_json, Reason});
        {_Rest, EJson} ->
            EJson
    end.
    
encode(Term) ->
    case (catch couch_json_enc:value(Term)) of
        {error, Reason} ->
            throw({invalid_erljson, Reason});
        Else ->
            Else
    end.
