% Based on mochijson2

-module(jsonsplice).
-export([splice/2]).


-define(ADV_COL(S, N), S#state{
    offset=N+S#state.offset,
    column=N+S#state.column
}).

-define(INC_COL(S), S#state{
    offset=1+S#state.offset,
    column=1+S#state.column
}).

-define(INC_LINE(S), S#state{
    offset=1+S#state.offset,
    column=1,
    line=1+S#state.line
}).

-define(INC_CHAR(S, C),
    case C of
        $\n ->
            S#state{
                column=1,
                line=1+S#state.line,
                offset=1+S#state.offset
            };
        _ ->
            S#state{
                column=1+S#state.column,
                offset=1+S#state.offset
            }
    end
).

-define(IS_WHITESPACE(C), (
    C =:= $\s orelse C =:= $\t orelse C =:= $\r orelse C =:= $\n
)).

-define(IS_NUM_START(C), (
    (C >= $0 andalso C =< $9) orelse C =:= $-
)).


-record(state, {
    offset=0,
    line=1,
    column=1,
    state=null,
    inserted=[],
    had_members=false,
    split=0
}).


splice(M, J) ->
    splice(M, J, #state{}).


splice(M, L, S) when is_list(L) ->
    splice(M, iolist_to_binary(L), S);
splice(M, B, S=#state{state=null}) ->
    case validate_next(B, S#state{state=any}) of
        {start_object, S1} ->
            splice(M, B, S1#state{state=start_object, split=S1#state.offset})
    end;
splice([{K, V} | Rest], B, S=#state{state=start_object}) ->
    S2 = encode_json(K, V, S),
    splice(Rest, B, S2);
splice([], B, S=#state{state=start_object}) ->
    {ok, S1} = validate_object(B, S#state{state=key}),
    {eof, _} = validate_next(B, S1#state{state=trim}),
    Sep = case length(S1#state.inserted) > 0 andalso S1#state.had_members of
        true -> ",";
        _ -> ""
    end,
    O = S1#state.split,
    <<Prefix:O/binary, Suffix/binary>> = B,
    [Prefix, lists:reverse(S1#state.inserted), Sep, Suffix].


encode_json(K, V, S=#state{inserted=Acc}) when is_atom(K) orelse is_binary(K) ->
    K1 = couch_util:json_encode(K),
    V1 = couch_util:json_encode(V),
    case length(Acc) > 0 of
        true -> S#state{inserted=["," ++ K1 ++ ":" ++ V1 | Acc]};
        _ -> S#state{inserted=[K1 ++ ":" ++ V1 | Acc]}
    end.


validate(B, S=#state{state=null}) ->
    case validate_next(B, S#state{state=any}) of
        {ok, S1} ->
            {ok, S1};
        {start_array, S1} ->
            validate_array(B, S1#state{state=any});
        {start_object, S1} ->
            validate_object(B, S1#state{state=key})
    end.


validate_next(B, S=#state{offset=O}) ->
    case B of
        <<_:O/binary, C, _/binary>> when ?IS_WHITESPACE(C) ->
            validate_next(B, ?INC_CHAR(S, C));
        <<_:O/binary, "{", _/binary>> ->
            {start_object, ?INC_COL(S)};
        <<_:O/binary, "}", _/binary>> ->
            {end_object, ?INC_COL(S)};
        <<_:O/binary, "[", _/binary>> ->
            {start_array, ?INC_COL(S)};
        <<_:O/binary, "]", _/binary>> ->
            {end_array, ?INC_COL(S)};
        <<_:O/binary, ",", _/binary>> ->
            {comma, ?INC_COL(S)};
        <<_:O/binary, ":", _/binary>> ->
            {colon, ?INC_COL(S)};
        <<_:O/binary, "null", _/binary>> ->
            {ok, ?ADV_COL(S, 4)};
        <<_:O/binary, "true", _/binary>> ->
            {ok, ?ADV_COL(S, 4)};
        <<_:O/binary, "false", _/binary>> ->
            {ok, ?ADV_COL(S, 5)};
        <<_:O/binary, "\"", _/binary>> ->
            validate_string(B, ?INC_COL(S));
        <<_:O/binary, C, _/binary>> when ?IS_NUM_START(C) ->
            validate_number(B, S);
        <<_:O/binary>> ->
            trim = S#state.state,
            {eof, S}
    end.


validate_object(B, S=#state{state=key}) ->
    case validate_next(B, S) of
        {end_object, S1} ->
            {ok, S1#state{state=null}};
        {ok, S1} ->
            {colon, S2} = validate_next(B, S1),
            {ok, S3} = validate(B, S2#state{state=null}),
            validate_object(B, S3#state{state=comma, had_members=true})
    end;
validate_object(B, S=#state{state=comma}) ->
    case validate_next(B, S) of
        {end_object, S1} ->
            {ok, S1#state{state=null}};
        {comma, S1} ->
            validate_object(B, S1#state{state=key})
    end.


validate_array(B, S=#state{state=any}) ->
    case validate_next(B, S) of
        {end_array, S1} ->
            {ok, S1#state{state=null}};
        {start_array, S1} ->
            {ok, S2} = validate_array(B, S1),
            validate_array(B, S2#state{state=comma});
        {start_object, S1} ->
            {ok, S2} = validate_object(B, S1#state{state=key}),
            validate_array(B, S2#state{state=comma});
        {ok, S1} ->
            validate_array(B, S1#state{state=comma})
    end;
validate_array(B, S=#state{state=comma}) ->
    case validate_next(B, S) of
        {end_array, S1} ->
            {ok, S1#state{state=null}};
        {comma, S1} ->
            validate_array(B, S1#state{state=any})
    end.


validate_string(B, S=#state{offset=O}) ->
    case B of
        <<_:O/binary, $", _/binary>> ->
            {ok, ?INC_COL(S)};
        <<_:O/binary, "\\\"", _/binary>> ->
            validate_string(B, ?ADV_COL(S, 2));
        <<_:O/binary, "\\\\", _/binary>> ->
            validate_string(B, ?ADV_COL(S, 2));
        <<_:O/binary, "\\/", _/binary>> ->
            validate_string(B, ?ADV_COL(S, 2));
        <<_:O/binary, "\\b", _/binary>> ->
            validate_string(B, ?ADV_COL(S, 2));
        <<_:O/binary, "\\f", _/binary>> ->
            validate_string(B, ?ADV_COL(S, 2));
        <<_:O/binary, "\\n", _/binary>> ->
            validate_string(B, ?ADV_COL(S, 2));
        <<_:O/binary, "\\r", _/binary>> ->
            validate_string(B, ?ADV_COL(S, 2));
        <<_:O/binary, "\\t", _/binary>> ->
            validate_string(B, ?ADV_COL(S, 2));
        <<_:O/binary, "\\u", C3, C2, C1, C0, Rest/binary>> ->
            C = erlang:list_to_integer([C3, C2, C1, C0], 16),
            if C > 16#D7FF, C < 16#DC00 ->
                %% coalesce UTF-16 surrogate pair
                <<"\\u", D3, D2, D1, D0, _/binary>> = Rest,
                D = erlang:list_to_integer([D3,D2,D1,D0], 16),
                [CodePoint] = xmerl_ucs:from_utf16be(<<
                        C:16/big-unsigned-integer,
                        D:16/big-unsigned-integer
                    >>),
                xmerl_ucs:to_utf8(CodePoint),
                validate_string(B, ?ADV_COL(S, 12));
            true ->
                xmerl_ucs:to_utf8(C),
                validate_string(B, ?ADV_COL(S, 6))
            end;
        <<_:O/binary, C1, _/binary>> when C1 < 128 ->
            validate_string(B, ?INC_CHAR(S, C1));
        <<_:O/binary, C1, C2, _/binary>> when
                C1 >= 194, C1 =< 223,
                C2 >= 128, C2 =< 191 ->
            validate_string(B, ?ADV_COL(S, 2));
        <<_:O/binary, C1, C2, C3, _/binary>> when
                C1 >= 224, C1 =< 239,
                C2 >= 128, C2 =< 191,
                C3 >= 128, C3 =< 191 ->
            validate_string(B, ?ADV_COL(S, 3));
        <<_:O/binary, C1, C2, C3, C4, _/binary>> when
                C1 >= 240, C1 =< 244,
                C2 >= 128, C2 =< 191,
                C3 >= 128, C3 =< 191,
                C4 >= 128, C4 =< 191 ->
            validate_string(B, ?ADV_COL(S, 4));
        _ ->
            throw(invalid_utf8)
    end.


validate_number(B, S) ->
    validate_number(B, sign, S).

validate_number(B, sign, S=#state{offset=O}) ->
    case B of
        <<_:O/binary, $-, _/binary>> ->
            validate_number(B, int, ?INC_COL(S));
        _ ->
            validate_number(B, int, S)
    end;
validate_number(B, int, S=#state{offset=O}) ->
    case B of
        <<_:O/binary, $0, _/binary>> ->
            validate_number(B, frac, ?INC_COL(S));
        <<_:O/binary, C, _/binary>> when C >= $1 andalso C =< $9 ->
            validate_number(B, int1, ?INC_COL(S))
    end;
validate_number(B, int1, S=#state{offset=O}) ->
    case B of
        <<_:O/binary, C, _/binary>> when C >= $0 andalso C =< $9 ->
            validate_number(B, int1, ?INC_COL(S));
        _ ->
            validate_number(B, frac, S)
    end;
validate_number(B, frac, S=#state{offset=O}) ->
    case B of
        <<_:O/binary, $., C, _/binary>> when C >= $0, C =< $9 ->
            validate_number(B, frac1, ?ADV_COL(S, 2));
        <<_:O/binary, E, _/binary>> when E =:= $e orelse E =:= $E ->
            validate_number(B, esign, ?INC_COL(S));
        _ ->
            {ok, S}
    end;
validate_number(B, frac1, S=#state{offset=O}) ->
    case B of
        <<_:O/binary, C, _/binary>> when C >= $0 andalso C =< $9 ->
            validate_number(B, frac1, ?INC_COL(S));
        <<_:O/binary, E, _/binary>> when E =:= $e orelse E =:= $E ->
            validate_number(B, esign, ?INC_COL(S));
        _ ->
            {ok, S}
    end;
validate_number(B, esign, S=#state{offset=O}) ->
    case B of
        <<_:O/binary, C, _/binary>> when C =:= $- orelse C=:= $+ ->
            validate_number(B, eint, ?INC_COL(S));
        _ ->
            validate_number(B, eint, S)
    end;
validate_number(B, eint, S=#state{offset=O}) ->
    case B of
        <<_:O/binary, C, _/binary>> when C >= $0 andalso C =< $9 ->
            validate_number(B, eint1, ?INC_COL(S))
    end;
validate_number(B, eint1, S=#state{offset=O}) ->
    case B of
        <<_:O/binary, C, _/binary>> when C >= $0 andalso C =< $9 ->
            validate_number(B, eint1, ?INC_COL(S));
        _ ->
            {ok, S}
    end.



