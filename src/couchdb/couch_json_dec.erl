
-module(ejson_decode).

-export([value/1]).

-include("ejson.hrl").

value(Bin) ->
    case strip(Bin) of
        <<?LC:8/integer, Rest/binary>> ->
            object(strip(Rest), []);
        <<?LB:8/integer, Rest/binary>> ->
            array(strip(Rest), []);
        <<?DQ:8/integer, Rest/binary>> ->
            string(Rest, []);
        <<"false", Rest/binary>> ->
            {Rest, false};
        <<"true", Rest/binary>> ->
            {Rest, true};
        <<"null", Rest/binary>> ->
            {Rest, null};
        <<First:8/integer, Rest/binary>> ->
            case First of
                ?HY ->
                    neg_number(Rest, [First]);
                ?ZR ->
                    flt_number(Rest, [First]);
                _ when First > ?ZR andalso First =< ?NI ->
                    number(Rest, [First]);
                _ ->
                    ?EXIT({invalid_value, no_decodable_value})
            end;
        <<>> ->
            ?EXIT({invalid_value, no_more_data})
    end.

object(Bin, Acc) ->
    case pair(Bin) of
        {Rest, close_obj} when length(Acc) == 0 ->
            {Rest, {[]}};
        {_, close_obj} ->
            ?EXIT({invalid_object, no_key_after_comma});
        {Rest, KVPair} ->
            case strip(Rest) of
                <<?RC:8/integer, Rest2/binary>> ->
                    {strip(Rest2), {lists:reverse([KVPair | Acc])}};
                <<?CM:8/integer, Rest2/binary>> ->
                    object(strip(Rest2), [KVPair | Acc]);
                <<>> ->
                    ?EXIT({invalid_object, no_more_data});
                _ ->
                    ?EXIT({invalid_object, no_comma_or_close})
            end
    end.

pair(Bin) ->
    case strip(Bin) of
        <<?RC:8/integer, Rest/binary>> ->
            {Rest, close_obj};
        <<?DQ:8/integer, Rest/binary>> ->
            {Rest2, Key} = string(Rest, []),
            case strip(Rest2) of
                <<?CL:8/integer, Rest3/binary>> ->
                    {Rest4, Val} = value(Rest3),
                    {Rest4, {Key, Val}};
                <<>> ->
                    ?EXIT({invalid_object, no_more_data});
                _ ->
                    ?EXIT({invalid_object, no_colon_after_key})
            end;
        <<>> ->
            ?EXIT({invalid_object, no_more_data});
        _ ->
            ?EXIT({invalid_object, no_key_or_close})
    end.

array(<<>>, _) ->
    ?EXIT({invalid_array, no_more_data});
array(<<?CM:8/integer, Rest/binary>>, Acc) when length(Acc) > 0 ->
    {Rest2, Value} = value(Rest),
    array(strip(Rest2), [Value | Acc]);
array(<<?RB:8/integer, Rest/binary>>, Acc) ->
    {Rest, lists:reverse(Acc)};
array(Bin, Acc) when length(Acc) == 0 ->
    {Rest, Value} = value(Bin),
    array(strip(Rest), [Value | Acc]);
array(_, _) ->
    ?EXIT({invalid_array, trailing_data}).

string(Bin, Acc) ->
    case Bin of
        <<?DQ:8/integer, Rest/binary>> ->
            {Rest, list_to_binary(lists:reverse(Acc))};
        <<?ESDQ:16/integer, Rest/binary>> ->
            string(Rest, [<<?DQ:8/integer>> | Acc]);
        <<?ESRS:16/integer, Rest/binary>> ->
            string(Rest, [<<?RS:8/integer>> | Acc]);
        <<?ESFS:16/integer, Rest/binary>> ->
            string(Rest, [<<?FS:8/integer>> | Acc]);
        <<?ESBS:16/integer, Rest/binary>> ->
            string(Rest, [<<?BS:8/integer>> | Acc]);
        <<?ESFF:16/integer, Rest/binary>> ->
            string(Rest, [<<?FF:8/integer>> | Acc]);
        <<?ESNL:16/integer, Rest/binary>> ->
            string(Rest, [<<?NL:8/integer>> | Acc]);
        <<?ESCR:16/integer, Rest/binary>> ->
            string(Rest, [<<?CR:8/integer>> | Acc]);
        <<?ESTB:16/integer, Rest/binary>> ->
            string(Rest, [<<?TB:8/integer>> | Acc]);
        <<?ESUE:16/integer, Rest/binary>> ->
            {Rest2, Unescaped} = unicode_escape(Rest),
            string(Rest2, [Unescaped | Acc]);
        <<>> ->
            ?EXIT({invalid_string, no_more_data});
        _ ->
            case fast_string(Bin, 0) of
                Pos when Pos > 0 ->
                    <<Fast:Pos/binary, Rest/binary>> = Bin,
                    string(Rest, [Fast | Acc]);
                _ ->
                    ?EXIT({invalid_string, unknown_escape})
            end     
    end.

unicode_escape(Bin) ->
    case Bin of
        <<C1, C2, C3, C4, ?RS:8/integer, "u", D1, D2, D3, D4, Rest/binary>> ->
            C = erlang:list_to_integer([C1, C2, C3, C4], 16),
            D = erlang:list_to_integer([D1, D2, D3, D4], 16),
            BinCodePoint = <<C:16/unsigned-integer, D:16/unsigned-integer>>,
            case (catch xmerl_ucs:from_utf16be(BinCodePoint)) of
                [CodePoint] ->
                    {Rest, list_to_binary(xmerl_ucs:to_utf8(CodePoint))};
                _Else ->
                    Rest2 = <<?RS:8/integer, "u", D1, D2, D3, D4, Rest/binary>>,
                    {Rest2, crap_point(C)}
            end;
        <<C1, C2, C3, C4, Rest/binary>> ->
            C = erlang:list_to_integer([C1, C2, C3, C4], 16),
            case (catch xmerl_ucs:to_utf8(C)) of
                CodePoint when is_list(CodePoint) ->
                    {Rest, list_to_binary(CodePoint)};
                _Else ->
                    {Rest, crap_point(C)}
            end;
        _ ->
            ?EXIT({invalid_unicode_escape, no_more_data})
    end.

% http://en.wikipedia.org/wiki/UTF-8#Description
crap_point(C) when C =< 16#7F ->
    <<C:16/integer>>;
crap_point(C) when C =< 16#7FF ->
    <<0:5/bits, C1:5/bits, C2:6/bits>> = <<C:16/integer>>,
    <<6:3/integer, C1:5/bits, 2:2/integer, C2:6/bits>>;
crap_point(C) when C =< 16#FFFF ->
    <<C1:4/bits, C2:6/bits, C3:6/bits>> = <<C:16/integer>>,
    <<14:4/integer, C1:4/bits, 2:2/integer, C2:6/bits, 2:2/integer, C3:6/bits>>;
crap_point(C) when C =< 16#1FFFFF ->
    C0 = <<C:32/integer>>,
    <<0:11/bits, C1:3/bits, C2:6/bits, C3:6/bits, C4:6/bits>> = C0,
    <<30:5/integer, C1:3/bits, 2:2/integer, C2:6/bits,
            2:2/integer, C3:6/bits, 2:2/integer, C4:6/bits>>;
crap_point(_) ->
    ?EXIT({invalid_rfc, email_douglas_crockford_to_complain}).

fast_string(<<>>, _) ->
    ?EXIT({invalid_string, no_more_data});
fast_string(<<?DQ:8/integer, _/binary>>, Pos) ->
    Pos;
fast_string(<<?RS:8/integer, _/binary>>, Pos) ->
    Pos;

% Checking UTF-8
fast_string(Bin, Pos) ->
    case Bin of
        <<C1, Rest/binary>> when
                C1 >= 16#20, C1 =< 16#7F ->
            fast_string(Rest, Pos+1);
        <<C1, C2, Rest/binary>> when
                C1 >= 16#C2, C1 =< 16#DF,
                C2 >= 16#7F, C2 =< 16#BF ->
            fast_string(Rest, Pos+2);
        <<C1, C2, C3, Rest/binary>> when
                C1 >= 16#E0, C1 =< 16#EF,
                C2 >= 16#7F, C2 =< 16#BF,
                C3 >= 16#7F, C3 =< 16#BF ->
            fast_string(Rest, Pos+3);
        <<C1, C2, C3, C4, Rest/binary>> when
                C1 >= 16#F0, C1 =< 16#F4,
                C2 >= 16#7F, C2 =< 16#BF,
                C3 >= 16#7F, C3 =< 16#BF,
                C4 >= 16#7F, C4 =< 16#BF ->
            fast_string(Rest, Pos+4);
        _ ->
            ?EXIT({invalid_string, invalid_utf_8})
    end.


number(<<>>, Acc) ->
    {<<>>, list_to_integer(lists:reverse(Acc))};
number(<<First:8/integer, Rest/binary>>, Acc) ->
    case First of
        ?PR ->
            flt_number(<<First:8/integer, Rest/binary>>, Acc);
        _ when First >= ?ZR andalso First =< ?NI ->
            number(Rest, [First | Acc]);
        _ when First =:= ?UE orelse First =:= ?LE ->
            exp_sign(Rest, [First, ?ZR, ?PR | Acc]);
        _ ->
            case lists:member(First, [?TB, ?NL, ?CR, ?SP, ?CM, ?RB, ?RC]) of
                true ->
                    Value = list_to_integer(lists:reverse(Acc)),
                    {<<First:8/integer, Rest/binary>>, Value};
                _ ->
                    ?EXIT({invalid_number, leading_zero})
            end
    end.

neg_number(<<>>, [?HY]) ->
    ?EXIT({invalid_number, lonely_hyphen});
neg_number(<<>>, Acc) ->
    {<<>>, list_to_integer(lists:reverse(Acc))};
neg_number(<<?ZR:8/integer, Rest/binary>>, Acc) ->
    flt_number(Rest, [?ZR | Acc]);
neg_number(<<First:8/integer, Rest/binary>>, Acc) ->
    case First of
        ?PR ->
            flt_number(<<First:8/integer, Rest/binary>>, Acc);
        _ when First > ?ZR andalso First =< ?NI ->
            number(Rest, [First | Acc]);
        _ when First =:= ?UE orelse First =:= ?LE ->
            exp_sign(Rest, [First, ?ZR, ?PR | Acc]);
        _ ->
            case lists:member(First, [?TB, ?NL, ?CR, ?SP, ?CM, ?RB, ?RC]) of
                true ->
                    Value = list_to_integer(lists:reverse(Acc)),
                    {<<First:8/integer, Rest/binary>>, Value};
                _ ->
                    ?EXIT({invalid_number, leading_zero})
            end
    end.

% Special cases for <<"0">>, <<"0E3">>, etc.
flt_number(<<>>, [?ZR]) ->
    {<<>>, 0};
flt_number(<<?UE:8/integer, Rest/binary>>, [?ZR]) ->
    exp_sign(Rest, [?UE, ?ZR, ?PR, ?ZR]);
flt_number(<<?LE:8/integer, Rest/binary>>, [?ZR]) ->
    exp_sign(Rest, [?LE, ?ZR, ?PR, ?ZR]);
flt_number(<<First:8/integer, Rest/binary>>, [?ZR]) ->
    case lists:member(First, [?TB, ?NL, ?CR, ?SP, ?CM, ?RB, ?RC]) of
        true ->
            {<<First:8/integer, Rest/binary>>, 0};
        _ ->
            ?EXIT({invalid_number, leading_zero})
    end;

flt_number(<<>>, _Acc) ->
    ?EXIT({invalid_number, no_more_data});
flt_number(<<?PR:8/integer>>, _) ->
    ?EXIT({invalid_number, missing_fraction});
flt_number(<<?PR:8/integer, First:8/integer, Rest/binary>>, Acc) ->
    case First of
        _ when First >= ?ZR andalso First =< ?NI ->
            frac_number(Rest, [First, ?PR | Acc]);
        _ ->
            ?EXIT({invalid_number, not_digit_after_decimal})
    end;
flt_number(<<First:8/integer, _/binary>>, Acc) ->
    ?EXIT({invalid_number, lists:reverse([First | Acc])}).

frac_number(<<>>, Acc) ->
    {<<>>, list_to_float(lists:reverse(Acc))};
frac_number(<<First:8/integer, Rest/binary>>=Bin, Acc) ->
    case First of
        _ when First >= ?ZR andalso First =< ?NI ->
            frac_number(Rest, [First | Acc]);
        _ when First =:= ?UE orelse First =:= ?LE ->
            exp_sign(Rest, [First | Acc]);
        _ ->
            {Bin, list_to_float(lists:reverse(Acc))}
    end.

exp_sign(<<>>, _) ->
    ?EXIT({invalid_number, no_more_data});
exp_sign(<<?PL:8/integer, Rest/binary>>, Acc) ->
    exp_number(Rest, Acc);
exp_sign(<<?HY:8/integer, Rest/binary>>, Acc) ->
    exp_number(Rest, [?HY | Acc]);
exp_sign(Bin, Acc) ->
    exp_number(Bin, Acc).

exp_number(<<>>, Acc) ->
    {<<>>, list_to_float(lists:reverse(Acc))};
exp_number(<<First:8/integer, Rest/binary>>, Acc) ->
    case First of
        _ when First >= ?ZR andalso First =< ?NI ->
            exp_number(Rest, [First | Acc]);
        _ ->
            case Acc of
                [?UE|_] ->
                    ?EXIT({invalid_number, no_exponent_after_e});
                [?LE|_] ->
                    ?EXIT({invalid_number, no_exponent_after_e});
                _ ->
                    Value = erlang:list_to_float(lists:reverse(Acc)),
                    {<<First:8/integer, Rest/binary>>, Value}
            end
    end.

strip(<<?TB:8/integer, Rest/binary>>) ->
    strip(Rest);
strip(<<?NL:8/integer, Rest/binary>>) ->
    strip(Rest);
strip(<<?CR:8/integer, Rest/binary>>) ->
    strip(Rest);
strip(<<?SP:8/integer, Rest/binary>>) ->
    strip(Rest);
strip(Bin) ->
    Bin.
