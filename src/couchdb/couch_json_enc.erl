
-module(ejson_encode).

-export([value/1]).

-include("ejson.hrl").

value({O}) when is_list(O) ->
    object(O, [<<?LC:8/integer>>]);
value(L) when is_list(L) ->
    array(L, [<<?LB:8/integer>>]);
value(true) ->
    <<"true">>;
value(false) ->
    <<"false">>;
value(null) ->
    <<"null">>;
value(S) when is_atom(S); is_binary(S) ->
    string(S);
value(I) when is_integer(I) ->
    list_to_binary(integer_to_list(I));
value(F) when is_float(F) ->
    list_to_binary(io_lib:format("~p", [F]));
value(B) ->
    throw({invalid_json, {badarg, B}}).

object([], Acc) ->
    list_to_binary(lists:reverse([<<?RC:8/integer>> | Acc]));
object([{K, V} | Rest], Acc) when length(Acc) =:= 1 ->
    Acc2 = [value(V), <<?CL:8/integer>>, string(K) | Acc],
    object(Rest, Acc2);
object([{K, V} | Rest], Acc) ->
    Acc2 = [value(V), <<?CL:8/integer>>, string(K), <<?CM:8/integer>> | Acc],
    object(Rest, Acc2).

array([], Acc) ->
    list_to_binary(lists:reverse([<<?RB:8/integer>> | Acc]));
array([V | Rest], Acc) when length(Acc) =:= 1 ->
    array(Rest, [value(V) | Acc]);
array([V | Rest], Acc) ->
    array(Rest, [value(V), <<?CM:8/integer>> | Acc]).

string(A) when is_atom(A) ->
    string(list_to_binary(atom_to_list(A)), [<<?DQ:8/integer>>]);
string(B) when is_binary(B) ->
    string(B, [<<?DQ:8/integer>>]);
string(Bad) ->
    ?EXIT({invalid_string, {bad_term, Bad}}).

string(Bin, Acc) ->
    case Bin of
        <<>> ->
            list_to_binary(lists:reverse([<<?DQ:8/integer>> | Acc]));
        <<?DQ:8/integer, Rest/binary>> ->
            string(Rest, [<<?ESDQ:16/integer>> | Acc]);
        <<?RS:8/integer, Rest/binary>> ->
            string(Rest, [<<?ESRS:16/integer>> | Acc]);
        <<?FS:8/integer, Rest/binary>> ->
            string(Rest, [<<?ESFS:16/integer>> | Acc]);
        <<?BS:8/integer, Rest/binary>> ->
            string(Rest, [<<?ESBS:16/integer>> | Acc]);
        <<?FF:8/integer, Rest/binary>> ->
            string(Rest, [<<?ESFF:16/integer>> | Acc]);
        <<?NL:8/integer, Rest/binary>> ->
            string(Rest, [<<?ESNL:16/integer>> | Acc]);
        <<?CR:8/integer, Rest/binary>> ->
            string(Rest, [<<?ESCR:16/integer>> | Acc]);
        <<?TB:8/integer, Rest/binary>> ->
            string(Rest, [<<?ESTB:16/integer>> | Acc]);
        _ ->
            case fast_string(Bin, 0) of
                Pos when Pos > 0 ->
                    <<Fast:Pos/binary, Rest/binary>> = Bin,
                    string(Rest, [Fast | Acc]);
                _ ->
                    {Rest, Escaped} = unicode_escape(Bin),
                    string(Rest, [Escaped | Acc])
            end     
    end.

fast_string(<<>>, Pos) ->
    Pos;
fast_string(<<C, Rest/binary>>, Pos) when C >= ?SP, C < 16#7F ->
    fast_string(Rest, Pos+1);
fast_string(_, Pos) ->
    Pos.

unicode_escape(Bin) ->
    case Bin of
        <<D:8/integer, Rest/binary>> when D =< ?SP ->
            {Rest, hex_escape(D)};
        <<6:3/integer, C1:5/bits, 2:2/integer, C2:6/bits, Rest/binary>> ->
            <<C:16/integer>> = <<0:5/integer, C1:5/bits, C2:6/bits>>,
            {Rest, hex_escape(C)};
        <<14:4/integer, C1:4/bits, 2:2/integer, C2:6/bits,
                2:2/integer, C3:6/bits, Rest/binary>> ->
            <<C:16/integer>> = <<C1:4/bits, C2:6/bits, C3:6/bits>>,
            {Rest, hex_escape(C)};
        <<30:5/integer, C1:3/bits, 2:2/integer, C2:6/bits, 2:2/integer,
                C3:6/bits, 2:2/integer, C4:6/bits, Rest/binary>> ->
            <<C:32/integer>> =
                <<0:11/integer, C1:3/bits, C2:6/bits, C3:6/bits, C4:6/bits>>,
            {Rest, hex_escape(C)};
        _ ->
            ?EXIT(invalid_utf8)
    end.

hex_escape(C) when C =< 16#FFFF ->
    <<C1:4, C2:4, C3:4, C4:4>> = <<C:16>>,
    ["\\u"] ++ [hex_digit(C0) || C0 <- [C1, C2, C3, C4]];
hex_escape(C) ->
    BinCodePoint = list_to_binary(xmerl_ucs:to_utf16be(C)),
    <<D:16/unsigned-integer, E:16/unsigned-integer>> = BinCodePoint,
    [hex_escape(D), hex_escape(E)].

hex_digit(D) when D >= 0, D =< 9 ->
    $0 + D;
hex_digit(D) when D >= 10, D =< 15 ->
    $A + (D - 10).

