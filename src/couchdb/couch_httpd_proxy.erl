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
-module(couch_httpd_proxy).

-export([handle_proxy_req/2]).

-include("couch_db.hrl").
-include("../ibrowse/ibrowse.hrl").

-define(TIMEOUT, infinity).
-define(PKT_SIZE, 4096).


handle_proxy_req(#httpd{}=Req, ProxyDest) ->
    Method = get_method(Req),
    Url = get_url(Req, ProxyDest),
    Version = get_version(Req),
    Headers = get_headers(Req),
    Body = get_body(Req),
    Options = [
        {http_vsn, Version},
        {headers_as_is, true},
        {response_format, binary},
        {stream_to, {self(), once}}
    ],
    case ibrowse:send_req(Url, Headers, Method, Body, Options, ?TIMEOUT) of
        {ibrowse_req_id, ReqId} ->
            stream_response(Req, ReqId);
        {error, Reason} ->
            throw({error, Reason})
    end.
    

get_method(#httpd{mochi_req=MochiReq}) ->
    case MochiReq:get(method) of
        Method when is_atom(Method) ->
            list_to_atom(string:to_lower(atom_to_list(Method)));
        Method when is_list(Method) ->
            list_to_atom(string:to_lower(Method));
        Method when is_binary(Method) ->
            list_to_atom(string:to_lower(?b2l(Method)))
    end.


get_url(Req, ProxyDest) when is_binary(ProxyDest) ->
    get_url(Req, ?b2l(ProxyDest));
get_url(Req, ProxyDest) ->
    BaseUrl = case mochiweb_util:partition(ProxyDest, "/") of
        {[], "/", _} -> couch_httpd:absolute_uri(Req, ProxyDest);
        _ -> ProxyDest
    end,
    NoSlash = remove_trailing_slash(BaseUrl),
    [_Name | Parts] = Req#httpd.path_parts,
    StrParts = lists:map(fun binary_to_list/1, Parts),
    WithPath = case length(Parts) > 0 of
        true ->
            remove_trailing_slash(NoSlash ++ "/" ++ string:join(StrParts, "/"));
        _ ->
            NoSlash
    end,
    case couch_httpd:qs(Req) of
        [] -> WithPath;
        Qs -> WithPath ++ "?" ++ mochiweb_util:urlencode(Qs)
    end.


get_version(#httpd{mochi_req=MochiReq}) ->
    MochiReq:get(version).


get_headers(#httpd{mochi_req=MochiReq}) ->
    mochiweb_headers:to_list(MochiReq:get(headers)).


get_body(#httpd{method='GET'}) ->
    fun() -> eof end;
get_body(#httpd{method='HEAD'}) ->
    fun() -> eof end;
get_body(#httpd{method='DELETE'}) ->
    fun() -> eof end;
get_body(#httpd{mochi_req=MochiReq}) ->
    case MochiReq:get(body_length) of
        undefined ->
            <<>>;
        {unknown_transfer_encoding, Unknown} ->
            exit({unknown_transfer_encoding, Unknown});
        chunked ->
            {fun stream_chunked_body/1, {init, MochiReq, 0}};
        0 ->
            <<>>;
        Length when is_integer(Length) andalso Length > 0 ->
            {fun stream_length_body/1, {init, MochiReq, Length}};
        Length ->
            exit({invalid_body_length, Length})
    end.


remove_trailing_slash(Url) ->
    rem_slash(lists:reverse(Url)).

rem_slash([]) ->
    [];
rem_slash([$\s | RevUrl]) ->
    rem_slash(RevUrl);
rem_slash([$\t | RevUrl]) ->
    rem_slash(RevUrl);
rem_slash([$\r | RevUrl]) ->
    rem_slash(RevUrl);
rem_slash([$\n | RevUrl]) ->
    rem_slash(RevUrl);
rem_slash([$/ | RevUrl]) ->
    rem_slash(RevUrl);
rem_slash(RevUrl) ->
    lists:reverse(RevUrl).


stream_chunked_body({init, MReq}) ->
    % First chunk, do expect-continue dance.
    init_body_stream(MReq),
    stream_chunked_body({stream, MReq, 0, [], ?PKT_SIZE});
stream_chunked_body({stream, MReq, 0, Buf, BRem}) ->
    % Finished a chunk, get next length. If next length
    % is 0, its time to try and read trailers.
    {CRem, Data} = read_chunk_length(MReq),
    NewState = case CRem of
        0 -> {trailers, MReq, [Data | Buf], BRem-size(Data)};
        _ -> {stream, MReq, CRem, [Data | Buf], BRem-size(Data)}
    end,
    stream_chunked_body(NewState);
stream_chunked_body({stream, MReq, CRem, Buf, BRem}) when BRem =< 0 ->
    % Time to empty our buffers to the upstream socket.
    BodyData = iolist_to_binary(lists:reverse(Buf)),
    {ok, BodyData, {stream, MReq, CRem, [], ?PKT_SIZE}};
stream_chunked_body({stream, MReq, CRem, Buf, BRem}) ->
    % Buffer some more data from the client.
    Length = lists:min([CRem, BRem]),
    Socket = MReq:get(socket),
    NewState = case mochiweb_socket:recv(Socket, Length, ?TIMEOUT) of
        {ok, Data} -> {stream, MReq, CRem-Length, [Data | Buf], BRem-Length};
        _ -> exit(normal)
    end,
    stream_chunked_body(NewState);
stream_chunked_body({trailers, MReq, Buf, BRem}) when BRem =< 0 ->
    % Empty our buffers and send data upstream.
    BodyData = iolist_to_binary(lists:reverse(Buf)),
    {ok, BodyData, {trailers, MReq, [], ?PKT_SIZE}};
stream_chunked_body({trailers, MReq, Buf, BRem}) ->
    % Read another trailer into the buffer or stop on an
    % empty line.
    Socket = MReq:get(socket),
    mochiweb_socket:setopts(Socket, [{packet, line}]),
    case mochiweb_socket:recv(Socket, 0, ?TIMEOUT) of
        {ok, <<"\r\n">>} ->
            BodyData = iolist_to_binary(lists:reverse(Buf, <<"\r\n">>)),
            {ok, BodyData, eof};
        {ok, Footer} ->
            NewState = {trailers, MReq, [Footer | Buf], BRem-size(Footer)},
            stream_chunked_body(NewState);
        _ ->
            exit(normal)
    end;
stream_chunked_body(eof) ->
    % Tell ibrowse we're done sending data.
    {ok, eof}.


stream_length_body({init, MochiReq, Length}) ->
    % Do the expect-continue dance
    init_body_stream(MochiReq),
    stream_length_body({stream, MochiReq, Length});
stream_length_body({stream, _MochiReq, 0}) ->
    % Finished streaming.
    {ok, eof};
stream_length_body({stream, MochiReq, Length}) ->
    BufLen = lists:min([Length, ?PKT_SIZE]),
    case MochiReq:recv(BufLen) of
        <<>> -> {ok, eof};
        Bin -> {ok, Bin, {stream, MochiReq, Length-BufLen}}
    end.


init_body_stream(MochiReq) ->
    Expect = case MochiReq:get_header_value("expect") of
        undefined ->
            undefined;
        Value when is_list(Value) ->
            string:to_lower(Value)
    end,
    case Expect of
        "100-continue" ->
            MochiReq:start_raw_response({100, gb_trees:empty()});
        _Else ->
            ok
    end.


read_chunk_length(MochiReq) ->
    Socket = MochiReq:get(socket),
    mochiweb_socket:setopts(Socket, [{packet, line}]),
    case mochiweb_socket:recv(Socket, 0, ?TIMEOUT) of
        {ok, Header} ->
            mochiweb_socket:setopts(Socket, [{packet, raw}]),
            Splitter = fun(C) ->
                C =/= $\r andalso C =/= $\n andalso C =/= $\s
            end,
            {Hex, _Rest} = lists:splitwith(Splitter, ?b2l(Header)),
            {mochihex:to_int(Hex), Header};
        _ ->
            exit(normal)
    end.


stream_response(Req, ReqId) ->
    receive
        {ibrowse_async_headers, ReqId, "100", _} ->
            ibrowse:stream_next(ReqId),
            stream_response(Req, ReqId);
        {ibrowse_async_headers, ReqId, Status, Headers} ->
            case body_length(Headers) of
                chunked ->
                    {ok, Resp} = couch_httpd:start_chunked_response(
                        Req, Status, Headers
                    ),
                    ibrowse:stream_next(ReqId),
                    stream_chunked_response(Req, ReqId, Resp),
                    {ok, Resp};
                Length when is_integer(Length) ->
                    {ok, Resp} = couch_httpd:start_response_length(
                        Req, Status, Headers, Length
                    ),
                    ibrowse:stream_next(ReqId),
                    stream_length_response(Req, ReqId, Resp),
                    {ok, Resp}
            end
    end.


stream_chunked_response(Req, ReqId, Resp) ->
    receive
        {ibrowse_async_response, ReqId, Chunk} ->
            couch_httpd:send_chunk(Resp, Chunk),
            ibrowse:stream_next(ReqId),
            stream_chunked_response(Req, ReqId, Resp);
        {ibrowse_async_response, ReqId, {error, Reason}} ->
            throw({error, Reason});
        {ibrowse_async_response_end, ReqId} ->
            couch_httpd:last_chunk(Resp)
    end.


stream_length_response(Req, ReqId, Resp) ->
    receive
        {ibrowse_async_response, ReqId, Chunk} ->
            couch_httpd:send(Resp, Chunk),
            ibrowse:stream_next(ReqId),
            stream_length_response(Req, ReqId, Resp);
        {ibrowse_async_response, {error, Reason}} ->
            throw({error, Reason});
        {ibrowse_async_response_end, ReqId} ->
            ok
    end.


body_length(Headers) ->
    case is_chunked(Headers) of
        true -> chunked;
        _ -> content_length(Headers)
    end.


is_chunked([]) ->
    false;
is_chunked([{K, V} | Rest]) ->
    case string:to_lower(K) of
        "transfer-encoding" ->
            string:to_lower(V) == "chunked";
        _ ->
            is_chunked(Rest)
    end.

content_length([]) ->
    undefined;
content_length([{K, V} | Rest]) ->
    case string:to_lower(K) of
        "content-length" ->
            list_to_integer(V);
        _ ->
            content_length(Rest)
    end.

