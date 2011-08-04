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

-module(couch_file).
-behaviour(gen_server).

-include("couch_db.hrl").

-define(BLOCK_SIZE, 4096).

-record(file, {
    fd,
    eof = 0
}).

% public API
-export([open/1, open/2, close/1, bytes/1, sync/1, truncate/2]).
-export([pread_term/2, pread_iolist/2, pread_binary/2]).
-export([append_binary/2, append_binary_md5/2]).
-export([append_raw_chunk/2, assemble_file_chunk/1, assemble_file_chunk/2]).
-export([append_term/2, append_term/3, append_term_md5/2, append_term_md5/3]).
-export([write_header/2, read_header/1]).
-export([delete/2, delete/3, init_delete_dir/1]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

%%----------------------------------------------------------------------
%% Args:   Valid Options are [create] and [create,overwrite].
%%  Files are opened in read/write mode.
%% Returns: On success, {ok, Fd}
%%  or {error, Reason} if the file could not be opened.
%%----------------------------------------------------------------------

open(Filepath) ->
    open(Filepath, []).

open(Filepath, Options) ->
    case gen_server:start_link(couch_file,
            {Filepath, Options, self(), Ref = make_ref()}, []) of
    {ok, Fd} ->
        {ok, Fd};
    ignore ->
        % get the error
        receive
        {Ref, Pid, Error} ->
            case process_info(self(), trap_exit) of
            {trap_exit, true} -> receive {'EXIT', Pid, _} -> ok end;
            {trap_exit, false} -> ok
            end,
            case Error of
            {error, eacces} -> {file_permission_error, Filepath};
            _ -> Error
            end
        end;
    Error ->
        Error
    end.


%%----------------------------------------------------------------------
%% Purpose: To append an Erlang term to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos, NumBytesWritten} where Pos is the file offset to
%%  the beginning the serialized  term. Use pread_term to read the term
%%  back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------

append_term(Fd, Term) ->
    append_term(Fd, Term, []).

append_term(Fd, Term, Options) ->
    Comp = couch_util:get_value(compression, Options, ?DEFAULT_COMPRESSION),
    append_binary(Fd, couch_compress:compress(Term, Comp)).

append_term_md5(Fd, Term) ->
    append_term_md5(Fd, Term, []).

append_term_md5(Fd, Term, Options) ->
    Comp = couch_util:get_value(compression, Options, ?DEFAULT_COMPRESSION),
    append_binary_md5(Fd, couch_compress:compress(Term, Comp)).

%%----------------------------------------------------------------------
%% Purpose: To append an Erlang binary to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos, NumBytesWritten} where Pos is the file offset to the
%%  beginning the serialized term. Use pread_term to read the term back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------

append_binary(Fd, Bin) ->
    Chunk = assemble_file_chunk(Bin),
    gen_server:call(Fd, {append_bin, iolist_to_binary(Chunk)}, infinity).
    
append_binary_md5(Fd, Bin) ->
    Chunk = assemble_file_chunk(Bin, couch_util:md5(Bin)),
    gen_server:call(Fd, {append_bin, iolist_to_binary(Chunk)}, infinity).

append_raw_chunk(Fd, Chunk) ->
    gen_server:call(Fd, {append_bin, iolist_to_binary(Chunk)}, infinity).


assemble_file_chunk(Bin) ->
    <<0:1/integer, (iolist_size(Bin)):31/integer, Bin/binary>>.

assemble_file_chunk(Bin, Md5) ->
    <<1:1/integer, (iolist_size(Bin)):31/integer, Md5/binary, Bin/binary>>.

%%----------------------------------------------------------------------
%% Purpose: Reads a term from a file that was written with append_term
%% Args:    Pos, the offset into the file where the term is serialized.
%% Returns: {ok, Term}
%%  or {error, Reason}.
%%----------------------------------------------------------------------


pread_term(Fd, Pos) ->
    {ok, Bin} = pread_binary(Fd, Pos),
    {ok, couch_compress:decompress(Bin)}.


%%----------------------------------------------------------------------
%% Purpose: Reads a binrary from a file that was written with append_binary
%% Args:    Pos, the offset into the file where the term is serialized.
%% Returns: {ok, Term}
%%  or {error, Reason}.
%%----------------------------------------------------------------------

pread_binary(Fd, Pos) ->
    {ok, L} = pread_iolist(Fd, Pos),
    {ok, iolist_to_binary(L)}.


pread_iolist(Fd, Pos) ->
    case gen_server:call(Fd, {pread_iolist, Pos}, infinity) of
    {ok, IoList, <<>>} ->
        {ok, IoList};
    {ok, IoList, Md5} ->
        case couch_util:md5(IoList) of
        Md5 ->
            {ok, IoList};
        _ ->
            exit({file_corruption, <<"file corruption">>})
        end;
    Error ->
        Error
    end.

%%----------------------------------------------------------------------
%% Purpose: The length of a file, in bytes.
%% Returns: {ok, Bytes}
%%  or {error, Reason}.
%%----------------------------------------------------------------------

% length in bytes
bytes(Fd) ->
    gen_server:call(Fd, bytes, infinity).

%%----------------------------------------------------------------------
%% Purpose: Truncate a file to the number of bytes.
%% Returns: ok
%%  or {error, Reason}.
%%----------------------------------------------------------------------

truncate(Fd, Pos) ->
    gen_server:call(Fd, {truncate, Pos}, infinity).

%%----------------------------------------------------------------------
%% Purpose: Ensure all bytes written to the file are flushed to disk.
%% Returns: ok
%%  or {error, Reason}.
%%----------------------------------------------------------------------

sync(Filepath) when is_list(Filepath) ->
    {ok, Fd} = file:open(Filepath, [append, raw]),
    try file:sync(Fd) after file:close(Fd) end;
sync(Fd) ->
    gen_server:call(Fd, sync, infinity).

%%----------------------------------------------------------------------
%% Purpose: Close the file.
%% Returns: ok
%%----------------------------------------------------------------------
close(Fd) ->
    couch_util:shutdown_sync(Fd).


delete(RootDir, Filepath) ->
    delete(RootDir, Filepath, true).


delete(RootDir, Filepath, Async) ->
    DelFile = filename:join([RootDir,".delete", ?b2l(couch_uuids:random())]),
    case file:rename(Filepath, DelFile) of
    ok ->
        if (Async) ->
            spawn(file, delete, [DelFile]),
            ok;
        true ->
            file:delete(DelFile)
        end;
    Error ->
        Error
    end.


init_delete_dir(RootDir) ->
    Dir = filename:join(RootDir,".delete"),
    % note: ensure_dir requires an actual filename companent, which is the
    % reason for "foo".
    filelib:ensure_dir(filename:join(Dir,"foo")),
    filelib:fold_files(Dir, ".*", true,
        fun(Filename, _) ->
            ok = file:delete(Filename)
        end, ok).


read_header(Fd) ->
    case gen_server:call(Fd, find_header, infinity) of
    {ok, Bin} ->
        {ok, binary_to_term(Bin)};
    Else ->
        Else
    end.

write_header(Fd, Data) ->
    Bin = term_to_binary(Data),
    Md5 = couch_util:md5(Bin),
    % now we assemble the final header binary and write to disk
    FinalBin = <<Md5/binary, Bin/binary>>,
    gen_server:call(Fd, {write_header, FinalBin}, infinity).




init_status_error(ReturnPid, Ref, Error) ->
    ReturnPid ! {Ref, self(), Error},
    ignore.

% server functions

init({Filepath, Options, ReturnPid, Ref}) ->
    process_flag(trap_exit, true),
    OpenOptions = file_open_options(Options),
    case lists:member(create, Options) of
    true ->
        filelib:ensure_dir(Filepath),
        case file:open(Filepath, OpenOptions) of
        {ok, Fd} ->
            {ok, Length} = file:position(Fd, eof),
            case Length > 0 of
            true ->
                % this means the file already exists and has data.
                % FYI: We don't differentiate between empty files and non-existant
                % files here.
                case lists:member(overwrite, Options) of
                true ->
                    {ok, 0} = file:position(Fd, 0),
                    ok = file:truncate(Fd),
                    ok = file:sync(Fd),
                    maybe_track_open_os_files(Options),
                    {ok, #file{fd=Fd}};
                false ->
                    ok = file:close(Fd),
                    init_status_error(ReturnPid, Ref, file_exists)
                end;
            false ->
                maybe_track_open_os_files(Options),
                {ok, #file{fd=Fd}}
            end;
        Error ->
            init_status_error(ReturnPid, Ref, Error)
        end;
    false ->
        % open in read mode first, so we don't create the file if it doesn't exist.
        case file:open(Filepath, [read, raw]) of
        {ok, Fd_Read} ->
            {ok, Fd} = file:open(Filepath, OpenOptions),
            ok = file:close(Fd_Read),
            maybe_track_open_os_files(Options),
            {ok, Eof} = file:position(Fd, eof),
            {ok, #file{fd=Fd, eof=Eof}};
        Error ->
            init_status_error(ReturnPid, Ref, Error)
        end
    end.

file_open_options(Options) ->
    [read, raw, binary] ++ case lists:member(read_only, Options) of
    true ->
        [];
    false ->
        [append]
    end.

maybe_track_open_os_files(FileOptions) ->
    case lists:member(sys_db, FileOptions) of
    true ->
        ok;
    false ->
        couch_stats_collector:track_process_count({couchdb, open_os_files})
    end.

terminate(_Reason, #file{fd = Fd}) ->
    ok = file:close(Fd).


handle_call({pread_iolist, Pos}, _From, #file{fd=Fd}=File) ->
    {ok, <<Flag:1/integer, Len:31/integer>>} = file:pread(Fd, Pos, 4),
    Resp = case Flag of
        0 ->
            {ok, Data} = file:read(Fd, Pos+4, Len),
            {Data, <<>>};
        1 ->
            {ok, Md5} = file:read(Fd, Pos+4, 16),
            case Md5 of
                <<0:16/binary>> ->
                    read_iolist(Fd, Pos+20, Len);
                _ ->
                    {ok, Data} = file:pread(Fd, Pos+20, Len),
                    {Data, Md5}
            end
    end,
    {reply, {ok, Resp}, File};

handle_call(bytes, _From, #file{fd = Fd} = File) ->
    {reply, file:position(Fd, eof), File};

handle_call(sync, _From, #file{fd=Fd}=File) ->
    {reply, file:sync(Fd), File};

handle_call({truncate, Pos}, _From, #file{fd=Fd}=File) ->
    {ok, Pos} = file:position(Fd, Pos),
    case file:truncate(Fd) of
    ok ->
        {reply, ok, File#file{eof = Pos}};
    Error ->
        {reply, Error, File}
    end;

handle_call({append_bin, Bin}, _From, #file{fd = Fd, eof = Pos} = File) ->
    Offset = Pos rem ?BLOCK_SIZE,
    SizeList = block_sizes(Bin, byte_size(Bin), Offset, [0]),
    {IoInfo, IoData} = sizes_to_ioinfo(Bin, SizeList, Pos, [], []),
    case write_iodata(Fd, IoInfo, IoData) of
        {ok, Size} ->
            IoSize = iolist_size(IoData) + Size,
            {reply, {ok, Pos, IoSize}, File#file{eof=Pos+IoSize}};
        Error ->
            {reply, Error, File}
    end;

handle_call({write_header, Bin}, _From, #file{fd = Fd, eof = Pos} = File) ->
    BinSize = byte_size(Bin),
    case Pos rem ?BLOCK_SIZE of
    0 ->
        Padding = <<>>;
    BlockOffset ->
        Padding = <<0:(8*(?BLOCK_SIZE-BlockOffset))>>
    end,
    FinalBin = [Padding, <<1, BinSize:32/integer>> | Bin],
    case file:write(Fd, FinalBin) of
    ok ->
        {reply, ok, File#file{eof = Pos + iolist_size(FinalBin)}};
    Error ->
        {reply, Error, File}
    end;

handle_call(find_header, _From, #file{fd = Fd, eof = Pos} = File) ->
    {reply, find_header(Fd, Pos div ?BLOCK_SIZE), File}.

handle_cast(close, Fd) ->
    {stop,normal,Fd}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info({'EXIT', _, normal}, Fd) ->
    {noreply, Fd};
handle_info({'EXIT', _, Reason}, Fd) ->
    {stop, Reason, Fd}.


find_header(_Fd, -1) ->
    no_valid_header;
find_header(Fd, Block) ->
    case (catch load_header(Fd, Block)) of
    {ok, Bin} ->
        {ok, Bin};
    _Error ->
        find_header(Fd, Block -1)
    end.

load_header(Fd, Block) ->
    {ok, <<1, TotalBytes:32/integer, RestBlock/binary>>} =
        file:pread(Fd, Block * ?BLOCK_SIZE, ?BLOCK_SIZE),
    case TotalBytes > byte_size(RestBlock) of
    false ->
        <<RawBin:TotalBytes/binary, _/binary>> = RestBlock;
    true ->
        {ok, Missing} = file:pread(
            Fd, (Block * ?BLOCK_SIZE) + 5 + byte_size(RestBlock),
            TotalBytes - byte_size(RestBlock)),
        RawBin = <<RestBlock/binary, Missing/binary>>
    end,
    <<Md5Sig:16/binary, HeaderBin/binary>> = RawBin,
    Md5Sig = couch_util:md5(HeaderBin),
    {ok, HeaderBin}.


read_iolist(Fd, Pos, Len) ->
    {ok, IoBin} = file:pread(Fd, Pos, Len),
    {ok, IoData} = file:pread(Fd, binary_to_term(IoBin)),
    case IoData of
        <<0:1/integer, Len:31/integer, Data:Len/binary>> ->
            {Data, <<>>};
        <<1:1/integer, Len:31/integer, Md5:16/binary, Data:Len/binary>> ->
            {Data, Md5}
    end.


block_sizes(<<>>, 0, _Off, Acc) ->
    lists:reverse(Acc);
block_sizes(<<1, _/binary>>=Bin, Sz, 0, Acc) ->
    block_sizes(Bin, Sz, ?BLOCK_SIZE-1, [0 | Acc]);
block_sizes(_Bin, Sz, Off, [Len | Rest]) when Sz =< Off ->
    lists:reverse([Len+Sz, Rest]);
block_sizes(Bin, Sz, Off, [Len | Rest]) ->
    <<_:Off/binary, Rest/binary>> = Bin,
    block_sizes(Rest, Sz-Off, ?BLOCK_SIZE, [Len+Off | Rest]).


sizes_to_ioinfo(Bin, [Size], Pos, Info, Acc) when size(Bin) == Size ->
    {lists:reverse([{Pos, Size} | Info]), lists:reverse([Bin | Acc])};
sizes_to_ioinfo(Bin, [Size | RestSizes], Pos, Info, Acc) ->
    <<Begin:Size/binary, RestBin/binary>> = Bin,
    Info1 = [{Pos, Size} | Info],
    Acc1 = [<<0>>, Begin | Acc],
    sizes_to_ioinfo(RestBin, RestSizes, Pos+Size+1, Info1, Acc1).


write_iodata(Fd, [_], [Bin]) ->
    case file:write(Fd, Bin) of
        ok -> {ok, byte_size(Bin), 0};
        Error -> Error
    end;
write_iodata(Fd, IoSizes, IoData) ->
    case file:write(Fd, IoData) of
        ok ->
            Bin = term_to_binary(IoSizes),
            Size = byte_size(Bin),
            Data = <<1:1/integer, Size:31/integer, 0:(16*8), Bin/binary>>,
            case file:write(Fd, Data) of
                ok ->
                    {ok, Size+20};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

