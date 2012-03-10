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

-export([open/1, open/2, close/1, set_db_pid/2]).
-export([bytes/1, sync/1, truncate/2]).
-export([pread_term/2, pread_binary/2, pread_iolist/2]).
-export([append_term/2, append_term/3, append_term_md5/2, append_term_md5/3]).
-export([append_binary/2, append_binary_md5/2, append_raw_binary/2]).
-export([assemble_file_chunk/1, assemble_file_chunk/2]).
-export([read_header/1, write_header/2]).
-export([init_delete_dir/1, delete/2, delete/3, nuke_dir/2]).


-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).


-include("couch_db.hrl").


-define(INITIAL_WAIT, 60000).
-define(MONITOR_CHECK, 10000).
-define(SIZE_BLOCK, 16#1000). % 4 KiB


-record(file, {
    fd,
    eof = 0,
    db_pid
}).


open(FileName) ->
    open(FileName, []).


open(FileName, Options) ->
    proc_lib:start_link(?MODULE, init, [{FileName, Options}]).


close(Fd) ->
    Ref = erlang:monitor(process, Fd),
    ok = gen_server:cast(Fd, close),
    receive {'DOWN', Ref, _, _, _} -> ok end,
    ok.


set_db_pid(Fd, Pid) ->
    gen_server:call(Fd, {set_db_pid, Pid}).


bytes(Fd) ->
    gen_server:call(Fd, bytes, infinity).


sync(Filepath) when is_list(Filepath) ->
    {ok, Fd} = file:open(Filepath, [append, raw]),
    try ok = file:sync(Fd) after ok = file:close(Fd) end;
sync(Fd) ->
    gen_server:call(Fd, sync, infinity).


truncate(Fd, Pos) ->
    gen_server:call(Fd, {truncate, Pos}, infinity).


pread_term(Fd, Pos) ->
    {ok, Bin} = pread_binary(Fd, Pos),
    {ok, couch_compress:decompress(Bin)}.


pread_binary(Fd, Pos) ->
    {ok, L} = pread_iolist(Fd, Pos),
    {ok, iolist_to_binary(L)}.


pread_iolist(Fd, Pos) ->
    case gen_server:call(Fd, {pread_iolist, Pos}, infinity) of
    {ok, IoList, <<>>} ->
        {ok, IoList};
    {ok, IoList, Md5} ->
        case couch_util:md5(IoList) of
            Md5 -> {ok, IoList};
            _ -> exit({file_corruption, <<"md5 does not match content">>})
        end;
    Error ->
        Error
    end.


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


append_binary(Fd, Bin) ->
    gen_server:call(Fd, {append_bin, assemble_file_chunk(Bin)}, infinity).

    
append_binary_md5(Fd, Bin) ->
    gen_server:call(Fd,
        {append_bin, assemble_file_chunk(Bin, couch_util:md5(Bin))}, infinity).


append_raw_binary(Fd, Chunk) ->
    gen_server:call(Fd, {append_bin, Chunk}, infinity).


assemble_file_chunk(Bin) ->
    [<<0:1/integer, (iolist_size(Bin)):31/integer>>, Bin].


assemble_file_chunk(Bin, Md5) ->
    [<<1:1/integer, (iolist_size(Bin)):31/integer>>, Md5, Bin].


read_header(Fd) ->
    case gen_server:call(Fd, read_header, infinity) of
        {ok, Bin} -> {ok, binary_to_term(Bin)};
        Else -> Else
    end.


write_header(Fd, Data) ->
    Bin = term_to_binary(Data),
    Md5 = couch_util:md5(Bin),
    gen_server:call(Fd, {write_header, <<Md5/binary, Bin/binary>>}, infinity).


init_delete_dir(RootDir) ->
    Dir = filename:join(RootDir,".delete"),
    filelib:ensure_dir(filename:join(Dir,"foo")),
    RemFun = fun(Filename, _) -> ok = file:delete(Filename) end,
    filelib:fold_files(Dir, ".*", true, RemFun, ok).


delete(RootDir, Filepath) ->
    delete(RootDir, Filepath, true).


delete(RootDir, Filepath, Async) ->
    DelFile = filename:join([RootDir,".delete", ?b2l(couch_uuids:random())]),
    case file:rename(Filepath, DelFile) of
        ok when Async ->
            spawn(file, delete, [DelFile]),
            ok;
        ok ->
            file:delete(DelFile)
        Error ->
            Error
    end.


nuke_dir(RootDelDir, Dir) ->
    FoldFun = fun(File) ->
        Path = Dir ++ "/" ++ File,
        case delete(RootDelDir, Path, false) of
            {error, eperm} -> ok = nuke_dir(RootDelDir, Path);
            {error, enoent} -> ok;
            ok -> ok
        end
    end,
    case file:list_dir(Dir) of
        {ok, Files} ->
            lists:foreach(FoldFun, Files),
            ok = file:del_dir(Dir);
        {error, enoent} ->
            ok
    end.


% gen_server functions


init({FileName, Options}) ->
    process_flag(trap_exit, true),
    Resp = try
        open_int(FileName, Options)
    catch
        throw:{error, eaccess} ->
            {error, {invalid_permissions, FileName}}
        throw:{error, eexists} ->
            {error, {file_exists, FileName}}
        throw:Error ->
            Error
    end,
    case Resp of
        {ok, File} ->
            maybe_track_open_os_files(Options),
            proc_lib:init_ack({ok, self()}),
            gen_server:enter_loop(?MODULE, [], File);
        Error ->
            proc_lib:init_ack(Error)
    end.


terminate(_Reason, #file{fd = Fd}) ->
    ok = file:close(Fd).


handle_call({set_db_pid, Pid}, _From, #file{db_pid=OldPid}=File) ->
    case is_pid(OldPid) of
        true -> unlink(OldPid);
        false -> ok
    end,
    link(Pid),
    {reply, ok, File#file{db_pid=Pid}, 0};

handle_call(bytes, _From, #file{fd = Fd} = File) ->
    {reply, file:position(Fd, eof), File};

handle_call(sync, _From, #file{fd=Fd}=File) ->
    {reply, file:sync(Fd), File};

handle_call({truncate, Pos}, _From, #file{fd=Fd}=File) ->
    {ok, Pos} = file:position(Fd, Pos),
    case file:truncate(Fd) of
        ok -> {reply, ok, File#file{eof = Pos}};
        Error -> {reply, Error, File}
    end;

handle_call({pread_iolist, Pos}, _From, File) ->
    {RawData, NextPos} = try
        % up to 8Kbs of read ahead
        read_raw_iolist_int(File, Pos, 2 * ?SIZE_BLOCK - (Pos rem ?SIZE_BLOCK))
    catch
    _:_ ->
        read_raw_iolist_int(File, Pos, 4)
    end,
    <<Prefix:1/integer, Len:31/integer, RestRawData/binary>> =
        iolist_to_binary(RawData),
    case Prefix of
    1 ->
        {Md5, IoList} = extract_md5(
            maybe_read_more_iolist(RestRawData, 16 + Len, NextPos, File)),
        {reply, {ok, IoList, Md5}, File};
    0 ->
        IoList = maybe_read_more_iolist(RestRawData, Len, NextPos, File),
        {reply, {ok, IoList, <<>>}, File}
    end;

handle_call({append_bin, Bin}, _From, #file{fd = Fd, eof = Pos} = File) ->
    Blocks = make_blocks(Pos rem ?SIZE_BLOCK, Bin),
    Size = iolist_size(Blocks),
    case file:write(Fd, Blocks) of
    ok ->
        {reply, {ok, Pos, Size}, File#file{eof = Pos + Size}};
    Error ->
        {reply, Error, File}
    end;

handle_call(read_header, _From, #file{fd = Fd, eof = Pos} = File) ->
    {reply, find_header(Fd, Pos div ?SIZE_BLOCK), File};

handle_call({write_header, Bin}, _From, #file{fd = Fd, eof = Pos} = File) ->
    BinSize = byte_size(Bin),
    case Pos rem ?SIZE_BLOCK of
    0 ->
        Padding = <<>>;
    BlockOffset ->
        Padding = <<0:(8*(?SIZE_BLOCK-BlockOffset))>>
    end,
    FinalBin = [Padding, <<1, BinSize:32/integer>> | make_blocks(5, [Bin])],
    case file:write(Fd, FinalBin) of
    ok ->
        {reply, ok, File#file{eof = Pos + iolist_size(FinalBin)}};
    Error ->
        {reply, Error, File}
    end;

handle_call(Mesg, From, File) ->
    {stop, {invalid_call, Mesg}, error, File}.


handle_cast(close, File) ->
    {stop, normal, File};

handle_cast(Mesg, File)
    {stop, {invalid_cast, Mesg}, File}.


handle_info(maybe_close, File) ->
    case is_idle() of
        true ->
            {stop, normal, File};
        false ->
            erlang:send_after(?MONITOR_CHECK, self(), maybe_close),
            {noreply, File}
    end;

handle_info({'EXIT', Pid, _}, #file{db_pid=Pid}=File) ->
    case is_idle() of
        true -> {stop, normal, File};
        false -> {noreply, File}
    end;

handle_info({'EXIT', _, normal}, File) ->
    {noreply, File};

handle_info({'EXIT', _, Reason}, File) ->
    {stop, Reason, File};

handle_info(Mesg, File) ->
    {stop, {invalid_info, Mesg}, File}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% Private API


open_int(FileName, Options) ->
    case lists:member(create, Options) of
        true -> create_file(FileName, Options);
        false -> open_file(FileName, Options)
    end.


create_file(FileName, Options) ->
    filelib:ensure_dir(Filepath),
    OpenOptions = file_open_options(Options),
    case file:open(Filepath, OpenOptions) of
        {ok, Fd} ->
            {ok, Length} = file:position(Fd, eof),
            case Length > 0 of
                true ->
                    % This means the file already exists and has data.
                    % FYI: We don't differentiate between empty files and
                    % non-existant files here.
                    case lists:member(overwrite, Options) of
                        true ->
                            {ok, 0} = file:position(Fd, 0),
                            ok = file:truncate(Fd),
                            ok = file:sync(Fd),
                            {ok, #file{fd=Fd, eof=0}};
                        false ->
                            ok = file:close(Fd),
                            throw({error, eexist})
                    end;
                false ->
                    {ok, #file{fd=Fd, eof=Length}}
            end;
        {error, _} = Error ->
            throw(Error)
    end.


open_file(FileName, Options)
    OpenOptions = file_open_options(Options),
    % Open in read mode first, so we don't create the file if
    % it doesn't exist.
    case file:open(Filepath, [read, raw]) of
        {ok, Fd_Read} ->
            {ok, Fd} = file:open(Filepath, OpenOptions),
            ok = file:close(Fd_Read),
            {ok, Eof} = file:position(Fd, eof),
            {ok, #file{fd=Fd, eof=Eof}};
        {error, _} = Error ->
            throw(Error)
    end.


file_open_options(Options) ->
    case lists:member(read_only, Options) of
        true -> [];
        false -> [append]
    end ++ [read, raw, binary].


maybe_track_open_os_files(FileOptions) ->
    case lists:member(sys_db, FileOptions) of
        true ->
            ok;
        false ->
            couch_stats_collector:track_process_count({couchdb, open_os_files})
    end.


flush(File0) ->
    flush_reads(flush_writes(File)).


flush_reads(File) ->
    File.


flush_writes(File) ->
    File.




find_header(_Fd, -1) ->
    no_valid_header;
find_header(Fd, Block) ->
    case (catch load_header(Fd, Block)) of
        {ok, Bin} -> {ok, Bin};
        _Error -> find_header(Fd, Block - 1)
    end.


load_header(Fd, Block) ->
    {ok, <<1, HeaderLen:32/integer, RestBlock/binary>>} =
        file:pread(Fd, Block * ?SIZE_BLOCK, ?SIZE_BLOCK),
    TotalBytes = calculate_total_read_len(5, HeaderLen),
    case TotalBytes > byte_size(RestBlock) of
    false ->
        <<RawBin:TotalBytes/binary, _/binary>> = RestBlock;
    true ->
        {ok, Missing} = file:pread(
            Fd, (Block * ?SIZE_BLOCK) + 5 + byte_size(RestBlock),
            TotalBytes - byte_size(RestBlock)),
        RawBin = <<RestBlock/binary, Missing/binary>>
    end,
    <<Md5Sig:16/binary, HeaderBin/binary>> =
        iolist_to_binary(remove_block_prefixes(5, RawBin)),
    Md5Sig = couch_util:md5(HeaderBin),
    {ok, HeaderBin}.


maybe_read_more_iolist(Buffer, DataSize, _, _)
    when DataSize =< byte_size(Buffer) ->
    <<Data:DataSize/binary, _/binary>> = Buffer,
    [Data];
maybe_read_more_iolist(Buffer, DataSize, NextPos, File) ->
    {Missing, _} =
        read_raw_iolist_int(File, NextPos, DataSize - byte_size(Buffer)),
    [Buffer, Missing].


read_raw_iolist_int(#file{fd = Fd}, Pos, Len) ->
    BlockOffset = Pos rem ?SIZE_BLOCK,
    TotalBytes = calculate_total_read_len(BlockOffset, Len),
    {ok, <<RawBin:TotalBytes/binary>>} = file:pread(Fd, Pos, TotalBytes),
    {remove_block_prefixes(BlockOffset, RawBin), Pos + TotalBytes}.


extract_md5(FullIoList) ->
    {Md5List, IoList} = split_iolist(FullIoList, 16, []),
    {iolist_to_binary(Md5List), IoList}.


calculate_total_read_len(0, FinalLen) ->
    calculate_total_read_len(1, FinalLen) + 1;
calculate_total_read_len(BlockOffset, FinalLen) ->
    case ?SIZE_BLOCK - BlockOffset of
        BlockLeft when BlockLeft >= FinalLen ->
            FinalLen;
        BlockLeft ->
            FinalLen + ((FinalLen - BlockLeft) div (?SIZE_BLOCK -1)) +
                if ((FinalLen - BlockLeft) rem (?SIZE_BLOCK -1)) =:= 0 -> 0;
                    true -> 1 end
    end.


remove_block_prefixes(_BlockOffset, <<>>) ->
    [];
remove_block_prefixes(0, <<_BlockPrefix,Rest/binary>>) ->
    remove_block_prefixes(1, Rest);
remove_block_prefixes(BlockOffset, Bin) ->
    BlockBytesAvailable = ?SIZE_BLOCK - BlockOffset,
    case size(Bin) of
        Size when Size > BlockBytesAvailable ->
            <<DataBlock:BlockBytesAvailable/binary,Rest/binary>> = Bin,
            [DataBlock | remove_block_prefixes(0, Rest)];
        _Size ->
            [Bin]
    end.


make_blocks(_BlockOffset, []) ->
    [];
make_blocks(0, IoList) ->
    [<<0>> | make_blocks(1, IoList)];
make_blocks(BlockOffset, IoList) ->
    case split_iolist(IoList, (?SIZE_BLOCK - BlockOffset), []) of
        {Begin, End} -> [Begin | make_blocks(0, End)];
        _SplitRemaining -> IoList
    end.


%% @doc Returns a tuple where the first element contains the leading SplitAt
%% bytes of the original iolist, and the 2nd element is the tail. If SplitAt
%% is larger than byte_size(IoList), return the difference.
split_iolist(List, 0, BeginAcc) ->
    {lists:reverse(BeginAcc), List};
split_iolist([], SplitAt, _BeginAcc) ->
    SplitAt;
split_iolist([<<Bin/binary>> | Rest], SplitAt, BeginAcc) when SplitAt > byte_size(Bin) ->
    split_iolist(Rest, SplitAt - byte_size(Bin), [Bin | BeginAcc]);
split_iolist([<<Bin/binary>> | Rest], SplitAt, BeginAcc) ->
    <<Begin:SplitAt/binary,End/binary>> = Bin,
    split_iolist([End | Rest], 0, [Begin | BeginAcc]);
split_iolist([Sublist| Rest], SplitAt, BeginAcc) when is_list(Sublist) ->
    case split_iolist(Sublist, SplitAt, BeginAcc) of
    {Begin, End} ->
        {Begin, [End | Rest]};
    SplitRemaining ->
        split_iolist(Rest, SplitAt - (SplitAt - SplitRemaining), [Sublist | BeginAcc])
    end;
split_iolist([Byte | Rest], SplitAt, BeginAcc) when is_integer(Byte) ->
    split_iolist(Rest, SplitAt - 1, [Byte | BeginAcc]).


is_idle() ->
    case process_info(self(), monitored_by) of
        {monitored_by, []} -> true;
        {monitored_by, [_]} -> true;
        _ -> false
    end.
