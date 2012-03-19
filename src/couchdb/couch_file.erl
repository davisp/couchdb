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


-export([open/1, open/2, close/1]).
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


-define(SIZE_BLOCK, 16#1000). % 4 KiB


-record(file, {
    fd,
    eof,
    pos,
    rbuf=[],
    rlen=0,
    rmax,
    wbuf=[],
    wlen=0,
    wmax
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
    {ok, Md5, IoList} ->
        case couch_util:md5(IoList) of
            Md5 -> {ok, IoList};
            _ -> exit({file_corruption, <<"md5 does not match content">>})
        end;
    Else ->
        Else
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
            file:delete(DelFile);
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
            {error, {invalid_permissions, FileName}};
        throw:{error, eexists} ->
            {error, {file_exists, FileName}};
        throw:Error ->
            Error
    end,
    case Resp of
        {ok, File} ->
            RMax = proplists:get_value(read_buffer, Options, 100),
            WMax0 = proplists:get_value(write_buffer, Options, 0),
            WMax = case WMax0 of true -> 16#A00000; _ -> WMax0 end,
            maybe_track_open_os_files(Options),
            proc_lib:init_ack({ok, self()}),
            gen_server:enter_loop(?MODULE, [], File#file{rmax=RMax, wmax=WMax});
        Other ->
            proc_lib:init_ack(Other)
    end.


terminate(_Reason, #file{fd = Fd}) ->
    ok = file:close(Fd).


handle_call(bytes, _From, File) ->
    {reply, {ok, File#file.eof}, File, 0};

handle_call(sync, _From, File0) ->
    File = flush(File0),
    {reply, file:sync(File#file.fd), File, 0};

handle_call({truncate, Pos}, _From, File0) ->
    File = flush(File0),
    {ok, Pos} = file:position(File#file.fd, Pos),
    case file:truncate(File#file.fd) of
        ok -> {reply, ok, File#file{eof = Pos}, 0};
        Error -> {reply, Error, File, 0}
    end;

handle_call({pread_iolist, {Pos, _Size}}, From, File) ->
    % 0110 UPGRADE CODE
    handle_call({pread_iolist, Pos}, From, File);
handle_call({pread_iolist, Pos}, From, File0) ->
    File = insert_read(File0#file.rbuf, Pos, From, File0#file{rbuf=[]}),
    {noreply, maybe_flush(File), 0};

handle_call({append_bin, Bin}, _From, #file{wbuf=WB, wlen=WL, eof=Eof}=File0) ->
    Bytes = iolist_size(Bin),
    Size = total_read_len(Eof rem ?SIZE_BLOCK, Bytes),
    File = File0#file{wbuf=[Bin | WB], wlen=WL+Size, eof=Eof+Size},
    {reply, {ok, Eof, Size}, maybe_flush(File), 0};

handle_call(read_header, _From, File0) ->
    File = #file{fd = Fd, eof = Pos} = flush(File0),
    {reply, find_header(Fd, Pos div ?SIZE_BLOCK), File, 0};

handle_call({write_header, Bin}, _From, File0) ->
    File = #file{fd = Fd, eof = Eof} = flush(File0),
    BinSize = byte_size(Bin),
    case Eof rem ?SIZE_BLOCK of
        0 -> Padding = <<>>;
        BlockOffset -> Padding = <<0:(8*(?SIZE_BLOCK-BlockOffset))>>
    end,
    FinalBin = [Padding, <<1, BinSize:32/integer>> | make_blocks(5, [Bin])],
    NewEof = Eof + iolist_size(FinalBin),
    case file:write(Fd, FinalBin) of
        ok -> {reply, ok, File#file{eof = NewEof, pos = NewEof}, 0};
        Error -> {reply, Error, File, 0}
    end;

handle_call(Mesg, _From, File) ->
    {stop, {invalid_call, Mesg}, error, flush(File)}.


handle_cast(close, File) ->
    {stop, normal, flush(File)};

handle_cast(Mesg, File) ->
    {stop, {invalid_cast, Mesg}, flush(File)}.


handle_info(timeout, File) ->
    {noreply, flush(File)};

handle_info({'EXIT', _, normal}, File) ->
    {noreply, File};

handle_info({'EXIT', _, Reason}, File) ->
    {stop, Reason, flush(File)};

handle_info(Mesg, File) ->
    {stop, {invalid_info, Mesg}, flush(File)}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% Private API


open_int(FileName, Options) ->
    case lists:member(create, Options) of
        true -> create_file(FileName, Options);
        false -> open_file(FileName, Options)
    end.


create_file(FileName, Options) ->
    filelib:ensure_dir(FileName),
    OpenOptions = file_open_options(Options),
    case file:open(FileName, OpenOptions) of
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
                            {ok, #file{fd=Fd, eof=0, pos=0}};
                        false ->
                            ok = file:close(Fd),
                            throw({error, eexist})
                    end;
                false ->
                    {ok, #file{fd=Fd, eof=Length, pos=Length}}
            end;
        {error, _} = Error ->
            throw(Error)
    end.


open_file(FileName, Options) ->
    OpenOptions = file_open_options(Options),
    % Open in read mode first, so we don't create the file if
    % it doesn't exist.
    case file:open(FileName, [read, raw]) of
        {ok, ReadFd} ->
            {ok, Fd} = file:open(FileName, OpenOptions),
            ok = file:close(ReadFd),
            {ok, Eof} = file:position(Fd, eof),
            {ok, #file{fd=Fd, eof=Eof, pos=Eof}};
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


maybe_flush(#file{wlen=WL, wmax=WM}=File) when WL >= WM ->
    flush(File);
maybe_flush(#file{rlen=RL, rmax=RM}=File) when RL >= RM ->
    flush(File);
maybe_flush(File) ->
    File.


flush(File) ->
    flush_reads(flush_writes(File)).


flush_writes(#file{wbuf=[], wlen=0}=File) ->
    File;
flush_writes(#file{fd=Fd, eof=Eof, pos=Pos, wbuf=WB}=File) ->
    Blocks = make_blocks(Pos rem ?SIZE_BLOCK, lists:reverse(WB)),
    ok = file:write(Fd, Blocks),
    File#file{pos=Eof, wbuf=[], wlen=0}.


flush_reads(#file{rbuf=[], rlen=0}=File) ->
    File;
flush_reads(#file{fd=Fd, rbuf=RBuf}=File) ->
    RemPref = fun
        ({Bin, {P, Clients}}, Acc) when is_binary(Bin) ->
            Offset = P rem ?SIZE_BLOCK,
            Data = remove_block_prefixes(Offset, Bin),
            [{iolist_to_binary(Data), P + byte_size(Bin), Clients} | Acc];
        ({Error, {_, Clients}}, Acc) ->
            reply_all(Clients, Error),
            Acc
    end,
    MakeReqs = fun
        ({<<Prefix:1/integer, Len:31/integer, Rest/binary>>, P, C}, Acc) ->
            case Prefix of
                % These first two cases are for when the pre-fetch
                % was successful. Here we just chop off what we need
                % and send it as a reply back to the waiting clients.
                1 when Len + 16 =< byte_size(Rest) ->
                    BLen = Len+16,
                    <<Data:BLen/binary, _/binary>> = Rest,
                    {Md5, IoList} = extract_md5(Data),
                    reply_all(C, {ok, Md5, IoList}),
                    Acc;
                0 when Len =< byte_size(Rest) ->
                    <<Data:Len/binary, _/binary>> = Rest,
                    reply_all(C, {ok, Data}),
                    Acc;
                % We need to read more data from disk for this request
                % so add the necessary data to our lists
                1 ->
                    O = P rem ?SIZE_BLOCK,
                    R = {P, total_read_len(O, (Len + 16) - byte_size(Rest))},
                    S = {md5, Len + 16, O, Rest, C},
                    {Reqs0, Stat0} = Acc,
                    {[R | Reqs0], [S | Stat0]};
                0 ->
                    O = P rem ?SIZE_BLOCK,
                    R = {P, total_read_len(O, Len - byte_size(Rest))},
                    S = {raw, Len, O, Rest, C},
                    {Reqs0, Stat0} = Acc,
                    {[R | Reqs0], [S | Stat0]}
            end;
        ({B, _, C}, Acc) ->
            reply_all(C, {invalid_length_data, B}),
            Acc
    end,
    Reply = fun
        ({Rest0, {md5, L, O, Data, Clients}}) when is_binary(Rest0) ->
            Rest = remove_block_prefixes(O, Rest0),
            IoData = [Data | Rest],
            case iolist_size(IoData) of
                L ->
                    {Md5, Data} = extract_md5(IoData),
                    reply_all(Clients, {ok, Md5, Data});
                _ ->
                    reply_all(Clients, {error, not_enough_data})
            end;
        ({Rest0, {raw, L, O, Data, Clients}}) when is_binary(Rest0) ->
            Rest = remove_block_prefixes(O, Rest0),
            IoData = [Data | Rest],
            case iolist_size(IoData) of
                L -> reply_all(Clients, {ok, IoData});
                _ -> reply_all(Clients, {error, not_enough_data})
            end;
        ({Else, {_, _, _, _, Clients}}) ->
            reply_all(Clients, Else)
    end,
    % Initial reads read up to 8KiB to do app level pre-fetch
    Reads0 = [{P, (2 * ?SIZE_BLOCK) - (P rem ?SIZE_BLOCK)} || {P, _} <- RBuf],
    Resps0 = lists:zip(do_read(Fd, Reads0), RBuf),
    % Translate raw data from disk into binaries that have
    % the block prefixes removed.
    Lengths = lists:foldl(RemPref, [], Resps0),
    % Given the length data we go through and reply to
    % anything that was satisfied by the pre-fetch as well
    % as calculate what's required for the remaining
    % read requests
    {Reads1, Stats} = lists:foldl(MakeReqs, {[], []}, Lengths),
    % Construct and send the final response for anything that
    % required multiple read calls and
    Resps1 = lists:zip(do_read(Fd, Reads1), Stats),
    lists:foreach(Reply, Resps1),
    % And we're flushed
    File#file{rbuf=[], rlen=0}.


insert_read([], Pos, From, #file{rbuf=RB, rlen=RL}=File) ->
    File#file{rbuf=[{Pos, [From]} | RB], rlen=RL+1};
insert_read([{Pos, FList} | Rest], Pos, From, #file{rbuf=RB, rlen=RL}=File) ->
    NewRBuf = [{Pos, [From | FList]} | Rest] ++ RB,
    File#file{rbuf=NewRBuf, rlen=RL+1};
insert_read([R | Rest], Pos, From, #file{rbuf=RB}=File) ->
    insert_read(Rest, Pos, From, File#file{rbuf=[R | RB]}).


do_read(Fd, Reads) ->
    case file:pread(Fd, Reads) of
        {ok, Ret} -> Ret;
        Error -> [Error || _ <- Reads]
    end.


extract_md5(FullIoList) ->
    {Md5List, IoList} = split_iolist([FullIoList], 16, []),
    {iolist_to_binary(Md5List), IoList}.


total_read_len(0, FinalLen) ->
    total_read_len(1, FinalLen) + 1;
total_read_len(BlockOffset, FinalLen) ->
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


make_blocks(0, IoList) ->
    case iolist_size(IoList) of
        0 -> [];
        _ -> [<<0>> | make_blocks(1, IoList)]
    end;
make_blocks(BlockOffset, IoList) ->
    case iolist_size(IoList) of
        0 ->
            [];
        _ ->
            case split_iolist(IoList, (?SIZE_BLOCK - BlockOffset), []) of
                {Begin, End} -> [Begin | make_blocks(0, End)];
                _SplitRemaining -> IoList
            end
    end.


%% @doc Returns a tuple where the first element contains the leading SplitAt
%% bytes of the original iolist, and the 2nd element is the tail. If SplitAt
%% is larger than byte_size(IoList), return the difference.
split_iolist(List, 0, BeginAcc) ->
    {lists:reverse(BeginAcc), List};
split_iolist([], SplitAt, _BeginAcc) ->
    SplitAt;
split_iolist([<<Bin/binary>> | Rest], SplitAt, BeginAcc)
        when SplitAt > byte_size(Bin) ->
    split_iolist(Rest, SplitAt - byte_size(Bin), [Bin | BeginAcc]);
split_iolist([<<Bin/binary>> | Rest], SplitAt, BeginAcc) ->
    <<Begin:SplitAt/binary,End/binary>> = Bin,
    split_iolist([End | Rest], 0, [Begin | BeginAcc]);
split_iolist([Sublist| Rest], SplitAt, BeginAcc) when is_list(Sublist) ->
    case split_iolist(Sublist, SplitAt, BeginAcc) of
    {Begin, End} ->
        {Begin, [End | Rest]};
    SplitRem ->
        split_iolist(Rest, SplitAt - (SplitAt - SplitRem), [Sublist | BeginAcc])
    end;
split_iolist([Byte | Rest], SplitAt, BeginAcc) when is_integer(Byte) ->
    split_iolist(Rest, SplitAt - 1, [Byte | BeginAcc]).


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
    TotalBytes = total_read_len(5, HeaderLen),
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


reply_all(Clients, Resp) ->
    [gen_server:reply(C, Resp) || C <- Clients].
