-module(couch_emsort).


-export([open/1, open/2, get_fd/1, get_state/1, add/2, sort/3]).


-record(ems, {
    fd,
    root = nil,
    count = 256
}).


open(Fd) ->
    {ok, #ems{fd=Fd}}.


open(Fd, Options) ->
    {ok, set_options(#ems{fd=Fd}, Options)}.


set_options(Ems, []) ->
    Ems;
set_options(Ems, [{root, Root} | Rest]) ->
    set_options(Ems#ems{root=Root}, Rest);
set_options(Ems, [{count, Count} | Rest]) when is_integer(Count) ->
    set_options(Ems#ems{count=Count}, Rest).


get_fd(#ems{fd=Fd}) ->
    Fd.


get_state(#ems{root=Root}) ->
    Root.


add(#ems{root=nil}=Ems, KVs) ->
    Ptr = write_kvs(Ems, KVs),
    {ok, Ems#ems{root={Ptr, nil}}};
add(#ems{root=Prev}=Ems, KVs) ->
    Ptr = write_kvs(Ems, KVs),
    {ok, PPos} = couch_file:append_term(Ems#ems.fd, Prev),
    {ok, Ems#ems{root={Ptr, PPos}}}.


sort(#ems{root=nil}, _Fun, Acc) ->
    Acc;
sort(#ems{}=Ems0, Fun, Acc) ->
    #ems{root={Ptr, Pos}} = Ems1 = decimate(Ems0),
    {Ptrs0, nil} = read_ptrs(Ems1, Pos, [Ptr]),
    Ptrs1 = init_ptrs(small, Ptrs0),
    fold(Ems1, Ptrs1, Fun, Acc).


write_kvs(Ems, KVs) ->
    % Write a list of KV's to disk in sorted order returning
    % a {Key, VPos, NextPos} pointer.
    lists:foldr(fun({K, V}, Acc) ->
        {ok, VPos} = couch_file:append_term(Ems#ems.fd, V),
        case Acc of
            nil ->
                {K, VPos, nil};
            Prev ->
                {ok, PPos} = couch_file:append_term(Ems#ems.fd, Prev),
                {K, VPos, PPos}
        end
    end, nil, lists:sort(KVs)).


decimate(#ems{root={Ptr0, Pos0}}=Ems) ->
    % To make sure we have a bounded amount of data in RAM
    % at any given point we first need to decimate the data
    % by performing the first couple iterations of a merge
    % sort writing the intermediate results back to disk.
    {Ptrs1, LPos1} = read_ptrs(Ems, Pos0, [Ptr0]),
    case LPos1 of
        nil ->
            % We have fewer than count pointers which means we're
            % finished decimating.
            Ems;
        _ ->
            % The first pass gives us a sort with pointers linked from
            % largest to smallest.
            {Ptr2, Pos2} = merge_ptrs(Ems, small, LPos1, Ptrs1),

            % We have to run a second pass so that links are pointed
            % back from smallest to largest.
            {Ptrs2, LPos2} = read_ptrs(Ems, Pos2, [Ptr2]),
            {Ptr3, Pos3} = merge_ptrs(Ems, big, LPos2, Ptrs2),

            % Continue deicmating until we have an acceptable bound
            % on the number of keys to use.
            decimate(Ems#ems{root={Ptr3, Pos3}})
    end.


fold(_Ems, [], _Fun, Acc) ->
    Acc;
fold(Ems, Ptrs0, Fun, Acc0) ->
    % The last step stage of the merge sort. No need
    % to write keys to disk one last time here.
    {Key, VPos, Ptrs1} = choose_ptr(small, Ems, Ptrs0),
    {ok, Val} = couch_file:pread_term(Ems#ems.fd, VPos),
    Acc1 = Fun(Key, Val, Acc0),
    fold(Ems, Ptrs1, Fun, Acc1).


merge_ptrs(Ems, Choose, Pos, Ptrs) ->
    Ptr = merge_lists(Ems, Choose, Ptrs),
    merge_rest_ptrs(Ems, Choose, Pos, {Ptr, nil}).


merge_rest_ptrs(_Ems, _Choose, nil, Acc) ->
    Acc;
merge_rest_ptrs(Ems, Choose, Pos, Prev) ->
    {MPtrs, NPos} = read_ptrs(Ems, Pos, []),
    PtrInfo = merge_lists(Ems, Choose, MPtrs),
    {ok, PPos} = couch_file:append_term(Ems#ems.fd, Prev),
    merge_rest_ptrs(Ems, Choose, NPos, {PtrInfo, PPos}).


read_ptrs(_Ems, nil, Ptrs) ->
    {Ptrs, nil};
read_ptrs(#ems{count=Count}, Pos, Ptrs) when length(Ptrs) >= Count ->
    {Ptrs, Pos};
read_ptrs(Ems, Pos, Ptrs) ->
    {ok, {Ptr, PPos}} = couch_file:pread_term(Ems#ems.fd, Pos),
    read_ptrs(Ems, PPos, [Ptr | Ptrs]).


merge_lists(Ems, Choose, Ptrs0) ->
    Ptrs1 = init_ptrs(Choose, Ptrs0),
    {FKey, FVPos, Ptrs2} = choose_ptr(Choose, Ems, Ptrs1),
    merge_lists(Ems, Choose, Ptrs2, {FKey, FVPos, nil}).


merge_lists(_Ems, _Choose, [], Ptr) ->
    Ptr;
merge_lists(Ems, Choose, Ptrs0, Prev) ->
    {NKey, NVPos, Ptrs1} = choose_ptr(Choose, Ems, Ptrs0),
    {ok, PPos} = couch_file:append_term(Ems#ems.fd, Prev),
    merge_lists(Ems, Choose, Ptrs1, {NKey, NVPos, PPos}).


init_ptrs(small, Ptrs) -> lists:sort(Ptrs);
init_ptrs(big, Ptrs) -> lists:reverse(lists:sort(Ptrs)).


choose_ptr(_Choose, _Ems, [{K, V, nil} | Rest]) ->
    {K, V, Rest};
choose_ptr(Choose, Ems, [{K, V, P} | Rest]) ->
    {ok, Prev} = couch_file:pread_term(Ems#ems.fd, P),
    case Choose of
        small -> {K, V, add_small_ptr(Rest, Prev, [])};
        big -> {K, V, add_big_ptr(Rest, Prev, [])}
    end.


add_small_ptr([P | Rest], Prev, Acc) when P < Prev ->
    add_small_ptr(Rest, Prev, [P | Acc]);
add_small_ptr(Rest, Prev, Acc) ->
    lists:reverse(Acc, [Prev | Rest]).


add_big_ptr([P | Rest], Prev, Acc) when P > Prev ->
    add_big_ptr(Rest, Prev, [P | Acc]);
add_big_ptr(Rest, Prev, Acc) ->
    lists:reverse(Acc, [Prev | Rest]).

