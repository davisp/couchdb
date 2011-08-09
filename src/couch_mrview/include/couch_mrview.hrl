

-record(mrst, {
    sig=nil,
    fd=nil,
    db_name,
    idx_name,
    language,
    design_opts=[],
    lib,
    views,
    id_btree=nil,
    update_seq=0,
    purge_seq=0,
    root_dir=[],

    first_build,
    updater_pid,
    partial_resp_pid,
    write_queue,
    query_server=nil
}).


-record(mrview, {
    id_num,
    update_seq=0,
    purge_seq=0,
    map_names=[],
    reduce_funs=[],
    def,
    btree=nil,
    options=[]
}).


-record(mrheader, {
    seq=0,
    purge_seq=0,
    id_btree_state=nil,
    view_states=nil
}).


-record(mrargs, {
    view_type,
    reduce,

    preflight_fun,

    start_key,
    start_key_docid,
    end_key,
    end_key_docid,
    keys,

    direction = fwd,
    limit = 16#10000000,
    skip = 0,
    group_level = 0,
    stale = false,
    inclusive_end = true,
    include_docs = false,
    update_seq=false,
    conflicts,
    callback,
    list,
    extra = []
}).
