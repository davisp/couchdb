-record(mrst, {
    sig=nil,
    fd=nil,
    name,
    language,
    design_opts=[],
    views,
    lib,
    id_btree=nil,
    update_seq=0,
    purge_seq=0,
    query_server=nil
}).

-record(view, {
    id_num,
    update_seq=0,
    purge_seq=0,
    map_names=[],
    def,
    btree=nil,
    reduce_funs=[],
    options=[]
}).

-record(index_header, {
    seq=0,
    purge_seq=0,
    id_btree_state=nil,
    view_states=nil
}).