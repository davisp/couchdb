#!/usr/bin/env escript
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

-record(btree, {fd, root, extract_kv, assemble_kv, less, reduce, chunk_size}).

filename() -> test_util:build_file("test/etap/temp.022").
rows() -> 4096.

main(_) ->
    test_util:init_code_path(),
    etap:plan(48),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag("Test died abnormally:~n~p", [Other]),
            timer:sleep(500),
            etap:bail()
    end,
    ok.

test()->
    {ok, Fd} = couch_file:open(filename(), [create, overwrite]),
    {ok, Bt} = couch_btree:open(Fd),
    etap:diag("Bt: ~p", [Bt]),
    {ok, Bt2} = test_add_rows(Bt, rows(), 0),
    etap:diag("Bt2: ~p", [Bt2]),
    ok.


test_add_rows(Bt, 0, _InBtree) ->
    {ok, Bt};
test_add_rows(Bt, NumRows, InBtree) ->
    Count = random:uniform(NumRows),
    Rows = [{I+InBtree, random:uniform()} || I <- lists:seq(1, Count)],
    Rows2 = lists:sort(fun({_, V1}, {_, V2}) -> V1 =< V2 end, Rows),
    {ok, Bt2} = couch_btree2:add(Bt, Rows),
    test_add_rows(Bt2, NumRows-Count, InBtree+Count).
    
    
    