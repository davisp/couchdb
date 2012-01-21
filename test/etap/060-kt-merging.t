#!/usr/bin/env escript
%% -*- erlang -*-

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

main(_) ->
    test_util:init_code_path(),
    etap:plan(16),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.

test() ->

    Simple = {1, {"1","foo",[]}},
    TwoDeep0 = {1, {"1", "foo", [{"2_0", "bar", []}]}},
    TwoDeep1 = {1, {"1", "foo", [{"2_1", "baz", []}]}},
    NewLeaf = {2, {"2_0", "bar", [{"3", "bing", []}]}},
    WithNewLeaf = {1, {"1", "foo", [{"2_0", "bar", [{"3", "bing", []}]}]}},
    NewBranch = {1, {"1", "foo", [{"2_0", "bar", []}, {"2_1", "baz", []}]}},
    NewDeepBranch = {2, {"2_0", "bar", [{"3_1", "bang", []}]}},

    StemmedEdit = {3, {"3", "bing", []}},
    StemmedConflicts = [Simple, StemmedEdit],

    NewBranchLeaf = {1,
        {"1", "foo", [
            {"2_0", "bar", [
                {"3", "bing", []}
            ]},
            {"2_1", "baz", []}
        ]}
    },

    NewBranchLeafBranch = {1,
        {"1", "foo", [
            {"2_0", "bar", [
                {"3", "bing", []},
                {"3_1", "bang", []}
            ]},
            {"2_1", "baz", []}
        ]}
    },

    Stemmed2 = [
        {1, {"1", "foo", [
            {"2_1", "baz", []}
        ]}},
        {2, {"2_0", "bar", [
            {"3", "bing", []},
            {"3_1", "bang", []}
        ]}}
    ],

    Stemmed3 = [
        {2, {"2_1", "baz", []}},
        {3, {"3", "bing", []}},
        {3, {"3_1", "bang", []}}
    ],

    PartialRecover = [
        {1, {"1", "foo", [
            {"2_0", "bar", [
                {"3", "bing", []}
            ]}
        ]}},
        {2, {"2_1", "baz", []}},
        {3, {"3_1", "bang", []}}
    ],

    etap:is(
        couch_key_tree:merge([], Simple, 10),
        {[Simple], new_leaf},
        "Merging a path into an empty tree is the path"
    ),

    etap:is(
        couch_key_tree:merge([Simple], Simple, 10),
        {[Simple], internal_node},
        "Remerge path into path is reflexive"
    ),

    etap:is(
        couch_key_tree:merge([], TwoDeep0, 10),
        {[TwoDeep0], new_leaf},
        "Merging a path with multiple entries is the path"
    ),

    etap:is(
        couch_key_tree:merge([TwoDeep0], TwoDeep0, 10),
        {[TwoDeep0], internal_node},
        "Merging a path with multiple entries is reflexive"
    ),

    etap:is(
        couch_key_tree:merge([TwoDeep0], Simple, 10),
        {[TwoDeep0], internal_node},
        "Merging a subpath into a path results in the path"
    ),

    etap:is(
        couch_key_tree:merge([TwoDeep0], NewLeaf, 10),
        {[WithNewLeaf], new_leaf},
        "Merging a new leaf gives us a new leaf"
    ),

    etap:is(
        couch_key_tree:merge([TwoDeep0], TwoDeep1, 10),
        {[NewBranch], new_branch},
        "Merging a new branch returns a proper tree"
    ),

    etap:is(
        couch_key_tree:merge([TwoDeep1], TwoDeep0, 10),
        {[NewBranch], new_branch},
        "Order of merging does not affect the resulting tree"
    ),

    etap:is(
        couch_key_tree:merge([NewBranch], NewLeaf, 10),
        {[NewBranchLeaf], new_leaf},
        "Merging a new_leaf doesn't return new_branch when branches exist"
    ),

    etap:is(
        couch_key_tree:merge([NewBranchLeaf], NewDeepBranch, 10),
        {[NewBranchLeafBranch], new_branch},
        "Merging a deep branch with branches works"
    ),

    etap:is(
        couch_key_tree:merge(StemmedConflicts, WithNewLeaf, 10),
        {[WithNewLeaf], new_leaf},
        "New information reconnects steming induced conflicts"
    ),

    etap:is(
        couch_key_tree:merge([TwoDeep0], NewLeaf, 2),
        {[NewLeaf], new_leaf},
        "Simple stemming works"
    ),

    etap:is(
        couch_key_tree:merge([NewBranchLeafBranch], Simple, 2),
        {Stemmed2, internal_node},
        "Merge with stemming works correctly for branches"
    ),

    etap:is(
        couch_key_tree:merge([NewBranchLeafBranch], Simple, 1),
        {Stemmed3, internal_node},
        "Merge with stemming to leaves works fine"
    ),

    etap:is(
        couch_key_tree:merge(Stemmed3, WithNewLeaf, 10),
        {PartialRecover, internal_node},
        "Merging unstemmed recovers as much as possible without losing info"
    ),


    %% this test is based on couch-902-test-case2.py
    %% foo has conflicts from replication at depth two
    %% foo3 is the current value
    Foo = {1, {"foo",
               "val1",
               [{"foo2","val2",[]},
                {"foo3", "val3", []}
               ]}},
    %% foo now has an attachment added, which leads to foo4 and val4
    %% off foo3
    Bar = {1, {"foo",
               [],
               [{"foo3",
                 [],
                 [{"foo4","val4",[]}
                  ]}]}},
    %% this is what the merge returns
    %% note that it ignore the conflicting branch as there's no match
    FooBar = {1, {"foo",
               "val1",
               [{"foo2","val2",[]},
                {"foo3", "val3", [{"foo4","val4",[]}]}
               ]}},

    etap:is(
      {[FooBar], new_leaf},
      couch_key_tree:merge([Foo],Bar,10),
      "Merging trees with conflicts ought to behave."
    ),

    ok.
