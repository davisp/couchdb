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

-module(couch_httpd_ape).

-include("couch_db.hrl").

-export([handle_ape_req/3]).

% /db_name/_design/ddocid/_app/ ++ REST
handle_ape_req(#httpd{
        path_parts=[DbName, _, _, _ | _Rest]
    }=Req, Db, #doc{id=DDocId, revs={Start, [DiskRev | _]}}=DDoc) ->

    DDocRev = couch_doc:rev_to_str({Start, DiskRev}),
    JsonDDoc = couch_util:json_doc(DDoc),
    {ok, Pid} = couch_ape_server:get_context(DbName, DDocId, DDocRev, JsonDDoc),

    {ok, ExtResp} = couch_ape_context:handle_request(Pid, Req, Db),
    couch_httpd_external:send_external_response(Req, ExtResp).
