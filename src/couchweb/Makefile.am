## Licensed under the Apache License, Version 2.0 (the "License"); you may not
## use this file except in compliance with the License. You may obtain a copy of
## the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
## License for the specific language governing permissions and limitations under
## the License.

couchweblibdir = $(localerlanglibdir)/couchweb-$(version)
couchwebincludedir = $(couchweblibdir)/include
couchwebebindir = $(couchweblibdir)/ebin

couchwebebin_DATA = $(compiled_files)

CLEANFILES = $(compiled_files)

source_files = \
	couchweb.app \
	couchweb.erl \
	couchweb_app.erl \
	couchweb_res_all_dbs.erl \
	couchweb_res_db.erl \
	couchweb_res_info.erl \
	couchweb_res_utils.erl \
	couchweb_res_uuids.erl \
	couchweb_sup.erl \
	couchweb_utils.erl

EXTRA_DIST = $(source_files)

compiled_files = \
    couchweb.beam \
    couchweb_app.beam \
	couchweb_res_all_dbs.beam \
	couchweb_res_db.beam \
	couchweb_res_info.beam \
	couchweb_res_utils.beam \
	couchweb_res_uuids.beam \
 	couchweb_sup.beam \
 	couchweb_utils.beam

%.beam: %.erl
	$(ERLC) -I$(top_srcdir)/src/ $(ERLC_FLAGS) $<

