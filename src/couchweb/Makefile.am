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
	couchweb_res_info.erl \
	couchweb_sup.erl

EXTRA_DIST = $(source_files)

compiled_files = \
    couchweb.beam \
    couchweb_app.beam \
	couchweb_res_all_dbs.beam \
	couchweb_res_info.beam \
	couchweb_sup.beam

%.beam: %.erl
	$(ERLC) -I$(top_srcdir)/src/ $(ERLC_FLAGS) $<

