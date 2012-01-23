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

couch_mrviewlibdir = $(localerlanglibdir)/couch_mrview-0.1
couch_mrviewincludedir = $(couch_mrviewlibdir)/include
couch_mrviewebindir = $(couch_mrviewlibdir)/ebin

couch_mrviewinclude_DATA = $(include_files)
couch_mrviewebin_DATA = $(compiled_files)

app_file = \
	src/couch_mrview.app.src

include_files = \
    include/couch_mrview.hrl

source_files = \
    src/couch_mrview.erl \
    src/couch_mrview_cleanup.erl \
    src/couch_mrview_compactor.erl \
    src/couch_mrview_http.erl \
    src/couch_mrview_index.erl \
    src/couch_mrview_show.erl \
    src/couch_mrview_test_util.erl \
    src/couch_mrview_updater.erl \
    src/couch_mrview_util.erl

test_files = \
    test/01-load.t \
    test/02-map-views.t \
    test/03-red-views.t \
    test/04-index-info.t \
    test/05-collation.t \
    test/06-all-docs.t \
	test/07-compact-swap.t

compiled_files = \
    ebin/couch_mrview.app \
    ebin/couch_mrview.beam \
    ebin/couch_mrview_cleanup.beam \
    ebin/couch_mrview_compactor.beam \
    ebin/couch_mrview_http.beam \
    ebin/couch_mrview_index.beam \
    ebin/couch_mrview_show.beam \
    ebin/couch_mrview_test_util.beam \
    ebin/couch_mrview_updater.beam \
    ebin/couch_mrview_util.beam

EXTRA_DIST = $(app_file) $(include_files) $(source_files) $(test_files)
CLEANFILES = $(compiled_files)
ERLC_OPTS=$(ERLC_FLAGS) -I$(top_srcdir)/src -I$(top_srcdir)/src/couchdb -o ebin

check:
	$(abs_top_builddir)/test/etap/run $(abs_top_srcdir)/src/couch_mrview/test

ebin/%.app: src/%.app.src
	@mkdir -p ebin/
	sed -e "s|%version%|@version@|g" \
	< $< > $@

ebin/%.beam: src/%.erl $(include_files) build_erl
	@mkdir -p ebin/

build_erl:
	$(ESCRIPT) $(abs_top_srcdir)/bin/couch_erlc $(ERLC_OPTS) $(source_files)

