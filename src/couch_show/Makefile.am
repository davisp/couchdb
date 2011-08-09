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

couch_showlibdir = $(localerlanglibdir)/couch_show-0.1
couch_showebindir = $(couch_showlibdir)/ebin

couch_showebin_DATA = $(compiled_files)

EXTRA_DIST = $(source_files)
CLEANFILES = $(compiled_files)

source_files = \
	src/couch_show.app.src \
	src/couch_show.erl

compiled_files = \
	ebin/couch_show.app \
	ebin/couch_show.beam

ebin/%.app: src/%.app.src
	@mkdir -p ebin/
	sed -e "s|%version%|@version@|g" \
	< $< > $@

ebin/%.beam: src/%.erl $(include_files)
	@mkdir -p ebin/
	$(ERLC) -Wall -I../ -I../couchdb -o ebin/ $(ERLC_FLAGS) ${TEST} $<;

