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

erlang_jslibdir = $(localerlanglibdir)/erlang_js-0.1
erlang_jsprivlibdir = $(erlang_jslibdir)/priv
erlang_jsebindir = $(erlang_jslibdir)/ebin

SPIDERMONKEY_DRV_SRCS = \
	priv/driver_comm.c \
	priv/spidermonkey.c \
	priv/spidermonkey_drv.c

erlang_jsprivlib_LTLIBRARIES = spidermonkey_drv.la
spidermonkey_drv_la_SOURCES = $(SPIDERMONKEY_DRV_SRCS)
spidermonkey_drv_la_LDFLAGS = -module -avoid-version
spidermonkey_drv_la_LIBADD = -ljs_static

erlang_js_file_collection = \
	erlang_js.app.in \
    erlang_js.erl \
   	erlang_js_sup.erl \
    js.erl \
    js_benchmark.erl \
    js_cache.erl \
    js_driver.erl \
    js_drv_comm.erl \
	js_memory.erl \
	js_mochijson2.erl \
	js_mochinum.erl

erlang_jsebin_make_generated_file_list = \
	erlang_js.app \
    erlang_js.beam \
    erlang_js_sup.beam \
    js.beam \
    js_benchmark.beam \
    js_cache.beam \
    js_driver.beam \
    js_drv_comm.beam \
	js_memory.beam \
	js_mochijson2.beam \
	js_mochinum.beam

erlang_jsebin_DATA = $(erlang_jsebin_make_generated_file_list)

EXTRA_DIST = $(erlang_js_file_collection)

CLEANFILES = $(erlang_jsebin_make_generated_file_list)

%.app: %.app.in
	cp $< $@

%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) $<

dev:
	cp .libs/spidermonkey_drv.so priv/
