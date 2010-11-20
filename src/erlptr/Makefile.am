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

erlptrlibdir = $(localerlanglibdir)/erlptr-$(version)
erlptrebindir = $(erlptrlibdir)/ebin
erlptrprivdir = $(erlptrlibdir)/priv

erlptrebin_DATA = $(beam_files) $(app_files)
CLEANFILES = $(beam_files)
EXTRA_DIST = $(erl_files) $(app_files)

erlptrpriv_LTLIBRARIES = erlptr.la
erlptr_la_SOURCES = $(c_files)
erlptr_la_LDFLAGS = -module -avoid-version

priv/erlptr.so: erlptr.la
	@mkdir -p priv
	@rm -f $@
	$(LN_S) $(abs_top_builddir)/src/erlptr/.libs/erlptr.so $@

all: priv/erlptr.so

erl_files = \
	src/erlptr.erl

beam_files = \
	ebin/erlptr.beam

app_files = \
	ebin/erlptr.app

c_files = \
	c_src/erlptr.c

ebin/%.beam: src/%.erl
	$(ERLC) $(ERLC_FLAGS) -o `dirname $@` $<


