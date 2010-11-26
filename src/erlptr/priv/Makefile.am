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

erlptrlibdir = $(localerlanglibdir)/couch-$(version)
erlptrprivdir = $(couchlibdir)/priv

erlptrpriv_LTLIBRARIES = erlptr.la
erlptr_la_SOURCES = $(c_files)
erlptr_la_LDFLAGS = -module -avoid-version

erlptr.so: erlptr.la
	@rm -f $@
	$(LN_S) $(abs_top_builddir)/src/erlptr/priv/.libs/erlptr.so $@

all: erlptr.so

c_files = \
	erlptr.c

