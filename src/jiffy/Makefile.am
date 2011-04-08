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

jiffyebindir = $(localerlanglibdir)/jiffy-0.1.0/ebin
jiffyprivdir = $(localerlanglibdir)/jiffy-0.1.0/priv

EXTRA_DIST =  \
	jiffy.h

CLEANFILES = \
    $(jiffyebin_make_generated_file_list) \
    $(jiffypriv_make_generated_file_list) \
    priv/jiffy.so

JIFFY_C_SRCS = \
	jiffy.c \
	decoder.c \
	encoder.c \
	utf8.c \
	util.c

jiffy_file_collection = \
    jiffy.app.in \
    jiffy.erl \
    $(JSON_C_SRCS)

jiffyebin_make_generated_file_list = \
    jiffy.app \
    jiffy.beam

jiffyebin_DATA = \
    $(jiffyebin_make_generated_file_list)

if USE_OTP_NIFS
jiffypriv_LTLIBRARIES = jiffy.la

jiffy_la_SOURCES = $(JIFFY_C_SRCS)
jiffy_la_LDFLAGS = -module -avoid-version

if WINDOWS
jiffy_la_LDFLAGS += -no-undefined
endif

priv/jiffy.so: jiffy.la
	@mkdir -p ./priv
	cp .libs/jiffy.so $@

all: priv/jiffy.so
endif

%.app: %.app.in
	cp $< $@

%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) $<
