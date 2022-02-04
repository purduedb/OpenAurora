# src/bin/initdb/nls.mk
CATALOG_NAME     = initcomp
AVAIL_LANGUAGES  = cs de es fr ja ko ru sv tr uk zh_CN
GETTEXT_FILES    = $(FRONTEND_COMMON_GETTEXT_FILES) initcomp.c ../../common/exec.c ../../common/fe_memutils.c ../../common/file_utils.c ../../common/pgfnames.c ../../common/restricted_token.c ../../common/rmtree.c ../../common/username.c ../../common/wait_error.c ../../port/dirmod.c
GETTEXT_TRIGGERS = $(FRONTEND_COMMON_GETTEXT_TRIGGERS) simple_prompt
GETTEXT_FLAGS    = $(FRONTEND_COMMON_GETTEXT_FLAGS)