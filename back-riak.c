#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include "back-riak.h"

static Slapi_PluginDesc pdesc = { "riak-backend",
				  "University of Queensland",
				  DS_PACKAGE_VERSION,
				  "riak backend database plugin" };
json_t *
entry2json(Slapi_Entry *e)
{
	int i = -1;
	Slapi_Attr *attr, *prevattr = NULL;
	Slapi_Value v;

	while (slapi_entry_next_attr(e, prevattr, &attr) != -1) {
		while ((i = slapi_valueset_next_value(attr->a_present_values, i, &v)) != -1)
			slapi_log_error(SLAPI_LOG_PLUGIN, 
					"riak-backend", "%s: %s\n",
					attr->a_type, 
					slapi_value_get_string(&v));
		prevattr = attr;
	}
}

int 
riak_back_init(Slapi_PBlock *pb)
{
	if (slapi_pblock_set(pb, SLAPI_PLUGIN_DB_BIND_FN, (void *)riak_back_bind) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_DB_UNBIND_FN, (void *)riak_back_unbind) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_DB_SEARCH_FN, (void *)riak_back_search) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_DB_COMPARE_FN, (void *)riak_back_compare) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_DB_MODIFY_FN, (void *)riak_back_modify) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_DB_MODRDN_FN, (void *)riak_back_modrdn) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_DB_ADD_FN, (void *)riak_back_add) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_DB_DELETE_FN, (void *)riak_back_delete) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_DB_CONFIG_FN, (void *)riak_back_config) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_DB_CLOSE_FN, (void *)riak_back_close) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_DB_FLUSH_FN, (void *)riak_back_flush) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_START_FN, (void *)riak_back_start)) {
		slapi_log_error(SLAPI_LOG_FATAL, "riak-backend", "Could not setup functions\n");
		return (-1);
	}

	return 0;
}

int
riak_back_add(Slapi_PBlock *pb)
{
	Slapi_Entry *e;
	char *dn = NULL;

	if (slapi_pblock_get(pb, SLAPI_ADD_TARGET, &dn) < 0 ||
	    slapi_pblock_get(pb, SLAPI_ADD_ENTRY, &e) < 0) {
		slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "Couldn't get stuff\n");
		slapi_send_ldap_result(pb, LDAP_OPERATIONS_ERROR, NULL, NULL, 0, NULL);
	}

	entry2json(e);
	return 0;
}

int
riak_back_bind(Slapi_PBlock *pb)
{
	char *dn;
	int method;
	struct berval *cred, **bvals;
	Slapi_Attr *attr;

	if (slapi_pblock_get(pb, SLAPI_BIND_TARGET, &dn) < 0 ||
	    slapi_pblock_get(pb, SLAPI_BIND_METHOD, &method) < 0 ||
	    slapi_pblock_get(pb, SLAPI_BIND_CREDENTIALS, &cred) < 0) {
		slapi_send_ldap_result(pb, LDAP_OPERATIONS_ERROR, NULL, NULL, 0, NULL);
		return (-1);
	}

	switch (method) {
	case LDAP_AUTH_SIMPLE:
		if (cred->bv_len == 0)
			return SLAPI_BIND_ANONYMOUS;
		break;
	default:
		slapi_send_ldap_result(pb, 
				       LDAP_STRONG_AUTH_NOT_SUPPORTED, 
				       NULL,
				       "auth method not supported",
				       0,
				       NULL);
		return SLAPI_BIND_FAIL;
	}

	/* return success for now */
	return SLAPI_BIND_SUCCESS;
}

int
riak_back_unbind(Slapi_PBlock *pb) 
{
	return 0;
}

int
riak_back_search(Slapi_PBlock *pb)
{
	return 0;
}

int
riak_back_compare(Slapi_PBlock *pb)
{
	return 0;
}

int
riak_back_modify(Slapi_PBlock *pb)
{
	return 0;
}

int
riak_back_modrdn(Slapi_PBlock *pb)
{
	return 0;
}

int
riak_back_delete(Slapi_PBlock *pb)
{
	return 0;
}

int
riak_back_config(Slapi_PBlock *pb)
{
	return 0;
}

int
riak_back_close(Slapi_PBlock *pb)
{
	return 0;
}

int
riak_back_flush(Slapi_PBlock *pb)
{
	return 0;
}

int
riak_black_start(Slapi_PBlock *pb)
{
	return 0;
}
