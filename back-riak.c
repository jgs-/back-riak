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
