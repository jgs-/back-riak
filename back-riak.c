#define _GNU_SOURCE

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <curl/curl.h>

#include "slapi-plugin.h" 
#include "slapi-private.h"
#include "jansson.h"
#include "back-riak.h"

#define DS_PACKAGE_VERSION "1.2.10.14"
#define RIAK_URL "http://localhost:8098/riak/ldap/"
#define MAX_URL_SIZE 1024

static Slapi_PluginDesc pdesc = { "riak-backend",
				  "University of Queensland",
				  DS_PACKAGE_VERSION,
				  "riak backend database plugin" };

int 
riak_put(const char *key, const char *data) {
	CURL *h;
	CURLcode res;
	struct curl_slist *headers;
	char *url;
	FILE *m;
	unsigned int l;

	curl_global_init(CURL_GLOBAL_ALL);
	if ((h = curl_easy_init())) {
		curl_easy_setopt(h, CURLOPT_UPLOAD, 1L);
		curl_easy_setopt(h, CURLOPT_PUT, 1L);

		l = strlen(key) + strlen(RIAK_URL) + 1;
		if (!(url = malloc(l))) {
			curl_easy_cleanup(h);
			return (-1);
		}
		snprintf(url, l, "%s%s", RIAK_URL, key);
		slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "%s\n", url);
		m = fmemopen((void *)data, strlen(data), "r");

		curl_easy_setopt(h, CURLOPT_READDATA, m);
		curl_easy_setopt(h, CURLOPT_URL, url);
		curl_easy_setopt(h, CURLOPT_INFILESIZE_LARGE, (curl_off_t)l);

		headers = curl_slist_append(NULL, "Content-Type: application/json");
		curl_easy_setopt(h, CURLOPT_HTTPHEADER, headers);

		res = curl_easy_perform(h);
		if (res != CURLE_OK) {
			slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "%s\n", curl_easy_strerror(res));
			return -1;
		}
		curl_easy_cleanup(h);
		fclose(m);
		return 0;
	}

	slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "end of riak_put()\n");
	return -1;
}

char *
entry2json(char *dn, Slapi_Entry *e)
{
	int i = -1;
	char *json;
	Slapi_Attr *attr, *prevattr = NULL;
	Slapi_Value *v;
	json_t *ent, *arr;

	ent = json_object();
	json_object_set(ent, "dn", json_string(dn));

	while (slapi_entry_next_attr(e, prevattr, &attr) != -1) {
		arr = json_array();

		while ((i = slapi_valueset_next_value(&attr->a_present_values, i, &v)) != -1)
			json_array_append_new(arr, json_string(slapi_value_get_string(v)));

		json_object_set(ent, attr->a_type, arr);
		prevattr = attr;
	}

	json = json_dumps(ent, 0);
	return json;
}

int
riak_back_add(Slapi_PBlock *pb)
{
	Slapi_Entry *e;
	char *dn, *data;

	if (slapi_pblock_get(pb, SLAPI_ADD_TARGET, &dn) < 0 ||
	    slapi_pblock_get(pb, SLAPI_ADD_ENTRY, &e) < 0) {
		slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "Couldn't get stuff\n");
		slapi_send_ldap_result(pb, LDAP_OPERATIONS_ERROR, NULL, NULL, 0, NULL);
	}

	data = entry2json(dn, e);
	if (riak_put(dn, data)) {
		slapi_log_error(SLAPI_LOG_FATAL, "riak-backend", "riak put failed\n");
		slapi_send_ldap_result(pb, LDAP_OPERATIONS_ERROR, NULL, NULL, 0, NULL);
	}

	slapi_send_ldap_result(pb, LDAP_SUCCESS, NULL, NULL, 0, NULL);
	return 0;
}


int
riak_back_bind(Slapi_PBlock *pb)
{
	return 0;
}

int 
riak_back_init(Slapi_PBlock *pb)
{
	if (slapi_pblock_set(pb, SLAPI_PLUGIN_DESCRIPTION, (void *)&pdesc) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_VERSION, SLAPI_PLUGIN_VERSION_01) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_PRE_ADD_FN, (void *)&riak_back_add)) {
		slapi_log_error(SLAPI_LOG_FATAL, "riak-backend", "Couldn't setup functions\n");
		return (-1);
	}

	slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "Initialized\n");
	return 0;
}
