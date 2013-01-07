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

#define DS_PACKAGE_VERSION 	"1.2.10.14"
#define RIAK_URL 		"http://localhost:8098/riak/ldap/"
#define MAPRED_URL		"http://localhost:8098/mapred"
#define MAX_URL_SIZE 		1024
#define INDEX_HEADER		"x-riak-index-%s_bin: %s"
#define VCLOCK_HEADER 		"X-Riak-Vclock"

struct response *riak_get(const char *);
static Slapi_PluginDesc pdesc = { "riak-backend",
				  "University of Queensland",
				  DS_PACKAGE_VERSION,
				  "riak backend database plugin" };

struct chunk {
	char *mem;
	size_t size;
};

struct response {
	char *headers;
	char *content;
};

struct entry {
	char *key;
	char **attrs;
	Slapi_Entry *ent;
};

void
convert_space(char *s)
{
	int i;

	for (i = 0; s[i] != '\0'; i++) {
		if (s[i] == ' ')
			s[i] = '+';
	}
}

void
tok(char *in, char **out)
{
	char *p = in;
	char *n;

	n = p;
	while (*n != ',' && *n != '\0')
		n++;

	if (*n == ',') {
		tok(n+1, out);
		*(*out)++ = ',';
	}

	while (p != n)
		*(*out)++ = *p++;
}

char *
reverse_dn(char *i)
{
	char *o = calloc(1, strlen(i) + 1);
	char *p = o;
	tok(i, &o);

	return p;
}

void
normalize_dn(char *s)
{
	int i;
	
	for (i = 0; s[i] != '\0'; i++)
		s[i] = tolower(s[i]);
}

static size_t
write_callback(void *contents, size_t size, size_t nmemb, void *p)
{
	size_t s = size * nmemb;
	struct chunk *c = (struct chunk *)p;

	c->mem = realloc(c->mem, c->size + s + 1);
	if (c->mem == NULL)
		return -1;

	memcpy(&(c->mem[c->size]), contents, s);
	c->size += s;
	c->mem[c->size] = 0;

	return s;
}

struct curl_slist *
add_index(const char *data, struct curl_slist *headers)
{
	int i, j;
	char *h;
	size_t sz, n;
	const char *attr, *v;
	struct response *r = NULL;
	json_t *entry, *indexes, *idx, *values;

	slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "add_index\n");
	r = riak_get("indexes");
	if (!r)
		return headers;

	indexes = json_loads(r->content, 0, NULL);
	entry = json_loads(data, 0, NULL);
	slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "indexes: %s\n", r->content);

	n = json_array_size(indexes);
	if (!n) {
		slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "!n\n");
		goto fin;
	}

	for (i = 0; i < n; i++) {
		idx = json_array_get(indexes, i);
		attr = json_string_value(idx);	
		if (attr == NULL) {
			slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "!attr\n");
			goto fin;
		}
		slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "attr: %s\n", attr);

		if (!(values = json_object_get(entry, attr)))
			continue;

		for (j = 0; j < json_array_size(values); j++) {
			v = json_string_value(json_array_get(values, j));
			sz = strlen(INDEX_HEADER) + strlen(attr) + strlen(v);

			if (!(h = malloc(sz)))
				goto fin;
	
			snprintf(h, sz, INDEX_HEADER, attr, v); 
			slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "adding index: %s\n", h);
			headers = curl_slist_append(headers, h);
			free(h);
		}

	}

fin:
	if (r) {
		free(r->headers);
		free(r->content);
		free(r);
	}
	return headers;
}

int
is_index(const char *index)
{
	int i, n, rc = 0;
	const char *v;
	struct response *r = NULL;
	json_t *indexes, *idx;

	if (!index || !(r = riak_get("indexes")))
		return rc;

	indexes = json_loads(r->content, 0, NULL);
	if (!(n = json_array_size(indexes)))
		goto fin;

	for (i = 0; i < n; i++) {
		idx = json_array_get(indexes, i);
		v = json_string_value(idx);
		if (!strcmp(v, index)) {
			rc = 1;
			break;
		}
	}

fin:
	if (r) {
		free(r->headers);
		free(r->content);
		free(r);
	}

	return rc;
}

struct response *
riak_get(const char *key)
{
	size_t len;
	char *url, *dn;
	struct chunk retr, headers;
	struct response *resp;
	CURL *h;
	CURLcode res;

	if ((dn = strdup(key)) == NULL)
		return NULL;
	convert_space(dn);

	headers.mem = malloc(1);
	retr.size = headers.size = 0;
	retr.mem = malloc(1);
	
	if ((resp = malloc(sizeof(struct response))) == NULL)
		return NULL;

	len = strlen(RIAK_URL) + strlen(dn) + 1;
	if ((url = malloc(len)) == NULL || !(h = curl_easy_init()))
		return NULL;
	snprintf(url, len, "%s%s", RIAK_URL, dn);

	curl_easy_setopt(h, CURLOPT_URL, url);
	curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(h, CURLOPT_WRITEDATA, (void *)&retr);
	curl_easy_setopt(h, CURLOPT_WRITEHEADER, (void *)&headers);
	curl_easy_setopt(h, CURLOPT_USERAGENT, "389ds-riak/0.1");
	curl_easy_setopt(h, CURLOPT_FAILONERROR, 1L);

	res = curl_easy_perform(h);
	curl_easy_cleanup(h);

	if (res != CURLE_OK)
		return NULL;

	resp->headers = headers.mem;
	resp->content = retr.mem;
	return resp;
}

int 
riak_put(const char *key, const char *data, const char *vclock) {
	CURL *h;
	CURLcode res;
	struct curl_slist *headers = NULL;
	unsigned int l;
	char *url, *dn;
	FILE *m;

	curl_global_init(CURL_GLOBAL_ALL);
	if ((h = curl_easy_init())) {
		dn = strdup(key);
		convert_space(dn);

		l = strlen(dn) + strlen(RIAK_URL) + 1;
		if (!(url = malloc(l))) {
			curl_easy_cleanup(h);
			return (-1);
		}
		snprintf(url, l, "%s%s", RIAK_URL, dn);

		m = fmemopen((void *)data, strlen(data), "r");

		curl_easy_setopt(h, CURLOPT_UPLOAD, 1L);
		curl_easy_setopt(h, CURLOPT_PUT, 1L);
		curl_easy_setopt(h, CURLOPT_READDATA, m);
		curl_easy_setopt(h, CURLOPT_URL, url);
		curl_easy_setopt(h, CURLOPT_INFILESIZE_LARGE, (curl_off_t)strlen(data));
		
		if (vclock != NULL)
			headers = curl_slist_append(headers, vclock);
		headers = curl_slist_append(headers, "Content-Type: application/json");
		headers = add_index(data, headers);
		curl_easy_setopt(h, CURLOPT_HTTPHEADER, headers);
	
		res = curl_easy_perform(h);
		curl_slist_free_all(headers);
		curl_easy_cleanup(h);
		fclose(m);
		free(dn);

		if (res == CURLE_OK)
			return 0;
	}

	slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "riak_put() error\n");
	return -1;
}

int
riak_del(const char *key)
{
	char *dn, *url;
	size_t l;

	CURL *h;
	CURLcode res;

	curl_global_init(CURL_GLOBAL_ALL);
	if (!(h = curl_easy_init()))
		return -1;

	dn = strdup(key);
	convert_space(dn);

	l = strlen(dn) + strlen(RIAK_URL) + 1;
	if (!(url = malloc(l))) {
		curl_easy_cleanup(h);
		return -1;
	}
	snprintf(url, l, "%s%s", RIAK_URL, dn);

	curl_easy_setopt(h, CURLOPT_CUSTOMREQUEST, "DELETE");
	curl_easy_setopt(h, CURLOPT_URL, url);

	res = curl_easy_perform(h);
	curl_easy_cleanup(h);
	free(dn);
	free(url);

	if (res == CURLE_OK)
		return 0;

	return -1;
}

char *
mapreduce(char *job) {
	struct chunk retr;
	struct curl_slist *headers = NULL;
	CURL *h;
	CURLcode res;	

	if (!(h = curl_easy_init()))
		return NULL;
	
	retr.size = 0;
	retr.mem = malloc(1);	

	curl_easy_setopt(h, CURLOPT_POST, 1L);
	curl_easy_setopt(h, CURLOPT_URL, MAPRED_URL);
	curl_easy_setopt(h, CURLOPT_POSTFIELDS, job); 
	curl_easy_setopt(h, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(h, CURLOPT_WRITEDATA, (void *)&retr);
	curl_easy_setopt(h, CURLOPT_USERAGENT, "389ds-riak/0.1");
	curl_easy_setopt(h, CURLOPT_FAILONERROR, 1L);

	headers = curl_slist_append(headers, "Content-Type: application/json");
	curl_easy_setopt(h, CURLOPT_HTTPHEADER, headers);
	
	res = curl_easy_perform(h);
	curl_easy_cleanup(h);

	if (res != CURLE_OK)
		return NULL;

	return retr.mem;
}

int
is_substring(char *s)
{
	int i;

	for (i = 0; s[i] != '\0'; i++) {
		if (s[i] == '*')
			return i;
	}
	
	return -1;
}

char *
make_map(const char *filter, char *base, char *index, char *index_value)
{
	int i;
	char *json, *index_bin;
	json_t *job, *inputs, *query, *map, *key_filters, *key, *o, *t;
	
	inputs = json_object();
	job = json_object();
	map = json_object();
	o = json_object();
	key_filters = json_array();
	query = json_array();
	key = json_array();
	t = json_array();

	if (!index) {
		slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "using base\n");
		json_array_append_new(t, json_string("to_lower"));
		json_array_append(key_filters, t);
		json_array_append_new(key, json_string("starts_with"));
		json_array_append_new(key, json_string(reverse_dn(base)));
		json_array_append(key_filters, key);
	}

	json_object_set_new(map, "language", json_string("javascript"));
	json_object_set_new(map, "bucket", json_string("code"));
	json_object_set_new(map, "key", json_string("map_ldap_search"));
	json_object_set_new(map, "keep", json_true());
	json_object_set_new(map, "arg", json_string(filter));
	
	json_object_set(o, "map", map);
	json_array_append(query, o);

	json_object_set_new(inputs, "bucket", json_string("ldap"));

	if (!index)
		json_object_set(inputs, "key_filters", key_filters);
	else {
		index_bin = malloc(strlen(index) + 5);
		snprintf(index_bin, strlen(index) + 5, "%s_bin", index);
		json_object_set(inputs, "index", json_string(index_bin));

		if (index_value[0] == '*') {
			json_object_set(inputs, "start", json_string("A"));
			json_object_set(inputs, "end", json_string("z"));
		} else if ((i = is_substring(index_value)) >= 0) {	
			index_value[i] = 'A';
			json_object_set(inputs, "start", json_string(index_value));
			index_value[i] = 'z';
			json_object_set(inputs, "end", json_string(index_value));
			index_value[i] = '*';
		} else
			json_object_set(inputs, "key", json_string(index_value));
	}

	json_object_set(job, "inputs", inputs);
	json_object_set(job, "query", query);

	json = json_dumps(job, JSON_INDENT(4));
	slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "job: %s\n", json);
	return json;
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

Slapi_Entry *
json2entry(const char *key, json_t *j)
{
	size_t i, n;
	const char *s, *attr;
	json_t *a, *val;
	Slapi_Entry *e;
	
	e = slapi_entry_alloc();
	slapi_entry_init(e, reverse_dn(strdup(key)), NULL);

	json_object_foreach(j, attr, a) {
		if (!attr || !(n = json_array_size(a)))
			continue;

		for (i = 0; i < n; i++) {
			val = json_array_get(a, i);
			s = json_string_value(val);
			if (!s)
				continue;

			slapi_entry_add_string(e, attr, s);
		}
	}

	return e;
}

int
parse_search_results(Slapi_PBlock *pb, char *blob, char *base)
{
	char **attrs, *dn, *b, *t;
	int attrsonly;
	size_t i, n;
	const char *key;
	Slapi_Entry *e;
	json_t *results, *r, *data;

	slapi_pblock_get(pb, SLAPI_SEARCH_ATTRS, &attrs);
	slapi_pblock_get(pb, SLAPI_SEARCH_ATTRSONLY, &attrsonly);

	/* reverse the base dn */
	t = strdup(base);
	b = reverse_dn(t);
	normalize_dn(b);
	free(t);
	
	if (!(results = json_loads(blob, 0, NULL)) || !(n = json_array_size(results)))
		return 0;

	for (i = 0; i < n; i++) {
		if (!(r = json_array_get(results, i)) ||
		    !(key = json_string_value(json_array_get(r, 0))) ||
		    !(data = json_array_get(r, 1)))
			continue;

		e = json2entry(json_string_value(json_array_get(r, 0)), data);

		/* reverse the dns */
		t = strdup(slapi_entry_get_dn(e));
		dn = reverse_dn(t);
		free(t);
		normalize_dn(dn);

		if (!strncmp(dn, b, strlen(b)))
			slapi_send_ldap_search_entry(pb, e, NULL, attrs, attrsonly);
	}

	return i;
}

int
riak_back_add(Slapi_PBlock *pb)
{
	Slapi_Entry *e;
	char *dn, *data;
	struct response *r;

	slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "ADD begin\n");

	if (slapi_pblock_get(pb, SLAPI_ADD_TARGET, &dn) < 0 ||
	    slapi_pblock_get(pb, SLAPI_ADD_ENTRY, &e) < 0) {
		slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "Couldn't get stuff\n");
		slapi_send_ldap_result(pb, LDAP_OPERATIONS_ERROR, NULL, NULL, 0, NULL);
	}

	dn = reverse_dn(dn);
	slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "%s\n", dn);

	if ((r = riak_get(dn)) != NULL) {
		free(r->headers);
		free(r->content);
		free(r);
		slapi_send_ldap_result(pb, LDAP_ALREADY_EXISTS, NULL, NULL, 0, NULL);
		return 0;
	}	

	data = entry2json(dn, e);
	if (riak_put(dn, data, NULL)) {
		slapi_log_error(SLAPI_LOG_FATAL, "riak-backend", "riak put failed\n");
		slapi_send_ldap_result(pb, LDAP_OPERATIONS_ERROR, NULL, NULL, 0, NULL);
		free(data);
		return -1;
	}

	free(data);
	slapi_send_ldap_result(pb, LDAP_SUCCESS, NULL, NULL, 0, NULL);
	return 0;
}

int
riak_back_mod(Slapi_PBlock *pb)
{
	int i, j;
	char *dn, *str, *headers, *json, *vclock = NULL;
	struct response *r;
	LDAPMod **mods;
	json_t *entry, *attr;

	slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "MODIFY begin\n");

	if (slapi_pblock_get(pb, SLAPI_MODIFY_TARGET, &dn) ||
	    slapi_pblock_get(pb, SLAPI_MODIFY_MODS, &mods)) {
		slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "Could not get parameters\n");
		return -1;
	}

	dn = reverse_dn(dn);

	if ((r = riak_get(dn)) == NULL) {
		slapi_send_ldap_result(pb, LDAP_OPERATIONS_ERROR, NULL, NULL, 0, NULL);
		return -1;
	}

	headers = r->headers;
	while ((vclock = strtok_r(headers, "\n", &str)) != NULL) {
		headers = NULL;
		if (!strncmp(vclock, VCLOCK_HEADER, strlen(VCLOCK_HEADER)))
			break;
	}

	if (vclock == NULL) {
		slapi_send_ldap_result(pb, LDAP_NO_SUCH_OBJECT, NULL, NULL, 0, NULL);
		return -1;
	}

	entry = json_loads(r->content, 0, NULL);
	for (i = 0; mods[i] != NULL; i++) {
		if (SLAPI_IS_MOD_ADD(mods[i]->mod_op)) {
			attr = json_object_get(entry, mods[i]->mod_type);
			if (attr == NULL)
				attr = json_array();
		} else
			attr = json_array();

		for (j = 0; mods[i]->mod_bvalues[j] != NULL; j++)
			json_array_append_new(attr, json_string((char *)mods[i]->mod_bvalues[j]->bv_val));
		json_object_set(entry, mods[i]->mod_type, attr);
	}

	json = json_dumps(entry, 0);
	if (riak_put(dn, json, vclock)) {
		slapi_send_ldap_result(pb, LDAP_OPERATIONS_ERROR, NULL, NULL, 0, NULL);
		return -1;
	}

	slapi_send_ldap_result(pb, LDAP_SUCCESS, NULL, NULL, 0, NULL);
	return 0;
}

int
riak_back_del(Slapi_PBlock *pb)
{
	char *dn;

	if (slapi_pblock_get(pb, SLAPI_DELETE_TARGET, &dn))
		return (-1);

	dn = reverse_dn(dn);
	if (riak_del(dn))
		slapi_send_ldap_result(pb, LDAP_NO_SUCH_OBJECT, NULL, NULL, 0, NULL);
	else
		slapi_send_ldap_result(pb, LDAP_SUCCESS, NULL, NULL, 0, NULL);

	return 0;
}

void
filter_attrs(const struct slapi_filter *f, char **index, char **index_value) {
	char *i = NULL, *v = NULL;
	struct slapi_filter *p;

	if (f == NULL)
		return;

	switch (f->f_choice) {
	case LDAP_FILTER_EQUALITY:
	case LDAP_FILTER_GE:
	case LDAP_FILTER_LE:
	case LDAP_FILTER_APPROX:
		/* slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "attr: %s\n", f->f_ava.ava_type); */
		if (is_index(f->f_ava.ava_type)) {
			i = f->f_ava.ava_type;
			v = f->f_ava.ava_value.bv_val;
		}
		break;
	case LDAP_FILTER_PRESENT:
		/* slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "pres: %s\n", f->f_type); */
		if (is_index(f->f_type)) {
			i = f->f_type;
			v = "*";
		}
		break;
	case LDAP_FILTER_AND:
	case LDAP_FILTER_OR:
		for (p = f->f_list; p != NULL; p = p->f_next)
			filter_attrs(p, index, index_value);
		break;
	default:
		break;
	}

	if (i && v) {
		if (*index)
			free(*index);
		*index = strdup(i);

		if (*index_value)
			free(*index_value);
		*index_value = strdup(v);
	}
}

int
riak_back_search(Slapi_PBlock *pb)
{
	int scope;
	char *q, *r, *filter, *index_value = NULL, *index = NULL, *base = NULL;
	Slapi_Operation *op;

	struct slapi_filter *f;
	
	if (slapi_pblock_get(pb, SLAPI_OPERATION, &op) ||
	    slapi_pblock_get(pb, SLAPI_SEARCH_FILTER, &f) ||
	    slapi_pblock_get(pb, SLAPI_SEARCH_STRFILTER, &filter) ||
	    slapi_pblock_get(pb, SLAPI_SEARCH_SCOPE, &scope) ||
	    slapi_pblock_get(pb, SLAPI_SEARCH_TARGET, &base)) {
		slapi_log_error(SLAPI_LOG_FATAL, "riak-backend", "riak_back_search couldn't get PBs\n");
		return -1;
	}
	
	if (base[0] == '\0') {
		slapi_send_ldap_result(pb, LDAP_NO_SUCH_OBJECT, NULL, NULL, 0, NULL);
		return 0;
	}

	filter_attrs(f, &index, &index_value);
	q = make_map(filter, base, index, index_value);

	if (!(r = mapreduce(q))) {
		slapi_send_ldap_result(pb, LDAP_NO_SUCH_OBJECT, NULL, NULL, 0, NULL);
		return 0;
	}

	slapi_send_ldap_result(pb, LDAP_SUCCESS, NULL, NULL, parse_search_results(pb, r, base), NULL);
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
	    slapi_pblock_set(pb, SLAPI_PLUGIN_PRE_SEARCH_FN, (void *)&riak_back_search) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_PRE_ADD_FN, (void *)&riak_back_add) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_PRE_DELETE_FN, (void *)&riak_back_del) ||
	    slapi_pblock_set(pb, SLAPI_PLUGIN_PRE_MODIFY_FN, (void *)&riak_back_mod)) {
		slapi_log_error(SLAPI_LOG_FATAL, "riak-backend", "Couldn't setup functions\n");
		return -1;
	}

	slapi_log_error(SLAPI_LOG_PLUGIN, "riak-backend", "Initialized\n");
	return 0;
}
