char *entry2json(char *, Slapi_Entry *);

struct csn
{
	time_t 		tstamp;
	PRUint16 	seqnum;
	ReplicaId	rid;
	PRUint16	subseqnum;
};

struct csnset_node
{
	CSNType type;
	CSN csn;
	CSNSet *next;
};

struct slapi_value
{
	struct berval bv;
	CSNSet *v_csnset;
	unsigned long v_flags;
};

struct slapi_value_set
{
	struct slapi_value **va;
};

struct slapi_attr {
	char				    *a_type;
	struct slapi_value_set  a_present_values;
	unsigned long	       a_flags;	    /* SLAPI_ATTR_FLAG_... */
	struct slapdplugin	  *a_plugin; /* for the attribute syntax */
	struct slapi_value_set  a_deleted_values;
	struct bervals2free     *a_listtofree; /* JCM: EVIL... For DS4 Slapi compatibility. */
	struct slapi_attr	       *a_next;
	CSN		     *a_deletioncsn; /* The point in time at which this attribute was last deleted */
	struct slapdplugin	  *a_mr_eq_plugin; /* for the attribute EQUALITY matching rule, if any */
	struct slapdplugin	  *a_mr_ord_plugin; /* for the attribute ORDERING matching rule, if any */
	struct slapdplugin	  *a_mr_sub_plugin; /* for the attribute SUBSTRING matching rule, if any */
};

struct subfilt {
        char    *sf_type;
        char    *sf_initial;
        char    **sf_any;
        char    *sf_final;
        void    *sf_private;    /* data private to syntax handler */
};

typedef int (*mrf_plugin_fn) (Slapi_PBlock*);

typedef struct mr_filter_t {
    char*         mrf_oid;
    char*         mrf_type;
    struct berval mrf_value;
    char          mrf_dnAttrs;
    mrFilterMatchFn mrf_match;
    mrf_plugin_fn mrf_index;
    unsigned int  mrf_reusable; /* MRF_ANY_xxx */
    mrf_plugin_fn mrf_reset;
    void*         mrf_object; /* whatever the implementation needs */
    mrf_plugin_fn mrf_destroy;
} mr_filter_t;

struct slapi_filter {
	int f_flags;
	unsigned long   f_choice;       /* values taken from ldap.h */
	PRUint32	f_hash;	 /* for quick comparisons */
	void *assigned_decoder;

	union {
		/* present */
		char	    *f_un_type;

		/* equality, lessorequal, greaterorequal, approx */
		struct ava      f_un_ava;

		/* and, or, not */
		struct slapi_filter     *f_un_complex;

		/* substrings */
		struct subfilt  f_un_sub;

		/* extended -- v3 only */
		mr_filter_t     f_un_extended;
	} f_un;
#define f_type	  f_un.f_un_type
#define f_ava	   f_un.f_un_ava
#define f_avtype	f_un.f_un_ava.ava_type
#define f_avvalue       f_un.f_un_ava.ava_value
#define f_and	   f_un.f_un_complex
#define f_or	    f_un.f_un_complex
#define f_not	   f_un.f_un_complex
#define f_list	  f_un.f_un_complex
#define f_sub	   f_un.f_un_sub
#define f_sub_type      f_un.f_un_sub.sf_type
#define f_sub_initial   f_un.f_un_sub.sf_initial
#define f_sub_any       f_un.f_un_sub.sf_any
#define f_sub_final     f_un.f_un_sub.sf_final
#define f_mr	    f_un.f_un_extended
#define f_mr_oid	f_un.f_un_extended.mrf_oid
#define f_mr_type       f_un.f_un_extended.mrf_type
#define f_mr_value      f_un.f_un_extended.mrf_value
#define f_mr_dnAttrs    f_un.f_un_extended.mrf_dnAttrs

	struct slapi_filter     *f_next;
};

struct slapi_dn
{
	unsigned char flag;
	const char *udn;
	const char *dn;
	const char *ndn;
	int ndn_len;
};
