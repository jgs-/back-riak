json_t *entry2json(Slapi_Entry *);

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
