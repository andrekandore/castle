#include <linux/sched.h>
#include <linux/delay.h>
#include <linux/kthread.h>

#include "castle_public.h"
#include "castle_utils.h"
#include "castle.h"
#include "castle_cache.h"
#include "castle_btree.h"
#include "castle_time.h"
#include "castle_versions.h"
#include "castle_extent.h"
#include "castle_ctrl.h"
#include "castle_da.h"
#include "castle_trace.h"
#include "castle_sysfs.h"
#include "castle_objects.h"
#include "castle_bloom.h"

#ifndef CASTLE_PERF_DEBUG
#define ts_delta_ns(a, b)                       ((void)0)
#define castle_perf_debug_getnstimeofday(ts)    ((void)0)
#define castle_perf_debug_bump_ctr(ctr, a, b)   ((void)0)
#else
#define ts_delta_ns(a, b)                       (timespec_to_ns(&a) - timespec_to_ns(&b))
#define castle_perf_debug_getnstimeofday(ts)    (getnstimeofday(ts))
#define castle_perf_debug_bump_ctr(ctr, a, b)   (ctr += ts_delta_ns(a, b))
#endif

//#define DEBUG
#ifndef DEBUG
#define debug(_f, ...)            ((void)0)
#define debug_verbose(_f, ...)    ((void)0)
#define debug_iter(_f, ...)       ((void)0)
#if 1
#define debug_merges(_f, ...)     ((void)0)
#else
#define debug_merges(_f, _a...)   (castle_printk(LOG_DEBUG, "%s:%.4d: DA=%d, level=%d: " \
                                        _f, __FILE__, __LINE__ , da->id, level, ##_a))
#endif
#else
#define debug(_f, _a...)          (castle_printk(LOG_DEBUG, "%s:%.4d: " _f, __FILE__, __LINE__ , ##_a))
#define debug_verbose(_f, ...)    (castle_printk(LOG_DEBUG, "%s:%.4d: " _f, __FILE__, __LINE__ , ##_a))
#define debug_iter(_f, _a...)     (castle_printk(LOG_DEBUG, "%s:%.4d: " _f, __FILE__, __LINE__ , ##_a))
#define debug_merges(_f, _a...)   (castle_printk(LOG_DEBUG, "%s:%.4d: DA=%d, level=%d: " \
                                        _f, __FILE__, __LINE__ , da->id, level, ##_a))
#endif

#define MAX_DYNAMIC_INTERNAL_SIZE       (5)     /* In C_CHK_SIZE. */
#define MAX_DYNAMIC_TREE_SIZE           (20)    /* In C_CHK_SIZE. */
#define MAX_DYNAMIC_DATA_SIZE           (20)    /* In C_CHK_SIZE. */

#define CASTLE_DA_HASH_SIZE             (1000)
#define CASTLE_CT_HASH_SIZE             (4000)
static struct list_head        *castle_da_hash       = NULL;
static struct list_head        *castle_ct_hash       = NULL;
static struct castle_mstore    *castle_da_store      = NULL;
static struct castle_mstore    *castle_tree_store    = NULL;
static struct castle_mstore    *castle_lo_store      = NULL;
static struct castle_mstore    *castle_dmser_store   = NULL;
static struct castle_mstore    *castle_dmser_in_tree_store   = NULL;
       c_da_t                   castle_next_da_id    = 1;
static atomic_t                 castle_next_tree_seq = ATOMIC(0);
static int                      castle_da_exiting    = 0;

static int                      castle_dynamic_driver_merge = 1;

static int                      castle_merges_abortable = 1; /* 0 or 1, default=enabled */
static DECLARE_WAIT_QUEUE_HEAD (castle_da_promote_wq);  /**< castle_da_level0_modified_promote()  */

module_param(castle_merges_abortable, int, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
MODULE_PARM_DESC(castle_merges_abortable, "Allow on-going merges to abort upon exit condition");

int                             castle_merges_checkpoint = 1; /* 0 or 1, default=enabled */
module_param(castle_merges_checkpoint, int, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
MODULE_PARM_DESC(castle_merges_checkpoint, "Checkpoint on-going merges");

/* We don't need to set upper/lower bounds for the promition frequency as values
 * < 2 will all results in RWCTs being promoted every checkpoint, while very
 * large values will result in RWCTs 'never' being promoted. */
int                             castle_rwct_checkpoint_frequency = 10;  /**< Number of checkpoints
                                                                             before RWCTs are
                                                                             promoted to level 1. */
module_param(castle_rwct_checkpoint_frequency, int, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
MODULE_PARM_DESC(castle_rwct_checkpoint_frequency, "Number checkpoints before RWCTs are promoted.");

static struct
{
    int                     cnt;    /**< Size of cpus array.                        */
    int                    *cpus;   /**< Array of CPU ids for handling requests.    */
} request_cpus;

module_param(castle_dynamic_driver_merge, int, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
MODULE_PARM_DESC(castle_dynamic_driver_merge, "Dynamic driver merge");

/* set to 0 to disable using SSDs for btree leaf nodes */
static int                      castle_use_ssd_leaf_nodes = 0;

module_param(castle_use_ssd_leaf_nodes, int, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
MODULE_PARM_DESC(castle_use_ssd_leaf_nodes, "Use SSDs for btree leaf nodes");

/**********************************************************************************************/
/* Notes about the locking on doubling arrays & component trees.
   Each doubling array has a spinlock which protects the lists of component trees rooted in
   the trees array.
   Each component tree has a reference count, initialised to 1 at the tree creation. Each IO
   and other operation which uses the tree needs to take a reference to the tree. Reference
   should be taken under doubling array lock (which guarantees that the component tree is
   currently threaded onto the doubling array tree list, and vice versa. When a tree is
   removed from the doubling array, no-one else will take references to it any more.
   Component trees are destroyed when reference count reaches 0. The only operation which
   causes trees to be destroyed is the merge process. It decrements the reference count by 1,
   if there are any outstanding IOs, the ref count will reach 0 when last IO completes.
   When a new RW component tree (rwct) is created, previous rwct is moved onto level one. There
   may be ongoing writes to this component tree. This is safe, because all further reads to
   the tree (either doubling array reads, or merge) chain lock the tree nodes appropriately.
   RW tree creation and merges are serialised using the flags field.

   For DAs, only an attached DA is guaranteed to be in the hash.
 */

DEFINE_HASH_TBL(castle_da, castle_da_hash, CASTLE_DA_HASH_SIZE, struct castle_double_array, hash_list, c_da_t, id);
DEFINE_HASH_TBL(castle_ct, castle_ct_hash, CASTLE_CT_HASH_SIZE, struct castle_component_tree, hash_list, tree_seq_t, seq);
static LIST_HEAD(castle_deleted_das);

typedef enum {
    DAM_MARSHALL_ALL = 0,  /**< Marshall all merge state          */
    DAM_MARSHALL_ITERS,    /**< Marshall only iterator state      */
    DAM_MARSHALL_OUTTREE   /**< Marshall only output cct state    */
} c_da_merge_marshall_set_t;

/**********************************************************************************************/
/* Prototypes */
static struct castle_component_tree* castle_ct_alloc(struct castle_double_array *da,
                                                     btree_t type,
                                                     int level, tree_seq_t seq);
void castle_ct_get(struct castle_component_tree *ct, int write);
void castle_ct_put(struct castle_component_tree *ct, int write);
static void castle_component_tree_add(struct castle_double_array *da,
                                      struct castle_component_tree *ct,
                                      struct list_head *head,
                                      int in_init);
static void castle_component_tree_del(struct castle_double_array *da,
                                      struct castle_component_tree *ct);
static void castle_component_tree_promote(struct castle_double_array *da,
                                          struct castle_component_tree *ct,
                                          int in_init);
struct castle_da_merge;
static USED void castle_da_merges_print(struct castle_double_array *da);
static int castle_da_merge_restart(struct castle_double_array *da, void *unused);
static int castle_da_merge_start(struct castle_double_array *da, void *unused);
void castle_double_array_merges_fini(void);
static void castle_da_merge_budget_consume(struct castle_da_merge *merge);
static struct castle_component_tree* castle_da_rwct_get(struct castle_double_array *da,
                                                        int cpu_index);
static void castle_da_queue_kick(struct work_struct *work);
static void castle_da_read_bvec_start(struct castle_double_array *da, c_bvec_t *c_bvec);
static void castle_da_write_bvec_start(struct castle_double_array *da, c_bvec_t *c_bvec);
static void castle_da_reserve(struct castle_double_array *da, c_bvec_t *c_bvec);
static void castle_da_get(struct castle_double_array *da);
static void castle_da_put(struct castle_double_array *da);
static void castle_da_merge_serialise(struct castle_da_merge *merge);
static void castle_da_merge_marshall(struct castle_dmserlist_entry *merge_mstore,
                                     struct castle_in_tree_merge_state_entry *in_tree_merge_mstore,
                                     struct castle_da_merge *merge,
                                     c_da_merge_marshall_set_t partial_marshall);
/* out_tree_check checks only output tree state */
static void castle_da_merge_serdes_out_tree_check(struct castle_dmserlist_entry *merge_mstore,
                                                  struct castle_double_array *da,
                                                  int level);
/* des_check checks things like making sure the merge has the right input trees */
static void castle_da_merge_des_check(struct castle_da_merge *merge, struct castle_double_array *da,
                                      int level, int nr_trees,
                                      struct castle_component_tree **in_trees);
static void castle_da_merge_deserialise(struct castle_da_merge *merge,
                                        struct castle_double_array *da, int level);
static int castle_da_ct_bloom_build_param_deserialise(struct castle_component_tree *ct,
                                                      struct castle_bbp_entry *bbpm);
void castle_da_ct_marshall(struct castle_clist_entry *ctm, struct castle_component_tree *ct);
static c_da_t castle_da_ct_unmarshall(struct castle_component_tree *ct,
                                      struct castle_clist_entry *ctm);
static inline void castle_da_merge_node_size_get(struct castle_da_merge *merge, uint8_t level,
                                                 uint16_t *node_size);
static int __castle_da_rwct_create(struct castle_double_array *da,
                                   int cpu_index,
                                   int in_tran,
                                   c_lfs_vct_type_t lfs_type);
static int castle_da_rwct_create(struct castle_double_array *da,
                                 int cpu_index,
                                 int in_tran,
                                 c_lfs_vct_type_t lfs_type);
static int castle_da_no_disk_space(struct castle_double_array *da);

struct workqueue_struct *castle_da_wqs[NR_CASTLE_DA_WQS];
char *castle_da_wqs_names[NR_CASTLE_DA_WQS] = {"castle_da0"};

tree_seq_t castle_da_next_ct_seq(void);

/**********************************************************************************************/
/* Utils */

/**
 * Set DA's growing bit and return previous state.
 *
 * @return  0   DA was not being grown (but is now)
 * @return  1   DA was already being grown
 */
static inline int castle_da_growing_rw_test_and_set(struct castle_double_array *da)
{
    return test_and_set_bit(DOUBLE_ARRAY_GROWING_RW_TREE_BIT, &da->flags);
}

/**
 * Is the growing bit set on DA?
 *
 * @return  0   DA is currently growing
 * @return  1   DA is not currently being grown
 */
static inline int castle_da_growing_rw_test(struct castle_double_array *da)
{
    return test_bit(DOUBLE_ARRAY_GROWING_RW_TREE_BIT, &da->flags);
}

/**
 * Clear DA's growing bit.
 */
static inline void castle_da_growing_rw_clear(struct castle_double_array *da)
{
    clear_bit(DOUBLE_ARRAY_GROWING_RW_TREE_BIT, &da->flags);
}

#define FOR_EACH_MERGE_TREE(_i, _merge) for((_i)=0; (_i)<(_merge)->nr_trees; (_i)++)

static inline int castle_da_deleted(struct castle_double_array *da)
{
    return test_bit(DOUBLE_ARRAY_DELETED_BIT, &da->flags);
}

static inline void castle_da_deleted_set(struct castle_double_array *da)
{
    set_bit(DOUBLE_ARRAY_DELETED_BIT, &da->flags);
}

static inline int castle_da_need_compaction(struct castle_double_array *da)
{
    return test_bit(DOUBLE_ARRAY_NEED_COMPACTION_BIT, &da->flags);
}

static inline void castle_da_need_compaction_set(struct castle_double_array *da)
{
    set_bit(DOUBLE_ARRAY_NEED_COMPACTION_BIT, &da->flags);
}

static inline void castle_da_need_compaction_clear(struct castle_double_array *da)
{
    clear_bit(DOUBLE_ARRAY_NEED_COMPACTION_BIT, &da->flags);
}

int castle_da_compacting(struct castle_double_array *da)
{
    return test_bit(DOUBLE_ARRAY_COMPACTING_BIT, &da->flags);
}

static inline void castle_da_compacting_set(struct castle_double_array *da)
{
    set_bit(DOUBLE_ARRAY_COMPACTING_BIT, &da->flags);
}

static inline void castle_da_compacting_clear(struct castle_double_array *da)
{
    clear_bit(DOUBLE_ARRAY_COMPACTING_BIT, &da->flags);
}

int castle_da_merge_running(struct castle_double_array *da, int level)
{
    return test_bit(DA_MERGE_RUNNING_BIT, &da->levels[level].merge.flags);
}

static inline void castle_da_merge_running_set(struct castle_double_array *da, int level)
{
    set_bit(DA_MERGE_RUNNING_BIT, &da->levels[level].merge.flags);
}

static inline void castle_da_merge_running_clear(struct castle_double_array *da, int level)
{
    clear_bit(DA_MERGE_RUNNING_BIT, &da->levels[level].merge.flags);
}

/**********************************************************************************************/
/* Iterators */
struct castle_immut_iterator;

typedef void (*castle_immut_iter_node_start) (struct castle_immut_iterator *);

typedef struct castle_immut_iterator {
    c_async_iterator_t            async_iter;
    struct castle_component_tree *tree;
    struct castle_btree_type     *btree;
    int                           completed;  /**< set to 1 when iterator is exhausted            */
    c2_block_t                   *curr_c2b;   /**< node c2b currently providing entries           */
    struct castle_btree_node     *curr_node;  /**< btree node (curr_c2b->buffer)                  */
    int32_t                       curr_idx;   /**< offset within curr_node of current entry
                                                   (where current is really next())               */
    int32_t                       cached_idx; /**< this is needed for merge SERDES because curr_idx
                                                   is moved ahead after the iterator value is
                                                   cached; if the call to iter_entry_find was moved
                                                   before the entry_get within immut_iter_next, it
                                                   will be unnecessary to save cached_idx         */
    c2_block_t                   *next_c2b;   /**< node c2b to provide next entires               */
    int32_t                       next_idx;   /**< offset within next_c2b of first entry to return*/
    castle_immut_iter_node_start  node_start; /**< callback handler to fire whenever iterator moves
                                                   to a new node within the btree                 */
    void                         *private;
} c_immut_iter_t;

static int castle_ct_immut_iter_entry_find(c_immut_iter_t *iter,
                                           struct castle_btree_node *node,
                                           int start_idx)
{
    int disabled;
    c_val_tup_t cvt;
    c_ver_t version;

    for(; start_idx<node->used; start_idx++)
    {

        disabled = iter->btree->entry_get(node, start_idx, NULL, &version, &cvt);
        if(!CVT_LEAF_PTR(cvt) && !disabled && castle_version_is_ancestor(node->version, version))
            return start_idx;
    }

    return -1;
}

/**
 * Update iterator with new btree node.
 *
 * @param iter  Iterator to update
 * @param node  Proposed next node
 *
 * @return 0    Node has no entries.
 * @return 1    Node has entries.
 */
static int castle_ct_immut_iter_next_node_init(c_immut_iter_t *iter,
                                               struct castle_btree_node *node)
{
    /* We should never encounter a non-leaf node. */
    BUG_ON(!node->is_leaf);

    /* Non-dynamic trees do not contain leaf pointers => the node must be non-empty,
       and will not contain leaf pointers */
    if(!iter->tree->dynamic)
    {
        iter->next_idx = 0;
        BUG_ON(castle_ct_immut_iter_entry_find(iter, node, 0 /* start_idx */) != iter->next_idx);
        BUG_ON(node->used == 0);
        return 1;
    }

    /* Finally, for dynamic trees, check if we have at least non-leaf pointer */
    iter->next_idx = castle_ct_immut_iter_entry_find(iter, node, 0 /* start_idx */);
    if(iter->next_idx >= 0)
        return 1;

    return 0;
}

/**
 * Returns the extent position of the next leaf node after the cep specified.
 * If one isn't avilable it returns the invalid position.
 */
static c_ext_pos_t castle_ct_immut_iter_next_node_cep_find(c_immut_iter_t *iter,
                                                           c_ext_pos_t cep,
                                                           uint16_t node_size)
{
    uint16_t btree_node_size;

    if(EXT_POS_INVAL(cep))
        return INVAL_EXT_POS;

    /* We should only be inspecting leaf nodes, work out the node size. */
    btree_node_size = iter->btree->node_size(iter->tree, 0);

    /* Work out the position of the next node. */
    cep.offset += ((c_byte_off_t)btree_node_size) * C_BLK_SIZE;

    /* If this cep is past the extent allocation end, return invalid cep. */
    if(cep.offset >= atomic64_read(&iter->tree->tree_ext_free.used))
        return INVAL_EXT_POS;

    /* Make sure that node size provided agrees with leaf node size worked out. */
    BUG_ON(btree_node_size != node_size);

    /* Otherwise return the cep. */
    return cep;
}

/**
 * Find the next leaf node starting from cep.
 *
 * iter->next_c2b==NULL on failure, else success
 *
 * @also castle_ct_immut_iter_next_node_init()
 */
static void castle_ct_immut_iter_next_node_find(c_immut_iter_t *iter,
                                                c_ext_pos_t cep,
                                                uint16_t node_size)
{
    struct castle_btree_node *node;
    c2_block_t *c2b;
#ifdef CASTLE_PERF_DEBUG
    struct timespec ts_start, ts_end;
#endif

    castle_printk(LOG_DEBUG, "%s::Looking for next node starting with "cep_fmt_str_nl,
            __FUNCTION__, cep2str(cep));
    BUG_ON(iter->next_c2b);
    c2b=NULL;
    while(!EXT_POS_INVAL(cep))
    {
        /* Release c2b if we've got one */
        if(c2b)
            put_c2b(c2b);
        /* Get cache block for the current c2b */
        castle_perf_debug_getnstimeofday(&ts_start);
        c2b = castle_cache_block_get(cep, node_size);
        castle_perf_debug_getnstimeofday(&ts_end);
        /* Update time spent obtaining c2bs. */
        castle_perf_debug_bump_ctr(iter->tree->get_c2b_ns, ts_end, ts_start);
        debug("Node in immut iter.\n");
        castle_cache_advise(c2b->cep, C2_ADV_PREFETCH|C2_ADV_FRWD, -1, -1, 0);
        write_lock_c2b(c2b);
        /* If c2b is not up to date, issue a blocking READ to update */
        if(!c2b_uptodate(c2b))
        {
            castle_perf_debug_getnstimeofday(&ts_start);
            BUG_ON(submit_c2b_sync(READ, c2b));
            castle_perf_debug_getnstimeofday(&ts_end);
            castle_perf_debug_bump_ctr(iter->tree->bt_c2bsync_ns, ts_end, ts_start);
        }
        write_unlock_c2b(c2b);
        node = c2b_bnode(c2b);
        /* Determine if this is a leaf-node with entries */
        BUG_ON(node->magic != BTREE_NODE_MAGIC); //see trac #2844
        if(castle_ct_immut_iter_next_node_init(iter, node))
        {
            /* It is */
            debug("%s::Cep "cep_fmt_str " will be used next, exiting.\n",
                   __FUNCTION__,
                   cep2str(cep));
            iter->next_c2b = c2b;
            return;
        }
        cep = castle_ct_immut_iter_next_node_cep_find(iter, cep, node_size);
        castle_printk(LOG_DEBUG, "%s::Node non-leaf or no non-leaf-ptr entries, moving to " cep_fmt_str_nl,
               __FUNCTION__, cep2str(cep));
    }
    debug("%s::no leaf node found.\n", __FUNCTION__);
    /* Drop c2b if we failed to find a leaf node, but have an outstanding reference to
       a non-leaf node */
    if(c2b)
        put_c2b(c2b);
}

/**
 * Find the next leaf node for iter.
 *
 * @also castle_ct_immut_iter_next_node_find()
 */
static void castle_ct_immut_iter_next_node(c_immut_iter_t *iter)
{
    uint16_t node_size;

    BUG_ON(!iter->next_c2b);
    /* Drop the current c2b, if one exists. */
    if(iter->curr_c2b)
    {
        debug("Moving to the next block after: "cep_fmt_str_nl,
               cep2str(iter->curr_c2b->cep));
        put_c2b_and_demote(iter->curr_c2b);
    }
    /* next_c2b becomes curr_c2b */
    iter->curr_c2b = iter->next_c2b;
    node_size = iter->curr_c2b->nr_pages;
    BUG_ON(!c2b_uptodate(iter->curr_c2b));
    iter->curr_node = c2b_bnode(iter->curr_c2b);
    if(!iter->curr_node->is_leaf ||
           (iter->curr_node->used <= iter->next_idx))
    {
        castle_printk(LOG_INFO, "curr_node=%d, used=%d, next_idx=%d\n",
                iter->curr_node->is_leaf,
                iter->curr_node->used,
                iter->next_idx);
    }
    BUG_ON(!iter->curr_node->is_leaf ||
           (iter->curr_node->used <= iter->next_idx));
    iter->curr_idx  = iter->next_idx;
    castle_printk(LOG_DEBUG, "%s::Moved to cep="cep_fmt_str_nl,
            __FUNCTION__, cep2str(iter->curr_c2b->cep));

    /* Fire the node_start callback. */
    if (iter->node_start)
        iter->node_start(iter);

    /* Find next c2b following the extent order. */
    iter->next_c2b = NULL;
    castle_ct_immut_iter_next_node_find(iter,
                                        castle_ct_immut_iter_next_node_cep_find(
                                            iter,
                                            iter->curr_c2b->cep,
                                            node_size),
                                        node_size);
}

static int castle_ct_immut_iter_prep_next(c_immut_iter_t *iter)
{
    /* Check if we can read from the curr_node. If not move to the next node.
       Make sure that if entries exist, they are not leaf pointers. */
    if(iter->curr_idx >= iter->curr_node->used || iter->curr_idx < 0)
    {
        if (!iter->next_c2b)
        {
            iter->completed = 1;
            BUG_ON(!iter->curr_c2b);
            put_c2b(iter->curr_c2b);
            iter->curr_c2b = NULL;

            return 1;
        }

        debug("No more entries in the current node. Asking for next.\n");
        BUG_ON((iter->curr_idx >= 0) && (iter->curr_idx > iter->curr_node->used));
        castle_ct_immut_iter_next_node(iter);
        BUG_ON((iter->curr_idx >= 0) && (iter->curr_idx >= iter->curr_node->used));
    }

    return 1;
}

static void castle_ct_immut_iter_next(c_immut_iter_t *iter,
                                      void **key_p,
                                      c_ver_t *version_p,
                                      c_val_tup_t *cvt_p)
{
    int disabled;

    /* Check if we can read from the curr_node. If not move to the next node.
       Make sure that if entries exist, they are not leaf pointers. */
    BUG_ON(iter->curr_idx >= iter->curr_node->used || iter->curr_idx < 0);

    disabled = iter->btree->entry_get(iter->curr_node,
                                      iter->curr_idx,
                                      key_p,
                                      version_p,
                                      cvt_p);
    /* curr_idx should have been set to a non-leaf pointer */
    BUG_ON(CVT_LEAF_PTR(*cvt_p) || disabled);
    iter->cached_idx = iter->curr_idx;
    iter->curr_idx = castle_ct_immut_iter_entry_find(iter, iter->curr_node, iter->curr_idx + 1);
    debug("Returned next, curr_idx is now=%d / %d.\n", iter->curr_idx, iter->curr_node->used);
}

static int castle_ct_immut_iter_has_next(c_immut_iter_t *iter)
{
    if(unlikely(iter->completed))
        return 0;

    BUG_ON(iter->curr_idx >= iter->curr_node->used || iter->curr_idx < 0);

    return 1;
}

static void castle_ct_immut_iter_cancel(c_immut_iter_t *iter)
{
    debug("Cancelling immut enumerator for ct id=%d\n", iter->tree->seq);
    if (iter->curr_c2b)
        put_c2b(iter->curr_c2b);
    if (iter->next_c2b)
        put_c2b(iter->next_c2b);
}

struct castle_iterator_type castle_ct_immut_iter = {
    .register_cb = NULL,
    .prep_next   = (castle_iterator_prep_next_t)    castle_ct_immut_iter_prep_next,
    .has_next    = (castle_iterator_has_next_t)     castle_ct_immut_iter_has_next,
    .next        = (castle_iterator_next_t)         castle_ct_immut_iter_next,
    .skip        = NULL,
    .cancel      = (castle_iterator_cancel_t)       castle_ct_immut_iter_cancel,
};

/**
 * Initialise iterator for immutable btrees.
 *
 * @param iter          Iterator to initialise
 * @param node_start    CB handler when iterator moves to a new btree node
 * @param private       Private data to pass to CB handler
 */
static void castle_ct_immut_iter_init(c_immut_iter_t *iter,
                                      castle_immut_iter_node_start node_start,
                                      void *private)
{
    c_ext_pos_t first_node_cep;
    uint16_t first_node_size;

    castle_printk(LOG_DEBUG, "%s::Initialising immut enumerator for ct id=%d\n",
            __FUNCTION__, iter->tree->seq);
    iter->btree     = castle_btree_type_get(iter->tree->btree_type);
    iter->completed = 0;
    iter->curr_c2b  = NULL;
    iter->next_c2b  = NULL;
    iter->node_start= node_start;
    iter->private   = private;
    iter->async_iter.end_io = NULL;
    iter->async_iter.iter_type = &castle_ct_immut_iter;

    first_node_cep.ext_id = iter->tree->tree_ext_free.ext_id;
    first_node_cep.offset = 0;
    first_node_size = iter->btree->node_size(iter->tree, 0);
    castle_printk(LOG_DEBUG, "%s::first_node_cep = "cep_fmt_str"\n",
            __FUNCTION__, cep2str(first_node_cep));
    castle_ct_immut_iter_next_node_find(iter,
                                        first_node_cep,
                                        first_node_size);
    /* Check if we succeeded at finding at least a single node */
    BUG_ON(!iter->next_c2b);
    /* Init curr_c2b correctly */
    castle_ct_immut_iter_next_node(iter);
}

/**
 * Compare verion tuples k1,v1 against k2,v2.
 *
 * @param btree Source btree (for compare function)
 * @param k1    Key to compare against
 * @param v1    Version to compare against
 * @param k2    Key to compare with
 * @param v2    Version to compare with
 *
 * @return -1   (k1, v1) <  (k2, v2)
 * @return  0   (k1, v1) == (k2, v2)
 * @return  1   (k1, v1) >  (k2, v2)
 */
static int castle_kv_compare(struct castle_btree_type *btree,
                             void *k1, c_ver_t v1,
                             void *k2, c_ver_t v2)
{
    int ret = btree->key_compare(k1, k2);
    if(ret != 0)
        return ret;

    /* Reverse v achieved by inverting v1<->v2 given to version_compare() function */
    return castle_version_compare(v2, v1);
}

static void castle_da_node_buffer_init(struct castle_btree_type *btree,
                                       struct castle_btree_node *buffer,
                                       uint16_t node_size)
{
    debug("Resetting btree node buffer.\n");
    /* Buffers are proper btree nodes understood by castle_btree_node_type function sets.
       Initialise the required bits of the node, so that the types don't complain. */
    buffer->magic     = BTREE_NODE_MAGIC;
    buffer->type      = btree->magic;
    buffer->version   = 0;
    buffer->used      = 0;
    buffer->is_leaf   = 1;
    buffer->size      = node_size;
}

/**
 * Modlist B-tree iterator structure.
 *
 * @also castle_ct_modlist_iter_init()
 */
typedef struct castle_modlist_iterator {
    c_async_iterator_t async_iter;
    struct castle_btree_type *btree;
    struct castle_component_tree *tree;
    uint16_t leaf_node_size;
    struct castle_da_merge *merge;
    c_immut_iter_t *enumerator;
    uint8_t enum_advanced;          /**< Set if enumerator has advanced to a new node             */
    int err;
    uint32_t nr_nodes;              /**< Number of nodes in the buffer                            */
    void *node_buffer;              /**< Buffer to store all the nodes                            */
    uint32_t nr_items;              /**< Number of items in the buffer                            */
    uint32_t next_item;             /**< Next item to return in iterator                          */
    struct item_idx {
        uint32_t node;              /**< Which btree node                                         */
        uint32_t node_offset;       /**< Offset within btree node                                 */
    } *src_entry_idx;               /**< 1 of 2 arrays of entry pointers (used for sort)          */
    struct item_idx *dst_entry_idx; /**< 2nd array of entry pointers                              */
    struct entry_range {            /**< Entry range describes start,end within *_entry_idx       */
        uint32_t start;
        uint32_t end;
    } *ranges;
    uint32_t nr_ranges;             /**< Number of elements in node_ranges                        */
} c_modlist_iter_t;

struct mutex    castle_da_level1_merge_init;            /**< For level 1 merges serialise entry to
                                                             castle_da_merge_init()               */
atomic_t        castle_ct_modlist_iter_byte_budget;     /**< Byte budget remaining for in-flight
                                                             sorted modlist iter node buffers.    */

/**
 * Free memory allocated by iterator and replenish byte budget.
 */
static void castle_ct_modlist_iter_free(c_modlist_iter_t *iter)
{
    int buffer_size;

    if(iter->enumerator)
    {
        castle_ct_immut_iter.cancel(iter->enumerator);
        castle_kfree(iter->enumerator);
    }
    if(iter->node_buffer)
        castle_free(iter->node_buffer);
    if (iter->src_entry_idx)
        castle_free(iter->src_entry_idx);
    if (iter->dst_entry_idx)
        castle_free(iter->dst_entry_idx);
    if (iter->ranges)
        castle_free(iter->ranges);

    /* Replenish the budget - no need to serialise. */
    buffer_size = iter->nr_nodes * iter->leaf_node_size * C_BLK_SIZE;
    atomic_add(buffer_size, &castle_ct_modlist_iter_byte_budget);
}

/**
 * Get requested btree node from the node_buffer.
 */
static struct castle_btree_node* castle_ct_modlist_iter_buffer_get(c_modlist_iter_t *iter,
                                                                   uint32_t idx)
{
    char *buffer = iter->node_buffer;

    return (struct castle_btree_node *)(buffer + idx * iter->leaf_node_size * C_BLK_SIZE);
}

/**
 * Return key, version, cvt for entry sort_idx within iter->src_entry_idx[].
 */
static void castle_ct_modlist_iter_item_get(c_modlist_iter_t *iter,
                                            uint32_t sort_idx,
                                            void **key_p,
                                            c_ver_t *version_p,
                                            c_val_tup_t *cvt_p)
{
    struct castle_btree_type *btree = iter->btree;
    struct castle_btree_node *node;

    debug_verbose("Node_idx=%d, offset=%d\n",
                  iter->sort_idx[sort_idx].node,
                  iter->sort_idx[sort_idx].node_offset);
    node = castle_ct_modlist_iter_buffer_get(iter, iter->src_entry_idx[sort_idx].node);
    btree->entry_get(node,
                     iter->src_entry_idx[sort_idx].node_offset,
                     key_p,
                     version_p,
                     cvt_p);
}

/**
 * Return the next entry from the iterator.
 *
 * - Uses the final sorted src_entry_idx[].
 *
 * @also castle_ct_modlist_iter_fill()
 * @also castle_ct_modlist_iter_mergesort()
 */
static void castle_ct_modlist_iter_next(c_modlist_iter_t *iter,
                                        void **key_p,
                                        c_ver_t *version_p,
                                        c_val_tup_t *cvt_p)
{
    castle_ct_modlist_iter_item_get(iter, iter->next_item, key_p, version_p, cvt_p);
    iter->next_item++;
}

/**
 * Does the iterator have further entries.
 *
 * @return 1    Entry has more entries
 * @return 0    No further entries
 */
static int castle_ct_modlist_iter_has_next(c_modlist_iter_t *iter)
{
    return (!iter->err && (iter->next_item < iter->nr_items));
}

/**
 * Prepare the iterator for next key. This is a NOP for modlist_iter as the data is completely
 * in buffer.
 *
 * @return 1    Always
 */
static int castle_ct_modlist_iter_prep_next(c_modlist_iter_t *iter)
{
    return 1;
}

/**
 * Fill count entry pointers in dst_entry_idx from src_entry_idx.
 *
 * @param iter  Modlist iterator (provides src_entry_idx, dst_entry_idx)
 * @param src   Starting src_entry_idx entry to source entry pointers from
 * @param dst   Starting dst_entry_idx entry to populate from
 * @param count Number of entries to populate
 */
static inline void castle_ct_modlist_iter_merge_index_fill(c_modlist_iter_t *iter,
                                                           uint32_t src,
                                                           uint32_t dst,
                                                           uint32_t count)
{
    uint32_t i;

    for (i = 0; i < count; i++, src++, dst++)
    {
        iter->dst_entry_idx[dst].node        = iter->src_entry_idx[src].node;
        iter->dst_entry_idx[dst].node_offset = iter->src_entry_idx[src].node_offset;
    }
}

/**
 * Mergesort two contiguous entry ptr ranges (r1, r2) from src_entry_idx into dst_entry_idx.
 *
 * @param iter  Modlist iterator (provides src_entry_idx, dst_entry_idx)
 * @param r1    First range of node entry pointers
 * @param r2    Second range of node entry pointers
 *
 * - Iterate over entries pointed to by r1->start,r1->end and r2->start,r2->end
 *   from src_entry_idx[]
 * - Write out entry pointers in smallest to largest order into dst_entry_idx[]
 *   starting at index r1->start
 * - Result is that dst_entry_idx[r1->start] to dst_entry_idx[r2->end] will be
 *   sorted in smallest to largest order
 *
 * @also castle_ct_modlist_iter_mergesort()
 */
static void castle_ct_modlist_iter_merge_ranges(c_modlist_iter_t *iter,
                                                struct entry_range *r1,
                                                struct entry_range *r2)
{
    uint32_t r1_idx = r1->start;    /* current index for r1 */
    uint32_t r2_idx = r2->start;    /* current index for r2 */
    uint32_t dst_idx = r1->start;   /* output index */
    uint32_t src_idx = 0;           /* index of next smallest entry (from r1 or r2) */
    void *r1_key, *r2_key;
    c_ver_t r1_ver, r2_ver;

    BUG_ON(r1->end+1 != r2->start); /* ranges *MUST* be contiguous */

    for (dst_idx = r1->start; dst_idx <= r2->end; dst_idx++)
    {
        might_resched();

        /* Both ranges have more entries if their indexes lie within the range. */
        if (r1_idx <= r1->end && r2_idx <= r2->end)
        {
            /* Both ranges have more entries, we need to do a comparison to
             * determine which range has the next smallest value. */
            castle_ct_modlist_iter_item_get(iter, r1_idx, &r1_key, &r1_ver, NULL);
            castle_ct_modlist_iter_item_get(iter, r2_idx, &r2_key, &r2_ver, NULL);

            if (castle_kv_compare(iter->btree, r1_key, r1_ver, r2_key, r2_ver) < 0)
            {
                /* r1 smaller than r2. */
                src_idx = r1_idx;
                r1_idx++;
            }
            else
            {
                /* r1 larger than or equal to r2. */
                src_idx = r2_idx;
                r2_idx++;
            }

            /* Update dst_entry_idx with the smallest available entry pointer. */
            castle_ct_modlist_iter_merge_index_fill(iter, src_idx, dst_idx, 1);

            continue;
        }

        /* If we reached here then one of the two entry ranges has been
         * exhausted.  We need do no more comparisons and can just populate
         * the remainder of the output index with the entries from the range
         * that has not yet been exhausted. */

        if (r1_idx <= r1->end)
            castle_ct_modlist_iter_merge_index_fill(iter, r1_idx, dst_idx, r1->end-r1_idx+1);
        else if (r2_idx <= r2->end)
            castle_ct_modlist_iter_merge_index_fill(iter, r2_idx, dst_idx, r2->end-r2_idx+1);
        else
            BUG();

        /* We're done. */
        break;
    }
}

/**
 * Handler called when immutable iterator advances to a new source btree node.
 *
 * - Set modlist_iter->enum_advanced
 * - Provides a mechanism for the modlist iterator to know when the immutable
 *   iterator has advanced to a new node
 * - Used for sorting efficiency
 *
 * @also castle_ct_modlist_iter_fill()
 */
static void castle_ct_modlist_iter_next_node(c_immut_iter_t *immut_iter)
{
    c_modlist_iter_t *modlist_iter = immut_iter->private;
    modlist_iter->enum_advanced = 1;
}

/**
 * Populate node_buffer with leaf btree nodes, set up entry indexes and node ranges.
 *
 * - Using immutable iterator (iter->enumerator) iterate over entries in the
 *   unsorted btree
 * - Immutable iterator has a callback when it advances to a new btree node.
 *   castle_ct_modlist_iter_next_node() is registered as the callback handler
 *   and sets iter->enum_advanced whenever a new source node is used
 * - Get a new buffer btree node whenever the source iterator node advances
 * - Keep getting (unsorted) entries from the immutable iterator and store them
 *   in the node_buffer.  Put an entry in dst_entry_idx[] pointing to the node
 *   and node_offset
 * - As we move to a new node when the immutable iterator moves, we are
 *   guaranteed that individual btree nodes are sorted.  Fill ranges[] with
 *   start and end index within dst_entry_idx[]
 *
 * @also castle_ct_modlist_iter_mergesort()
 */
static void castle_ct_modlist_iter_fill(c_modlist_iter_t *iter)
{
    struct castle_btree_type *btree = iter->btree;
    struct castle_btree_node *node = NULL;
    uint32_t node_idx, item_idx, node_offset;
    c_ver_t version;
    c_val_tup_t cvt;
    void *key;

    node_idx = item_idx = node_offset = 0;
    while (castle_iterator_has_next_sync(&castle_ct_immut_iter, iter->enumerator))
    {
        might_resched();

        /* Get the next (unsorted) entry from the immutable iterator. */
        castle_ct_immut_iter.next(iter->enumerator, &key, &version, &cvt);
        debug("In enum got next: k=%p, version=%d, %u/%llu, cep="cep_fmt_str_nl,
                key, version, (uint32_t)cvt.type, cvt.length, cep2str(cvt.cep));
        debug("Dereferencing first 4 bytes of the key (should be length)=0x%x.\n",
                *((uint32_t *)key));
        debug("Inserting into the node=%d, under idx=%d\n", node_idx, node_offset);
        BUG_ON(CVT_LEAF_PTR(cvt));
        if(CVT_ACCUM_COUNTER(cvt))
            CVT_COUNTER_ACCUM_ONEV_TO_LOCAL(cvt, cvt);

        /* Advance to a new node if the immutable iterator has moved on.  This
         * is handled via the immutable iterator callback.  We rely on source
         * nodes being identically sized to our destination nodes. */
        if (iter->enum_advanced)
        {
            /* Set end entry for node range we just completed. */
            if (likely(node_idx))
                iter->ranges[node_idx-1].end = item_idx-1;
            /* Set start entry for node range we're moving to. */
            iter->ranges[node_idx].start = item_idx;

            /* Get a new node. */
            node = castle_ct_modlist_iter_buffer_get(iter, node_idx);
            castle_da_node_buffer_init(btree, node, btree->node_size(iter->tree, 0));

            /* We've advance, initialise a good state. */
            iter->enum_advanced = 0;
            node_offset = 0;
            node_idx++;
        }

        /* Insert entry into node. */
        btree->entry_add(node, node_offset, key, version, cvt);
        iter->dst_entry_idx[item_idx].node        = node_idx-1;
        iter->dst_entry_idx[item_idx].node_offset = node_offset;
        node_offset++;
        item_idx++;
    }

    if (likely(node_idx))
        iter->ranges[node_idx-1].end = item_idx-1;    /* @TODO this should be tidier */

    if (item_idx != atomic64_read(&iter->tree->item_count))
    {
        castle_printk(LOG_WARN, "Error. Different number of items than expected in CT=%d "
               "(dynamic=%d). Item_idx=%d, item_count=%ld\n",
               iter->tree->seq, iter->tree->dynamic,
               item_idx, atomic64_read(&iter->tree->item_count));
        WARN_ON(1);
    }
    iter->nr_items = item_idx;
    iter->nr_ranges = node_idx;
    //iter->err = iter->enumerator->err;
}

/**
 * Mergesort the underlying component tree into smallest->largest k,<-v order.
 *
 * L1 btrees are in insertion order but individual nodes have entries sorted in
 * k,<-v order.  To iterate over the btree we must first sort the whole tree.
 * This is done by merging leaf-nodes together repeatedly until we have a single
 * large k,<-v sorted set of entries.
 *
 * Internally the iterator uses:
 *
 * - node_buffer: contiguous buffer of btree leaf-nodes with entries
 * - src_entry_idx[], dst_entry_idx[]: two indirect indexes of entries within
 *   node_buffer.  We sort the data indirectly and hence for simplicity
 *   alternate src_entry_idx[] and dst_entry_idx[] for each round of merges
 * - ranges: ranges of entries within src_entry_idx[] that are guaranteed to
 *   be k,<-v sorted
 * - nr_ranges: number of ranges in src_entry_idx[]
 *
 * Mergesort implementation as follows:
 *
 * castle_ct_modlist_iter_fill() fills iter->entry_buffer with leaf-nodes from
 * the source btree.  For each entry that gets inserted into the buffer a
 * pointer to that entry goes into dst_entry_idx[].  Individual source btree
 * nodes are k,<-v sorted so we define ranges of entries on top of
 * dst_entry_idx[].  Each range encompasses the entries from a single source
 * btree node.  iter->nr_ranges contains the number of active ranges in
 * src_entry_idx[] (except after a fill when it is valid for dst_entry_idx[]).
 *
 * We go through the main mergesort loop until nr_ranges has reached 1 (single
 * sorted range of entries).  Each time we go through the loop we swap the src
 * and dst entry_idx[] such that src_entry_idx[] contains the most up-to-date
 * sorted data we have available.
 *
 * Take two ranges of entries and merge them together in _merge_ranges().  This
 * takes the entries from src_entry_idx[] and writes out sorted entries into
 * dst_entry_idx[].
 *
 * Update ranges[] with the new range start and end (new range start will be
 * range1.start and end will be range2.end - ranges must be contiguous).
 *
 * If we have an uneven number of ranges move the entry pointers from src_
 * to dst_entry_idx[] and ensure the range points to the correct entries.  No
 * merge is performed in this instance.  @TODO this is inefficient
 *
 * Update the total number of ranges and go again if necessary.
 *
 * @also castle_ct_modlist_iter_fill()
 * @also castle_ct_modlist_iter_merge_ranges()
 * @also castle_ct_modlist_iter_init()
 */
static void castle_ct_modlist_iter_mergesort(c_modlist_iter_t *iter)
{
    uint32_t src_range, dst_range;
    void *tmp_entry_idx;

    /* Populate internal entry buffer and initialise dst_entry_idx[] and the
     * initial node ranges for sorting. */
    castle_ct_modlist_iter_fill(iter);

    /* Repeatedly merge ranges of entry pointers until we have a single
     * all-encompassing smallest->largest sorted range we can use to return
     * entries when the iterator .has_next(), .next() functions are called. */
    while (iter->nr_ranges > 1)
    {
        /* Another merge.  Swap the src and dst entry indexes around.
         * We will now be sourcing from the previous iteration's dst_entry_idx
         * (also used by castle_ct_modlist_iter_fill()) and writing our values
         * out to our previous source. */
        tmp_entry_idx = iter->src_entry_idx;
        iter->src_entry_idx = iter->dst_entry_idx;  /* src = dst */
        iter->dst_entry_idx = tmp_entry_idx;        /* dst = src */

        src_range = dst_range = 0;

        /* So long as we have two remaining entry ranges, mergesort the entries
         * together to create a single range spanning the capacity of both. */
        while (src_range+1 < iter->nr_ranges)
        {
            /* Mergesort. */
            castle_ct_modlist_iter_merge_ranges(iter,
                                                &iter->ranges[src_range],
                                                &iter->ranges[src_range+1]);

            /* Update the destination range. */
            iter->ranges[dst_range].start = iter->ranges[src_range].start;
            iter->ranges[dst_range].end   = iter->ranges[src_range+1].end;

            src_range += 2;
            dst_range++;
        }

        /* Above we merged pairs of ranges.  Part of the merge process (handled
         * within castle_ct_modlist_iter_merge_ranges() is to populate the
         * dst_entry_idx.  If we started with an odd number of ranges we must
         * deal with the straggling range as a special case. */
        if (src_range < iter->nr_ranges)
        {
            /* We only have one range to merge so we fake up a range that
             * castle_ct_modlist_iter_merge_ranges() will determine to be
             * exhausted and therefore will populate dst_entry_idx with only
             * those entries from our one remaining src_range. */
            struct entry_range null_range;

            /* Mergesort. */
            null_range.start = iter->ranges[src_range].end+1;
            null_range.end   = iter->ranges[src_range].end;
            castle_ct_modlist_iter_merge_ranges(iter,
                                                &iter->ranges[src_range],
                                                &null_range);

            /* Update the destination range. */
            iter->ranges[dst_range].start = iter->ranges[src_range].start;
            iter->ranges[dst_range].end   = iter->ranges[src_range].end;

            src_range++;
            dst_range++;
        }
        /* else even number of source ranges */

        iter->nr_ranges = dst_range;
    }

    /* Finally ensure dst_entry_idx points to the final sorted index and free
     * the other temporary index right now. */
    castle_free(iter->src_entry_idx);
    iter->src_entry_idx = iter->dst_entry_idx;
    iter->dst_entry_idx = NULL;
}

struct castle_iterator_type castle_ct_modlist_iter = {
    .register_cb = NULL,
    .prep_next   = (castle_iterator_prep_next_t)    castle_ct_modlist_iter_prep_next,
    .has_next    = (castle_iterator_has_next_t)     castle_ct_modlist_iter_has_next,
    .next        = (castle_iterator_next_t)         castle_ct_modlist_iter_next,
    .skip        = NULL,
};

/**
 * Initialise modlist btree iterator.
 *
 * See castle_ct_modlist_iter_mergesort() for full implementation details.
 *
 * - Initialise members
 * - Consume bytes from the global modlist iter byte budget
 * - Allocate memory for node_buffer, src_ and dst_entry_idx[] and ranges
 * - Initialise immutable iterator (for sort)
 * - Kick off mergesort
 *
 * NOTE: Caller must hold castle_da_level1_merge_init mutex.
 *
 * @also castle_ct_modlist_iter_mergesort()
 */
static void castle_ct_modlist_iter_init(c_modlist_iter_t *iter)
{
    struct castle_component_tree *ct = iter->tree;
    int buffer_size;

    BUG_ON(!mutex_is_locked(&castle_da_level1_merge_init));
    BUG_ON(atomic64_read(&ct->item_count) == 0);
    BUG_ON(!ct); /* component tree must be provided */

    iter->err = 0;
    iter->btree = castle_btree_type_get(ct->btree_type);
    iter->leaf_node_size = iter->btree->node_size(ct, 0);
    iter->async_iter.end_io = NULL;
    iter->async_iter.iter_type = &castle_ct_modlist_iter;

    /* To prevent sudden kernel memory ballooning we impose a modlist byte
     * budget for all DAs.  Size the node buffer based on leaf nodes only. */
    buffer_size = atomic64_read(&ct->tree_ext_free.used);
    iter->nr_nodes = buffer_size / (iter->leaf_node_size * C_BLK_SIZE);
    if (atomic_sub_return(buffer_size, &castle_ct_modlist_iter_byte_budget) < 0)
    {
        castle_printk(LOG_INFO,
                "Couldn't allocate enough bytes for _modlist_iter_init from bytes budget.\n");
        atomic_add(buffer_size, &castle_ct_modlist_iter_byte_budget);
        iter->err = -ENOMEM;
        return;
    }

    /* Allocate immutable iterator.
     * For iterating over source entries during sort. */
    iter->enumerator = castle_malloc(sizeof(c_immut_iter_t), GFP_KERNEL);

    /* Allocate btree-entry buffer, two indexes for the buffer (for sorting)
     * and space to define ranges of sorted nodes within the index. */
    iter->node_buffer = castle_alloc(buffer_size);
    iter->src_entry_idx = castle_alloc(atomic64_read(&ct->item_count) * sizeof(struct item_idx));
    iter->dst_entry_idx = castle_alloc(atomic64_read(&ct->item_count) * sizeof(struct item_idx));
    iter->ranges = castle_alloc(iter->nr_nodes * sizeof(struct entry_range));

    /* Return ENOMEM if we failed any of our allocations. */
    if(!iter->enumerator || !iter->node_buffer || !iter->src_entry_idx || !iter->dst_entry_idx)
    {
        castle_ct_modlist_iter_free(iter);
        iter->err = -ENOMEM;
        return;
    }

    /* Initialise the immutable iterator */
    iter->enumerator->tree = ct;
    castle_ct_immut_iter_init(iter->enumerator, castle_ct_modlist_iter_next_node, iter);

    /* Finally, sort the data so we can return sorted entries to the caller. */
    castle_ct_modlist_iter_mergesort(iter);

    /* Good state before we accept requests. */
    iter->err = 0;
    iter->next_item = 0;
}

/**
 * Insert a component iterator (with cached (k,v)) into the RB-tree.
 *
 * In case of duplicate (k,v) tuples maintain only store the most recent iterator in the tree.
 * All other iterators in the list rooted at that newest iterator.same_kv_head.
 *
 * Each_skip callback will be used to notify the client about older CVTs with matching (k,v).
 * But only if they are non-counter types.
 *
 * @param iter [in]         Merged iterator that the RB tree belongs to
 * @param comp_iter [in]    Component iterator that the new kv pair belongs to
 *
 * @also _each_skip() callbacks
 */
static void castle_ct_merged_iter_rbtree_insert(c_merged_iter_t *iter,
                                                struct component_iterator *comp_iter)
{
    struct rb_root *root = &iter->rb_root;
    struct rb_node **p = &root->rb_node;
    struct rb_node *parent = NULL;
    struct rb_node *node = &comp_iter->rb_node;
    int nr_cmps = 0;

    /* Init the same_kv_head when the iterator is getting added to the tree.
       This guaratees that its going to be read for use, when we detect (k,v) collision(s). */
    INIT_LIST_HEAD(&comp_iter->same_kv_head);

    /* Go until end of the tree. */
    while (*p)
    {
        struct component_iterator *c_iter, *dup_iter = NULL, *new_iter = NULL;
        int kv_cmp;

        parent = *p;
        c_iter = rb_entry(parent, struct component_iterator, rb_node);

        BUG_ON(!c_iter->cached);
        BUG_ON(c_iter == comp_iter);

        /* Compare the entry in RB Tree with new entry. */
        kv_cmp = castle_kv_compare(iter->btree,
                                   comp_iter->cached_entry.k,
                                   comp_iter->cached_entry.v,
                                   c_iter->cached_entry.k,
                                   c_iter->cached_entry.v);
        nr_cmps++;

        /* New (key,version) is smaller than key in tree.  Traverse left. */
        if (kv_cmp < 0)
            p = &(*p)->rb_left;
        /* New (key,version) is bigger than key in tree.  Traverse right. */
        else if (kv_cmp > 0)
            p = &(*p)->rb_right;
        else
        {
            /* Both (key,version) pairs are equal. Here is how we deal with those:
             *
             * 1. Put the newest iterator (i.e. from the latest tree) in the rb_tree.
             *    Older iterators with same (k,v) will _not_ be accesible in the tree directly.
             * 2. Construct a list of iterators which cache same (k,v), rooted at the newest
             *    component iterator.same_kv_head. This list may contain both counter and
             *    non-counter CVTs.
             * 3. Call each_skip (if registered) for all iterators, except of the newest
             *    one (i.e. from the latest tree).
             *
             * Component iterators are threaded onto a list headed by the newset iterator
             * (same_kv list). This list is later used to construct responce for
             * counters. Otherwise its thrown away.
             *
             * Component iterator pointers are used to determine the recency order.
             */
            if(c_iter > comp_iter)
            {
                /* The current iterator is more recent than the one in the tree.
                   (Re-)splice the same_kv list onto current iterator head. */
                list_splice_init(&c_iter->same_kv_head, &comp_iter->same_kv_head);
                /* Replace the rb node. */
                rb_replace_node(&c_iter->rb_node, &comp_iter->rb_node, root);
                /* Add the older iterator onto same_kv list. */
                list_add(&c_iter->same_kv_list, &comp_iter->same_kv_head);

                /* Save which iterator is newer and older. */
                dup_iter = c_iter;
                new_iter = comp_iter;

            } else
            {
                /* The current iterator is less recent that the one in the tree.
                   Add the current iterator to the same_kv list (rooted at the most
                   recent iterator). */
                list_add(&comp_iter->same_kv_list, &c_iter->same_kv_head);

                /* Save which iterator is newer and older. */
                dup_iter = comp_iter;
                new_iter = c_iter;
            }

            /* (Conditionally), call the callback. */
            debug("Duplicate entry found, CVT type=%d.\n", dup_iter->cached_entry.cvt.type);
            if (iter->each_skip)
                iter->each_skip(iter, dup_iter, new_iter);

            /* The rb_tree and same_kv list have all been updated, return now. */
            return;
        }
    }

    /* Link the node to tree. */
    rb_link_node(node, parent, p);
    /* Set color and inturn balance the tree. */
    rb_insert_color(node, root);
}

static inline struct component_iterator* castle_ct_merged_iter_rbtree_min_get(
                                                                      c_merged_iter_t *iter)
{
    struct rb_root *root = &iter->rb_root;
    struct rb_node *min;

    /* Get the first element in the sorted order(minimum). */
    min = rb_first(root);
    if(!min)
        return NULL;

    /* Return the iterator. */
    return rb_entry(min, struct component_iterator, rb_node);
}

/**
 * Removes the iterator specified from the rbtree.
 */
static inline void castle_ct_merged_iter_rbtree_del(c_merged_iter_t *iter,
                                                    struct component_iterator *comp_iter)
{
    /* Erase the element from tree. */
    rb_erase(&comp_iter->rb_node, &iter->rb_root);
}

/**
 * Determines and removes+returns the component iterator which provided the smallest
 * key.
 */
static struct component_iterator* castle_ct_merge_iter_rbtree_min_del(c_merged_iter_t *iter)
{
    struct component_iterator *comp_iter;

    /* Get the smallest iter from the tree. */
    comp_iter = castle_ct_merged_iter_rbtree_min_get(iter);
    BUG_ON(!comp_iter);

    /* Delete from the rbtree. */
    castle_ct_merged_iter_rbtree_del(iter, comp_iter);

    /* Return the iterator. */
    return comp_iter;
}

void castle_iterator_sync_end_io(void *iter, int err)
{
    c_async_iterator_t          *async_iter = (c_async_iterator_t *)iter;
    struct castle_iterator_type *iter_type = async_iter->iter_type;

    /* Don't expect any errors. */
    BUG_ON(err);

    /* prep_next() should succeed now. */
    BUG_ON(iter_type->prep_next(iter));

    /* Mark the task as completed and invoke the thread. */
    complete((struct completion *)async_iter->private);
}

int castle_iterator_has_next_sync(struct castle_iterator_type *iter_type, void *iter)
{
    c_async_iterator_t *async_iter = (c_async_iterator_t *)iter;
    struct completion completion;

    /* Make sure the iterator type matches the type in iterator. */
    BUG_ON(async_iter->iter_type != iter_type);

    /* Set end_io(). It will overwrite any default callback. */
    /* FIXME: This might lead to a problem, if we mix has_next_sync() and prep_next() calls. */
    async_iter->end_io = castle_iterator_sync_end_io;

    /* Initialise completion structure. Need to wait for the iterator to fill its buffer. */
    init_completion(&completion);
    async_iter->private = (void *)&completion;

    /* If prep_next() fails, then wait for it to fill the buffer. */
    if (!iter_type->prep_next(iter))
        wait_for_completion(&completion);

    /* Return with has_next() response. */
    return iter_type->has_next(iter);
}

static int castle_ct_merged_iter_prep_next(c_merged_iter_t *iter)
{
    int i;
    struct component_iterator *comp_iter;

    debug_iter("%s:%p\n", __FUNCTION__, iter);

    /* Reset merged version iterator stats. */
    memset(&iter->stats, 0, sizeof(cv_nonatomic_stats_t));

    debug_iter("No of comp_iters: %u\n", iter->nr_iters);
    for (i = 0; i < iter->nr_iters; i++)
    {
        comp_iter = iter->iterators + i;

        debug_iter("%s:%p:%d\n", __FUNCTION__, iter, i);
        /* Replenish the cache */
        if(!comp_iter->completed && !comp_iter->cached)
        {
            debug("Reading next entry for iterator: %d.\n", i);
            if (!comp_iter->iterator_type->prep_next(comp_iter->iterator))
            {
                debug_iter("%s:%p:%p:%d - schedule\n", __FUNCTION__, iter, comp_iter->iterator, i);
                iter->iter_running = 1;
                return 0;
            }
            if (comp_iter->iterator_type->has_next(comp_iter->iterator))
            {
                comp_iter->iterator_type->next(comp_iter->iterator,
                                               &comp_iter->cached_entry.k,
                                               &comp_iter->cached_entry.v,
                                               &comp_iter->cached_entry.cvt);
                comp_iter->cached = 1;
                iter->src_items_completed++;
                debug_iter("%s:%p:%d - cached\n", __FUNCTION__, iter, i);
                /* Insert the kv pair into RB tree. */
                /* It is possible that. this call could delete kv pairs of the component
                 * iterators (which is fine, as we go through that component iterator anyway)
                 * coming after this or it could delete the current kv pair itself. */
                castle_ct_merged_iter_rbtree_insert(iter, comp_iter);
            }
            else
            {
                debug_iter("%s:%p:%d - nothing left\n", __FUNCTION__, iter, i);
                comp_iter->completed = 1;
                iter->non_empty_cnt--;
                debug("A component iterator run out of stuff, we are left with"
                      "%d iterators.\n",
                      iter->non_empty_cnt);
            }
        }
    }

    return 1;
}

static void castle_ct_merged_iter_register_cb(c_merged_iter_t *iter,
                                              castle_iterator_end_io_t cb,
                                              void *data)
{
    iter->async_iter.end_io  = cb;
    iter->async_iter.private = data;
}

static void castle_ct_merged_iter_end_io(void *rq_iter, int err)
{
    c_merged_iter_t *iter = ((c_rq_iter_t *) rq_iter)->async_iter.private;

    debug_iter("%s:%p\n", __FUNCTION__, iter);
    if (castle_ct_merged_iter_prep_next(iter))
    {
        debug_iter("%s:%p - Done\n", __FUNCTION__, iter);

        iter->iter_running = 0;
        iter->async_iter.end_io(iter, 0);

        return;
    }
}

static int castle_ct_merged_iter_has_next(c_merged_iter_t *iter)
{
    debug_iter("%s:%p\n", __FUNCTION__, iter);

    /* Iterator shouldn't be running(waiting for prep_next to complete) now. */
    BUG_ON(iter->iter_running);

    debug("Merged iterator has next, err=%d, non_empty_cnt=%d\n",
            iter->err, iter->non_empty_cnt);

    return (!iter->err && (iter->non_empty_cnt > 0));
}

/**
 * Consumes the current key from the component iterator provided, and from all
 * all iterators in same_kv list.
 * Additionally, skip can be performed in all the iterators mentioned, by setting
 * 'skip' argument to true, and providing the skip key.
 */
static void castle_ct_merged_iter_consume(struct component_iterator *iter,
                                          int skip,
                                          void *skip_key)
{
    struct component_iterator *other_iter;
    struct list_head *l;

    /* Clear cached flag for each iterator in the same_kv list. */
    list_for_each(l, &iter->same_kv_head)
    {
        other_iter = list_entry(l, struct component_iterator, same_kv_list);
        /* Each of the component iterators should have something cached. */
        BUG_ON(!other_iter->cached);
        /* Head should be newest. */
        BUG_ON(iter > other_iter);
        if(skip)
            other_iter->iterator_type->skip(other_iter->iterator, skip_key);
        other_iter->cached = 0;
    }

    /* Skip and clear cached flag for the min iterator too. */
    if(skip)
        iter->iterator_type->skip(iter->iterator, skip_key);
    iter->cached = 0;
}

/**
 * Accumulates and returns counter (wrapped into a cvt) from the component iterator
 * specified and older iterators present in the same_kv list.
 *
 * The function uses O(n*log(n)) sort on same_kv list.
 */
static c_val_tup_t castle_ct_merged_iter_counter_reduce(struct component_iterator *iter)
{
    c_val_tup_t accumulator;
    struct list_head *l, *t;
    struct rb_root rb_root;
    struct rb_node **p, *parent, *rb_entry;

    /* We expecting for the list head of the same_kv list to be a counter (at least). */
    BUG_ON(!CVT_ANY_COUNTER(iter->cached_entry.cvt));

    /* Prepare the accumulator. */
    CVT_COUNTER_LOCAL_ADD_INIT(accumulator, 0);

    /* Deal with the list head. */
    if(castle_counter_simple_reduce(&accumulator, iter->cached_entry.cvt))
        return accumulator;

    /* If the list same_kv list is emtpy, return too. */
    if(list_empty(&iter->same_kv_head))
        return accumulator;

    /* The same_kv list isn't sorted. Insert sort it. */
    rb_root = RB_ROOT;
    list_for_each_safe(l, t, &iter->same_kv_head)
    {
        struct component_iterator *current_iter;

        /* Work ot the iterator structure pointer, and delete list entry. */
        current_iter = list_entry(l, struct component_iterator, same_kv_list);
        list_del(l);

        /* Insert into rb tree. */
        parent = NULL;
        p = &rb_root.rb_node;
        while(*p)
        {
            struct component_iterator *tree_iter;

            parent = *p;
            tree_iter = rb_entry(parent, struct component_iterator, rb_node);
            /* We never expect to see the same iterator twice. */
            BUG_ON(tree_iter == current_iter);
            if(tree_iter > current_iter)
                p = &(*p)->rb_left;
            else
                p = &(*p)->rb_right;
        }
        rb_link_node(&current_iter->rb_node, parent, p);
        rb_insert_color(&current_iter->rb_node, &rb_root);
    }

    /* Now accumulate the results one iterator at the time. */
    rb_entry = rb_first(&rb_root);
    /* There was a check for emtpy same_kv list, so there should be something in the tree. */
    BUG_ON(!rb_entry);
    do {
        iter = rb_entry(rb_entry, struct component_iterator, rb_node);
        /* Continue iterating until a terminating cvt (e.g. a counter set) is found. */
        if(castle_counter_simple_reduce(&accumulator, iter->cached_entry.cvt))
            return accumulator;
    } while((rb_entry = rb_next(&iter->rb_node)));

    /* Never reached a terminating cvt, assume implicit set 0. */
    return accumulator;
}

static void castle_ct_merged_iter_next(c_merged_iter_t *iter,
                                       void **key_p,
                                       c_ver_t *version_p,
                                       c_val_tup_t *cvt_p)
{
    struct component_iterator *comp_iter;
    c_val_tup_t cvt;

    debug_iter("%s:%p\n", __FUNCTION__, iter);
    debug("Merged iterator next.\n");

    /* Iterator shouldn't be running(waiting for prep_next to complete) now. */
    BUG_ON(iter->iter_running);

    /* Get the smallest kv pair from RB tree. */
    comp_iter = castle_ct_merge_iter_rbtree_min_del(iter);
    debug("Smallest entry is from iterator: %p.\n", comp_iter);

    /* Consume (clear cached flags) from the component iterators. */
    castle_ct_merged_iter_consume(comp_iter, 0 /* don't skip. */, NULL);

    /* Work out the counter value (handle the case where the iterator contains a counter.
       NOTE: this destroys same_kv list. Don't use it after this point. */
    if(CVT_ANY_COUNTER(comp_iter->cached_entry.cvt))
        cvt = castle_ct_merged_iter_counter_reduce(comp_iter);
    else
        cvt = comp_iter->cached_entry.cvt;

    /* Return the smallest entry */
    if(key_p)     *key_p     = comp_iter->cached_entry.k;
    if(version_p) *version_p = comp_iter->cached_entry.v;
    if(cvt_p)     *cvt_p     = cvt;
}

static void castle_ct_merged_iter_skip(c_merged_iter_t *iter,
                                       void *key)
{
    struct component_iterator *comp_iter;
    int i;

    debug_iter("%s:%p\n", __FUNCTION__, iter);

    /* Iterator shouldn't be running(waiting for prep_next to complete) now. */
    BUG_ON(iter->iter_running);

    /* If we are skipping in the merged iterator, we are not able to inform the client
       about all duplicate entries. Check that the client isn't asking for it. */
    BUG_ON(iter->each_skip);

    /* Call skip on lower level iterators, if the iterator is not cached. */
    for(i=0; i<iter->nr_iters; i++)
    {
        comp_iter = iter->iterators + i;

        BUG_ON(!comp_iter->iterator_type->skip);
        if (!comp_iter->completed && !comp_iter->cached)
            comp_iter->iterator_type->skip(comp_iter->iterator, key);
    }

    /* Go through the rbtree, and extract all the keys smaller than the key we
       are skipping to. */
    while((comp_iter = castle_ct_merged_iter_rbtree_min_get(iter)) &&
          (iter->btree->key_compare(comp_iter->cached_entry.k, key) < 0))
    {
        /* Since the iterator is in the rb_tree, it must be cached. */
        BUG_ON(!comp_iter->cached);
        /* Delete from the rbtree. */
        castle_ct_merged_iter_rbtree_del(iter, comp_iter);
        /* Consume (clear cached flags & skip) from all component iterators
           on the same_kv list. */
        castle_ct_merged_iter_consume(comp_iter, 1 /* skip. */, key);
    }
}

static void castle_ct_merged_iter_cancel(c_merged_iter_t *iter)
{
    /* FIXME: Handle gracefully. It is possible for range queries to cancel iterator, when it
     * is running. But, usually prep_next completes before expire time. Still possible not to
     * complete.We need to wait for iterator to complete with prep_next. But, make sure
     * it is not racing. */
    /* Iterator shouldn't be running(waiting for prep_next to complete) now. */
    BUG_ON(iter->iter_running);

    if (iter->iterators)
        castle_kfree(iter->iterators);
}

struct castle_iterator_type castle_ct_merged_iter = {
    .register_cb = (castle_iterator_register_cb_t)castle_ct_merged_iter_register_cb,
    .prep_next   = (castle_iterator_prep_next_t)  castle_ct_merged_iter_prep_next,
    .has_next    = (castle_iterator_has_next_t)   castle_ct_merged_iter_has_next,
    .next        = (castle_iterator_next_t)       castle_ct_merged_iter_next,
    .skip        = (castle_iterator_skip_t)       castle_ct_merged_iter_skip,
    .cancel      = (castle_iterator_cancel_t)     castle_ct_merged_iter_cancel,
};

/**
 * Initialise a meta iterator from a number of component iterators.
 *
 * Once initialised the iterator will return the smallest entry from any of the
 * component trees when castle_ct_merged_iter_next() is called.
 *
 * This iterator is used for merges and range queries (non-exhaustive list).
 */
static void castle_ct_merged_iter_init(c_merged_iter_t *iter,
                                       void **iterators,
                                       struct castle_iterator_type **iterator_types,
                                       castle_merged_iterator_each_skip each_skip)
{
    int i;

    debug("Initing merged iterator for %d component iterators.\n", iter->nr_iters);
    BUG_ON(iter->nr_iters <= 0);
    BUG_ON(!iter->btree);
    iter->err = 0;
    iter->src_items_completed = 0;
    iter->async_iter.end_io = NULL;
    iter->async_iter.iter_type = &castle_ct_merged_iter;
    iter->iter_running = 0;
    iter->rb_root = RB_ROOT;
    iter->iterators = castle_malloc(iter->nr_iters * sizeof(struct component_iterator), GFP_KERNEL);
    if(!iter->iterators)
    {
        castle_printk(LOG_WARN, "Failed to allocate memory for merged iterator.\n");
        iter->err = -ENOMEM;
        return;
    }
    iter->each_skip = each_skip;
    /* Memory allocated for the iterators array, init the state.
       Assume that all iterators have something in them, and let the has_next_check()
       handle the opposite. */
    iter->non_empty_cnt = iter->nr_iters;
    for(i=0; i<iter->nr_iters; i++)
    {
        struct component_iterator *comp_iter = iter->iterators + i;

        comp_iter->iterator      = iterators[i];
        comp_iter->iterator_type = iterator_types[i];
        comp_iter->cached        = 0;
        comp_iter->completed     = 0;

        if (comp_iter->iterator_type->register_cb)
            comp_iter->iterator_type->register_cb(comp_iter->iterator,
                                                  castle_ct_merged_iter_end_io,
                                                  (void *)iter);
    }
}


#ifdef DEBUG
c_modlist_iter_t test_iter1;
c_modlist_iter_t test_iter2;
c_merged_iter_t  test_miter;
static USED void castle_ct_sort(struct castle_component_tree *ct1,
                                struct castle_component_tree *ct2)
{
    c_ver_t version;
    void *key;
    c_val_tup_t cvt;
    int i=0;
    void *iters[2];
    struct castle_iterator_type *iter_types[2];

    castle_printk(LOG_DEVEL, "Number of items in the ct1: %lld, ct2=%lld\n",
            atomic64_read(&ct1->item_count),
            atomic64_read(&ct2->item_count));

    test_iter1.tree = ct1;
    castle_ct_modlist_iter_init(&test_iter1);
    test_iter2.tree = ct2;
    castle_ct_modlist_iter_init(&test_iter2);

#if 0
    while(castle_ct_modlist_iter_has_next(&test_iter))
    {
        castle_ct_modlist_iter_next(&test_iter, &key, &version, &cep);
        debug("Sorted: %d: k=%p, version=%d, cep=(0x%x, 0x%x)\n",
                i, key, version, cep.ext_id, cep.offset);
        debug("Dereferencing first 4 bytes of the key (should be length)=0x%x.\n",
                *((uint32_t *)key));
        i++;
    }
#endif
    test_miter.nr_iters = 2;
    test_miter.btree = test_iter1.btree;
    iters[0] = &test_iter1;
    iters[1] = &test_iter2;
    iter_types[0] = &castle_ct_modlist_iter;
    iter_types[1] = &castle_ct_modlist_iter;
    castle_ct_merged_iter_init(&test_miter,
                               iters,
                               iter_types,
                               NULL);
    debug("=============== SORTED ================\n");
    while(castle_iterator_has_next_sync(&castle_ct_merged_iter, &test_miter))
    {
        castle_ct_merged_iter_next(&test_miter, &key, &version, &cvt);
        debug("Sorted: %d: k=%p, version=%d, cep=" cep_fmt_str_nl,
                i, key, version, cep2str(cvt.cep));
        debug("Dereferencing first 4 bytes of the key (should be length)=0x%x.\n",
                *((uint32_t *)key));
        i++;
    }
}
#endif

/* Has next, next and skip only need to call the corresponding functions on
   the underlying merged iterator */

static void castle_da_rq_iter_register_cb(c_da_rq_iter_t *iter,
                                          castle_iterator_end_io_t cb,
                                          void *data)
{
    iter->async_iter.end_io  = cb;
    iter->async_iter.private = data;
}

static int castle_da_rq_iter_prep_next(c_da_rq_iter_t *iter)
{
    return castle_ct_merged_iter_prep_next(&iter->merged_iter);
}


static int castle_da_rq_iter_has_next(c_da_rq_iter_t *iter)
{
    return castle_ct_merged_iter_has_next(&iter->merged_iter);
}

static void castle_da_rq_iter_end_io(void *merged_iter, int err)
{
    c_da_rq_iter_t *iter = ((c_merged_iter_t *)merged_iter)->async_iter.private;

    if (castle_da_rq_iter_prep_next(iter))
    {
        iter->async_iter.end_io(iter, 0);
        return;
    }
    else
        BUG();
}

static void castle_da_rq_iter_next(c_da_rq_iter_t *iter,
                                   void **key_p,
                                   c_ver_t *version_p,
                                   c_val_tup_t *cvt_p)
{
    castle_ct_merged_iter_next(&iter->merged_iter, key_p, version_p, cvt_p);
}

static void castle_da_rq_iter_skip(c_da_rq_iter_t *iter, void *key)
{
    castle_ct_merged_iter_skip(&iter->merged_iter, key);
}

void castle_da_rq_iter_cancel(c_da_rq_iter_t *iter)
{
    int i;

    castle_ct_merged_iter_cancel(&iter->merged_iter);
    for(i=0; i<iter->nr_cts; i++)
    {
        struct ct_rq *ct_rq = iter->ct_rqs + i;
        castle_rq_iter_cancel(&ct_rq->ct_rq_iter);
        castle_ct_put(ct_rq->ct, 0);
    }
    castle_kfree(iter->ct_rqs);
}

/**
 * Range query iterator initialiser.
 *
 * Implemented as a merged iterator of CTs at every level of the doubling array.
 */
void castle_da_rq_iter_init(c_da_rq_iter_t *iter,
                            c_ver_t version,
                            c_da_t da_id,
                            void *start_key,
                            void *end_key)
{
    void **iters;
    struct castle_iterator_type **iter_types;
    struct castle_double_array *da;
    struct list_head *l;
    int i, j;

    da = castle_da_hash_get(da_id);
    BUG_ON(!da);
    BUG_ON(!castle_version_is_ancestor(da->root_version, version));
again:
    /* Try to allocate the right amount of memory, but remember that nr_trees
       may change, because we are not holding the da lock (cannot kmalloc holding
       a spinlock). */
    iter->nr_cts = da->nr_trees;
    iter->err    = 0;
    iter->async_iter.end_io = NULL;
    iter->ct_rqs = castle_zalloc(iter->nr_cts * sizeof(struct ct_rq), GFP_KERNEL);
    iters        = castle_malloc(iter->nr_cts * sizeof(void *), GFP_KERNEL);
    iter_types   = castle_malloc(iter->nr_cts * sizeof(struct castle_iterator_type *), GFP_KERNEL);
    if(!iter->ct_rqs || !iters || !iter_types)
    {
        if(iter->ct_rqs)
            castle_kfree(iter->ct_rqs);
        if(iters)
            castle_kfree(iters);
        if(iter_types)
            castle_kfree(iter_types);
        iter->err = -ENOMEM;
        return;
    }

    read_lock(&da->lock);
    /* Check the number of trees under lock. Retry again if # changed. */
    if(iter->nr_cts != da->nr_trees)
    {
        read_unlock(&da->lock);
        castle_printk(LOG_WARN,
                "Warning. Untested. # of cts changed while allocating memory for rq.\n");
        castle_kfree(iter->ct_rqs);
        castle_kfree(iters);
        castle_kfree(iter_types);
        goto again;
    }
    /* Get refs to all the component trees, and release the lock */
    j=0;
    for(i=0; i<MAX_DA_LEVEL; i++)
    {
        list_for_each(l, &da->levels[i].trees)
        {
            struct castle_component_tree *ct;

            BUG_ON(j >= iter->nr_cts);
            ct = list_entry(l, struct castle_component_tree, da_list);
            iter->ct_rqs[j].ct = ct;
            castle_ct_get(ct, 0);
            BUG_ON((castle_btree_type_get(ct->btree_type)->magic != RW_VLBA_TREE_TYPE) &&
                   (castle_btree_type_get(ct->btree_type)->magic != RO_VLBA_TREE_TYPE));
            j++;
        }
    }
    read_unlock(&da->lock);
    BUG_ON(j != iter->nr_cts);

    /* Initialise range queries for individual cts */
    /* @TODO: Better to re-organize the code, such that these iterators belong to
     * merged iterator. Easy to manage resources - Talk to Gregor */
    for(i=0; i<iter->nr_cts; i++)
    {
        struct ct_rq *ct_rq = iter->ct_rqs + i;

        castle_rq_iter_init(&ct_rq->ct_rq_iter,
                            version,
                            ct_rq->ct,
                            start_key,
                            end_key);
        /* @TODO: handle errors! Don't know how to destroy ct_rq_iter ATM. */
        BUG_ON(ct_rq->ct_rq_iter.err);
        iters[i]        = &ct_rq->ct_rq_iter;
        iter_types[i]   = &castle_rq_iter;
    }

    /* Iterators have been initialised, now initialise the merged iterator */
    iter->merged_iter.nr_iters = iter->nr_cts;
    iter->merged_iter.btree    = castle_btree_type_get(RO_VLBA_TREE_TYPE);
    castle_ct_merged_iter_init(&iter->merged_iter,
                                iters,
                                iter_types,
                                NULL);
    castle_ct_merged_iter_register_cb(&iter->merged_iter,
                                      castle_da_rq_iter_end_io,
                                      iter);
    castle_kfree(iters);
    castle_kfree(iter_types);
}

struct castle_iterator_type castle_da_rq_iter = {
    .register_cb= (castle_iterator_register_cb_t)castle_da_rq_iter_register_cb,
    .prep_next  = (castle_iterator_prep_next_t)  castle_da_rq_iter_prep_next,
    .has_next   = (castle_iterator_has_next_t)   castle_da_rq_iter_has_next,
    .next       = (castle_iterator_next_t)       castle_da_rq_iter_next,
    .skip       = (castle_iterator_skip_t)       castle_da_rq_iter_skip,
    .cancel     = (castle_iterator_cancel_t)     castle_da_rq_iter_cancel,
};

/**********************************************************************************************/
/* Merges */
struct castle_da_merge {
    struct castle_double_array   *da;
    struct castle_btree_type     *out_btree;
    int                           level;
    int                           nr_trees;     /**< num of component trees being merged */
    struct castle_component_tree **in_trees;    /**< array of component trees to be merged */
    struct castle_component_tree *out_tree;
    void                         **iters;       /**< iterators for component trees */
    c_merged_iter_t              *merged_iter;
    int                           root_depth;
    c2_block_t                   *last_leaf_node_c2b; /**< Previous node c2b at depth 0. */
    void                         *last_key;           /**< last_key added to
                                                           out tree at depth 0. */
    int                           completing;
    uint64_t                      nr_entries;
    uint64_t                      large_chunks;
    int                           is_new_key;      /**< Is the current key different
                                                        from last key added to out_tree. */
    struct castle_da_merge_level {
        /* Node we are currently generating, and book-keeping variables about the node. */
        c2_block_t               *node_c2b;
        void                     *last_key;
        int                       next_idx;
        int                       valid_end_idx;
        c_ver_t                   valid_version;
    } levels[MAX_BTREE_DEPTH];

    /* Deamortization variables */
    struct work_struct            work;
    int                           budget_cons_rate;
    int                           budget_cons_units;
    int                           leafs_on_ssds;        /**< Are leaf btree nodes stored on SSD.*/
    int                           internals_on_ssds;    /**< Are internal nodes stored on SSD.  */
    struct list_head              new_large_objs;       /**< Large objects added since last
                                                             checkpoint (for merge serdes).     */
    struct castle_version_states  version_states;       /**< Merged version states.             */
    struct castle_version_delete_state snapshot_delete; /**< Snapshot delete state.             */

#ifdef CASTLE_PERF_DEBUG
    u64                           get_c2b_ns;           /**< ns in castle_cache_block_get()     */
    u64                           merged_iter_next_ns;
    u64                           da_medium_obj_copy_ns;
    u64                           nodes_complete_ns;
    u64                           budget_consume_ns;
    u64                           progress_update_ns;
    u64                           merged_iter_next_hasnext_ns;
    u64                           merged_iter_next_compare_ns;
#endif
#ifdef CASTLE_DEBUG
    uint8_t                       is_recursion;
#endif
    uint32_t                      skipped_count;        /**< Count of entries from deleted
                                                             versions.                          */
};

#define MAX_IOS             (1000) /* Arbitrary constants */
/* @TODO: Merges are now effectively always full throughput, because MIN is set high. */
#define MIN_BUDGET_DELTA    (1000000)
#define MAX_BUDGET          (1000000)
#define BIG_MERGE           (0)

/************************************/
/* Marge rate control functionality */
static void castle_da_merge_budget_consume(struct castle_da_merge *merge)
{
    struct castle_double_array *da;

    BUG_ON(in_atomic());
    if(castle_da_exiting)
        return;

    /* Check if we need to consume some merge budget */
    merge->budget_cons_units++;
    if(merge->budget_cons_units < merge->budget_cons_rate)
        return;

    da = merge->da;
    /* Consume a single unit of budget. */
    while(atomic_dec_return(&da->merge_budget) < 0)
    {
        /* We failed to get merge budget, readd the unit, and wait for some to appear. */
        atomic_inc(&da->merge_budget);
        /* Extra warning message, which we shouldn't see. Increase the MIN, if we do. */
        castle_printk(LOG_WARN,
                "WARNING, possible error: Merges running fast, but not throttling.\n");
        atomic_add(MIN_BUDGET_DELTA, &da->merge_budget);
        return;
        //wait_event(da->merge_budget_waitq, atomic_read(&da->merge_budget) > 0);
    }
}

#define REPLENISH_FREQUENCY (10)        /* Replenish budgets every 100ms. */
static int castle_da_merge_budget_replenish(struct castle_double_array *da, void *unused)
{
    int ios = atomic_read(&da->epoch_ios);
    int budget_delta = 0, merge_budget;

    atomic_set(&da->epoch_ios, 0);
    debug("Merge replenish, number of ios in last second=%d.\n", ios);
    if(ios < MAX_IOS)
        budget_delta = MAX_IOS - ios;
    if(budget_delta < MIN_BUDGET_DELTA)
        budget_delta = MIN_BUDGET_DELTA;
    BUG_ON(budget_delta <= 0);
    merge_budget = atomic_add_return(budget_delta, &da->merge_budget);
    if(merge_budget > MAX_BUDGET)
        atomic_sub(merge_budget - MAX_BUDGET, &da->merge_budget);
    wake_up(&da->merge_budget_waitq);

    return 0;
}

static void castle_merge_budgets_replenish(void *unused)
{
   castle_da_hash_iterate(castle_da_merge_budget_replenish, NULL);
}

/**
 * Replenish ios_budget from ios_rate and schedule IO wait queue kicks.
 *
 * NOTE: This might remove rather than replenish the budget, depending on
 * whether inserts are enabled/disabled(/throttled) on the DA.
 *
 * ios_rate is used to throttle inserts into the btree.  It is used as an
 * initialiser for ios_budget.
 *
 * This function is expected to be called periodically (e.g. via a timer) with
 * values of ios_rate that maintain a sustainable flow of inserts.
 *
 * - Update ios_budget
 * - Schedule queue kicks for all IO wait queues that have elements
 *
 * @also struct castle_double_array
 * @also castle_da_queue_kick()
 */
static int castle_da_ios_budget_replenish(struct castle_double_array *da, void *unused)
{
    int i;

    atomic_set(&da->ios_budget, da->ios_rate);

    if (da->ios_rate || castle_fs_exiting || castle_da_no_disk_space(da))
    {
        /* We just replenished the DA's ios_budget.
         *
         * We need to kick all of the write IO wait queues.  In the current
         * context we hold spin_lock_irq(&castle_da_hash_lock) so schedule this
         * work so we can drop the lock and return immediately. */
        for (i = 0; i < castle_double_array_request_cpus(); i++)
        {
            struct castle_da_io_wait_queue *wq = &da->ios_waiting[i];

            spin_lock(&wq->lock);
            if (!list_empty(&wq->list))
                /* wq->work is initialised as castle_da_queue_kick(). */
                queue_work_on(request_cpus.cpus[i], castle_wqs[0], &wq->work);
            spin_unlock(&wq->lock);
        }
    }

    return 0;
}

/**
 * Replenish ios_budget for all DAs on the system.
 */
static void castle_ios_budgets_replenish(void *unused)
{
   castle_da_hash_iterate(castle_da_ios_budget_replenish, NULL);
}

static inline void castle_da_merge_budget_io_end(struct castle_double_array *da)
{
    atomic_inc(&da->epoch_ios);
}

static DECLARE_WORK(merge_budgets_replenish_work, castle_merge_budgets_replenish, NULL);
static DECLARE_WORK(ios_budgets_replenish_work, castle_ios_budgets_replenish, NULL);

/************************************/
/* Throttling timers */
static struct timer_list throttle_timer;
static void castle_throttle_timer_fire(unsigned long first)
{
    schedule_work(&merge_budgets_replenish_work);
    schedule_work(&ios_budgets_replenish_work);
    /* Reschedule ourselves */
    setup_timer(&throttle_timer, castle_throttle_timer_fire, 0);
    mod_timer(&throttle_timer, jiffies + HZ/REPLENISH_FREQUENCY);
}

/************************************/
/* Actual merges */
static void castle_da_iterator_destroy(struct castle_component_tree *tree,
                                       void *iter)
{
    if(!iter)
        return;

    if(tree->dynamic)
    {
        /* For dynamic trees we are using modlist iterator. */
        castle_ct_modlist_iter_free(iter);
        castle_kfree(iter);
    } else
    {
        /* For static trees, we are using immut iterator. */
        /* @TODO: do we need to do better resource release here? */
        castle_ct_immut_iter_cancel(iter);
        castle_kfree(iter);
    }
}

/**
 * Allocate/initialise correct iterator type for level of merge.
 *
 * - Allocate a castle_ct_modlist_iter for T1 merges
 * - Allocate a castle_ct_immut_iter for all higher level merges
 */
static void castle_da_iterator_create(struct castle_da_merge *merge,
                                      struct castle_component_tree *tree,
                                      void **iter_p)
{
    if (tree->dynamic)
    {
        c_modlist_iter_t *iter = castle_malloc(sizeof(c_modlist_iter_t), GFP_KERNEL);
        BUG_ON(merge->da->levels[merge->level].merge.serdes.des); /* we should only serialise merges
                                                                     with immut in_trees */
        if (!iter)
            return;
        iter->tree = tree;
        iter->merge = merge;
        if (tree->level == 1)
            castle_trace_da_merge(TRACE_START, TRACE_DA_MERGE_MODLIST_ITER_INIT_ID,
                    merge->da->id, tree->level, 0, 0);
        castle_ct_modlist_iter_init(iter);
        if (tree->level == 1)
            castle_trace_da_merge(TRACE_END, TRACE_DA_MERGE_MODLIST_ITER_INIT_ID,
                    merge->da->id, tree->level, 0, 0);
        if (iter->err)
        {
            castle_kfree(iter);
            return;
        }
        /* Success */
        *iter_p = iter;
    }
    else
    {
        c_immut_iter_t *iter = castle_malloc(sizeof(c_immut_iter_t), GFP_KERNEL);
        if (!iter)
            return;
        iter->tree = tree;
        castle_ct_immut_iter_init(iter, NULL, NULL);
        /* @TODO: after init errors? */
        *iter_p = iter;
    }
}

static struct castle_iterator_type* castle_da_iter_type_get(struct castle_component_tree *ct)
{
    if(ct->dynamic)
        return &castle_ct_modlist_iter;
    else
        return &castle_ct_immut_iter;
}

/**
 * each_skip() callback for merge merged iterator.
 *
 * Called whenever a duplicate entry gets skipped during merges.
 *
 * Store any stat changes in merged iterator stats structure.  These are to
 * be handled via the merge process (castle_da_merge_unit_do()).
 */
static void castle_da_each_skip(c_merged_iter_t *iter,
                                struct component_iterator *dup_iter,
                                struct component_iterator *new_iter)
{
    BUG_ON(!dup_iter->cached);
    BUG_ON(!new_iter->cached);
    BUG_ON(dup_iter->cached_entry.v != new_iter->cached_entry.v);

    if (CVT_LARGE_OBJECT(dup_iter->cached_entry.cvt))
    {
        /* No need to remove this large object, it gets deleted part of Tree
         * deletion. */
    }

    /* Update per-version statistics. */
    if (!CVT_TOMBSTONE(dup_iter->cached_entry.cvt))
    {
        iter->stats.keys--;

        if (CVT_TOMBSTONE(new_iter->cached_entry.cvt))
            iter->stats.tombstone_deletes++;
        else
            iter->stats.key_replaces++;
    }
    else
    {
        iter->stats.tombstones--;

        /* If the new entry is also a tombstone, don't bump the tombstone delete
         * counter: deleting something that is already deleted makes no sense. */

        /* If the new entry is a key, don't bump the key replaces counter:
         * merging a newer key with an older tombstone is logically the same as
         * inserting a new key. */
    }
}

/**
 * Extracts two oldest component trees from the DA, and waits for all the write references
 * to disappear. If either of the trees turns out to be empty is deallocated and an error
 * is returned.
 *
 * @return  -EAGAIN     A tree was deallocated, restart the merge.
 * @return   0          Trees were found, and stored in cts array.
 */
static int castle_da_merge_cts_get(struct castle_double_array *da,
                                   int level,
                                   struct castle_component_tree **cts)
{
    struct castle_component_tree *ct;
    struct list_head *l;
    int i;

    /* Zero out the CTs array. */
    cts[0] = cts[1] = NULL;
    read_lock(&da->lock);

    /* Find two oldest trees walking the list backwards. */
    list_for_each_prev(l, &da->levels[level].trees)
    {
        struct castle_component_tree *ct =
                            list_entry(l, struct castle_component_tree, da_list);

        /* If there are any trees being compacted, they must be older than the
           two trees we want to merge here. */
        if (ct->compacting)
            continue;

        if(!cts[1])
            cts[1] = ct;
        else
        if(!cts[0])
            cts[0] = ct;
    }
    read_unlock(&da->lock);

    /* Wait for RW refs to dissapear. Free the CT if it is empty after that. */
    for(i = 0; i < 2; i++)
    {
        ct = cts[i];

        /* Wait until write ref count reaches zero. */
        BUG_ON(!ct->dynamic && (atomic_read(&ct->write_ref_count) != 0));
        while(atomic_read(&ct->write_ref_count))
        {
            debug("Found non-zero write ref count on ct=%d scheduled for merge cnt=%d\n",
                ct->seq, atomic_read(&ct->write_ref_count));
            msleep_interruptible(10);
        }

        /* Check that the tree has non-zero elements. */
        if(atomic64_read(&ct->item_count) == 0)
        {
            printk("Found empty CT=%d, freeing it up.\n", ct->seq);
            /* No items in this CT, deallocate it by removing it from the DA,
               and dropping the ref. */
            CASTLE_TRANSACTION_BEGIN;
            write_lock(&da->lock);
            castle_component_tree_del(da, ct);
            write_unlock(&da->lock);
            CASTLE_TRANSACTION_END;
            castle_ct_put(ct, 0);

            return -EAGAIN;
        }
    }

    return 0;
}

/**
 * Creates iterators for each of the input trees. And merged iterator used to
 * construct the output tree.
 *
 * Doesn't cleanup half-created state on failure. It is done by castle_da_merge_dealloc()
 * which would be called from castle_da_merge_init().
 *
 * @param merge [in] merge to be created
 *
 * @return non-zero if failed to create iterators
 *
 * @see castle_da_merge_init
 * @see castle_da_merge_dealloc
 */
static int castle_da_iterators_create(struct castle_da_merge *merge)
{
    struct castle_btree_type *btree;
    int ret;
    struct castle_iterator_type *iter_types[merge->nr_trees];
    int i;

    /* Make sure iter_types is not too big.  It's on stack. */
    BUG_ON(sizeof(iter_types) > 512);

    debug("Creating iterators for the merge.\n");
    FOR_EACH_MERGE_TREE(i, merge)
        BUG_ON(!merge->in_trees[i]);

    btree = castle_btree_type_get(merge->in_trees[0]->btree_type);

    /* The wait for write ref count to reach zero should have already be done. */
    FOR_EACH_MERGE_TREE(i, merge)
        BUG_ON(atomic_read(&merge->in_trees[i]->write_ref_count) != 0);

    /* Alloc space for iterators. */
    ret = -ENOMEM;
    merge->iters = castle_zalloc(sizeof(void *) * merge->nr_trees, GFP_KERNEL);
    if (!merge->iters)
        goto err_out;

    /* Create appropriate iterators for all of the trees. */
    ret = -EINVAL;
    FOR_EACH_MERGE_TREE(i, merge)
    {
        castle_da_iterator_create(merge, merge->in_trees[i], &merge->iters[i]);

        /* Check if the iterators got created properly. */
        if (!merge->iters[i])
            goto err_out;
    }
    debug("Tree iterators created.\n");

    /* Init the merged iterator */
    ret = -ENOMEM;
    merge->merged_iter = castle_malloc(sizeof(c_merged_iter_t), GFP_KERNEL);
    if(!merge->merged_iter)
        goto err_out;
    debug("Merged iterator allocated.\n");

    merge->merged_iter->nr_iters = merge->nr_trees;
    merge->merged_iter->btree    = btree;
    FOR_EACH_MERGE_TREE(i, merge)
        iter_types[i] = castle_da_iter_type_get(merge->in_trees[i]);
    castle_ct_merged_iter_init(merge->merged_iter,
                               merge->iters,
                               iter_types,
                               castle_da_each_skip);
    ret = merge->merged_iter->err;
    debug("Merged iterator inited with ret=%d.\n", ret);
    if(ret)
        goto err_out;

    /* Fast-forward merge iterator and immutable iterators states */
    if(merge->da->levels[merge->level].merge.serdes.des)
    {
        int i;
        struct component_iterator *curr_comp;
        c_immut_iter_t *curr_immut;
        struct castle_dmserlist_entry *merge_mstore =
                                        merge->da->levels[merge->level].merge.serdes.mstore_entry;
        struct castle_in_tree_merge_state_entry    *in_tree_merge_mstore_arr =
                                        merge->da->levels[merge->level].merge.serdes.in_tree_mstore_entry_arr;

        debug("%s::Fast-forward iterators on merge %p (da %d level %d)\n",
                      __FUNCTION__, merge, merge->da->id, merge->level);

        /* merged iter state */
        merge->merged_iter->err                 = merge_mstore->iter_err;
        merge->merged_iter->non_empty_cnt       = merge_mstore->iter_non_empty_cnt;
        merge->merged_iter->src_items_completed = merge_mstore->iter_src_items_completed;

        /* immut iters states */
        curr_comp = merge->merged_iter->iterators;
        for(i=0; i<merge->nr_trees; i++)
        {
            BUG_ON(!curr_comp);
            curr_immut=(c_immut_iter_t *)curr_comp->iterator;
            BUG_ON(!curr_immut);

            curr_comp->completed   = curr_immut->completed = in_tree_merge_mstore_arr[i].iter.component_completed;
            curr_comp->cached      = in_tree_merge_mstore_arr[i].iter.component_cached;

            curr_immut->curr_idx   = in_tree_merge_mstore_arr[i].iter.immut_curr_idx;
            curr_immut->cached_idx = in_tree_merge_mstore_arr[i].iter.immut_cached_idx;
            curr_immut->next_idx   = in_tree_merge_mstore_arr[i].iter.immut_next_idx;

            if(curr_immut->curr_c2b)
                put_c2b(curr_immut->curr_c2b);
            curr_immut->curr_c2b = NULL;

            if(curr_immut->next_c2b)
                put_c2b(curr_immut->next_c2b);
            curr_immut->next_c2b = NULL;

            /* Restore curr_c2b */
            if(!EXT_POS_INVAL(in_tree_merge_mstore_arr[i].iter.immut_curr_c2b_cep))
            {
                uint16_t node_size;
                c_ext_pos_t cep;

                cep                = in_tree_merge_mstore_arr[i].iter.immut_curr_c2b_cep;
                castle_da_merge_node_size_get(merge, 0 /* always at leaf node? */, &node_size);
                curr_immut->curr_c2b = castle_cache_block_get(cep, node_size);
                BUG_ON(!curr_immut->curr_c2b);

                write_lock_c2b(curr_immut->curr_c2b);
                if(!c2b_uptodate(curr_immut->curr_c2b))
                    BUG_ON(submit_c2b_sync(READ, curr_immut->curr_c2b));
                write_unlock_c2b(curr_immut->curr_c2b);

                /* Restore current btree node */
                curr_immut->curr_node = c2b_bnode(curr_immut->curr_c2b);
                BUG_ON(!curr_immut->curr_node);
                BUG_ON(curr_immut->curr_node->magic != BTREE_NODE_MAGIC);

                if(curr_comp->cached)
                {
                    /* Restore the cache */
                    curr_immut->btree->entry_get(curr_immut->curr_node, curr_immut->cached_idx,
                            &(curr_comp->cached_entry.k),
                            &(curr_comp->cached_entry.v),
                            &(curr_comp->cached_entry.cvt));
                    /* Restore the rbtree */
                    /* Assume we should never serialise on a deleted kv pair */
                    castle_ct_merged_iter_rbtree_insert(merge->merged_iter, curr_comp);
                } /* replenished cache */
            } /* restored curr_c2b */
            else
                BUG_ON(!curr_immut->completed);

            /* Restore next_c2b */
            if(!EXT_POS_INVAL(in_tree_merge_mstore_arr[i].iter.immut_next_c2b_cep))
            {
                uint16_t node_size;
                c_ext_pos_t cep;
                struct castle_btree_node *node;

                cep                = in_tree_merge_mstore_arr[i].iter.immut_next_c2b_cep;
                castle_da_merge_node_size_get(merge, 0 /* always at leaf node? */, &node_size);
                curr_immut->next_c2b = castle_cache_block_get(cep, node_size);
                BUG_ON(!curr_immut->next_c2b);

                write_lock_c2b(curr_immut->next_c2b);
                if(!c2b_uptodate(curr_immut->next_c2b))
                    BUG_ON(submit_c2b_sync(READ, curr_immut->next_c2b));
                write_unlock_c2b(curr_immut->next_c2b);

                /* Sanity check the node */
                node = c2b_bnode(curr_immut->next_c2b);
                BUG_ON(!node);
                BUG_ON(node->magic != BTREE_NODE_MAGIC);
            } /* restored next_c2b */
            curr_comp++;
        } /* restored c_immut_iters */
    } /* merged iterator fast-forwarded to serialised state */

    /* Success */
    return 0;

err_out:
    debug("Failed to create iterators. Ret=%d\n", ret);

    BUG_ON(!ret);
    return ret;
}

/**
 * Check if any of the components of DA are blocked on Low Free-Space.
 *
 * @param [in] da - Double array to be checked.
 *
 * @return 1 - DA is already suffering due to LFS
 *         0 - So far no component suffered due to LFS
 */
static int castle_da_no_disk_space(struct castle_double_array *da)
{
    if (atomic_read(&da->lfs_victim_count))
        return 1;

    return 0;
}

/**
 * Set structure for Low Free Space (LFS) handler. This functions sets the size of each
 * extent that got to be created by the LFS handler when more space is available. LFS handler
 * would allocate space and fill the extent ids in the same structure.
 *
 * @param [inout] LFS Structure.
 * @param [in] Size of Internal tree extent size (in chunks).
 * @param [in] Size of B-Tree extent size (in chunks).
 * @param [in] Size of Medium Object extent size (in chunks).
 *
 * @also castle_da_lfs_ct_init
 * @also castle_da_lfs_ct_reset
 * @also castle_da_lfs_ct_space_alloc
 * @also castle_da_lfs_ct_init_tree
 */
static void castle_da_lfs_ct_init(struct castle_da_lfs_ct_t *lfs,
                                  c_chk_cnt_t internal_tree_size,
                                  c_chk_cnt_t tree_size,
                                  c_chk_cnt_t data_size,
                                  int rwct)
{
    /* Setting up the strucuture, there shouldn't be any reserved space. */
    BUG_ON(lfs->space_reserved);

    /* Save whether we are allocating RWCT. */
    lfs->rwct = rwct;

    /* Shouldn't see any valid ext_ids. */
    BUG_ON(!EXT_ID_INVAL(lfs->internal_ext.ext_id) ||
           !EXT_ID_INVAL(lfs->tree_ext.ext_id) ||
           !EXT_ID_INVAL(lfs->data_ext.ext_id));
    BUG_ON(lfs->internal_ext.size || lfs->tree_ext.size || lfs->data_ext.size);

    /* Set-up LFS structure, assuming space allocation will fail. */
    lfs->tree_ext.size = tree_size;
    lfs->data_ext.size = data_size;
    lfs->internal_ext.size = internal_tree_size;
    lfs->leafs_on_ssds = lfs->internals_on_ssds = 0;
}

/**
 * Reset the LFS structure.
 *
 * @param [inout] LFS Structure.
 *
 * @also castle_da_lfs_ct_init
 * @also castle_da_lfs_ct_space_alloc
 * @also castle_da_lfs_ct_init_tree
 */
static void castle_da_lfs_ct_reset(struct castle_da_lfs_ct_t *lfs)
{
    lfs->internal_ext.ext_id = lfs->tree_ext.ext_id = lfs->data_ext.ext_id = INVAL_EXT_ID;
    lfs->internal_ext.size = lfs->tree_ext.size = lfs->data_ext.size = 0;
    lfs->space_reserved = 0;
    lfs->leafs_on_ssds = lfs->internals_on_ssds = 0;

    /* Make sure all writes are completed, before next thread tries to do some checks
     * during lfs_init. */
    wmb();
}

/**
 * Use the space allocated by LFS handler in CT. Make sure LFS structure has extents of same
 * size required by CT and set up CT.
 *
 * @param [out] Component Tree to be set.
 * @param [in] LFS structure with reserved space (allocated extents).
 * @param [in] Size of Internal tree extent size (in chunks).
 * @param [in] Size of B-Tree extent size (in chunks).
 * @param [in] Size of Medium Object extent size (in chunks).
 *
 * @also castle_da_lfs_ct_init
 * @also castle_da_lfs_ct_reset
 * @also castle_da_lfs_ct_space_alloc
 */
static int castle_da_lfs_ct_init_tree(struct castle_component_tree *ct,
                                       struct castle_da_lfs_ct_t *lfs,
                                       c_chk_cnt_t internal_tree_size,
                                       c_chk_cnt_t tree_size,
                                       c_chk_cnt_t data_size)
{
    /* We shouldnt be here, if the space is not already reserved. */
    BUG_ON(!lfs->space_reserved);

    /* Space is already reserved, we should have had valid extents already. */
    BUG_ON(EXT_ID_INVAL(lfs->internal_ext.ext_id));
    BUG_ON(EXT_ID_INVAL(lfs->tree_ext.ext_id));
    BUG_ON(EXT_ID_INVAL(lfs->data_ext.ext_id));

    /* Sizes of extents should match. */
    if (tree_size > lfs->tree_ext.size ||
        data_size > lfs->data_ext.size ||
        internal_tree_size > lfs->internal_ext.size)
    {
        /* Reserved space is not enough. Free this space. And try to allcoate again.
         * Note: This can be made better by freeing only unmatched extents. */
        castle_extent_free(lfs->internal_ext.ext_id);
        castle_extent_free(lfs->tree_ext.ext_id);
        castle_extent_free(lfs->data_ext.ext_id);

        castle_da_lfs_ct_reset(lfs);

        return -1;
    }

    /* Setup extent IDs. */
    ct->internal_ext_free.ext_id = lfs->internal_ext.ext_id;
    ct->tree_ext_free.ext_id     = lfs->tree_ext.ext_id;
    ct->data_ext_free.ext_id     = lfs->data_ext.ext_id;

    /* Setup extent freespaces. */
    castle_ext_freespace_init(&ct->internal_ext_free,
                               ct->internal_ext_free.ext_id);
    castle_ext_freespace_init(&ct->tree_ext_free,
                               ct->tree_ext_free.ext_id);
    castle_ext_freespace_init(&ct->data_ext_free,
                               ct->data_ext_free.ext_id);

    return 0;
}

/**
 * Low Freespace handler for Component Tree extents. Gets called by extents code, when more
 * space is available.
 *
 * @param [inout] lfs           - Low Free Space structure.
 * @param [in]    is_realloc    - Is re-allocation (previoud allocation failed due to
 *                                low free space).
 * @param [in]    lfs_callback  - Callback to be used in case of low freespace.
 * @param [in]    lfs_data      - Data pointer to be used by callback.
 * @param [in]    use_ssd       - Use SSD for Internal nodes.
 *
 * @also castle_da_lfs_ct_init
 * @also castle_da_lfs_ct_reset
 * @also castle_da_lfs_ct_space_alloc
 * @also castle_da_lfs_ct_init_tree
 */
static int castle_da_lfs_ct_space_alloc(struct castle_da_lfs_ct_t *lfs,
                                        int                        is_realloc,
                                        c_ext_event_callback_t     lfs_callback,
                                        void                      *lfs_data,
                                        int                        use_ssd)
{
    struct castle_double_array *da = lfs->da;
    c_ext_id_t internal_ext_id, tree_ext_id, data_ext_id;

    /* If the DA is dead already, no need to handle the event anymore. */
    if (da == NULL)
        return 0;

    /* Function shouldnt have been called, if space is already reserved. */
    BUG_ON(lfs->space_reserved);

    debug("Allocating space for a ct for da: %u, and extents of size - %u, %u, %u\n",
          lfs->da_id,
          lfs->internal_ext.size,
          lfs->tree_ext.size,
          lfs->data_ext.size);

    /* Size of extents to be created should have been set. */
    BUG_ON(!lfs->internal_ext.size || !lfs->tree_ext.size || !lfs->data_ext.size);
    BUG_ON(!EXT_ID_INVAL(lfs->internal_ext.ext_id) || !EXT_ID_INVAL(lfs->tree_ext.ext_id) ||
           !EXT_ID_INVAL(lfs->data_ext.ext_id));

    internal_ext_id = tree_ext_id = data_ext_id = INVAL_EXT_ID;

    /* Start an extent transaction, to make sure all the extent operations are atomic. */
    castle_extent_transaction_start();

    /* Attempt to allocate an SSD extent for internal nodes. */
    if (use_ssd)
    {
        lfs->internals_on_ssds = 1;
        lfs->internal_ext.ext_id = castle_extent_alloc(SSD_RDA,
                                                       da->id,
                                                       lfs->rwct ?
                                                            EXT_T_T0_INTERNAL_NODES :
                                                            EXT_T_INTERNAL_NODES,
                                                       lfs->internal_ext.size, 1,
                                                       NULL, NULL);
    }

    if (EXT_ID_INVAL(lfs->internal_ext.ext_id))
    {
        /* FAILED to allocate internal node SSD extent.
         * ATTEMPT to allocate internal node HDD extent. */
        lfs->internals_on_ssds = 0;
        lfs->internal_ext.ext_id = castle_extent_alloc(DEFAULT_RDA,
                                                       da->id,
                                                       lfs->rwct ?
                                                            EXT_T_T0_INTERNAL_NODES :
                                                            EXT_T_INTERNAL_NODES,
                                                       lfs->internal_ext.size, 1,
                                                       lfs_data, lfs_callback);
        if (EXT_ID_INVAL(lfs->internal_ext.ext_id))
        {
            /* FAILED to allocate internal node HDD extent. */
            castle_printk(LOG_WARN, "Merge failed due to space constraint for internal node tree.\n");
            goto no_space;
        }
    }
    else
    {
        /* SUCCEEDED allocating internal node SSD extent.
         * ATTEMPT to allocate leaf node SSD extent, but only if explicitly requested. */
        if(castle_use_ssd_leaf_nodes && use_ssd)
            lfs->tree_ext.ext_id = castle_extent_alloc(SSD_RDA,
                                                       da->id,
                                                       lfs->rwct ?
                                                            EXT_T_T0_LEAF_NODES :
                                                            EXT_T_LEAF_NODES,
                                                       lfs->tree_ext.size, 1,
                                                       NULL, NULL);
    }

    lfs->leafs_on_ssds = 1;
    if (EXT_ID_INVAL(lfs->tree_ext.ext_id))
    {
        /* FAILED to allocate leaf node SSD extent.
         * ATTEMPT to allocate leaf node HDD extent. */
        lfs->leafs_on_ssds = 0;
        lfs->tree_ext.ext_id = castle_extent_alloc(DEFAULT_RDA,
                                                   da->id,
                                                   lfs->rwct ?
                                                        EXT_T_T0_LEAF_NODES :
                                                        EXT_T_LEAF_NODES,
                                                   lfs->tree_ext.size, 1,
                                                   lfs_data, lfs_callback);
    }

    if (EXT_ID_INVAL(lfs->tree_ext.ext_id))
    {
        /* FAILED to allocate leaf node HDD extent. */
        castle_printk(LOG_WARN, "Extents allocation failed due to space constraint for "
                                "leaf node tree.\n");
        goto no_space;
    }

    /* Allocate an extent for medium objects of merged tree for the size equal to
     * sum of both the trees. */
    lfs->data_ext.ext_id = castle_extent_alloc(DEFAULT_RDA,
                                               da->id,
                                               lfs->rwct ?
                                                    EXT_T_T0_MEDIUM_OBJECTS :
                                                    EXT_T_MEDIUM_OBJECTS,
                                               lfs->data_ext.size, 1,
                                               lfs_data, lfs_callback);
    if (EXT_ID_INVAL(lfs->data_ext.ext_id))
    {
        castle_printk(LOG_WARN, "Merge failed due to space constraint for data\n");
        goto no_space;
    }

    /* Mark it as space reserved. */
    lfs->space_reserved = 1;

    /* Make sure all writes are completed, before decrementing victim count. */
    wmb();

    /* If it's a reallocation (last allocation failed due to low free space), reduce the count of lfs
     * victims in DA; If there are no more victims left for DA, then restart merges. */
    if (is_realloc && atomic_dec_and_test(&da->lfs_victim_count))
        castle_da_merge_restart(da, NULL);

    /* End extent transaction. */
    castle_extent_transaction_end();

    return 0;

no_space:
    /* If the allocation is not a reallocation, update victim count. */
    if (!is_realloc)
        atomic_inc(&da->lfs_victim_count);

    /* Take a copy of ext IDs. */
    internal_ext_id = lfs->internal_ext.ext_id;
    tree_ext_id = lfs->tree_ext.ext_id;
    BUG_ON(!EXT_ID_INVAL(lfs->data_ext.ext_id));

    /* Reset ext ids. */
    lfs->internal_ext.ext_id = lfs->tree_ext.ext_id = lfs->data_ext.ext_id = INVAL_EXT_ID;
    lfs->leafs_on_ssds = lfs->internals_on_ssds = 0;

    /* End extent transaction. */
    castle_extent_transaction_end();

    /* Incase of failure release free space. It is safe to call castle_extent_free as it doesnt
     * try to get global extent lock again. */
    if (!EXT_ID_INVAL(internal_ext_id))
        castle_extent_free(internal_ext_id);
    if (!EXT_ID_INVAL(tree_ext_id))
        castle_extent_free(tree_ext_id);

    debug("Failed to allocate from realloc\n");

    return -ENOSPC;
}

/**
 * Low Freespace event handler function for T0 extents. Will be called by extent code
 * when more space is available.
 *
 * @param [inout] data - void * for lfs structure
 *
 * @also castle_da_lfs_ct_space_alloc
 */
static int castle_da_lfs_rwct_callback(void *data)
{
    return castle_da_lfs_ct_space_alloc(data,
                                        1,    /* Reallocation. */
                                        castle_da_lfs_rwct_callback,
                                        data,
                                        0);   /* T0. Dont use SSDs. */
}

/**
 * Low Freespace event handler function for merge extents. Will be called by extent code
 * when more space is available.
 *
 * @param [inout] data - void * for lfs structure
 *
 * @also castle_da_lfs_ct_space_alloc
 */
static int castle_da_lfs_merge_ct_callback(void *data)
{
    return castle_da_lfs_ct_space_alloc(data,
                                        1,    /* Reallocation. */
                                        castle_da_lfs_merge_ct_callback,
                                        data,
                                        1);   /* Not a T0. Use SSD. */
}

/**
 * Allocates extents for the output tree, medium objects and Bloom filetrs. Tree may be split
 * between two extents (internal nodes in an SSD-backed extent, leaf nodes on HDDs).
 *
 * @param merge     Merge state structure.
 */
static int castle_da_merge_extents_alloc(struct castle_da_merge *merge)
{
    c_byte_off_t internal_tree_size, tree_size, data_size, bloom_size;
    int i, ret;
    struct castle_da_lfs_ct_t *lfs = &merge->da->levels[merge->level].lfs;

    /* Allocate an extent for merged tree for the size equal to sum of all the
     * trees being merged (could be a total merge).
     */
    internal_tree_size = tree_size = data_size = bloom_size = 0;
    FOR_EACH_MERGE_TREE(i, merge)
    {
        BUG_ON(!castle_ext_freespace_consistent(&merge->in_trees[i]->tree_ext_free));
        tree_size += atomic64_read(&merge->in_trees[i]->tree_ext_free.used);

        BUG_ON(!castle_ext_freespace_consistent(&merge->in_trees[i]->data_ext_free));
        data_size += atomic64_read(&merge->in_trees[i]->data_ext_free.used);
        data_size = MASK_CHK_OFFSET(data_size + C_CHK_SIZE);

        bloom_size += atomic64_read(&merge->in_trees[i]->item_count);
    }
    /* In case of multiple version test-case, in worst case tree could grow upto
     * double the size. Ex: For every alternative k_n in o/p stream of merged
     * iterator, k_n has only one version and k_(n+1) has (p-1) versions, where p
     * is maximum number of versions that can fit in a node. */
    tree_size = 2 * (MASK_CHK_OFFSET(tree_size) + C_CHK_SIZE);
    /* Calculate total size of internal nodes, assuming that leafs are stored on HDDs ... */
    internal_tree_size = tree_size;
    /* ... number of leaf nodes ... */
    internal_tree_size /= (VLBA_HDD_RO_TREE_NODE_SIZE * C_BLK_SIZE);
    /* ... number of level 1 nodes ... */
    internal_tree_size /= castle_btree_vlba_max_nr_entries_get(VLBA_SSD_RO_TREE_NODE_SIZE);
    internal_tree_size ++;
    /* ... size of level 1 ... */
    internal_tree_size *= (VLBA_SSD_RO_TREE_NODE_SIZE * C_BLK_SIZE);
    /* ... chunk rounding ... */
    internal_tree_size  = MASK_CHK_OFFSET(internal_tree_size + C_CHK_SIZE);
    /* ... factor of 2 explosion, just as before ... */
    internal_tree_size *= 2;
    /* NOTE: Internal nodes on HDDs will always require less space than internal nodes
       on SSDs, because the overheads are smaller (node headers amortised between greater
       number of entries in the node). */

    BUG_ON(!EXT_ID_INVAL(merge->out_tree->internal_ext_free.ext_id) ||
           !EXT_ID_INVAL(merge->out_tree->tree_ext_free.ext_id));

__again:
    /* If the space is not already reserved for the merge, allocate it from freespace. */
    if (!lfs->space_reserved)
    {
        /* Initialize the lfs structure with required extent sizes. */
        castle_da_lfs_ct_init(lfs,
                              CHUNK(internal_tree_size),
                              CHUNK(tree_size),
                              CHUNK(data_size),
                              0 /* Not a T0. */);

        /* Allocate space from freespace. */
        ret = castle_da_lfs_ct_space_alloc(lfs,
                                           0,   /* First allocation. */
                                           castle_da_lfs_merge_ct_callback,
                                           lfs,
                                           1);  /* Not a T0. Use SSD. */

        /* If failed to allocate space, return error. lfs structure is already set.
         * Low freespace handler would allocate space, when more freespace is available. */
        if (ret)    return ret;
    }

    /* Successfully allocated space. Initialize the component tree with alloced extents.
     * castle_da_lfs_ct_init_tree() would fail if the space reserved by lfs handler is not
     * enough for CT. */
    if (castle_da_lfs_ct_init_tree(merge->out_tree,
                                   lfs,
                                   CHUNK(internal_tree_size),
                                   CHUNK(tree_size),
                                   CHUNK(data_size)))
        goto __again;

    merge->internals_on_ssds = lfs->internals_on_ssds;
    merge->leafs_on_ssds = lfs->leafs_on_ssds;

    /* Done with lfs strcuture; reset it. */
    castle_da_lfs_ct_reset(lfs);

    /* Allocate Bloom filters. */
    if ((ret = castle_bloom_create(&merge->out_tree->bloom, merge->da->id, bloom_size)))
        merge->out_tree->bloom_exists = 0;
    else
        merge->out_tree->bloom_exists = 1;

    return 0;
}


static c_val_tup_t castle_da_medium_obj_copy(struct castle_da_merge *merge,
                                             c_val_tup_t old_cvt)
{
    c_ext_pos_t old_cep, new_cep;
    c_val_tup_t new_cvt;
    int total_blocks, blocks, i;
    c2_block_t *s_c2b, *c_c2b;
#ifdef CASTLE_PERF_DEBUG
    struct castle_component_tree *tree = NULL;
    struct timespec ts_start, ts_end;
#endif

    old_cep = old_cvt.cep;
    /* Old cvt needs to be a medium object. */
    BUG_ON(!CVT_MEDIUM_OBJECT(old_cvt));
    /* It needs to be of the right size. */
    BUG_ON(!is_medium(old_cvt.length));
    /* It must belong to one of the in_trees data extent. */
    FOR_EACH_MERGE_TREE(i, merge)
        if (old_cvt.cep.ext_id == merge->in_trees[i]->data_ext_free.ext_id)
            break;
    BUG_ON(i == merge->nr_trees);
    /* We assume objects are page aligned. */
    BUG_ON(BLOCK_OFFSET(old_cep.offset) != 0);

    /* Allocate space for the new copy. */
    total_blocks = (old_cvt.length - 1) / C_BLK_SIZE + 1;
    BUG_ON(castle_ext_freespace_get(&merge->out_tree->data_ext_free,
                                     total_blocks * C_BLK_SIZE,
                                     0,
                                    &new_cep) < 0);
    BUG_ON(BLOCK_OFFSET(new_cep.offset) != 0);
    /* Save the cep to return later. */
    new_cvt = old_cvt;
    new_cvt.cep = new_cep;

    /* Do the actual copy. */
    debug("Copying "cep_fmt_str" to "cep_fmt_str_nl,
            cep2str(old_cep), cep2str(new_cep));
#ifdef CASTLE_PERF_DEBUG
    /* Figure out which tree to update stats for. */
    FOR_EACH_MERGE_TREE(i, merge)
        if (old_cep.ext_id == merge->in_trees[i]->data_ext_free.ext_id)
            tree = merge->in_trees[i];
#endif

    while (total_blocks > 0)
    {
        int chk_off, pgs_to_end;

        /* Chunk-align blocks if total_blocks is large enough to make it worthwhile. */
        chk_off = CHUNK_OFFSET(old_cep.offset);
        if (chk_off)
            pgs_to_end = (C_CHK_SIZE - chk_off) >> PAGE_SHIFT;

        /* Be careful about subtraction, if it goes negative, and is compared to
           BLKS_PER_CHK the test is likely not to work correctly. */
        if (chk_off && (total_blocks >= 2*BLKS_PER_CHK + pgs_to_end))
            /* Align for a minimum of 2 full blocks (1 can be inefficient) */
            blocks = pgs_to_end;
        else if (total_blocks > BLKS_PER_CHK)
            blocks = BLKS_PER_CHK;
        else
            blocks = total_blocks;
        total_blocks -= blocks;

        castle_perf_debug_getnstimeofday(&ts_start);
        s_c2b = castle_cache_block_get(old_cep, blocks);
        c_c2b = castle_cache_block_get(new_cep, blocks);
        castle_perf_debug_getnstimeofday(&ts_end);
        castle_perf_debug_bump_ctr(tree->get_c2b_ns, ts_end, ts_start);
        castle_cache_advise(s_c2b->cep, C2_ADV_PREFETCH|C2_ADV_SOFTPIN|C2_ADV_FRWD, -1, -1, 0);
        /* Make sure that we lock _after_ prefetch call. */
        write_lock_c2b(s_c2b);
        write_lock_c2b(c_c2b);
        if(!c2b_uptodate(s_c2b))
        {
            /* c2b is not marked as up-to-date.  We hope this is because we are
             * at the start of the extent and have just issued a prefetch call.
             * If this is true, the underlying c2p is up-to-date so a quick call
             * into submit_c2b_sync() should detect this and update the c2b to
             * reflect this change.
             *
             * Alternatively it could mean that some of our prefetched c2bs have
             * been evicted.
             *
             * By analysing the time spent in submit_c2b_sync() it should be
             * possible to determine which of these scenarios are occurring. */
            castle_perf_debug_getnstimeofday(&ts_start);
            BUG_ON(submit_c2b_sync(READ, s_c2b));
            castle_perf_debug_getnstimeofday(&ts_end);
            castle_perf_debug_bump_ctr(tree->data_c2bsync_ns, ts_end, ts_start);
        }
        update_c2b(c_c2b);
        memcpy(c2b_buffer(c_c2b), c2b_buffer(s_c2b), blocks * PAGE_SIZE);
        dirty_c2b(c_c2b);
        write_unlock_c2b(c_c2b);
        write_unlock_c2b(s_c2b);
        put_c2b(c_c2b);
        put_c2b_and_demote(s_c2b);
        old_cep.offset += blocks * PAGE_SIZE;
        new_cep.offset += blocks * PAGE_SIZE;
    }
    debug("Finished copy, i=%d\n", i);

    return new_cvt;
}

/**
 * Works out what node size should be used for given level in the btree in a given merge.
 *
 * @param merge     Merge state structure.
 * @param level     Level counted from leaves.
 * @param node_size Return argument: size of the node.
 */
static inline void castle_da_merge_node_size_get(struct castle_da_merge *merge,
                                                 uint8_t level,
                                                 uint16_t *node_size)
{
    if(level > 0)
    {
        if(merge->internals_on_ssds)
            *node_size = VLBA_SSD_RO_TREE_NODE_SIZE;
        else
            *node_size = VLBA_HDD_RO_TREE_NODE_SIZE;
        return;
    }

    if(merge->leafs_on_ssds)
        *node_size = VLBA_SSD_RO_TREE_NODE_SIZE;
    else
        *node_size = VLBA_HDD_RO_TREE_NODE_SIZE;
}

/**
 * Works out which extent, and what node size should be used for given level in the btree
 * in a given merge.
 *
 * @param merge     Merge state structure.
 * @param level     Level counted from leaves.
 * @param node_size Return argument: size of the node.
 * @param ext_free  Return argument: extent freespace structure.
 */
static inline void castle_da_merge_node_info_get(struct castle_da_merge *merge,
                                                 uint8_t level,
                                                 uint16_t *node_size,
                                                 c_ext_free_t **ext_free)
{
    castle_da_merge_node_size_get(merge, level, node_size);
    /* If level is zero, we are allocating from tree_ext. Size depends on whether the
       extent is on SSDs or HDDs. */
    if(level > 0)
    {
        /* Internal nodes extent should always exist. */
        BUG_ON(EXT_ID_INVAL(merge->out_tree->internal_ext_free.ext_id));
        *ext_free = &merge->out_tree->internal_ext_free;
        return;
    }

    BUG_ON(level != 0);
    /* Leaf nodes extent should always exist. */
    BUG_ON(EXT_ID_INVAL(merge->out_tree->tree_ext_free.ext_id));
    *ext_free = &merge->out_tree->tree_ext_free;
}

/**
 * Add an entry to the nodes that are being constructed in merge.
 *
 * @param merge [in, out] Doubling array merge structure
 * @param depth [in] B-tree depth at which entry should be added. 0 being leaf nodes.
 * @param key [in] key of the entry to be added
 * @param version [in]  version of the entry to be added
 * @param cvt [in] value tuple of the entry to be added
 * @param is_re_add [in] are we trying to re-add the entry to output tree?
 *                       (possible when we are trying to move entries from one node to
 *                       another node while completing the former node.)
 * @return c_val_tup_t*  If a new node is created, return a cvt specifying how to link this node
                         to a higher level node.
 * Note: if is_re_add flag is set, then the data wont be processed again, just
 * the key gets added.  Used when entry is being moved from one node to another
 * node.
 */
static inline c_val_tup_t* _castle_da_entry_add(struct castle_da_merge *merge,
                                                int depth,
                                                void *key,
                                                c_ver_t version,
                                                c_val_tup_t cvt,
                                                int is_re_add)
{
    struct castle_da_merge_level *level = merge->levels + depth;
    struct castle_btree_type *btree = merge->out_btree;
    struct castle_btree_node *node;
    int key_cmp;
#ifdef CASTLE_PERF_DEBUG
    struct timespec ts_start, ts_end;
#endif
    c_val_tup_t* preadoption_cvt = NULL;

    /* Deal with medium and large objects first. For medium objects, we need to copy them
       into our new medium object extent. For large objects, we need to save the aggregate
       size. plus take refs to extents? */
    /* It is possible to do castle_da_entry_add() on the same entry multiple
     * times. Don't process data again. */
    if (!is_re_add)
    {
        if(CVT_MEDIUM_OBJECT(cvt))
        {
            castle_perf_debug_getnstimeofday(&ts_start);
            cvt = castle_da_medium_obj_copy(merge, cvt);
            castle_perf_debug_getnstimeofday(&ts_end);
            castle_perf_debug_bump_ctr(merge->da_medium_obj_copy_ns, ts_end, ts_start);
        }
        if(CVT_LARGE_OBJECT(cvt))
        {
            merge->large_chunks += castle_extent_size_get(cvt.cep.ext_id);
            /* No need to add Large Objects under lock as merge is done in sequence. No concurrency
             * issues on the tree. With merge serialisation, checkpoint thread uses the list on
             * the output tree, which is only spliced in da_merge_marshall (under serdes lock) and
             * da_merge_package (which explicitly takes serdes lock).*/
            /* Adding LO to a temp list, wait for merge_serialise to splice when appropriate, or
               da_merge_package to do final splice. */
            castle_ct_large_obj_add(cvt.cep.ext_id, cvt.length, &merge->new_large_objs, NULL);
            BUG_ON(castle_extent_link(cvt.cep.ext_id) < 0);
            debug("%s::large object ("cep_fmt_str") for da %d level %d.\n",
                __FUNCTION__, cep2str(cvt.cep), merge->da->id, merge->level);
        }
    }

    BUG_ON(is_re_add &&
           CVT_MEDIUM_OBJECT(cvt) &&
           (cvt.cep.ext_id != merge->out_tree->data_ext_free.ext_id));

    debug("Adding an entry at depth: %d for merge on da %d level %d\n",
        depth, merge->da->id, merge->level);
    BUG_ON(depth >= MAX_BTREE_DEPTH);
    /* Alloc a new block if we need one */
    if(!level->node_c2b)
    {
        c_ext_free_t *ext_free;
        uint16_t node_size;
        c_ext_pos_t cep;

        castle_da_merge_node_info_get(merge, depth, &node_size, &ext_free);
        if(merge->root_depth < depth)
        {
            debug("%s::Creating a new root level: %d\n", __FUNCTION__, depth);
            BUG_ON(merge->root_depth != depth - 1);
            merge->root_depth = depth;
            merge->out_tree->node_sizes[depth] = node_size;
        }
        BUG_ON(level->next_idx      != 0);
        BUG_ON(level->valid_end_idx >= 0);

        debug("Allocating a new node at depth: %d\n", depth);
        BUG_ON(node_size != btree->node_size(merge->out_tree, depth));
        BUG_ON(castle_ext_freespace_get(ext_free,
                                        node_size * C_BLK_SIZE,
                                        0,
                                        &cep) < 0);
        debug("Got "cep_fmt_str_nl, cep2str(cep));

        castle_perf_debug_getnstimeofday(&ts_start);
        level->node_c2b = castle_cache_block_get(cep, node_size);
        castle_perf_debug_getnstimeofday(&ts_end);
        castle_perf_debug_bump_ctr(merge->get_c2b_ns, ts_end, ts_start);
        debug("Locking the c2b, and setting it up to date.\n");
        write_lock_c2b(level->node_c2b);
        update_c2b(level->node_c2b);
        /* Init the node properly */
        node = c2b_bnode(level->node_c2b);
        castle_da_node_buffer_init(btree, node, node_size);
        debug("%s::Allocating a new node at depth: %d for merge %p (da %d level %d)\n",
            __FUNCTION__, depth, merge, merge->da->id, merge->level);

        /* if a parent node exists, return preadoption cvt for caller to perform preadoption */
        if(depth < merge->root_depth)
        {
            preadoption_cvt = castle_zalloc(sizeof(c_val_tup_t), GFP_KERNEL); /* free'd by caller */
            CVT_NODE_INIT(*preadoption_cvt,
                          level->node_c2b->nr_pages * C_BLK_SIZE,
                          level->node_c2b->cep);
        }
    }
    else if (depth > 0)
            write_lock_c2b(level->node_c2b);

    node = c2b_bnode(level->node_c2b);
    debug("Adding at idx=%d depth=%d into node %p for merge on da %d level %d\n",
        level->next_idx, depth, node, merge->da->id, merge->level);
    debug("Adding an idx=%d, key=%p, *key=%d, version=%d\n",
            level->next_idx, key, *((uint32_t *)key), version);
    /* Add the entry to the node (this may get dropped later, but leave it here for now */
    BUG_ON(CVT_LEAF_PTR(cvt));
    btree->entry_add(node, level->next_idx, key, version, cvt);

    /* unlock the c2b so checkpoint can writeback merge state, but for the sake of performance,
       don't do this for leaf nodes, which will be unlocked and relocked on unit merge boundary. */
    /* TODO@tr actually come to think of it, this might rate-limit checkpoint's progress to unit
       merge boundary anyway. i.o.w. we may have had the same effect if we left the locking
       scheme as it was in da_entry_add, node_complete, etc, and simply unlocked and locked ALL
       node level c2bs at unit merge boundary. Nevertheless, the current "frequent internal node
       lock toggling" approach might be beneficial for partial merges... */
    if (depth > 0)
        write_unlock_c2b(level->node_c2b);

    /* Compare the current key to the last key. Should never be smaller */
    /* key_compare() is a costly function. Trying to avoid duplicates. We already
     * did comparision between last key added to the out_tree and current key in
     * snapshot_delete algorithm (in castle_da_entry_skip()). Reuse the result
     * of it here again. */
    /* Note: In case of re-adds is_new_key doesnt represent comparision between key being
     * added and last key added to the node. But, it repesents the comparision between last
     * 2 keys added to the tree. Still, it is okay as in case of re-adds both the comparisions
     * yield same value. */

    key_cmp = (level->next_idx != 0) ?
               ((depth == 0)? merge->is_new_key: btree->key_compare(key, level->last_key)) :
               0;
    debug("Key cmp=%d\n", key_cmp);
    BUG_ON(key_cmp < 0);

    /* Work out if the current/previous entry could be a valid node end.
       Case 1: We've just started a new node (node_idx == 0) => current must be a valid node entry */
    if(level->next_idx == 0)
    {
        debug("Node valid_end_idx=%d, Case1.\n", level->next_idx);
        BUG_ON(level->valid_end_idx >= 0);
        /* Save last_key, version as a valid_version, and init valid_end_idx.
           Note: last_key has to be taken from the node, bacuse current key pointer
                 may get invalidated on the iterator next() call.
         */
        level->valid_end_idx = 0;
        btree->entry_get(node, level->next_idx, &level->last_key, NULL, NULL);
        level->valid_version = version;
    } else
    /* Case 2: We've moved on to a new key. Previous entry is a valid node end. */
    if(key_cmp > 0)
    {
        debug("Node valid_end_idx=%d, Case2.\n", level->next_idx);
        btree->entry_get(node, level->next_idx, &level->last_key, NULL, NULL);
        BUG_ON(level->next_idx <= 0);
        level->valid_end_idx = level->next_idx - 1;
        level->valid_version = 0;
    }
#if 0
    /* This is disabled now, because we don't want keys crossing the node boundries.
       Otherwise counter accumulation may not work correctly on gets/rqs. */
    else
    /* Case 3: Version is STRONGLY ancestoral to valid_version. */
    if(castle_version_is_ancestor(version, level->valid_version))
    {
        debug("Node valid_end_idx=%d, Case3.\n", level->next_idx);
        BUG_ON(version == level->valid_version);
        level->valid_end_idx = level->next_idx;
        level->valid_version = version;
    }
#endif

    /* Node may be (over-)complete now, if it is full. Set next_idx to -1 (invalid) */
    if(btree->need_split(node, 0))
    {
        debug("Node now complete.\n");
        level->next_idx = -1;
    }
    else
        /* Go to the next node_idx */
        level->next_idx++;

    /* Get the last_key stored in leaf nodes. */
    if (depth == 0)
    {
        merge->last_key = level->last_key;
        BUG_ON(merge->last_key == NULL);
    }
    return preadoption_cvt;
}

/* wrapper around the real castle_da_entry_add that performs orphan node preadoption iteratively */
static inline void castle_da_entry_add(struct castle_da_merge *merge,
                                       int depth,
                                       void *key,
                                       c_ver_t version,
                                       c_val_tup_t cvt,
                                       int is_re_add)
{
    c_val_tup_t* preadoption_cvt = NULL;
    int initial_root_depth = merge->root_depth;

    do{
        preadoption_cvt = _castle_da_entry_add(merge, depth, key, version, cvt, is_re_add);
        if(!preadoption_cvt) return; /* no new node created */

        /*  if _castle_da_entry_add returned non-NULL, then a new node was created */
        memcpy(&cvt, preadoption_cvt, sizeof(c_val_tup_t)); //TODO@tr get rid of this memcpy
        castle_kfree(preadoption_cvt); /* malloc'd by _castle_da_entry_add */
        preadoption_cvt = NULL;

        key = merge->out_btree->max_key;
        is_re_add = 0;
        depth++;

        /* at most this loop can add 1 level */
        BUG_ON(depth > initial_root_depth+1);
        BUG_ON(merge->root_depth > initial_root_depth+1);
        debug("%s::preadopting new orphan node for merge on da %d level %d.\n",
                __FUNCTION__, merge->da->id, merge->level);
    } while(true); /* rely on the return value from _castle_da_entry_add to break */
}

static void castle_da_node_complete(struct castle_da_merge *merge, int depth)
{
    struct castle_da_merge_level *level = merge->levels + depth;
    struct castle_btree_type *btree = merge->out_btree;
    struct castle_btree_node *node;
    int node_idx;
    void *key;
    c_ver_t version;
    c_val_tup_t cvt, node_cvt;
    c2_block_t *node_c2b;
    int valid_end_idx;

    /* Make sure we are not in recursion. */
#ifdef CASTLE_DEBUG
    BUG_ON(merge->is_recursion);
    merge->is_recursion = 1;
#endif

    debug("%s::Completing node at depth=%d for da %d level %d\n",
        __FUNCTION__, depth, merge->da->id, merge->level);
    BUG_ON(depth >= MAX_BTREE_DEPTH);

    node      = c2b_bnode(level->node_c2b);
    BUG_ON(!node);
    /* Version of the node should be the last valid_version */
    debug("Node version=%d\n", level->valid_version);
    node->version = level->valid_version;
    if(depth > 0)
        node->is_leaf = 0;

    /* Note: This code calls castle_da_entry_add(), which would change all
     * parameters in level. Taking a copy of required members. */
    node_c2b        = level->node_c2b;
    valid_end_idx   = level->valid_end_idx;

    /* for non-leaf nodes, the c2b will be unlocked */
    if (depth > 0)
        write_lock_c2b(node_c2b);

    btree->entry_get(node, valid_end_idx, &key, &version, &cvt);
    debug("Inserting into parent key=%p, *key=%d, version=%d\n",
            key, *((uint32_t*)key), node->version);
    BUG_ON(CVT_LEAF_PTR(cvt));

    /* Insert correct pointer in the parent, unless we've just completed the
       root node at the end of the merge. */
    if(!(merge->completing && (merge->root_depth == depth)))
    {
        CVT_NODE_INIT(node_cvt, (node_c2b->nr_pages * C_BLK_SIZE), node_c2b->cep);
        if (depth < merge->root_depth)
        {
            /* this is not the top level, so there must be a higher level which contains a
               preadoption link that must be replaced with a "real" link. */
            struct castle_da_merge_level *parent_level = merge->levels + depth + 1;
            c2_block_t *parent_node_c2b                = parent_level->node_c2b;
            struct castle_btree_node *parent_node      = c2b_bnode(parent_node_c2b);

            write_lock_c2b(parent_node_c2b);
            parent_node      = c2b_bnode(parent_node_c2b);

            debug("%s::replacing preadoption link with real link.\n",
                    __FUNCTION__);

            btree->entry_replace(parent_node, parent_node->used - 1, key, node->version, node_cvt);
            write_unlock_c2b(parent_node_c2b);
        }
        else
        {
            debug("%s::linking completed node to parent.\n", __FUNCTION__);
            /* add a "real" link */
            castle_da_entry_add(merge, depth+1, key, node->version, node_cvt, 0);
        }

    }


    /* Reset the variables to the correct state for castle_da_entry_add(). */
    level->node_c2b      = NULL;
    level->last_key      = NULL;
    level->next_idx      = 0;
    level->valid_end_idx = -1;
    level->valid_version = INVAL_VERSION;

    /* When a node is complete, we need to copy the entries after valid_end_idx to
       the corresponding buffer */
    node_idx = valid_end_idx + 1;
    BUG_ON(node_idx <= 0 || node_idx > node->used);
    debug("%s::Entries to be copied to the buffer are in range [%d, %d)\n",
            __FUNCTION__, node_idx, node->used);
    while(node_idx < node->used)
    {
        /* If merge is completing, there shouldn't be any splits any more. */
        BUG_ON(merge->completing);
        btree->entry_get(node, node_idx,  &key, &version, &cvt);
        BUG_ON(CVT_LEAF_PTR(cvt));
        debug("%s::spliting node at depth %d for da %d level %d.\n",
            __FUNCTION__, depth, merge->da->id, merge->level);
        castle_da_entry_add(merge, depth, key, version, cvt, 1);
        node_idx++;
        BUG_ON(level->node_c2b == NULL);
        /* Check if the node completed, it should never do */
        BUG_ON(level->next_idx < 0);
    }

    debug("Dropping entries [%d, %d] from the original node\n",
            valid_end_idx + 1, node->used - 1);
    /* Now that entries are safely in the new node, drop them from the node */
    if((valid_end_idx + 1) <= (node->used - 1))
        btree->entries_drop(node, valid_end_idx + 1, node->used - 1);

    BUG_ON(node->used != valid_end_idx + 1);
    if(merge->completing && (merge->root_depth == depth))
    {
        /* Node c2b was set to NULL earlier in this function. When we are completing the merge
           we should never have to create new nodes at the same lavel (i.e. there shouldn't be
           any castle_da_entry_adds above). */
        BUG_ON(level->node_c2b);
        debug("Just completed the root node (depth=%d), at the end of the merge.\n",
                depth);
    }
    debug("Releasing c2b for cep=" cep_fmt_str_nl, cep2str(node_c2b->cep));
    debug("Completing a node with %d entries at depth %d\n", node->used, depth);
    /* Hold on to last leaf node for the sake of last_key. No need of lock, this
     * is a immutable node. */
    if (depth == 0)
    {
        c2_block_t *last_leaf_c2b = merge->last_leaf_node_c2b;

        /* Release the refernece to the previous last node. */
        if (last_leaf_c2b)
        {
            /* The last_key pointer mustn't be pointing to the node any more. */
            BUG_ON(ptr_in_range(merge->last_key,
                                c2b_buffer(last_leaf_c2b),
                                last_leaf_c2b->nr_pages * PAGE_SIZE));
            put_c2b(last_leaf_c2b);
        }

        merge->last_leaf_node_c2b = node_c2b;
        get_c2b(merge->last_leaf_node_c2b);
    }

    /* Release the c2b. */
    dirty_c2b(node_c2b);
    write_unlock_c2b(node_c2b);
    put_c2b(node_c2b);

#ifdef CASTLE_DEBUG
    merge->is_recursion = 0;
#endif
}

static inline int castle_da_nodes_complete(struct castle_da_merge *merge)
{
    struct castle_da_merge_level *level;
    int i;

    debug("Checking if we need to complete nodes.");
    /* Check if the level i node has been completed, which may trigger a cascade of
       completes up the tree. */
    for(i=0; i<MAX_BTREE_DEPTH-1; i++)
    {
        level = merge->levels + i;
        /* Complete if next_idx < 0 */
        if(level->next_idx < 0)
            castle_da_node_complete(merge, i);
        else
            /* As soon as we see an incomplete node, we need to break out: */
            goto out;
    }
    /* If we reached the top of the tree, we must fail the merge */
    if(i == MAX_BTREE_DEPTH - 1)
        return -EINVAL;

out:
    debug("We got as far as depth=%d\n", i);

    return 0;
}

static struct castle_component_tree* castle_da_merge_package(struct castle_da_merge *merge,
                                                             c_ext_pos_t root_cep)
{
    struct castle_component_tree *out_tree;
    int i;
    c_merge_serdes_state_t serdes_state;

    out_tree = merge->out_tree;
    debug("Using component tree id=%d to package the merge.\n", out_tree->seq);
    /* Root node is the last node that gets completed, and therefore will be saved in last_node */
    out_tree->tree_depth = merge->root_depth+1;
    castle_printk(LOG_INFO, "Depth of ct=%d (%p) is: %d\n",
            out_tree->seq, out_tree, out_tree->tree_depth);
    out_tree->root_node = root_cep;

    debug("Root for that tree is: " cep_fmt_str_nl, cep2str(out_tree->root_node));
    /* Write counts out */
    atomic64_set(&out_tree->item_count, merge->nr_entries);
    atomic64_set(&out_tree->large_ext_chk_cnt, merge->large_chunks);
    BUG_ON(atomic_read(&out_tree->ref_count)       != 1);
    BUG_ON(atomic_read(&out_tree->write_ref_count) != 0);

    /* update list of large objects */
    serdes_state = atomic_read(&merge->da->levels[merge->level].merge.serdes.valid);
    if(serdes_state > NULL_DAM_SERDES)
        mutex_lock(&merge->da->levels[merge->level].merge.serdes.mutex);
    list_splice_init(&merge->new_large_objs, &out_tree->large_objs);
    if(serdes_state > NULL_DAM_SERDES)
        mutex_unlock(&merge->da->levels[merge->level].merge.serdes.mutex);

    castle_printk(LOG_DEBUG, "%s::Number of entries=%ld, number of nodes=%ld\n", __FUNCTION__,
            atomic64_read(&out_tree->item_count));

    /* Add the new tree to the doubling array */
    BUG_ON(merge->da->id != out_tree->da);
    castle_printk(LOG_INFO, "Finishing merge of ");
    FOR_EACH_MERGE_TREE(i, merge)
        castle_printk(LOG_INFO, "ct%d=%d, ", i, merge->in_trees[i]->seq);
    castle_printk(LOG_INFO, "new_tree=%d\n", out_tree->seq);
    debug("Adding to doubling array, level: %d\n", out_tree->level);

    FAULT(MERGE_FAULT);

    return out_tree;
}

static void castle_da_max_path_complete(struct castle_da_merge *merge, c_ext_pos_t root_cep)
{
    struct castle_btree_type *btree = merge->out_btree;
    struct castle_btree_node *node;
    c2_block_t *node_c2b, *next_node_c2b;
    struct castle_component_tree *ct = merge->out_tree;
    uint8_t level;

    BUG_ON(!merge->completing);
    /* Start with the root node. */
    node_c2b = castle_cache_block_get(root_cep,
                                      btree->node_size(ct, merge->root_depth));
    /* Lock and update the c2b. */
    write_lock_c2b(node_c2b);
    if(!c2b_uptodate(node_c2b))
        BUG_ON(submit_c2b_sync(READ, node_c2b));
    node = c2b_bnode(node_c2b);
    debug("Maxifying the right most path, starting with root_cep="cep_fmt_str_nl,
            cep2str(node_c2b->cep));
    /* Init other temp vars. */
    level = 0;
    while(!node->is_leaf)
    {
        void *k;
        c_ver_t v;
        c_val_tup_t cvt;

        /* Replace right-most entry with (k=max_key, v=0) */
        btree->entry_get(node, node->used-1, &k, &v, &cvt);
        BUG_ON(!CVT_NODE(cvt) || CVT_LEAF_PTR(cvt));
        debug("The node is non-leaf, replacing the right most entry with (max_key, 0).\n");
        btree->entry_replace(node, node->used-1, btree->max_key, 0, cvt);
        /* Change the version of the node to 0 */
        node->version = 0;
        /* Dirty the c2b */
        dirty_c2b(node_c2b);
        /* Go to the next btree node */
        debug("Locking next node cep=" cep_fmt_str_nl,
              cep2str(cvt.cep));
        next_node_c2b = castle_cache_block_get(cvt.cep,
                                               btree->node_size(ct, merge->root_depth - level));
        write_lock_c2b(next_node_c2b);
        /* We unlikely to need a blocking read, because we've just had these
           nodes in the cache. */
        if(!c2b_uptodate(next_node_c2b))
            BUG_ON(submit_c2b_sync(READ, next_node_c2b));
        /* Release the old node. */
        debug("Unlocking prev node cep=" cep_fmt_str_nl,
               cep2str(node_c2b->cep));
        write_unlock_c2b(node_c2b);
        put_c2b(node_c2b);

        node_c2b = next_node_c2b;
        node = c2b_bnode(node_c2b);
        level++;
    }
    /* Release the leaf node. */
    debug("Unlocking prev node cep="cep_fmt_str_nl,
           cep2str(node_c2b->cep));
    write_unlock_c2b(node_c2b);
    put_c2b(node_c2b);
}

/**
 * Complete merge process.
 *
 * Each level can have atmost one uncompleted node. Complete each node with the
 * entries we got now, and link the node to its parent. During this process, each
 * non-leaf node can get one extra entry in worst case. Mark valid_end_idx in each
 * level to used-1. And call castle_da_node_complete on every level, which would
 * complete the node and might add one entry in next higher level.
 *
 * @param merge [in, out] merge strucutre to be completed.
 *
 * @return ct Complete out tree
 *
 * @see castle_da_node_complete
 */
static struct castle_component_tree* castle_da_merge_complete(struct castle_da_merge *merge)
{
    struct castle_da_merge_level *level;
    struct castle_btree_node *node;
    c_ext_pos_t root_cep = INVAL_EXT_POS;
    int next_idx, i;

    BUG_ON(!CASTLE_IN_TRANSACTION);

    merge->completing = 1;
    castle_printk(LOG_DEBUG, "Complete merge at level: %d|%d\n", merge->level, merge->root_depth);
    /* Force the nodes to complete by setting next_idx negative. Valid node idx
       can be set to the last entry in the node safely, because it happens in
       conjunction with setting the version to 0. This guarantees that all
       versions in the node are decendant of the node version. */
    for(i=0; i<MAX_BTREE_DEPTH; i++)
    {
        debug("Flushing at depth: %d\n", i);
        level = merge->levels + i;
        /* Node index == 0 indicates that there is no node at this level,
           therefore we don't have to complete anything. */
        next_idx = level->next_idx;
        /* Record the root cep for later use. */
        if(i == merge->root_depth)
        {
            /* Root node must always exist, and have > 0 entries.
               -1 is also allowed, if the node overflowed once the node for
               previous (merge->root_depth-1) got completed. */
            BUG_ON(next_idx == 0 || next_idx < -1);
            root_cep = merge->levels[i].node_c2b->cep;
        }
        if(next_idx != 0)
        {
            debug("Artificially completing the node at depth: %d\n", i);

            /* Complete the node by marking last entry as valid end. Also, mark
             * the version of this node to 0, as the node might contain multiple
             * entries. */
            node = c2b_bnode(level->node_c2b);
            /* Point the valid_end_idx past the last entry ... */
            level->valid_end_idx = next_idx < 0 ? node->used : level->next_idx;
            /* ... and now point it at the last entry. */
            level->valid_end_idx--;
            level->valid_version = 0;
            level->next_idx = -1;
            castle_da_node_complete(merge, i);
        }
    }
    /* Write out the max keys along the max path. */
    if (merge->nr_entries)
        castle_da_max_path_complete(merge, root_cep);

    /* Complete Bloom filters. */
    if (merge->out_tree->bloom_exists)
        castle_bloom_complete(&merge->out_tree->bloom);

    /* Package the merge result. */
    return castle_da_merge_package(merge, root_cep);
}

static void castle_ct_large_objs_remove(struct list_head *);

/**
 * Deallocate a serdes state of merge state from merge->da.
 *
 * @param merge [in] in-flight merge.
 *
 * @note assumes caller takes da->levels[].merge.serdes.mutex lock
 * @also castle_da_merge_dealloc
 */
static void castle_da_merge_serdes_dealloc(struct castle_da_merge *merge)
{
    struct castle_double_array *da;
    int level;
    c_merge_serdes_state_t serdes_state;
    da=merge->da;
    level=merge->level;

    BUG_ON(!merge);
    BUG_ON(!merge->da);
    BUG_ON( (level < MIN_DA_SERDES_LEVEL) && (level != BIG_MERGE) );

    serdes_state = atomic_read(&da->levels[level].merge.serdes.valid);
    if(serdes_state == NULL_DAM_SERDES)
    {
        castle_printk(LOG_WARN, "%s::deallocating non-initialised merge SERDES state on "
                "da %d level %d: repeated call???\n",
                __FUNCTION__, da->id, level);
        return;
    }

    debug("%s::deallocating merge serdes state for da %d level %d\n",
            __FUNCTION__, da->id, level);

    BUG_ON(!da->levels[level].merge.serdes.mstore_entry);
    BUG_ON(!merge->out_tree);
    BUG_ON(!da->levels[level].merge.serdes.out_tree);
    BUG_ON(merge->out_tree != da->levels[level].merge.serdes.out_tree);
    da->levels[level].merge.serdes.out_tree=NULL;
    castle_kfree(da->levels[level].merge.serdes.mstore_entry);
    da->levels[level].merge.serdes.mstore_entry=NULL;
    castle_kfree(da->levels[level].merge.serdes.in_tree_mstore_entry_arr);
    da->levels[level].merge.serdes.in_tree_mstore_entry_arr=NULL;

    serdes_state = NULL_DAM_SERDES;
    atomic_set(&da->levels[level].merge.serdes.valid, (int)serdes_state);
}

static void castle_da_merge_dealloc(struct castle_da_merge *merge, int err)
{
    int i;
    c_merge_serdes_state_t serdes_state;

    if (!merge)
    {
        castle_printk(LOG_ERROR, "%s::[da %d level %d] no merge structure.\n",
                __FUNCTION__, merge->da->id, merge->level);
        return;
    }

    if (castle_version_states_free(&merge->version_states) != EXIT_SUCCESS)
    {
        castle_printk(LOG_ERROR, "%s::[da %d level %d] version_states not fully"
                " allocated.\n", __FUNCTION__, merge->da->id, merge->level);
        return;
    }

    BUG_ON(!merge->da);

    serdes_state = atomic_read(&merge->da->levels[merge->level].merge.serdes.valid);
    if (serdes_state > NULL_DAM_SERDES)
        mutex_lock(&merge->da->levels[merge->level].merge.serdes.mutex);

    /* Release the last leaf node c2b. */
    if (merge->last_leaf_node_c2b)
        put_c2b(merge->last_leaf_node_c2b);

    /* Free all the buffers */
    if (merge->snapshot_delete.occupied)
        castle_kfree(merge->snapshot_delete.occupied);
    if (merge->snapshot_delete.need_parent)
        castle_kfree(merge->snapshot_delete.need_parent);

    for(i=0; i<MAX_BTREE_DEPTH; i++)
    {
        c2_block_t *c2b = merge->levels[i].node_c2b;
        if(c2b)
        {
            debug("%s::putting c2b of btree node %i "cep_fmt_str" for da %d level %d.\n",
                __FUNCTION__, i, cep2str(c2b->cep), merge->da->id, merge->level);
            /* leaf nodes remain locked throughout a merge */
            if(i==0)
                write_unlock_c2b(c2b);
            else
                BUG_ON(c2b_write_locked(c2b));
            put_c2b(c2b);
        }
    }
    FOR_EACH_MERGE_TREE(i, merge)
        castle_da_iterator_destroy(merge->in_trees[i], merge->iters[i]);
    if (merge->iters)
        castle_kfree(merge->iters);
    if (merge->merged_iter)
        castle_ct_merged_iter_cancel(merge->merged_iter);

    if(!err)
    {
        if(serdes_state > NULL_DAM_SERDES)
        {
            debug("%s::Merge (da id=%d, level=%d) completed; "
                    "deallocating merge serialisation state.\n",
                    __FUNCTION__, merge->da->id, merge->level);
            castle_da_merge_serdes_dealloc(merge);
        }

        debug("Destroying old CTs.\n");
        /* If succeeded at merging, old trees need to be destroyed (they've already been removed
           from the DA by castle_da_merge_package(). */
        FOR_EACH_MERGE_TREE(i, merge)
            castle_ct_put(merge->in_trees[i], 0);
        if (merge->nr_entries == 0)
        {
            castle_printk(LOG_WARN, "Empty merge at level: %u\n", merge->level);
            castle_ct_put(merge->out_tree, 0);
        }
    }
    else
    {
        /* merge either aborted (ret==-ESHUTDOWN) or failed. */

        int merge_out_tree_retain=0; /* if this is set, state will be left for the final checkpoint
                                        to write to disk, and cleanup duty will be left to
                                        da_dealloc. */

        /* Retain extents, if we are checkpointing merges and interrupting the merge. */
        /* Note: Don't retain extents, if DA is already marked for deletion. */
        if( (err==-ESHUTDOWN) && (!castle_da_deleted(merge->da)) && (serdes_state > NULL_DAM_SERDES) )
        {
            /* merge aborted, we are checkpointing, and this is a checkpointable merge level */

            /* It is possible to abort a merge even before doing a unit of work, so the following
               check is necessary before calling serdes_out_tree_check since serialisation
               state is lazily initialized (i.e. alloc and init on first write) and out_tree_check
               assumes serialisation state is valid (which of course implies it's initialized etc).
            */
            /* No need for mutex - we are merge thread, noone else should ever change ser state
               besides the freshness flag, which is irrelevant here */
            if( (serdes_state == VALID_AND_FRESH_DAM_SERDES) ||
                    (serdes_state == VALID_AND_STALE_DAM_SERDES) )
                castle_da_merge_serdes_out_tree_check(
                        merge->da->levels[merge->level].merge.serdes.mstore_entry,
                        merge->da,
                        merge->level);

            merge_out_tree_retain=1;
            debug("%s::leaving output extents for merge %p deserialisation "
                    "(da %d, level %d).\n", __FUNCTION__, merge, merge->da->id, merge->level);
        }
        else if (serdes_state > NULL_DAM_SERDES)
        {
            /* merge failed, we are checkpointing, and this is a checkpointable merge level */
            castle_printk(LOG_ERROR, "Merge %p (da id=%d, level=%d) failed with err=%d "
                    "(!=0, and !=-ESHUTDOWN); deallocating merge serialisation state.\n",
                    merge, merge->da->id, merge->level, err);
            castle_da_merge_serdes_dealloc(merge);
        }

        if (merge->out_tree->bloom_exists)
        {
            /* Abort (i.e. free) incomplete bloom filters */
            castle_bloom_abort(&merge->out_tree->bloom);
            if(!merge_out_tree_retain)
                castle_bloom_destroy(&merge->out_tree->bloom); /* free the extent */
            merge->out_tree->bloom_exists=0;
        }

        /* If we don't need to retain list of large objects for checkpoint writeback, free the
           list so we don't encounter a ref_count sanity check BUG. If we do need to save it
           for checkpoint, it will be freed in da_dealloc. */
        if(!merge_out_tree_retain)
        {
            mutex_lock(&merge->out_tree->lo_mutex);
            castle_ct_large_objs_remove(&merge->out_tree->large_objs);
            mutex_unlock(&merge->out_tree->lo_mutex);
        }
        else
        {
            struct list_head *lh, *tmp;
            int lo_count=0;
            mutex_lock(&merge->out_tree->lo_mutex);
            list_for_each_safe(lh, tmp, &merge->out_tree->large_objs)
            {
                struct castle_large_obj_entry *lo =
                    list_entry(lh, struct castle_large_obj_entry, list);
                int lo_ref_cnt = castle_extent_link_count_get(lo->ext_id);
                /* we expect the input cct and output cct to both have reference to the LO ext */
                BUG_ON(lo_ref_cnt < 2);
                lo_count++;
            }
            mutex_unlock(&merge->out_tree->lo_mutex);
            debug("%s::leaving %d large objects for checkpoint of merge %p "
                    "(da %d, level %d).\n", __FUNCTION__, lo_count, merge, merge->da->id,
                    merge->level);
        }

        /* Always free the list of new large_objs; we don't want to write them out because they
           won't correspond to serialised state. */
        castle_ct_large_objs_remove(&merge->new_large_objs);

        /* Free the component tree, if one was allocated. */
        if(merge->out_tree)
        {
            BUG_ON(atomic_read(&merge->out_tree->write_ref_count) != 0);
            BUG_ON(atomic_read(&merge->out_tree->ref_count) != 1);
            if(!merge_out_tree_retain)
            {
                castle_ct_put(merge->out_tree, 0);
                merge->out_tree=NULL;
            }
        }
    }

    if (serdes_state > NULL_DAM_SERDES)
        mutex_unlock(&merge->da->levels[merge->level].merge.serdes.mutex);

    /* Free the merged iterator, if one was allocated. */
    if (merge->merged_iter)
        castle_kfree(merge->merged_iter);

    castle_kfree(merge);
}

static int castle_da_merge_progress_update(struct castle_da_merge *merge, uint32_t unit_nr)
{
    uint64_t items_completed, total_items, unit_items;
    uint32_t total_units;
    int i;

    /* If the merge is not marked for deamortization complete the merge. */
    if (!merge->da->levels[merge->level].merge.deamortize)
        return 0;

    total_units = 1 << merge->level;
    /* Don't stop the last merge unit, let it run out of iterator. */
    if(unit_nr >= total_units)
        return 0;
    /* Otherwise, check whether we've got far enough. */
    total_items = 0;
    FOR_EACH_MERGE_TREE(i, merge)
        total_items  += atomic64_read(&merge->in_trees[i]->item_count);
    unit_items   = total_items * (uint64_t)unit_nr / (uint64_t)total_units;
    items_completed = merge->merged_iter->src_items_completed;
    if(items_completed >= unit_items)
        return 1;
    return 0;
}

/**
 * Is entry from a version marked for deletion that has no descendant keys.
 *
 * @param merge [in]    merge stream that entry comes from
 * @param key [in]      key of the entry
 * @param version [in]  version of the entry
 *
 * @return 1            Entry can be skipped
 *
 * @also castle_version_is_deletable
 */
static int castle_da_entry_skip(struct castle_da_merge *merge,
                                void *key,
                                c_ver_t version)
{
    struct castle_btree_type *btree = merge->out_btree;
    struct castle_version_delete_state *state = &merge->snapshot_delete;
    void *last_key = merge->last_key;

    merge->is_new_key = (last_key)? btree->key_compare(key, last_key): 1;
    /* Compare the keys. If looking at new key then reset data
     * structures. */
    if (merge->is_new_key)
    {
        int nr_bytes = state->last_version/8 + 1;

        memset(state->occupied, 0, nr_bytes);
        memset(state->need_parent, 0, nr_bytes);
        state->next_deleted = NULL;
    }

    return castle_version_is_deletable(state, version, merge->is_new_key);
}

/**
 * Deals with deletable counter, by 'pushing' its value to all direct children (so
 * grand-children etc won't be modified) of the counter version.
 *
 * 'Push' is performed according to the usual counter reduction semantics (i.e.
 * it'll only have an effect if the child is a counter add).
 *
 * Its assumed that all the children will exist in a single (current) leaf btree node.
 */
static void castle_da_counter_delete(struct castle_da_merge *merge,
                                     void *key,
                                     c_ver_t version,
                                     c_val_tup_t cvt)
{
    c_val_tup_t entry_cvt, accumulator_cvt;
    c_ver_t child_version, entry_version;
    struct castle_da_merge_level *level;
    struct castle_btree_type *btree;
    struct castle_btree_node *node;
    void *entry_key;
    int idx, ret;

    castle_printk(LOG_DEBUG, "Deleting a counter, merge %p\n", merge);
    /* Init vars. */
    level = &merge->levels[0];
    btree = merge->out_btree;

    /* If the node doesn't exist, it means there are no descendants to worry about. */
    if(!level->node_c2b)
        return;

    castle_printk(LOG_DEBUG, "Starting from idx=%d\n", level->next_idx-1);
    node = c2b_bnode(level->node_c2b);
    child_version = INVAL_VERSION;
    /* Go through entries in the node, accumulate the relevant ones. */
    for(idx=level->next_idx-1; idx>=0; idx--)
    {
        btree->entry_get(node, idx, &entry_key, &entry_version, &entry_cvt);
        castle_printk(LOG_DEBUG, "Idx=%d, entry_version=%d, cvt.type=%d\n",
            idx, entry_version, entry_cvt.type);

        /* If we reached another (smaller) key, we can terminate. */
        ret = btree->key_compare(entry_key, key);
        castle_printk(LOG_DEBUG, "Key compare=%d\n", ret);
        BUG_ON(ret > 0);
        if(ret < 0)
            return;

        castle_printk(LOG_DEBUG, "Deleted version=%d, entry version=%d, child_version=%d\n",
            version, entry_version, child_version);
        /* If we reached a version which isn't a descendant of the deleted version
           we must have dealt with entire subtree of the deleted version. Exit. */
        if(!castle_version_is_ancestor(version, entry_version))
            return;

        castle_printk(LOG_DEBUG, "Entry version is descendant of the deleted version.\n");
        /* Go to the next entry if current entry is a descendant of the current
           'child' version, which already accumulated the deleted entry. */
        if(!VERSION_INVAL(child_version) &&
            castle_version_is_ancestor(child_version, entry_version))
            continue;

        castle_printk(LOG_DEBUG, "Entry version is a direct child.\n");
        /* We reached a direct child (in this array) of the deleted version,
           do the accumulation, and update vars.
           If the entry isn't a counter add, accumulation is a noop.
         */
        child_version = version;
        if(!CVT_ADD_COUNTER(entry_cvt))
            continue;

        castle_printk(LOG_DEBUG, "Entry cvt is an add.\n");
        /* Accumulation is neccessary. Accumulate entry_cvt first. */
        CVT_COUNTER_LOCAL_ADD_INIT(accumulator_cvt, 0);
        ret = castle_counter_simple_reduce(&accumulator_cvt, entry_cvt);
        /* We know that entry_cvt is an add, therefore accumulation musn't terminate. */
        BUG_ON(ret);
        /* Accumulate deleted cvt next. */
        castle_counter_simple_reduce(&accumulator_cvt, cvt);
        castle_printk(LOG_DEBUG, "Accumulated to 0x%llx.\n", accumulator_cvt.counter);
        /* Write out the updated entry. */
        btree->entry_replace(node, idx, key, entry_version, accumulator_cvt);
    }
}

static int castle_da_merge_unit_do(struct castle_da_merge *merge, uint32_t unit_nr)
{
    void *key;
    c_ver_t version;
    c_val_tup_t cvt;
    int ret;
#ifdef CASTLE_PERF_DEBUG
    struct timespec ts_start, ts_end;
#endif

    while (castle_iterator_has_next_sync(&castle_ct_merged_iter, merge->merged_iter))
    {
        cv_nonatomic_stats_t stats;

        might_resched();

        /* @TODO: we never check iterator errors. We should! */

        castle_perf_debug_getnstimeofday(&ts_start);
        castle_ct_merged_iter_next(merge->merged_iter, &key, &version, &cvt);
        castle_perf_debug_getnstimeofday(&ts_end);
        castle_perf_debug_bump_ctr(merge->merged_iter_next_ns, ts_end, ts_start);
        debug("Merging entry id=%lld: k=%p, *k=%d, version=%d, cep="cep_fmt_str_nl,
                i, key, *((uint32_t *)key), version, cep2str(cvt.cep));
        BUG_ON(CVT_INVALID(cvt));

        /* Start with merged iterator stats (see castle_da_each_skip()). */
        stats = merge->merged_iter->stats;

        /* Skip entry if version marked for deletion and no descendant keys. */
        if (castle_da_entry_skip(merge, key, version))
        {
            /* If a counter is being deleted, it needs to be pushed to its
               descendants, otherwise we would loose its contribution. */
            if (CVT_ANY_COUNTER(cvt))
                castle_da_counter_delete(merge, key, version, cvt);
            /* Update per-version and merge statistics.
             *
             * We do not need to decrement keys/tombstones for level 1 merges
             * as these keys have not yet been accounted for; skip them. */
            merge->skipped_count++;
            stats.version_deletes++;
            if (CVT_TOMBSTONE(cvt))
                stats.tombstones--;
            else
                stats.keys--;
            castle_version_live_stats_adjust(version, stats);
            if (merge->level == 1)
            {
                /* Key & tombstones inserts have not been accounted for in
                 * level 1 merges so don't record removals. */
                stats.keys = 0;
                stats.tombstones = 0;
            }
            castle_version_private_stats_adjust(version, stats, &merge->version_states);

            /*
             * The skipped key gets freed along with the input extent.
             */

            goto entry_done;
        }

        /* Update merge serialisation state. */
        if ((castle_merges_checkpoint) &&
            ((merge->level >= MIN_DA_SERDES_LEVEL) || (merge->level == BIG_MERGE)) )
            castle_da_merge_serialise(merge);

        /* Add entry to the output btree.
         *
         * - Add to level 0 node (and recurse up the tree)
         * - Update the bloom filter */
        castle_da_entry_add(merge, 0, key, version, cvt, 0);
        if (merge->out_tree->bloom_exists)
            castle_bloom_add(&merge->out_tree->bloom, merge->out_btree, key);

        /* Update per-version and merge statistics.
         * We are starting with merged iterator stats (from above). */
        merge->nr_entries++;
        if (merge->level == 1)
        {
            /* Live stats to reflect adjustments by castle_da_each_skip(). */
            castle_version_live_stats_adjust(version, stats);

            /* Key & tombstone inserts have not been accounted for in private
             * level 1 merge version stats.  Zero any stat adjustments made in
             * castle_da_each_skip() and perform accounting now. */
            stats.keys = 0;
            stats.tombstones = 0;

            if (CVT_TOMBSTONE(cvt))
                stats.tombstones++;
            else
                stats.keys++;

            castle_version_private_stats_adjust(version, stats, &merge->version_states);
        }
        else
        {
            castle_version_live_stats_adjust(version, stats);
            castle_version_private_stats_adjust(version, stats, &merge->version_states);
        }

        /* Try to complete node. */
        castle_perf_debug_getnstimeofday(&ts_start);
        ret = castle_da_nodes_complete(merge);
        castle_perf_debug_getnstimeofday(&ts_end);
        castle_perf_debug_bump_ctr(merge->nodes_complete_ns, ts_end, ts_start);
        if (ret != EXIT_SUCCESS)
            goto err_out;

entry_done:
        castle_perf_debug_getnstimeofday(&ts_start);
        castle_da_merge_budget_consume(merge);
        castle_perf_debug_getnstimeofday(&ts_end);
        castle_perf_debug_bump_ctr(merge->budget_consume_ns, ts_end, ts_start);
        /* Update the progress, returns non-zero if we've completed the current unit. */
        castle_perf_debug_getnstimeofday(&ts_start);

        /* TODO@tr The 2nd line of the following condition is a hack that allows big merges to
           abort. Do we really want to do this? */
        if(castle_da_merge_progress_update(merge, unit_nr) ||
            ( (merge->level == BIG_MERGE) && (castle_da_exiting || castle_da_deleted(merge->da)) )  )
        {
            castle_perf_debug_getnstimeofday(&ts_end);
            castle_perf_debug_bump_ctr(merge->progress_update_ns, ts_end, ts_start);
            return EAGAIN;
        }

        FAULT(MERGE_FAULT);
    }

    /* If we got few number of entries than the number of units. We might need to do few empty units
     * at the end to be in sync with other merges. */
    if (unit_nr != (1U << merge->level))
    {
        debug_merges("Going through an empty unit\n");
        return EAGAIN;
    }

    /* Return success, if we are finished with the merge. */
    return EXIT_SUCCESS;

err_out:
    /* While we handle it, merges should never fail.
     *
     * Per-version statistics will now be inconsistent. */
    WARN_ON(1);
    if (ret)
        castle_printk(LOG_WARN, "Merge failed with %d\n", ret);
    castle_da_merge_dealloc(merge, ret);

    return ret;
}

static inline void castle_da_merge_token_return(struct castle_double_array *da,
                                                int level,
                                                struct castle_merge_token *token)
{
    int driver_level;

    BUG_ON(read_can_lock(&da->lock));
    BUG_ON(token->ref_cnt <= 0);
    driver_level = token->driver_level;
    token->ref_cnt--;
    if(token->ref_cnt == 0)
    {
        /* Return the token to the driver level => anihilate the token. */
        BUG_ON(da->levels[driver_level].merge.driver_token != token);
        da->levels[driver_level].merge.driver_token = NULL;
        token->driver_level = -1;
        token->ref_cnt      = 0;
        list_add(&token->list, &da->merge_tokens);
    }
}

static inline void castle_da_merge_token_push(struct castle_double_array *da,
                                              int level,
                                              struct castle_merge_token *token)
{
    BUG_ON(read_can_lock(&da->lock));
    /* Token push moves the token to the next level, if that level is in a merge,
       or returns it to the driver level if not. */
    BUG_ON(level+1 >= MAX_DA_LEVEL);
    token->ref_cnt++;
    if( (level+1 < MAX_DA_LEVEL-1) && (da->levels[level+1].nr_trees >= 2) )
        list_add(&token->list, &da->levels[level+1].merge.merge_tokens);
    else
        castle_da_merge_token_return(da, level, token);
}

static inline void castle_da_merge_token_activate(struct castle_double_array *da,
                                                  int level,
                                                  struct castle_merge_token *token)
{
    BUG_ON(read_can_lock(&da->lock));
    /* Token is activated by pushing it to the next level up, and saving it as the active
       token at this level. */
    BUG_ON(level+1 >= MAX_DA_LEVEL);
    /* Take a ref for this active token. */
    token->ref_cnt++;
    da->levels[level].merge.active_token = token;
    /* Attempt to push it to the higher level. */
    castle_da_merge_token_push(da, level, token);
}

static inline struct castle_merge_token* castle_da_merge_token_get(struct castle_double_array *da,
                                                                   int level)
{
    struct castle_merge_token *token;

    if(list_empty(&da->levels[level].merge.merge_tokens))
        return NULL;

    token = list_first_entry(&da->levels[level].merge.merge_tokens,
                              struct castle_merge_token,
                              list);
    /* Remove the token from list of inactive tokens. */
    list_del(&token->list);

    return token;
}

static inline struct castle_merge_token* castle_da_merge_token_generate(struct castle_double_array *da,
                                                                        int level)
{
    struct castle_merge_token *token;

    BUG_ON(list_empty(&da->merge_tokens));
    BUG_ON(da->levels[level].merge.driver_token);
    /* Get a token out of the list. */
    token = list_first_entry(&da->merge_tokens, struct castle_merge_token, list);
    list_del(&token->list);
    /* Initialise the token. */
    token->driver_level = level;
    token->ref_cnt      = 0;
    /* Save the token as our driver token. */
    da->levels[level].merge.driver_token = token;

    return token;
}

#define exit_cond (castle_da_exiting || castle_da_deleted(da))
static inline int castle_da_merge_wait_event(struct castle_double_array *da, int level)
{
    int32_t this_level_units, prev_level_units, nr_trees, backlog;
    struct castle_merge_token *token;
    int not_ready_wake;

    not_ready_wake = 0;
    /* Protect the reads/updates to merge variables with DA lock. */
    write_lock(&da->lock);
    /* If the merge isn't deamortised (total merges only), start immediately. */
    if (!da->levels[level].merge.deamortize)
    {
        BUG_ON(level != BIG_MERGE);
        da->levels[level].merge.units_commited++;
        write_unlock(&da->lock);
        return 1;
    }

    this_level_units = da->levels[level].merge.units_commited;
    /* Level 1 merges don't have any merges happening below. */
    prev_level_units = (level == 1) ? 0 : da->levels[level-1].merge.units_commited;
    nr_trees = da->levels[level].nr_trees;
    BUG_ON(nr_trees < 2);
    /* Backlog is - work to be done - work completed. */
    backlog = (1U << (level - 1)) * (nr_trees - 2) + prev_level_units - this_level_units;

    debug_merges("Checking whether to merge the next unit. tlu=%d, plu=%d, nt=%d\n",
            this_level_units, prev_level_units, nr_trees);

    /* We should not have any active tokens (tokens are returned to the driver merge on unit
       complete). */
    BUG_ON(da->levels[level].merge.active_token != NULL);

    /* If we have merge backlog of more than 1 unit, schedule it without any further checks. */
    if(exit_cond || ((level != 1) && (backlog > 1)))
    {
        debug_merges("Unthrottled merge.\n");
        goto ready_out;
    }

    /* Otherwise, there are two cases. Either this merge is a driver merge, or not. */
    if ((level == da->driver_merge) && (level == 1 || da->levels[level-1].nr_trees < 2))
    {
        debug_merges("This is a driver merge.\n");
        /* Return any tokens that we may have. Should that actually every happen?. */
        while((token = castle_da_merge_token_get(da, level)))
        {
            castle_printk(LOG_WARN, "WARNING: merge token in a driver merge!.\n");
            castle_da_merge_token_return(da, level, token);
            not_ready_wake = 1;
        }
        /* If we are a driver merge, check whether we can generate a token to make progress. */
        if(da->levels[level].merge.driver_token != NULL)
        {
            debug_merges("The merge has an outstanding driver token.\n");
            goto not_ready_out;
        }
        /* Generate the token. */
        token = castle_da_merge_token_generate(da, level);
        /* Activate the token. */
        castle_da_merge_token_activate(da, level, token);
        goto ready_out;
    }

    /* We are not driving merges, and the backlog <= 1. We are only allowed to make progress
       if backlog==1 _and_ we can activate a token. */
    if(backlog == 1)
    {
        token = castle_da_merge_token_get(da, level);
        if(!token)
        {
            debug_merges("Backlog of 1, but no token.\n");
            goto not_ready_out;
        }

        debug_merges("Deamortised merge currently at %d units, token from driver level %d.\n",
                this_level_units, token->driver_level);
        /* Activate the token. */
        castle_da_merge_token_activate(da, level, token);
        /* We already had a ref to this token, before doing activate. Activate took one more,
           return one of them back. */
        BUG_ON(token->ref_cnt < 2);
        /* This only does ref_cnt--, because ref_cnt is >= 2 */
        castle_da_merge_token_return(da, level, token);

        goto ready_out;
    }

    debug_merges("The merge is ahead (backlog=%d)\n", backlog);
    /* We are not driving merges, and the backlog <= 0. We are therefore ahead of other merges,
       and therefore we shoud not hold on to any tokens we may have on our inactive token list. */
    BUG_ON(backlog > 0);
    while((token = castle_da_merge_token_get(da, level)))
    {
        debug_merges("Pushing token for driver_level=%d\n", token->driver_level);
        castle_da_merge_token_push(da, level, token);
        /* We are getting rid of the token, therefore we must drop the ref to it. */
        castle_da_merge_token_return(da, level, token);
        not_ready_wake = 1;
    }

not_ready_out:
    write_unlock(&da->lock);
    if(not_ready_wake)
        wake_up(&da->merge_waitq);
    return 0;

ready_out:
    da->levels[level].merge.units_commited = this_level_units+1;
    write_unlock(&da->lock);
    wake_up(&da->merge_waitq);
    return 1;
}

static inline uint32_t castle_da_merge_units_inc_return(struct castle_double_array *da, int level)
{
    int ignore;

    /* Wait until we are allowed to proceed with the merge. */
    __wait_event_interruptible(da->merge_waitq, castle_da_merge_wait_event(da, level), ignore);
    debug_merges("Merging unit %d.\n", da->levels[level].merge.units_commited);

    return da->levels[level].merge.units_commited;
}

static inline void castle_da_merge_unit_complete(struct castle_double_array *da, int level)
{
    struct castle_merge_token *token;

    debug_merges("Completing unit %d\n", da->levels[level].merge.units_commited);
    BUG_ON(read_can_lock(&da->lock));
    /* We'll be looking at level+1, make sure we don't go out of bounds. */
    BUG_ON(level+1 >= MAX_DA_LEVEL);

    /* Return the token back to the driver merge, if we've got one. */
    if((token = da->levels[level].merge.active_token))
    {
        debug_merges("Returning an active merge token for driver_level=%d\n", token->driver_level);
        castle_da_merge_token_return(da, level, token);
        da->levels[level].merge.active_token = NULL;
    }
    /* Wakeup everyone waiting on merge state update. */
    wake_up(&da->merge_waitq);
}

static inline void castle_da_merge_intermediate_unit_complete(struct castle_double_array *da,
                                                              int level)
{
    write_lock(&da->lock);
    castle_da_merge_unit_complete(da, level);
    write_unlock(&da->lock);
}

/**
 *
 * WARNING: Caller must hold da write lock.
 */
static inline void castle_da_driver_merge_reset(struct castle_double_array *da)
{
    int level;

    /* Function should be called with DA locked. */
    BUG_ON(read_can_lock(&da->lock));
    if (!castle_dynamic_driver_merge)
    {
        da->driver_merge = 1;
        return;
    }

    /* Set the lowest level with two fullygrown trees as driver. */
    for (level=1; level<MAX_DA_LEVEL; level++)
    {
        /* Compute driver merge from currently ongong merges only. */
        if (castle_da_merge_running(da, level) && (da->levels[level].nr_trees >= 2))
        {
            if (level != da->driver_merge)
                castle_printk(LOG_INFO, "Changing driver merge %d -> %d\n",
                        da->driver_merge, level);
            da->driver_merge = level;
            break;
        }
    }

    wake_up(&da->merge_waitq);
}

static int __castle_da_driver_merge_reset(struct castle_double_array *da, void *unused)
{
    write_lock(&da->lock);
    castle_da_driver_merge_reset(da);
    write_unlock(&da->lock);

    return 0;
}

/**
 * Computes the appropriate level to put the output array from a total merge.
 */
static int castle_da_total_merge_output_level_get(struct castle_double_array *da,
                                                  struct castle_component_tree *out_tree)
{
    int i, nr_units, unit_is_tree, out_tree_level;

    /* DA should be write locked => we shouldn't be able to read lock. */
    BUG_ON(read_can_lock(&da->lock));
    out_tree_level = 1;
    /* Take either MAX_DYNAMIC_TREE_SIZE or MAX_DYNAMIC_DATA_SIZE as unit - based on
     * which part of the out_tree is bigger. */
    unit_is_tree = (atomic64_read(&out_tree->tree_ext_free.used) >
                    atomic64_read(&out_tree->data_ext_free.used));

    /* Calculate the output size (in terms of # of units). */
    nr_units = (unit_is_tree)?
               atomic64_read(&out_tree->tree_ext_free.used) / (MAX_DYNAMIC_TREE_SIZE * C_CHK_SIZE):
               atomic64_read(&out_tree->data_ext_free.used) / (MAX_DYNAMIC_DATA_SIZE * C_CHK_SIZE);

    /* Calculate the level it should go. Logarithm of nr_units. */
    out_tree_level = order_base_2(nr_units);
    /* Total merge output _must_ be put in level 2+, because we don't want to mix different tree
       types in level 1, and of course we don't want to put it in level 0 either. */
    if(out_tree_level <= 1)
        out_tree_level = 2;
    castle_printk(LOG_INFO, "Total merge: #units: %d, size appropriate for level: %d\n",
                   nr_units, out_tree_level);

    /* Find the highest level in DA with trees. */
    for (i=MAX_DA_LEVEL-1; i>0; i--)
        if (da->levels[i].nr_trees)
            break;

    /* Compacted tree should always be placed on top of all the trees. It contains the oldest
     * data. */
    if (i >= out_tree_level)
        out_tree_level = i + 1;

    /* Max level should never cross MAX_DA_LEVEL-1. */
    if (out_tree_level >= MAX_DA_LEVEL)
        out_tree_level = MAX_DA_LEVEL - 1;

    if (out_tree_level)
    castle_printk(LOG_INFO, "Outputting at level: %d\n", out_tree_level);

    return out_tree_level;
}

static tree_seq_t castle_da_merge_last_unit_complete(struct castle_double_array *da,
                                                     int level,
                                                     struct castle_da_merge *merge)
{
    struct castle_component_tree *out_tree;
    struct castle_merge_token *token;
    struct list_head *head = NULL;
    tree_seq_t out_tree_id;
    int i;

    out_tree = castle_da_merge_complete(merge);
    if(!out_tree)
        return INVAL_TREE;

    out_tree_id = out_tree->seq;
    /* If we succeeded at creating the last tree, remove the in_trees, and add the out_tree.
       All under appropriate locks. */

    /* Get the lock. */
    write_lock(&da->lock);
    /* Notify interested parties about merge completion, _before_ moving trees around. */
    castle_da_merge_unit_complete(da, level);

    /* Delete the old trees from DA list.
       Note 1: Old trees may still be used by IOs and will only be destroyed on the last ct_put.
               But we want to remove it from the DA straight away. The out_tree now takes over
               their functionality.
       Note 2: DA structure modifications don't race with checkpointing because transaction lock
               is taken.
     */
    FOR_EACH_MERGE_TREE(i, merge)
    {
        BUG_ON(merge->da->id != merge->in_trees[i]->da);
        castle_component_tree_del(merge->da, merge->in_trees[i]);
    }

    /* If this was a total merge, the output level needs to be computed.
       Otherwise the level should already be set to the next level up. */
    if(level == BIG_MERGE)
    {
        int olevel = castle_da_total_merge_output_level_get(da, out_tree);

        /* Output level should never be as big as MAX_DA_LEVEL. */
        BUG_ON(olevel >= MAX_DA_LEVEL);

        /* Compact tree should always be added to non empty level except if the level is
         * MAX_DA_LEVEL-1*/
        BUG_ON((olevel != (MAX_DA_LEVEL-1)) && (!list_empty(&da->levels[olevel].trees)));

        /* Done with compaction. Time to reset reserved tree seq ID. */
        BUG_ON(da->compaction_ct_seq != out_tree->seq);
        da->compaction_ct_seq = INVAL_TREE;

        out_tree->level = olevel;

        /* Always add the compacted tree at the end. */
        head = da->levels[out_tree->level].trees.prev;
    }
    else
        BUG_ON(out_tree->level != level + 1);

    if (merge->nr_entries)
        castle_component_tree_add(merge->da, out_tree, head, 0 /*not in init*/);

    /* Reset the number of completed units. */
    BUG_ON(da->levels[level].merge.units_commited != (1U << level));
    da->levels[level].merge.units_commited = 0;
    /* Return any merge tokens we may still hold if we are not going to be doing more merges. */
    if(da->levels[level].nr_trees < 2)
    {
        while((token = castle_da_merge_token_get(da, level)))
        {
            debug_merges("Returning merge token from completed merge, driver_level=%d\n",
                    token->driver_level);
            castle_da_merge_token_return(da, level, token);
        }
    }
    /* Reduce the number of ongoing merges count. Don't do it for compaction. */
    if (level != 0)
        atomic_dec(&da->ongoing_merges);

    /* Release the lock. */
    write_unlock(&da->lock);

    castle_da_merge_restart(da, NULL);

    castle_printk(LOG_INFO, "Completed merge at level: %d and deleted %u entries\n",
            merge->level, merge->skipped_count);

    return out_tree_id;
}

/**
 * Initialize merge process for multiple component trees. Merges, other than
 * compaction, process on 2 trees only.
 *
 * @param da        [in]    doubling array to be merged
 * @param level     [in]    merge level in doubling array
 * @param nr_trees  [in]    number of trees to be merged
 * @param in_trees  [in]    component trees to be merged
 *
 * @return intialized merge structure. NULL in case of error.
 */
static struct castle_da_merge* castle_da_merge_init(struct castle_double_array *da,
                                                    int level,
                                                    int nr_trees,
                                                    struct castle_component_tree **in_trees)
{
    struct castle_btree_type *btree;
    struct castle_da_merge *merge = NULL;
    int i, ret;

    debug("%s::Merging ct=%d (dynamic=%d) with ct=%d (dynamic=%d)\n",
            __FUNCTION__,
            in_trees[0]->seq, in_trees[0]->dynamic, in_trees[1]->seq, in_trees[1]->dynamic);

    /* Sanity checks. */
    BUG_ON(nr_trees < 2);
    BUG_ON(da->levels[level].merge.units_commited != 0);
    BUG_ON((level != BIG_MERGE) && (nr_trees != 2));
    /* Work out what type of trees are we going to be merging. Bug if in_trees don't match. */
    btree = castle_btree_type_get(in_trees[0]->btree_type);
    for (i=0; i<nr_trees; i++)
    {
        /* Btree types may, and often will be different during big merges. */
        BUG_ON((level != BIG_MERGE) && (btree != castle_btree_type_get(in_trees[i]->btree_type)));
        BUG_ON((level != BIG_MERGE) && (in_trees[i]->level != level));
    }

    /* Malloc everything ... */
    ret = -ENOMEM;
    merge = castle_zalloc(sizeof(struct castle_da_merge), GFP_KERNEL);
    if (!merge)
        goto error_out;
    if (castle_version_states_alloc(&merge->version_states,
                castle_versions_count_get(da->id, CVH_TOTAL)) != EXIT_SUCCESS)
        goto error_out;
    merge->da                   = da;
    merge->out_btree            = castle_btree_type_get(RO_VLBA_TREE_TYPE);
    merge->level                = level;
    merge->nr_trees             = nr_trees;
    merge->in_trees             = in_trees;
    merge->out_tree             = NULL;
    merge->merged_iter          = NULL;
    merge->root_depth           = -1;
    merge->last_leaf_node_c2b   = NULL;
    merge->last_key             = NULL;
    merge->completing           = 0;
    merge->nr_entries           = 0;
    merge->large_chunks         = 0;
    merge->budget_cons_rate     = 1;
    merge->budget_cons_units    = 0;
    merge->is_new_key           = 1;
    merge->skipped_count        = 0;
    for (i = 0; i < MAX_BTREE_DEPTH; i++)
    {
        merge->levels[i].last_key      = NULL;
        merge->levels[i].next_idx      = 0;
        merge->levels[i].valid_end_idx = -1;
        merge->levels[i].valid_version = INVAL_VERSION;
    }

    /* Deserialise ongoing merge state */
    /* only reason a lock might be needed here would be if we were racing with double_array_read,
       which should never happen */
    if(da->levels[level].merge.serdes.des){
        castle_printk(LOG_DEBUG, "%s::found serialised merge in da %d level %d, attempting des\n",
                __FUNCTION__, da->id, level);
        castle_da_merge_des_check(merge, da, level, nr_trees, in_trees);
        castle_da_merge_deserialise(merge, da, level);
        merge->out_tree = da->levels[level].merge.serdes.out_tree;
    }

    if(!merge->out_tree)
    {
        tree_seq_t ct_seq = INVAL_TREE;

        /* we cannot tolerate failure to recover an in-progress output ct if this is a
           deserialising merge */
        BUG_ON(da->levels[level].merge.serdes.des);

        if (level == BIG_MERGE)
            ct_seq = da->compaction_ct_seq;

        merge->out_tree = castle_ct_alloc(da, RO_VLBA_TREE_TYPE, level+1, ct_seq);
        if(!merge->out_tree)
            goto error_out;
        merge->out_tree->internal_ext_free.ext_id = INVAL_EXT_ID;
        merge->out_tree->tree_ext_free.ext_id = INVAL_EXT_ID;
        merge->out_tree->data_ext_free.ext_id = INVAL_EXT_ID;
        INIT_LIST_HEAD(&merge->out_tree->large_objs);
    }
    INIT_LIST_HEAD(&merge->new_large_objs);

#ifdef CASTLE_PERF_DEBUG
    merge->get_c2b_ns                   = 0;
    merge->merged_iter_next_ns          = 0;
    merge->da_medium_obj_copy_ns        = 0;
    merge->nodes_complete_ns            = 0;
    merge->budget_consume_ns            = 0;
    merge->progress_update_ns           = 0;
    merge->merged_iter_next_hasnext_ns  = 0;
    merge->merged_iter_next_compare_ns  = 0;
#endif
#ifdef CASTLE_DEBUG
    merge->is_recursion                 = 0;
#endif
    /* Bit-arrays for snapshot delete algorithm. */
    merge->snapshot_delete.last_version = castle_version_max_get();
    castle_printk(LOG_INFO, "MERGE Level: %d, #versions: %d\n",
            level, merge->snapshot_delete.last_version);
    merge->snapshot_delete.occupied     = castle_malloc(merge->snapshot_delete.last_version / 8 + 1,
                                                        GFP_KERNEL);
    if (!merge->snapshot_delete.occupied)
        goto error_out;
    merge->snapshot_delete.need_parent  = castle_malloc(merge->snapshot_delete.last_version / 8 + 1,
                                                        GFP_KERNEL);
    if (!merge->snapshot_delete.need_parent)
        goto error_out;
    merge->snapshot_delete.next_deleted = NULL;

    /* Iterators */
    ret = castle_da_iterators_create(merge); /* built-in handling of deserialisation, triggered by
                                                da->levels[].merge.serdes.des flag. */
    if(ret)
        goto error_out;

    if(!da->levels[level].merge.serdes.des)
    {
        ret = castle_da_merge_extents_alloc(merge);
        if(ret)
            goto error_out;
    }

    if(da->levels[level].merge.serdes.des)
    {
#ifdef DEBUG_MERGE_SERDES
        da->levels[level].merge.serdes.merge_completed=0;
#endif
        da->levels[level].merge.serdes.des=0;
        castle_printk(LOG_INIT, "Resuming merge on da %d level %d.\n", da->id, level);
    }
    return merge;

error_out:
    BUG_ON(!ret);
    castle_printk(LOG_ERROR, "%s::Failed a merge with ret=%d\n", __FUNCTION__, ret);
    castle_da_merge_dealloc(merge, ret);
    debug_merges("Failed a merge with ret=%d\n", ret);

    return NULL;
}

#ifdef CASTLE_PERF_DEBUG
static void castle_da_merge_cache_efficiency_stats_flush_reset(struct castle_double_array *da,
                                                               struct castle_da_merge *merge,
                                                               uint32_t units_cnt,
                                                           struct castle_component_tree *in_trees[])
{
    int i, percentage;
    int pref_chunks_not_up2date, pref_chunks_up2date;

    /* Btree (internal + leaf) cache efficiency. */
    percentage = 0;
    pref_chunks_not_up2date = 0;
    pref_chunks_up2date = 0;
    FOR_EACH_MERGE_TREE(i, merge)
    {
        c_ext_id_t ext_id;

        ext_id = in_trees[i]->internal_ext_free.ext_id;

        pref_chunks_not_up2date += castle_extent_not_up2date_get_reset(ext_id);
        pref_chunks_up2date += castle_extent_up2date_get_reset(ext_id);

        ext_id = in_trees[i]->tree_ext_free.ext_id;
        pref_chunks_not_up2date += castle_extent_not_up2date_get_reset(ext_id);
        pref_chunks_up2date += castle_extent_up2date_get_reset(ext_id);
    }
    if (pref_chunks_up2date)
        percentage = (100 * pref_chunks_up2date) / (pref_chunks_not_up2date + pref_chunks_up2date);

    if (pref_chunks_up2date || pref_chunks_not_up2date)
        castle_trace_da_merge_unit(TRACE_VALUE,
                                   TRACE_DA_MERGE_UNIT_CACHE_BTREE_EFFICIENCY_ID,
                                   da->id,
                                   merge->level,
                                   units_cnt,
                                   percentage);

    /* Medium object cache efficiency. */
    percentage = 0;
    pref_chunks_not_up2date = 0;
    pref_chunks_up2date = 0;
    FOR_EACH_MERGE_TREE(i, merge)
    {
        c_ext_id_t ext_id;

        ext_id = in_trees[i]->data_ext_free.ext_id;

        pref_chunks_not_up2date += castle_extent_not_up2date_get_reset(ext_id);
        pref_chunks_up2date += castle_extent_up2date_get_reset(ext_id);
    }
    if (pref_chunks_up2date)
        percentage = (100 * pref_chunks_up2date) / (pref_chunks_not_up2date + pref_chunks_up2date);

    if (pref_chunks_up2date || pref_chunks_not_up2date)
        castle_trace_da_merge_unit(TRACE_VALUE,
                                   TRACE_DA_MERGE_UNIT_CACHE_DATA_EFFICIENCY_ID,
                                   da->id,
                                   merge->level,
                                   units_cnt,
                                   percentage);
}

static void castle_da_merge_perf_stats_flush_reset(struct castle_double_array *da,
                                                   struct castle_da_merge *merge,
                                                   uint32_t units_cnt,
                                                   struct castle_component_tree *in_trees[])
{
    u64 ns;
    int i;

    /* Btree c2b_sync() time. */
    ns = 0;
    FOR_EACH_MERGE_TREE(i, merge)
    {
        ns += in_trees[i]->bt_c2bsync_ns;
        in_trees[i]->bt_c2bsync_ns = 0;
    }
    castle_trace_da_merge_unit(TRACE_VALUE,
                               TRACE_DA_MERGE_UNIT_C2B_SYNC_WAIT_BT_NS_ID,
                               da->id,
                               merge->level,
                               units_cnt,
                               ns);

    /* Data c2b_sync() time. */
    ns = 0;
    FOR_EACH_MERGE_TREE(i, merge)
    {
        ns += in_trees[i]->data_c2bsync_ns;
        in_trees[i]->data_c2bsync_ns = 0;
    }
    castle_trace_da_merge_unit(TRACE_VALUE,
                               TRACE_DA_MERGE_UNIT_C2B_SYNC_WAIT_DATA_NS_ID,
                               da->id,
                               merge->level,
                               units_cnt,
                               ns);

    /* castle_cache_block_get() time. */
    castle_trace_da_merge_unit(TRACE_VALUE,
                               TRACE_DA_MERGE_UNIT_GET_C2B_NS_ID,
                               da->id,
                               merge->level,
                               units_cnt,
                               merge->get_c2b_ns);
    merge->get_c2b_ns = 0;

    /* Merge time. */
    castle_trace_da_merge_unit(TRACE_VALUE,
                               TRACE_DA_MERGE_UNIT_MOBJ_COPY_NS_ID,
                               da->id,
                               merge->level,
                               units_cnt,
                               merge->da_medium_obj_copy_ns);
    merge->da_medium_obj_copy_ns = 0;

}
#endif /* CASTLE_PERF_DEBUG */


/**
 * Produce a serialisable "snapshot" of merge state. Saves state in merge->da.
 *
 * @param merge [in] in-flight merge.
 *
 * @also castle_da_merge_unit_do
 * @note blocks on serdes.mutex
 * @note positioning of the call is crucial: it MUST be after iter state is updated,
 *       and before the output tree is updated.
 */
static void castle_da_merge_serialise(struct castle_da_merge *merge)
{
    struct castle_double_array *da;
    int level;
    c_merge_serdes_state_t current_state;
    c_merge_serdes_state_t new_state;

    BUG_ON(!merge);
    BUG_ON(!merge->da);

    da=merge->da;
    level=merge->level;
    BUG_ON(level > MAX_DA_LEVEL);
    /* assert that we are not serialising merges on lower levels */
    BUG_ON((level < MIN_DA_SERDES_LEVEL) && (level != BIG_MERGE));
    BUG_ON(!merge->out_tree);

    current_state = atomic_read(&da->levels[level].merge.serdes.valid);
    /*
    Possible state transitions: (MT = this thread, CT = checkpoint thread)
        NULL_DAM_SERDES            -> INVALID_DAM_SERDES         [label="MT allocs"]
        INVALID_DAM_SERDES         -> INVALID_DAM_SERDES         [label="MT updates iter state or"]
        INVALID_DAM_SERDES         -> VALID_AND_FRESH_DAM_SERDES [label="MT found new key boundary, updates out cct state"]
        VALID_AND_FRESH_DAM_SERDES -> VALID_AND_STALE_DAM_SERDES [label="CT flushes extents"]
        VALID_AND_STALE_DAM_SERDES -> INVALID_DAM_SERDES         [label="MT updates iter state"]
    Note: CT writes merge state to mstore when state is VALID_AND_STALE or VALID_AND_FRESH, but if
          it is VALID_AND_STALE then it does not change SERDES state. When it is VALID_AND_FRESH
          then after checkpoint it changes state to VALID_AND_STALE.
    */

    BUG_ON(current_state >= MAX_DAM_SERDES);

    if( unlikely(current_state == NULL_DAM_SERDES ) )
    {
        /* first write - initialise */
        mutex_lock(&da->levels[level].merge.serdes.mutex);
        debug("%s::initialising mstore entry for merge %p in "
                "da %d, level %d\n", __FUNCTION__, merge, da->id, level);
        BUG_ON(da->levels[level].merge.serdes.mstore_entry);

        da->levels[level].merge.serdes.mstore_entry=
            castle_zalloc(sizeof(struct castle_dmserlist_entry), GFP_KERNEL);
        if(!da->levels[level].merge.serdes.mstore_entry) goto alloc_fail_1;

        /* Using vmalloc for the input tree merge state array...
           An array made up of non-contiguous pages sounds bad in this situation
           because in a heavily versioned workload there could be very frequent
           updates to the input tree merge state. However, each entry in the array
           is currently 80 bytes, which means one page can accomodate 50 trees worth
           of merge state. Therefore, the performance impact should be negligible.

           The counter argument: since one page can accomodate 50 trees, why
           even bother with vmalloc since malloc should be enough for all but the
           grandest of workloads? Because this vmalloc only happens once per merge
           at most, so we might as well. If it is felt that this is actually too
           costly, then an increase in MIN_DA_SERDES_LEVEL should be considered.
         */
        da->levels[level].merge.serdes.in_tree_mstore_entry_arr=
            castle_zalloc(sizeof(struct castle_in_tree_merge_state_entry)*merge->nr_trees,
            GFP_KERNEL);
        if(!da->levels[level].merge.serdes.in_tree_mstore_entry_arr) goto alloc_fail_2;

#ifdef DEBUG_MERGE_SERDES
        da->levels[level].merge.serdes.merge_completed=0;
#endif

        da->levels[level].merge.serdes.out_tree=merge->out_tree;
        castle_da_merge_marshall(da->levels[level].merge.serdes.mstore_entry,
                da->levels[level].merge.serdes.in_tree_mstore_entry_arr,
                merge,
                DAM_MARSHALL_ALL);

        new_state = INVALID_DAM_SERDES;
        atomic_set(&da->levels[level].merge.serdes.valid, (int)new_state);
        mutex_unlock(&da->levels[level].merge.serdes.mutex);
        return;
alloc_fail_2:
        castle_kfree(da->levels[level].merge.serdes.mstore_entry);
alloc_fail_1:
        castle_printk(LOG_ERROR, "%s::failed to malloc, partial merges disabled for this merge"
                "[da %d level %d] (warning: UNTESTED).\n", __FUNCTION__, da, level);
        //TODO@tr implement partial merges disabling
        return;
    }

    if( unlikely(current_state == INVALID_DAM_SERDES) )
    {
        mutex_lock(&da->levels[level].merge.serdes.mutex);
        BUG_ON(!da->levels[level].merge.serdes.mstore_entry);
        if( unlikely(merge->is_new_key) )
        {
            /* update output tree state */
            castle_da_merge_marshall(da->levels[level].merge.serdes.mstore_entry,
                    NULL, /* don't need it for this marshall mode */
                    merge,
                    DAM_MARSHALL_OUTTREE);

            debug("%s::found new_key boundary; existing serialisation for "
                    "da %d, level %d is now checkpointable, so stop updating it.\n",
                    __FUNCTION__, da->id, level);

            /* Commit and zero private stats to global crash-consistent tree. */
            castle_version_states_commit(&merge->version_states);

            /* mark serialisation as checkpointable, and no longer updatable */
            new_state = VALID_AND_FRESH_DAM_SERDES;
            atomic_set(&da->levels[level].merge.serdes.valid, (int)new_state);
            mutex_unlock(&da->levels[level].merge.serdes.mutex);
            return;
        }

        /* update iterator state */
        debug("%s::updating mstore entry for merge in "
                "da %d, level %d\n", __FUNCTION__, da->id, level);
        castle_da_merge_marshall(da->levels[level].merge.serdes.mstore_entry,
                da->levels[level].merge.serdes.in_tree_mstore_entry_arr,
                merge,
                DAM_MARSHALL_ITERS);

        mutex_unlock(&da->levels[level].merge.serdes.mutex);
        return;
    }

    if( unlikely(current_state == VALID_AND_STALE_DAM_SERDES) )
    {
        /* we just got back from checkpoint - so FORCE an update */
        mutex_lock(&da->levels[level].merge.serdes.mutex);
        BUG_ON(!da->levels[level].merge.serdes.mstore_entry);

        debug("%s::updating mstore entry for merge in "
                "da %d, level %d\n", __FUNCTION__, da->id, level);
        castle_da_merge_marshall(da->levels[level].merge.serdes.mstore_entry,
                da->levels[level].merge.serdes.in_tree_mstore_entry_arr,
                merge,
                DAM_MARSHALL_ITERS);

        new_state = INVALID_DAM_SERDES;
        atomic_set(&da->levels[level].merge.serdes.valid, (int)new_state);
        mutex_unlock(&da->levels[level].merge.serdes.mutex);
        return; /* state now 1,1 */
    }

    if( likely(current_state == VALID_AND_FRESH_DAM_SERDES) )
    {
        /* state 2,1 -- this is usually the most common case. */

        /* Noop: Wait for checkpoint to write existing serialisation point before updating it.
           This code branch is entered once per merge key... at the moment the cost is one atomic
           read, BUG_ONs aside.
        */

        BUG_ON(!da->levels[level].merge.serdes.mstore_entry);
        return;
    }

    /* all states should have been covered above */
    castle_printk(LOG_ERROR, "%s::should not have gotten here, with merge %p\n", __FUNCTION__, merge);
    BUG();
}

/**
 * Marshalls merge structure into a disk-writable mstore structure.
 *
 * @param merge_mstore [out] structure into which merge is packed
 * @param merge [in] merge state
 * @param partial_marshall [in] flag to indicate how much stuff to marshall;
 *        0 = everything, 1 = iterators, 2 = output tree
 *
 * @note no locking performed within, caller expected to lock
 * @note assumes merge is in serialisable state (e.g. new_key boundary)
 */
static void castle_da_merge_marshall(struct castle_dmserlist_entry *merge_mstore,
                                     struct castle_in_tree_merge_state_entry *in_tree_merge_mstore,
                                     struct castle_da_merge *merge,
                                     c_da_merge_marshall_set_t partial_marshall)
{
    unsigned int i;
    struct component_iterator *curr_comp;
    c_immut_iter_t *curr_immut;
    int lo_count=0;

    /* Try to catch (at compile-time) any screwups to dmserlist structure; if we fail to compile
       here, review struct castle_dmserlist_entry, MAX_BTREE_DEPTH, and
       SIZEOF_CASTLE_DMSERLIST_ENTRY */
    BUILD_BUG_ON( sizeof(struct castle_dmserlist_entry) != SIZEOF_CASTLE_DMSERLIST_ENTRY );
    /* ditto the input tree merge state structure */
    BUILD_BUG_ON( sizeof(struct castle_in_tree_merge_state_entry) !=
            SIZEOF_CASTLE_IN_TREE_MERGE_STATE_ENTRY );

    BUG_ON(!merge);
    BUG_ON(!merge_mstore);
    BUG_ON(!merge->merged_iter->iterators);

    if(unlikely(partial_marshall==DAM_MARSHALL_OUTTREE)) goto update_output_tree_state;

    /* iterators */
    /* iterator marshalling happens often... make it as cheap as possible! */

    debug("%s::merge %p (da %d, level %d) iterator marshall\n", __FUNCTION__,
            merge, merge->da->id, merge->level);

    merge_mstore->iter_err                    = merge->merged_iter->err;
    merge_mstore->iter_non_empty_cnt          = merge->merged_iter->non_empty_cnt;
    merge_mstore->iter_src_items_completed    = merge->merged_iter->src_items_completed;

    curr_comp = merge->merged_iter->iterators;
    for(i=0; i<merge->nr_trees; i++)
    {
        BUG_ON(!curr_comp);
        curr_immut=curr_comp->iterator;
        BUG_ON(!curr_immut);

        if(partial_marshall==DAM_MARSHALL_ALL)
        {
            /* this stuff should never change; per-tree once-per-merge marshalling done here */
            in_tree_merge_mstore[i].pos_in_merge_struct = i;
            in_tree_merge_mstore[i].seq                 = merge->in_trees[i]->seq;
            in_tree_merge_mstore[i].da_id               = merge->da->id;
            in_tree_merge_mstore[i].level               = merge->level;

            /* make sure we haven't mismatched trees and iterators */
            BUG_ON(curr_immut->tree->seq != in_tree_merge_mstore[i].seq);
        }
        else
            BUG_ON(in_tree_merge_mstore[i].pos_in_merge_struct != i);


        /* There are 'completed' flags for each immut iterator within c_immut_iter_t and also
           c_merged_iter_t. This seems like a duplicate so we serialise only 1 and restore the other
           on deserialisation. If the following BUGs, then the completed flags can have different
           states at serialisation boundary, which means we have to handle both seperately. */
        BUG_ON(curr_immut->completed != curr_comp->completed);
        in_tree_merge_mstore[i].iter.component_completed         = curr_comp->completed;
        in_tree_merge_mstore[i].iter.component_cached            = curr_comp->cached;
        in_tree_merge_mstore[i].iter.immut_curr_idx              = curr_immut->curr_idx;
        in_tree_merge_mstore[i].iter.immut_cached_idx            = curr_immut->cached_idx;
        in_tree_merge_mstore[i].iter.immut_next_idx              = curr_immut->next_idx;

        if(curr_immut->curr_c2b)
            in_tree_merge_mstore[i].iter.immut_curr_c2b_cep = curr_immut->curr_c2b->cep;
        else
        {
            /* the only known valid situation in which an immutable iterator may not have curr_c2b
               is when it is completed. */
            BUG_ON(!curr_comp->completed);
            in_tree_merge_mstore[i].iter.immut_curr_c2b_cep = INVAL_EXT_POS;
        }

        if(curr_immut->next_c2b)
            in_tree_merge_mstore[i].iter.immut_next_c2b_cep = curr_immut->next_c2b->cep;
        else
            in_tree_merge_mstore[i].iter.immut_next_c2b_cep = INVAL_EXT_POS;

#ifdef DEBUG_MERGE_SERDES
        if(curr_comp->cached)
        {
            vlba_key_t *key;
            key = (vlba_key_t *)curr_comp->cached_entry.k;
            debug("%s::Currently cached key (hash) 0x%llx of length %d on comp[%d] "
                    "for marshalling merge (da %d level %d).\n",
                    __FUNCTION__, murmur_hash_64(key->_key, key->length, 0), key->length, i,
                    merge->da->id, merge->level);
        }
#endif
        curr_comp++; /* go to next component iterator */
    }
    if (partial_marshall) return;

update_output_tree_state:
    /* output tree */
    /* output tree marshalling is expensive... make it rare (i.e. once per checkpoint) */

    debug("%s::merge %p (da %d, level %d) output tree marshall with "
            "%d new LOs.\n", __FUNCTION__, merge, merge->da->id, merge->level, lo_count);

    {
        struct list_head *lh, *tmp;
        list_for_each_safe(lh, tmp, &merge->new_large_objs)
        {
            struct castle_large_obj_entry *lo =
                list_entry(lh, struct castle_large_obj_entry, list);
            int lo_ref_cnt = castle_extent_link_count_get(lo->ext_id);
            /* we expect the input cct and output cct to both have reference to the LO ext */
            BUG_ON(lo_ref_cnt < 2);
            lo_count++;
        }
    }
    /* update list of large objects */
    /* no need to take out_tree lo_mutex because we are here with serdes mutex, which will block
       checkpoint thread, which is the only race candidate */
    list_splice_init(&merge->new_large_objs, &merge->out_tree->large_objs);

    BUG_ON(!merge->out_tree);
    BUG_ON(!merge->da->levels[merge->level].merge.serdes.out_tree);
    BUG_ON(merge->out_tree != merge->da->levels[merge->level].merge.serdes.out_tree );
    BUG_ON(EXT_POS_INVAL(merge->out_tree->internal_ext_free));
    BUG_ON(EXT_POS_INVAL(merge->out_tree->tree_ext_free));
    BUG_ON(EXT_POS_INVAL(merge->out_tree->data_ext_free));
    castle_da_ct_marshall(&merge_mstore->out_tree, merge->out_tree);
    merge_mstore->root_depth         = merge->root_depth;
    merge_mstore->large_chunks       = merge->large_chunks;
    merge_mstore->completing         = merge->completing;
    merge_mstore->is_new_key         = merge->is_new_key;
    merge_mstore->skipped_count      = merge->skipped_count;
    merge_mstore->nr_entries         = merge->nr_entries;
    merge_mstore->last_leaf_node_cep = INVAL_EXT_POS;
    if(merge->last_leaf_node_c2b)
        merge_mstore->last_leaf_node_cep = merge->last_leaf_node_c2b->cep;

    for(i=0; i<MAX_BTREE_DEPTH; i++)
    {
        BUG_ON(merge->levels[i].next_idx <= merge->levels[i].valid_end_idx);
        /* if the above ever BUGs, then an assumption about how to deserialise - specifically what
           entries should be dropped - is broken! */

        merge_mstore->levels[i].next_idx            = merge->levels[i].next_idx;
        merge_mstore->levels[i].valid_end_idx       = merge->levels[i].valid_end_idx;
        merge_mstore->levels[i].valid_version       = merge->levels[i].valid_version;

        merge_mstore->levels[i].node_c2b_cep        = INVAL_EXT_POS;
        merge_mstore->levels[i].node_used           = 0;

        if(merge->levels[i].node_c2b)
        {
            struct castle_btree_node *node;

            BUG_ON(EXT_POS_INVAL(merge->levels[i].node_c2b->cep));
            merge_mstore->levels[i].node_c2b_cep = merge->levels[i].node_c2b->cep;

            node=c2b_bnode(merge->levels[i].node_c2b);
            debug("%s::merge %p (da %d, level %d) sanity check node_c2b[%d] ("
                    cep_fmt_str")\n", __FUNCTION__, merge, merge->da->id, merge->level, i,
                    cep2str(merge_mstore->levels[i].node_c2b_cep));
            BUG_ON(!node);
            BUG_ON(node->magic != BTREE_NODE_MAGIC);
            merge_mstore->levels[i].node_used = node->used; /* to know which entries to drop at DES time */

            debug("%s::level[%d] for merge %p (da %d level %d) node size %d, node isleaf %d\n",
                    __FUNCTION__, i, merge, merge->da->id, merge->level, node->size, node->is_leaf);

            /* dirty the incomplete node so it will be flushed at next checkpoint */
            if(i > 0)
                write_lock_c2b(merge->levels[i].node_c2b);
            dirty_c2b(merge->levels[i].node_c2b);
            if(i > 0)
                write_unlock_c2b(merge->levels[i].node_c2b);
        }//fi
    }//rof

    /* bloom build parameters, so we can resume building the output CT's bloom filter */
    merge_mstore->have_bbp = 0;
    if(merge->out_tree->bloom_exists)
    {
        struct castle_bloom_build_params *bf_bp = merge->out_tree->bloom.private;
        BUG_ON(!bf_bp);

        /* just me testing a theory... */
        /* if these ever BUG, it means we need to flush the bloom build param extents as well */
        BUG_ON(bf_bp->chunk_cep.ext_id != bf_bp->node_cep.ext_id);
        BUG_ON(bf_bp->chunk_cep.ext_id != merge->out_tree->bloom.ext_id);

        debug("%s::merge %p (da %d, level %d) bloom_build_param marshall.\n",
                __FUNCTION__, merge, merge->da->id, merge->level);
        castle_bloom_build_param_marshall(&merge_mstore->out_tree_bbp, bf_bp);
        merge_mstore->have_bbp = 1;
    }

    if (partial_marshall) return;

    /* the rest of the merge state */
    /* this stuff should never change; once-per-merge marshalling done here */

    debug("%s::merge %p (da %d, level %d) total marshall\n", __FUNCTION__,
            merge, merge->da->id, merge->level);

    merge_mstore->da_id              = merge->da->id;
    merge_mstore->level              = merge->level;
    merge_mstore->nr_trees           = merge->nr_trees;
    merge_mstore->btree_type         = merge->out_btree->magic;

    merge_mstore->leafs_on_ssds      = merge->leafs_on_ssds;
    merge_mstore->internals_on_ssds  = merge->internals_on_ssds;

    return;
}

/* During merge deserialisation, recover c2bs on a serialised output tree. Get the c2b, update it
   (i.e. READ), then return the c2b pointer in writelocked for condition. */
static c2_block_t* castle_da_merge_des_out_tree_c2b_write_fetch(struct castle_da_merge *merge,
                                                                c_ext_pos_t cep,
                                                                int depth)
{
    c2_block_t *c2b=NULL;
    uint16_t node_size=0;

    BUG_ON(!merge);
    BUG_ON(EXT_POS_INVAL(cep));

    castle_da_merge_node_size_get(merge, depth, &node_size);
    BUG_ON(node_size==0);

    c2b = castle_cache_block_get(cep, node_size);
    BUG_ON(!c2b);

    write_lock_c2b(c2b);
    /* If c2b is not up to date, issue a blocking READ to update */
    if(!c2b_uptodate(c2b))
        BUG_ON(submit_c2b_sync(READ, c2b));
    /* do not write_unlock leaf node c2b; merge thread expects to find it in a locked state. */
    if(depth > 0)
        write_unlock_c2b(c2b);
    return c2b;
}

/**
 * Deserialise merge structure
 *
 * @param merge [out] structure into which state is unpackeded, and if error set 'deserialising'
 *        flag to 0
 * @param da [in] doubling array containing in-flight merge state
 * @param level [in] merge level in doubling array containing in-flight merge state
 */
static void castle_da_merge_deserialise(struct castle_da_merge *merge,
                                        struct castle_double_array *da,
                                        int level)
{
    struct castle_dmserlist_entry *merge_mstore;
    struct castle_btree_node *node;
    struct list_head *lh, *tmp;
    struct castle_component_tree *des_tree = da->levels[level].merge.serdes.out_tree;
    int i;

    merge_mstore=da->levels[level].merge.serdes.mstore_entry;
    /* recover bloom_build_params. */
    if(da->levels[level].merge.serdes.mstore_entry->have_bbp)
        castle_da_ct_bloom_build_param_deserialise(da->levels[level].merge.serdes.out_tree,
                                &da->levels[level].merge.serdes.mstore_entry->out_tree_bbp);

    /* out_btree (type) can be assigned directly because we passed the BUG_ON() btree_type->magic
       in da_merge_des_check. */
    merge->out_btree         = castle_btree_type_get(RO_VLBA_TREE_TYPE);
    merge->root_depth        = merge_mstore->root_depth;
    merge->large_chunks      = merge_mstore->large_chunks;
    merge->completing        = merge_mstore->completing;
    merge->is_new_key        = merge_mstore->is_new_key;
    merge->skipped_count     = merge_mstore->skipped_count;
    merge->nr_entries        = merge_mstore->nr_entries;
    merge->leafs_on_ssds     = merge_mstore->leafs_on_ssds;
    merge->internals_on_ssds = merge_mstore->internals_on_ssds;

    /* get reference to all LOs so the extents don't get dropped when the input cct is put */
    mutex_lock(&des_tree->lo_mutex);
    list_for_each_safe(lh, tmp, &des_tree->large_objs)
    {
        struct castle_large_obj_entry *lo =
            list_entry(lh, struct castle_large_obj_entry, list);
        BUG_ON(castle_extent_link(lo->ext_id) < 0);
    }
    mutex_unlock(&des_tree->lo_mutex);

    node=NULL;
    merge->last_key=NULL;
    for(i=0; i<MAX_BTREE_DEPTH; i++)
    {
        BUG_ON(merge->levels[i].node_c2b); /* Initialising merge - this should always be NULL */
        merge->levels[i].node_c2b      = NULL;
        merge->levels[i].last_key      = NULL;

        merge->levels[i].next_idx      = merge_mstore->levels[i].next_idx;
        merge->levels[i].valid_end_idx = merge_mstore->levels[i].valid_end_idx;
        merge->levels[i].valid_version = merge_mstore->levels[i].valid_version;

        /* Recover each btree level's node_c2b and last_key */
        if(!EXT_POS_INVAL(merge_mstore->levels[i].node_c2b_cep))
        {
            debug("%s::sanity check for merge %p (da %d level %d) node_c2b[%d] ("cep_fmt_str")\n",
                    __FUNCTION__, merge, da->id, level,
                    i, cep2str(merge_mstore->levels[i].node_c2b_cep) );

            merge->levels[i].node_c2b =
                castle_da_merge_des_out_tree_c2b_write_fetch(merge, merge_mstore->levels[i].node_c2b_cep, i);
            BUG_ON(!merge->levels[i].node_c2b);
            /* sanity check on btree node */
            node = c2b_bnode(merge->levels[i].node_c2b);
            debug("%s::recovered level %d node at %p with magic %lx for merge %p (da %d level %d) from "
                    cep_fmt_str" \n",
                    __FUNCTION__, i, node, node->magic, merge, da->id, level, cep2str(merge_mstore->levels[i].node_c2b_cep) );
            BUG_ON(!node);
            BUG_ON(node->magic != BTREE_NODE_MAGIC);

            BUG_ON(merge_mstore->levels[i].node_used > (node->used) );
            if(merge_mstore->levels[i].next_idx < (node->used))
            {
                int drop_start=0;
                int drop_end=0;
                debug("%s::for merge %p (da %d level %d) entries_drop on node_c2b[%d] "
                        "ser used = %d, current used = %d, valid_end_idx = %d, next_idx = %d, node size = %d\n",
                        __FUNCTION__, merge, da->id, level, i,
                        merge_mstore->levels[i].node_used,
                        node->used,
                        merge_mstore->levels[i].valid_end_idx,
                        merge_mstore->levels[i].next_idx,
                        node->size);

                /* if the following BUGs, then it seems possible that some node entries were dropped
                   after the serialisation point */
                BUG_ON(node->used < merge_mstore->levels[i].node_used);
                if(node->used != merge_mstore->levels[i].node_used)
                {
                    drop_start = merge_mstore->levels[i].node_used;
                    drop_end   = node->used - 1;
                    merge->out_btree->entries_drop(node, drop_start, drop_end);
                }
            }
            /* recover last key */
            if(node->used)
            {
                merge->out_btree->entry_get(node, node->used - 1,
                        &merge->levels[i].last_key, NULL, NULL);
                if(i==0)
                    merge->last_key = merge->levels[i].last_key;
            }
        }
    }

    merge->last_leaf_node_c2b = NULL;
    if(!EXT_POS_INVAL(merge_mstore->last_leaf_node_cep))
    {
        debug("%s::last_leaf_node_c2b for merge %p (da %d level %d)\n",
                __FUNCTION__, merge, da->id, level);
        merge->last_leaf_node_c2b =
            castle_da_merge_des_out_tree_c2b_write_fetch(merge, merge_mstore->last_leaf_node_cep,
                    0 /* leaf */);
        BUG_ON(!merge->last_leaf_node_c2b);
        node = c2b_bnode(merge->last_leaf_node_c2b);
        BUG_ON(!node);
        BUG_ON(node->magic != BTREE_NODE_MAGIC);

        /* if we don't already have the last_key, then it is on the already completed node. */
        if( (!merge->last_key) && (node->used) )
            merge->out_btree->entry_get(node, node->used - 1, &merge->last_key, NULL, NULL);

        /* this is a leaf node but it is not still being merged into, so unlock it */
        write_unlock_c2b(merge->last_leaf_node_c2b);

        debug("%s::recovered last leaf node for merge %p (da %d level %d) from "
                cep_fmt_str" \n",
                __FUNCTION__, merge, da->id, level, cep2str(merge_mstore->last_leaf_node_cep) );
    }

    return;
}

/**
 * Sanity checks on deserialising merge state.
 *
 * @param da [in] doubling array containing in-flight merge state
 * @param level [in] merge level in doubling array containing in-flight merge state
 * @param nr_trees [in] number of trees to be merged
 * @param in_trees [in] component trees to be merged
 *
 * @also castle_double_array_read()
 * @also castle_da_merge_init()
 * @also struct castle_da_merge
 */
static void castle_da_merge_des_check(struct castle_da_merge *merge, struct castle_double_array *da,
                                      int level, int nr_trees,
                                      struct castle_component_tree **in_trees)
{
    struct castle_dmserlist_entry *merge_mstore;
    struct castle_in_tree_merge_state_entry    *in_tree_merge_mstore_arr;
    int i;

    BUG_ON(!da);
    BUG_ON(!merge);
    BUG_ON((level < MIN_DA_SERDES_LEVEL) && (level != BIG_MERGE));

    merge_mstore             = da->levels[level].merge.serdes.mstore_entry;
    in_tree_merge_mstore_arr = da->levels[level].merge.serdes.in_tree_mstore_entry_arr;

    /* if BUG here, there is a problem with the mstore read (probably double_array_read) */
    BUG_ON(!merge_mstore);
    BUG_ON(!in_tree_merge_mstore_arr);
    /* if BUG on the following two, it is likely there is a problem with serialisation -
       the wrong state information was written, or it was written to the wrong place */
    BUG_ON(merge_mstore->da_id          != da->id);
    BUG_ON(merge_mstore->level          != level);
    BUG_ON(merge_mstore->out_tree.level != level + 1);
    BUG_ON(merge_mstore->btree_type     != castle_btree_type_get(RO_VLBA_TREE_TYPE)->magic);

    /* if seq numbers match up, everything else would be fine as well */
    for(i=0; i<nr_trees; i++)
        BUG_ON(in_tree_merge_mstore_arr[i].seq != in_trees[i]->seq);

    /* Sane. Proceed. */
    debug("Interrupted merge da %d level %d passed initial SERDES logic sanity checks.\n",
            da->id, level);

    return;
}

/**
 * Merge multiple trees into one. The same function gets used by both compaction
 * (total merges) and standard 2 tree merges.
 *
 * @param da [in] doubling array to be merged
 * @param nr_trees [in] number of trees to be merged
 * @param in_trees [in] list of trees
 * @param level [in] level of the double array - 0 for total merge
 *
 * @return non-zero if failure
 */
static int castle_da_merge_do(struct castle_double_array *da,
                              int nr_trees,
                              struct castle_component_tree *in_trees[],
                              int level)
{
    struct castle_da_merge *merge;
    uint32_t units_cnt;
    tree_seq_t out_tree_id=0;
    int ret;
    int i;
    c_merge_serdes_state_t serdes_state;

    castle_trace_da_merge(TRACE_START,
                          TRACE_DA_MERGE_ID,
                          da->id,
                          level,
                          in_trees[0]->seq,
                          in_trees[1]->seq);

    /* Initialise the merge, including merged iterator and component iterators.
     * Level 1 merges have modlist component btrees that need sorting - this is
     * currently done using a malloc'd buffer.  Serialise function entry across
     * all DAs to prevent races decrementing the modlist mem budget. */
    if (level == 1) mutex_lock(&castle_da_level1_merge_init);
    merge = castle_da_merge_init(da, level, nr_trees, in_trees);
    if (level == 1) mutex_unlock(&castle_da_level1_merge_init);
    if(!merge)
    {
        castle_printk(LOG_WARN, "Could not start a merge for DA=%d, level=%d.\n", da->id, level);
        return -EAGAIN;
    }
    debug_merges("%s::MERGE START - DA %d L %d, with input cts: ",
            __FUNCTION__, da->id, level);
    FOR_EACH_MERGE_TREE(i, merge)
        debug_merges(" [%d]", in_trees[i]->seq);
    debug_merges(".\n");

    /* Merge no fail zone starts here. Can't fail from here. Expected to complete, unless
     * someone aborts merge in between. */

    /* Mark merge as running. */
    castle_da_merge_running_set(da, level);

    __castle_da_driver_merge_reset(da, NULL);

    /* Hard-pin T1s in the cache. */
    if (level == 1)
    {
        castle_cache_advise((c_ext_pos_t){in_trees[0]->data_ext_free.ext_id, 0},
                C2_ADV_EXTENT|C2_ADV_HARDPIN, -1, -1, 0);
        castle_cache_advise((c_ext_pos_t){in_trees[1]->data_ext_free.ext_id, 0},
                C2_ADV_EXTENT|C2_ADV_HARDPIN, -1, -1, 0);
    }

    /* Do the merge. */
    do {
        int i;
        c2_block_t *node_c2b;
        /* We unlock some c2bs before the unit merge boundary to allow checkpoint thread to flush
           c2bs. It is assumed that no other thread would ever have a wlock on these c2bs. */
        uint8_t relock_bloom_node_c2b = 0;
        uint8_t relock_bloom_chunk_c2b = 0;
        /* unlock output ct active leaf c2b, so checkpoint can quickly flush partial merges */
        for(i=0; i<MAX_BTREE_DEPTH; i++)
        {
            node_c2b = merge->levels[i].node_c2b;
            if(node_c2b)
            {
                if(i==0)
                    write_unlock_c2b(node_c2b);
                else
                    BUG_ON(c2b_write_locked(node_c2b)); /* arriving here, only leaf node may be locked */
            }
        }
        /* ditto the in-progress bloom filter */
        if (merge->out_tree->bloom_exists)
        {
            struct castle_bloom_build_params *bf_bp =  merge->out_tree->bloom.private;
            if(bf_bp)
            {
                if(bf_bp->chunk_c2b)
                {
                    if(c2b_write_locked(bf_bp->chunk_c2b))
                    {
                        debug("%s::unlocking bloom filter chunk_c2b for merge on da %d level %d.\n",
                                __FUNCTION__, da->id, level);
                        write_unlock_c2b(bf_bp->chunk_c2b);
                        relock_bloom_chunk_c2b = 1;
                    }
                }
                if(bf_bp->node_c2b)
                {
                    if(c2b_write_locked(bf_bp->node_c2b))
                    {
                        debug("%s::unlocking bloom filter node_c2b for merge on da %d level %d.\n",
                                __FUNCTION__, da->id, level);
                        write_unlock_c2b(bf_bp->node_c2b);
                        relock_bloom_node_c2b = 1;
                    }
                }
            }
        }

        /* Wait until we are allowed to do next unit of merge. */
        units_cnt = castle_da_merge_units_inc_return(da, level);

        /* relock output ct active leaf c2b, as unit_do expects to find it */
        node_c2b = merge->levels[0].node_c2b;
        if(node_c2b)
            write_lock_c2b(node_c2b);
        /* ditto the in-progress bloom filter */
        if(relock_bloom_node_c2b)
        {
            struct castle_bloom_build_params *bf_bp = merge->out_tree->bloom.private;
            debug("%s::relocking bloom filter node_c2b for merge on da %d level %d.\n",
                    __FUNCTION__, da->id, level);
            write_lock_c2b(bf_bp->node_c2b);
        }
        if(relock_bloom_chunk_c2b)
        {
            struct castle_bloom_build_params *bf_bp = merge->out_tree->bloom.private;
            debug("%s::relocking bloom filter chunk_c2b for merge on da %d level %d.\n",
                    __FUNCTION__, da->id, level);
            write_lock_c2b(bf_bp->chunk_c2b);
        }

        debug("%s::doing unit %d on merge %p (da %d level %d)\n", __FUNCTION__,
            units_cnt, merge, da->id, level);
        /* Trace event. */
        castle_trace_da_merge_unit(TRACE_START,
                                   TRACE_DA_MERGE_UNIT_ID,
                                   da->id,
                                   level,
                                   units_cnt,
                                   0);
        /* Check for castle stop and merge abort */
        if (castle_merges_abortable && exit_cond)
        {
            castle_printk(LOG_INIT, "Merge for DA=%d, level=%d, aborted.\n", da->id, level);
            ret = -ESHUTDOWN;
            CASTLE_TRANSACTION_BEGIN;
            goto merge_aborted;
        }

        /* Perform the merge work. */
        ret = castle_da_merge_unit_do(merge, units_cnt);

#ifdef CASTLE_PERF_DEBUG
        /* Output & reset cache efficiency stats. */
        castle_da_merge_cache_efficiency_stats_flush_reset(da, merge, units_cnt, in_trees);
#endif

        serdes_state = atomic_read(&da->levels[level].merge.serdes.valid);
        if((serdes_state > NULL_DAM_SERDES) && (!castle_merges_checkpoint))
        {
            /* user changed castle_merges_checkpoint param from 1 to 0 */
            castle_printk(LOG_USERINFO,
                    "Discarding checkpoint state for in-flight merge on DA=%d, level=%d.\n",
                    da->id, level);
            mutex_lock(&merge->da->levels[merge->level].merge.serdes.mutex);
            castle_da_merge_serdes_dealloc(merge);
            mutex_unlock(&merge->da->levels[merge->level].merge.serdes.mutex);
        }
        /* Trace event. */
        castle_trace_da_merge_unit(TRACE_END,
                                   TRACE_DA_MERGE_UNIT_ID,
                                   da->id,
                                   level,
                                   units_cnt,
                                   0);
        debug_merges("Completing %d unit for merge at level: %d\n", units_cnt, level);

#ifdef CASTLE_PERF_DEBUG
        /* Output & reset performance stats. */
        castle_da_merge_perf_stats_flush_reset(da, merge, units_cnt, in_trees);
#endif
        /* Exit on errors. */
        if (ret < 0)
        {
            /* Merges should never fail.
             *
             * Per-version statistics will now be out of sync. */
            out_tree_id = INVAL_TREE;
            castle_printk(LOG_WARN, "%s::MERGE FAILED - DA %d L %d, with input cts %d and %d \n",
                    __FUNCTION__, da->id, level, in_trees[0]->seq, in_trees[1]->seq);
            CASTLE_TRANSACTION_BEGIN;
            goto merge_failed;
        }
        /* Only ret>0 we are expecting to continue, i.e. ret==EAGAIN. */
        BUG_ON(ret && (ret != EAGAIN));
        /* Notify interested parties that we've completed current merge unit. */
        if (ret == EAGAIN)
            castle_da_merge_intermediate_unit_complete(da, level);
    } while(ret);

    CASTLE_TRANSACTION_BEGIN;
    debug("%s::MERGE COMPLETING - DA %d L %d, with input cts %d and %d, "
        "and output ct %d.\n", __FUNCTION__, da->id, level, in_trees[0]->seq, in_trees[1]->seq,
        merge->out_tree->seq);

#ifdef DEBUG_MERGE_SERDES
    serdes_state = atomic_read(&da->levels[level].merge.serdes.valid);
    if(serdes_state > NULL_DAM_SERDES)
        da->levels[level].merge.serdes.merge_completed=1;
#endif

    /* Finish the last unit, packaging the output tree. */
    out_tree_id = castle_da_merge_last_unit_complete(da, level, merge);
    ret = TREE_INVAL(out_tree_id) ? -ENOMEM : 0;

    /* Commit and zero private stats to global crash-consistent tree. */
    castle_version_states_commit(&merge->version_states);

merge_aborted:
merge_failed:
    /* Unhard-pin T1s in the cache. Do this before we deallocate the merge and extents. */
    if (level == 1)
    {
        castle_cache_advise_clear((c_ext_pos_t){in_trees[0]->data_ext_free.ext_id, 0},
                C2_ADV_EXTENT|C2_ADV_HARDPIN, -1, -1, 0);
        castle_cache_advise_clear((c_ext_pos_t){in_trees[1]->data_ext_free.ext_id, 0},
                C2_ADV_EXTENT|C2_ADV_HARDPIN, -1, -1, 0);
    }

    debug_merges("%s::MERGE END with ret %d - DA %d L %d, produced out ct seq %d \n",
        __FUNCTION__, ret, da->id, level, out_tree_id);

    castle_da_merge_dealloc(merge, ret);

    /* safe for checkpoint to run now because we've completed and cleaned up all merge state */
    CASTLE_TRANSACTION_END;

    /* Mark merge as completed. */
    castle_da_merge_running_clear(da, level);

    __castle_da_driver_merge_reset(da, NULL);

    castle_trace_da_merge(TRACE_END, TRACE_DA_MERGE_ID, da->id, level, out_tree_id, 0);
    if(ret==-ESHUTDOWN) return -ESHUTDOWN; /* merge abort */
    if(ret)
    {
        castle_printk(LOG_WARN, "Merge for DA=%d, level=%d, failed to merge err=%d.\n",
                da->id, level, ret);
        return -EAGAIN;
    }

    return 0;
}

/**
 * Marks the DA 'dirty', i.e. that a total merge will be required to deal with snapshot deletion.
 *
 * @param da_id     DA id to mark as dirty.
 */
void castle_da_version_delete(c_da_t da_id)
{
    struct castle_double_array *da = castle_da_hash_get(da_id);

    atomic_inc(&(da->nr_del_versions));

    /* Compaction after each version delete is disabled. It leads to inefficient merges. */
#if 0
    /* Mark DA for compaction. */
    castle_da_need_compaction_set(da);

    /* Wakeup compaction thread. */
    wake_up(&da->merge_waitq);
#endif
}

/**
 * Checks for ongoing merge units in any of the merges above the given level.
 *
 * @param da    Doubling array.
 * @param level Search will start with level+1
 * @return 0:   If no ongoing merge units.
 * @return 1:   If there are some ongoing merge units.
 */
static int castle_da_merge_units_ongoing(struct castle_double_array *da, int level)
{
    int i;

    BUG_ON(write_can_lock(&da->lock));
    /* Check for ongoing merge units on top levels. */
    for(i=level+1; i<MAX_DA_LEVEL; i++)
    {
        /* Check for ongoing merge units. */
        if (da->levels[i].merge.active_token)
            return 1;
    }

    return 0;
}

/**
 * Determines whether to do a total merge.
 *
 * Do not do big-merge in case:
 *  - DA has few outstanding low free space victims
 *  - DA is not marked for compaction
 *  - there is a ongoing merge unit
 *
 * @param da [in] DA id.
 * @return whether to start big-merge or not.
 */
static int castle_da_big_merge_trigger(struct castle_double_array *da)
{
    int ret = 0;

    /* Don't start merge, if there is no disk space. */
    if (castle_da_no_disk_space(da))
        return 0;

    read_lock(&da->lock);

    /* Check if marked for compaction. */
    if (!castle_da_need_compaction(da))
    {
        debug_merges("Not marked for compaction.\n");
        goto out;
    }

    /* Make sure there are no ongoing merges. */
    if (atomic_read(&da->ongoing_merges))
    {
        debug_merges("Total merge cannot be triggered - ongoing merges : %u\n",
                     atomic_read(&da->ongoing_merges));
        goto out;
    }

    /* All checks succeeded, total merge can start. */
    ret = 1;

out:
    read_unlock(&da->lock);

    return ret;
}

/**
 * Do a total merge on all trees in a DA. Triggered, after completing the last level
 * merge, if any versions marked for deletion.
 *
 * @param da_p [in] doubling array to run total merge on.
 */
static int castle_da_big_merge_run(void *da_p)
{
    struct castle_double_array *da = (struct castle_double_array *)da_p;
    struct castle_component_tree **in_trees;
    struct list_head *l;
    int level=0, ignore;
    int nr_trees=0;
    int i, ret = 0;

    /* Disable deamortization of total merges. */
    da->levels[BIG_MERGE].merge.deamortize = 0;

    debug_merges("Starting big-merge thread.\n");
    do {
        /* Start big-merge only when the DA has versions marked for deletion
         * and only after completing the top-level merge(to make sure no merge
         * is going on). */
        __wait_event_interruptible(da->merge_waitq,
                                   exit_cond || castle_da_big_merge_trigger(da),
                                   ignore);

        /* Exit without doing a merge, if we are stopping execution, or da has been deleted. */
        if(exit_cond)
            break;

        /* Otherwise do a merge. */
        castle_printk(LOG_INFO, "Triggered a total merge.\n");

        /* Allocate array for in_tree pointers, but do that without holding the lock. */
        in_trees = NULL;

        /* Lock the DA, because we may reset the compacting flag. */
        write_lock(&da->lock);

        /* Count number of trees to compact. Don't compact level-1 trees.
         *
         * Note: Merging T1s need memory as we need to sort them before merge. If we try to
         * include T1s in compaction, we might run out of memory.
         */
        for (nr_trees=0, level=2; level<MAX_DA_LEVEL; level++)
            nr_trees += da->levels[level].nr_trees;

        /* Merge cannot be scheduled with < 2 trees. */
        if(nr_trees < 2)
        {
            /* Don't compact any more (not enough trees). */
            castle_printk(LOG_USERINFO, "Aborting compaction: Need minimum 2 trees above"
                                        " level 1.\n");
            castle_da_need_compaction_clear(da);
            write_unlock(&da->lock);
            goto wait_and_try;
        }

        /* Mark all the trees for compaction. So, we start compaction on them after allocating
         * resources. */
        for (level=2, i=0; level<MAX_DA_LEVEL; level++)
        {
            list_for_each(l, &da->levels[level].trees)
            {
                struct castle_component_tree *ct = list_entry(l, struct castle_component_tree, da_list);

                BUG_ON(ct->compacting);
                ct->compacting = 1;
                da->levels[level].nr_trees--;
                da->levels[level].nr_compac_trees++;
                i++;
                BUG_ON(i > nr_trees);
            }
        }
        BUG_ON(i != nr_trees);

        write_unlock(&da->lock);

        /* Allocate in_trees array for appropriate number of trees. */
        in_trees = castle_zalloc(sizeof(struct castle_component_tree *) * nr_trees,
                                 GFP_KERNEL);
        if (!in_trees)
        {
            castle_printk(LOG_USERINFO, "Aborting compaction: Failed to allocate memory.\n");
            goto wait_and_try;
        }

        /* Now, lock the DA, take the in trees and start the merge. */
        write_lock(&da->lock);

        /* Allocated memory for in_trees; store all trees on in_trees array. */
        for (level=2, i=0; level<MAX_DA_LEVEL; level++)
        {
            list_for_each(l, &da->levels[level].trees)
            {
                struct castle_component_tree *ct =
                            list_entry(l, struct castle_component_tree, da_list);

                /* Store trees marked for compaction. */
                if (ct->compacting)
                {
                    in_trees[i] = ct;
                    i++;
                }

                BUG_ON(i > nr_trees);
            }
        }

        /* We should have seen all marked in trees. */
        BUG_ON(i != nr_trees);

        /* Marked trees for compaction, register a component tree sequence number, before
         * letting other merges start.
         *
         * Note: This is important as, merges can race with compaction and it is possible to have
         * compaction out_tree with latest sequence number than the racing merge.
         */
        if (TREE_INVAL(da->compaction_ct_seq))
            da->compaction_ct_seq = castle_da_next_ct_seq();

        write_unlock(&da->lock);

        castle_da_need_compaction_clear(da);
        atomic_set(&da->nr_del_versions, 0);
        atomic_inc(&da->ongoing_merges);

        /* Wakeup everyone waiting on merge state update. */
        wake_up(&da->merge_waitq);

        castle_printk(LOG_USERINFO, "Starting total merge on %d trees\n", nr_trees);

        /* Mark DA as compaction is ongoing. */
        castle_da_compacting_set(da);

        /* Do the merge. If fails, retry after 10s. */
        if ((ret = castle_da_merge_do(da, nr_trees, in_trees, BIG_MERGE)))
        {
            castle_printk(LOG_WARN, "Total merge failed with error: %d\n", ret);
            atomic_dec(&da->ongoing_merges);
wait_and_try:
            if (nr_trees >= 2)
            {
                write_lock(&da->lock);

                /* If the merge was actually scheduled (i.e. some trees were collected),
                   but failed afterward (e.g. due to NOSPC), readjust the counters again. */

                /* Merge failed, unmark compacting bit for all trees. */
                for (level=2, i=0; level<MAX_DA_LEVEL; level++)
                {
                    list_for_each(l, &da->levels[level].trees)
                    {
                        struct castle_component_tree *ct =
                                    list_entry(l, struct castle_component_tree, da_list);

                        if (ct->compacting)
                        {
                            ct->compacting = 0;
                            i++;
                        }
                        BUG_ON(i > nr_trees);
                    }
                }
                BUG_ON(i != nr_trees);

                /* Change count for compaction trees on each level. */
                for (i=0; i<MAX_DA_LEVEL; i++)
                {
                    BUG_ON((i <=1 ) && da->levels[i].nr_compac_trees);

                    da->levels[i].nr_trees += da->levels[i].nr_compac_trees;
                    da->levels[i].nr_compac_trees = 0;
                }

                write_unlock(&da->lock);
            }
            /* Wakeup everyone waiting on merge state update. */
            wake_up(&da->merge_waitq);

            /* Mark DA as compaction is completed. */
            castle_da_compacting_clear(da);

            /* In case we failed the merge because of no memory for in_trees, wait and retry. */
            msleep_interruptible(10000);
        }
        else
        {
            atomic_dec(&da->ongoing_merges);
            /* Mark DA as compaction is completed. */
            castle_da_compacting_clear(da);

            castle_printk(LOG_USERINFO, "Successfully completed compaction\n");
        }

        /* Free in_trees structure. */
        if (in_trees)
        {
            castle_kfree(in_trees);
            in_trees = NULL;
        }

    } while(1);

    debug_merges("Merge thread exiting.\n");

    write_lock(&da->lock);
    /* Remove ourselves from the da merge threads array to indicate that we are finished. */
    da->levels[BIG_MERGE].merge.thread = NULL;
    write_unlock(&da->lock);
    /* castle_da_alloc() took a reference for us, we have to drop it now. */
    castle_da_put(da);

    return 0;
}

/**
 * Determines whether to do merge or not.
 *
 * Do not do merge if one of following is true:
 *  - DA has few outstanding low free space victims
 *  - DA is marked for compaction
 *  - There is a ongoing merge unit at a level above
 *
 * IMPORTANT: this function has side effect of increamenting ongoing merges counter,
 *            this happens iff the wait is supposed to terminate (non-zero return from
 *            this function).
 *
 * @param da [in] doubling array to check for
 * @param level [out] merge level
 *
 * @return whether to start merge or not.
 */
static int castle_da_merge_trigger(struct castle_double_array *da, int level)
{
    /* Don't start merge, if there is no disk space. */
    if (castle_da_no_disk_space(da))
        return 0;

    read_lock(&da->lock);

    if (da->levels[level].nr_trees < 2)
        goto out;

    /* Make sure there are no ongoing merge units on top levels. */
    if (castle_da_merge_units_ongoing(da, level))
    {
        debug_merges("Merge %d cant be triggered - ongoing merge units.\n", level);
        goto out;
    }

    /* If DA is marked for compaction, don't start a new merge if there are no ongoing merges.
     * (Time to start compaction). */
    if (castle_da_need_compaction(da) && !atomic_read(&da->ongoing_merges))
    {
        debug_merges("Merge %d cant be triggered - compaction going on.\n", level);
        goto out;
    }

    /* Everything is good for merges to start. Increment ongoing merge count. */
    atomic_inc(&da->ongoing_merges);
    read_unlock(&da->lock);

    return 1;

out:
    read_unlock(&da->lock);
    return 0;
}

/**
 * Merge doubling array trees at a level.
 *
 * @param da_p [in] Doubling array to do merge on.
 */
static int castle_da_merge_run(void *da_p)
{
    struct castle_double_array *da = (struct castle_double_array *)da_p;
    struct castle_component_tree *in_trees[2];
    int level, ignore, ret;

    /* Work out the level at which we are supposed to be doing merges.
       Do that by working out where is this thread in threads array. */
    for(level=1; level<MAX_DA_LEVEL; level++)
        if(da->levels[level].merge.thread == current)
            break;
    BUG_ON(level >= MAX_DA_LEVEL);

    /* Enable deamortization of normal merges. */
    da->levels[level].merge.deamortize = 1;
    castle_printk(LOG_DEBUG, "Starting merge thread.\n");
    do {
        /* Wait for 2+ trees to appear at this level.
           NOTE: we moved exit condition from */
        __wait_event_interruptible(da->merge_waitq,
                    (ret = exit_cond) || castle_da_merge_trigger(da, level),
                    ignore);

        /* If ret is set, exit_cond should return true as well. */
        BUG_ON(ret && !(exit_cond));
        /* Exit without doing a merge, if we are stopping execution, or da has been deleted.
           NOTE: this is the only case for which we haven't bumped up the ongoing merges counter.
         */
        if(ret)
            break;

        /* Extract the two oldest component trees. */
        ret = castle_da_merge_cts_get(da, level, in_trees);
        BUG_ON(ret && (ret != -EAGAIN));
        if(ret == -EAGAIN)
            goto __again;

        /* We expect to have 2 trees. */
        BUG_ON(!in_trees[0] || !in_trees[1]);
        debug_merges("Doing merge, trees=[%u]+[%u]\n", in_trees[0]->seq, in_trees[1]->seq);

        /* Do the merge.  If it fails, retry after 10s (unless it's a merge abort). */
        ret = castle_da_merge_do(da, 2, in_trees, level);
        if (ret == -ESHUTDOWN)
            /* Merge has been aborted. */
            goto __again;
        else if (ret)
        {
            /* Merge failed, wait 10s to retry. */
            msleep_interruptible(10000);
            goto __again;
        }

        continue;
__again:
        atomic_dec(&da->ongoing_merges);
        if (ret == -ESHUTDOWN)
            goto exit_thread;
    } while(1);
exit_thread:
    debug_merges("Merge thread exiting.\n");

    write_lock(&da->lock);
    /* Remove ourselves from the da merge threads array to indicate that we are finished. */
    da->levels[level].merge.thread = NULL;
    write_unlock(&da->lock);
    /* castle_da_alloc() took a reference for us, we have to drop it now. */
    castle_da_put(da);

    return 0;
}

static int __castle_da_threads_priority_set(struct castle_double_array *da, void *_value);

static int castle_da_merge_start(struct castle_double_array *da, void *unused)
{
    int i;

    /* Wake up all of the merge threads. */
    for(i=0; i<MAX_DA_LEVEL-1; i++)
        wake_up_process(da->levels[i].merge.thread);

    __castle_da_threads_priority_set(da, &castle_nice_value);

    return 0;
}

static int castle_da_merge_stop(struct castle_double_array *da, void *unused)
{
    int i;

    /* castle_da_exiting should have been set by now. */
    BUG_ON(!exit_cond);
    wake_up(&da->merge_waitq);
    for(i=0; i<MAX_DA_LEVEL-1; i++)
    {
        while(da->levels[i].merge.thread)
            msleep(10);
        castle_printk(LOG_INIT, "Stopped merge thread for DA=%d, level=%d\n", da->id, i);
    }

    return 0;
}

/**
 * Enable/disable inserts for da and wake-up merge thread.
 *
 * @param da    Doubling array to throttle and merge
 */
static int castle_da_merge_restart(struct castle_double_array *da, void *unused)
{
    debug("Restarting merge for DA=%d\n", da->id);

    write_lock(&da->lock);
    if (da->levels[1].nr_trees >= 4 * castle_double_array_request_cpus())
    {
        if (da->ios_rate != 0)
        {
            castle_printk(LOG_PERF, "Disabling inserts on da=%d.\n", da->id);
            castle_trace_da(TRACE_START, TRACE_DA_INSERTS_DISABLED_ID, da->id, 0);
        }
        da->ios_rate = 0;
    }
    else
    {
        if (da->ios_rate == 0)
        {
            castle_printk(LOG_PERF, "Enabling inserts on da=%d.\n", da->id);
            castle_trace_da(TRACE_END, TRACE_DA_INSERTS_DISABLED_ID, da->id, 0);
        }
        da->ios_rate = INT_MAX;
    }
    write_unlock(&da->lock);
    wake_up(&da->merge_waitq);

    return 0;
}

static void castle_da_merges_print(struct castle_double_array *da)
{
    struct castle_merge_token *token;
    struct list_head *l;
    int level, print;
    struct timeval time;

    print = 0;
    do_gettimeofday(&time);
    read_lock(&da->lock);
    castle_printk(LOG_INFO, "\nPrinting merging stats for DA=%d, t=(%ld,%ld)\n",
            da->id, time.tv_sec, time.tv_usec/1000);
    for(level=MAX_DA_LEVEL-2; level>0; level--)
    {
        if(!print && (da->levels[level].nr_trees == 0))
            continue;
        print = 1;
        castle_printk(LOG_INFO, " level[%.2d]: nr_trees=%d, units_commited=%.3d,"
              " active_token_dl=%.2d, driver_token_dl=%.2d\n",
              level,
              da->levels[level].nr_trees,
              da->levels[level].merge.units_commited,
              da->levels[level].merge.active_token ?
                da->levels[level].merge.active_token->driver_level : 0,
              da->levels[level].merge.driver_token ?
                da->levels[level].merge.driver_token->driver_level : 0);
        list_for_each(l, &da->levels[level].merge.merge_tokens)
        {
            token = list_entry(l, struct castle_merge_token, list);
            castle_printk(LOG_INFO, "  merge_token_dl=%d\n", token->driver_level);
        }
    }
    castle_printk(LOG_INFO, "\n");
    read_unlock(&da->lock);
}

/**********************************************************************************************/
/* Generic DA code */

static int castle_da_ct_dec_cmp(struct list_head *l1, struct list_head *l2)
{
    struct castle_component_tree *ct1 = list_entry(l1, struct castle_component_tree, da_list);
    struct castle_component_tree *ct2 = list_entry(l2, struct castle_component_tree, da_list);
    BUG_ON(ct1->seq == ct2->seq);

    return ct1->seq > ct2->seq ? -1 : 1;
}

/**
 * Calculate hash of castle key length key_len and modulo for cpu_index.
 *
 * @return  Offset into request_cpus.cpus[]
 */
int castle_double_array_key_cpu_index(c_vl_bkey_t *key, uint32_t key_len)
{
    uint32_t seed = 0;

    seed = murmur_hash_32(key, castle_object_btree_key_length(key), 0);

    return seed % castle_double_array_request_cpus();
}

/**
 * Get cpu id for specified cpu_index.
 *
 * @return  CPU id
 */
int castle_double_array_request_cpu(int cpu_index)
{
    return request_cpus.cpus[cpu_index];
}

/**
 * Get number of cpus handling requests.
 *
 * @return  Number of cpus handling requests.
 */
int castle_double_array_request_cpus(void)
{
    return request_cpus.cnt;
}

/**
 * Allocate write IO wait queues for specified DA.
 *
 * @return  EXIT_SUCCESS    Successfully allocated wait queues
 * @return  1               Failed to allocate wait queues
 *
 * @also castle_da_all_rwcts_create()
 */
static int castle_da_wait_queue_create(struct castle_double_array *da, void *unused)
{
    int i;

    da->ios_waiting = castle_malloc(castle_double_array_request_cpus()
            * sizeof(struct castle_da_io_wait_queue), GFP_KERNEL);
    if (!da->ios_waiting)
        return 1;

    for (i = 0; i < castle_double_array_request_cpus(); i++)
    {
        spin_lock_init(&da->ios_waiting[i].lock);
        INIT_LIST_HEAD(&da->ios_waiting[i].list);
        CASTLE_INIT_WORK(&da->ios_waiting[i].work, castle_da_queue_kick);
        da->ios_waiting[i].cnt = 0;
        da->ios_waiting[i].da = da;
    }

    return 0;
}

/* sanity check serialised state of DA merge output tree */
static void castle_da_merge_serdes_out_tree_check(struct castle_dmserlist_entry *merge_mstore,
                                                  struct castle_double_array *da,
                                                  int level)
{
    int i=0; /* btree level */

    BUG_ON(!merge_mstore);
    BUG_ON(!da);
    BUG_ON(merge_mstore->da_id          != da->id);
    BUG_ON(merge_mstore->level          != level);
    BUG_ON(merge_mstore->out_tree.level != level + 1);
    BUG_ON(merge_mstore->btree_type     != castle_btree_type_get(RO_VLBA_TREE_TYPE)->magic);

    debug("%s::sanity checking merge SERDES on da %d level %d.\n",
            __FUNCTION__, da->id, level);

    for(i=0; i<MAX_BTREE_DEPTH; i++)
    {
        if(!EXT_POS_INVAL(merge_mstore->levels[i].node_c2b_cep))
        {
            int node_size = 0;
            c2_block_t *node_c2b = NULL;
            struct castle_btree_node *node = NULL;

            if (i==0) /* leaf node */
                node_size = ((merge_mstore->leafs_on_ssds) ? VLBA_SSD_RO_TREE_NODE_SIZE
                        : VLBA_HDD_RO_TREE_NODE_SIZE);
            else /* internal node */
                node_size = ((merge_mstore->internals_on_ssds) ? VLBA_SSD_RO_TREE_NODE_SIZE
                        : VLBA_HDD_RO_TREE_NODE_SIZE);

            node_c2b = castle_cache_block_get(merge_mstore->levels[i].node_c2b_cep, node_size);
            BUG_ON(!node_c2b);
            write_lock_c2b(node_c2b);
            if(!c2b_uptodate(node_c2b))
                BUG_ON(submit_c2b_sync(READ, node_c2b));
            write_unlock_c2b(node_c2b);

            node = c2b_bnode(node_c2b);
            BUG_ON(!node);
            debug("%s::recovered node at %p with magic %lx for merge on "
                    "da %d level %d from"cep_fmt_str", btree level %d.\n",
                    __FUNCTION__, node, node->magic, da->id, level,
                    cep2str(merge_mstore->levels[i].node_c2b_cep), i);
            if(node->magic != BTREE_NODE_MAGIC)
            {
                castle_printk(LOG_ERROR, "%s::failed to recover node at "cep_fmt_str
                        "; found weird magic=%lx.\n",
                        __FUNCTION__, cep2str(merge_mstore->levels[i].node_c2b_cep), node->magic);
                BUG();
            }

            put_c2b(node_c2b);
        }//fi
    }//rof
    debug("%s::sanity check passed merge SERDES on da %d level %d.\n",
            __FUNCTION__, da->id, level);
}

/**
 * Deallocate doubling array and all associated data.
 *
 * @param da    Doubling array for deallocate
 *
 * - Merge threads
 * - IO wait queues
 */
static void castle_da_dealloc(struct castle_double_array *da)
{
    int i; /* DA level */
    BUG_ON(!da);
    for (i=0; i<MAX_DA_LEVEL-1; i++)
    {
        if(da->levels[i].merge.thread != NULL)
            kthread_stop(da->levels[i].merge.thread);

        /* if we have merge state, must have an associated output tree... */
        if(da->levels[i].merge.serdes.mstore_entry)
            BUG_ON(!da->levels[i].merge.serdes.out_tree);
        /* ... and vice versa. */
        if(da->levels[i].merge.serdes.out_tree)
            BUG_ON(!da->levels[i].merge.serdes.mstore_entry);

        if(da->levels[i].merge.serdes.out_tree)
        {
            debug("%s::cleaning up interrupted merge on da %d level %d.\n",
                    __FUNCTION__, da->id, i);
            castle_ct_hash_remove(da->levels[i].merge.serdes.out_tree);
            /* free up large objects list - checkpoint would already have written them back, and
               input cts will keep the extents alive through fini */
            mutex_lock(&da->levels[i].merge.serdes.out_tree->lo_mutex);
            castle_ct_large_objs_remove(&da->levels[i].merge.serdes.out_tree->large_objs);
            mutex_unlock(&da->levels[i].merge.serdes.out_tree->lo_mutex);

            /* don't put the tree - we want the extents kept alive for deserialisation */
            castle_kfree(da->levels[i].merge.serdes.out_tree);
            da->levels[i].merge.serdes.out_tree=NULL;
        }

        if(da->levels[i].merge.serdes.mstore_entry)
        {
            castle_da_merge_serdes_out_tree_check(da->levels[i].merge.serdes.mstore_entry, da, i);
            castle_kfree(da->levels[i].merge.serdes.mstore_entry);
            da->levels[i].merge.serdes.mstore_entry=NULL;
            castle_printk(LOG_INIT, "%s::merge on da %d level %d will resume at next FS start.\n",
                    __FUNCTION__, da->id, i);
        }

    }
    if (da->ios_waiting)
        castle_kfree(da->ios_waiting);
    if (da->t0_lfs)
        castle_kfree(da->t0_lfs);
    /* Poison and free (may be repoisoned on debug kernel builds). */
    memset(da, 0xa7, sizeof(struct castle_double_array));
    castle_kfree(da);
}

static struct castle_double_array* castle_da_alloc(c_da_t da_id)
{
    struct castle_double_array *da;
    int i = 0;
    int nr_cpus = castle_double_array_request_cpus();

    da = castle_zalloc(sizeof(struct castle_double_array), GFP_KERNEL);
    if(!da)
        return NULL;

    castle_printk(LOG_INFO, "Allocating DA=%d\n", da_id);
    da->id              = da_id;
    da->root_version    = INVAL_VERSION;
    rwlock_init(&da->lock);
    da->flags           = 0;
    da->nr_trees        = 0;
    atomic_set(&da->ref_cnt, 1);
    da->attachment_cnt  = 0;
    atomic_set(&da->ios_waiting_cnt, 0);
    if (castle_da_wait_queue_create(da, NULL) != EXIT_SUCCESS)
        goto err_out;
    atomic_set(&da->ios_budget, 0);
    da->ios_rate        = 0;
    da->top_level       = 0;
    atomic_set(&da->nr_del_versions, 0);
    /* For existing double arrays driver merge has to be reset after loading it. */
    da->driver_merge    = -1;
    da->compaction_ct_seq = INVAL_TREE;
    atomic_set(&da->epoch_ios, 0);
    atomic_set(&da->merge_budget, 0);
    atomic_set(&da->ongoing_merges, 0);

    atomic_set(&da->lfs_victim_count, 0);
    da->t0_lfs = castle_malloc(sizeof(struct castle_da_lfs_ct_t) * nr_cpus, GFP_KERNEL);
    if (!da->t0_lfs)
        goto err_out;
    for (i=0; i<nr_cpus; i++)
    {
        da->t0_lfs[i].da = da;
        castle_da_lfs_ct_reset(&da->t0_lfs[i]);
    }

    init_waitqueue_head(&da->merge_waitq);
    init_waitqueue_head(&da->merge_budget_waitq);
    /* Initialise the merge tokens list. */
    INIT_LIST_HEAD(&da->merge_tokens);
    for(i=0; i<MAX_DA_LEVEL-1; i++)
    {
        da->merge_tokens_array[i].driver_level = -1;
        da->merge_tokens_array[i].ref_cnt      = 0;
        list_add(&da->merge_tokens_array[i].list, &da->merge_tokens);
    }
    for(i=0; i<MAX_DA_LEVEL-1; i++)
    {
        /* Initialise merge serdes */
        mutex_init(&da->levels[i].merge.serdes.mutex);
        da->levels[i].merge.serdes.mstore_entry=NULL;
        da->levels[i].merge.serdes.in_tree_mstore_entry_arr=NULL;
        da->levels[i].merge.serdes.out_tree=NULL;
        atomic_set(&da->levels[i].merge.serdes.valid, NULL_DAM_SERDES);

        INIT_LIST_HEAD(&da->levels[i].trees);
        da->levels[i].nr_trees             = 0;
        da->levels[i].nr_compac_trees      = 0;
        da->levels[i].merge.flags          = 0;
        INIT_LIST_HEAD(&da->levels[i].merge.merge_tokens);
        da->levels[i].merge.active_token   = NULL;
        da->levels[i].merge.driver_token   = NULL;
        da->levels[i].merge.units_commited = 0;
        da->levels[i].merge.thread         = NULL;

        /* Low free space structure. */
        da->levels[i].lfs.da = da;
        castle_da_lfs_ct_reset(&da->levels[i].lfs);

        /* Create merge threads, and take da ref for all levels >= 1. */
        castle_da_get(da);
        da->levels[i].merge.thread =
            kthread_create((i == BIG_MERGE)? castle_da_big_merge_run: castle_da_merge_run,
                           da, "castle-m-%d-%.2d", da_id, i);

        if(IS_ERR(da->levels[i].merge.thread) || !da->levels[i].merge.thread)
        {
            castle_printk(LOG_WARN, "Failed to allocate memory for DA threads\n");
            da->levels[i].merge.thread = NULL;
            goto err_out;
        }
    }
    /* allocate top-level */
    INIT_LIST_HEAD(&da->levels[MAX_DA_LEVEL-1].trees);
    da->levels[MAX_DA_LEVEL-1].nr_trees = 0;
    da->levels[MAX_DA_LEVEL-1].lfs.da   = da;
    castle_da_lfs_ct_reset(&da->levels[MAX_DA_LEVEL-1].lfs);

    castle_printk(LOG_USERINFO, "Allocated DA=%d successfully.\n", da_id);

    return da;

err_out:
    {
        int j;
        for(j=0; j<MAX_DA_LEVEL-1; j++)
        {
            BUG_ON((j<i)  && (da->levels[j].merge.thread == NULL));
            BUG_ON((j>=i) && (da->levels[j].merge.thread != NULL));
        }
    }
    castle_da_dealloc(da);

    return NULL;
}

void castle_da_marshall(struct castle_dlist_entry *dam,
                        struct castle_double_array *da)
{
    dam->id           = da->id;
    dam->root_version = da->root_version;
}

static void castle_da_unmarshall(struct castle_double_array *da,
                                 struct castle_dlist_entry *dam)
{
    da->id           = dam->id;
    da->root_version = dam->root_version;
    castle_sysfs_da_add(da);
}

struct castle_component_tree* castle_component_tree_get(tree_seq_t seq)
{
    return castle_ct_hash_get(seq);
}

/**
 * Insert ct into da->levels[ct->level].trees list at index.
 *
 * @param   da      To insert onto
 * @param   ct      To be inserted
 * @param   head    List head to add ct after (or NULL)
 * @param   in_init Set if we are adding a just demarshalled CT from disk
 *
 * WARNING: Caller must hold da->lock
 */
static void castle_component_tree_add(struct castle_double_array *da,
                                      struct castle_component_tree *ct,
                                      struct list_head *head,
                                      int in_init)
{
    struct castle_component_tree *cmp_ct;

    BUG_ON(da->id != ct->da);
    BUG_ON(ct->level >= MAX_DA_LEVEL);
    BUG_ON(read_can_lock(&da->lock));
    BUG_ON(!CASTLE_IN_TRANSACTION);

    /* Default insert point is the front of the list. */
    if (!head)
        head = &da->levels[ct->level].trees;

    /* CTs are sorted by decreasing seq number (newer trees towards the front
     * of the list) to guarantee newest values are returned during gets.
     *
     * Levels 0,1 are a special case as their seq numbers are 'prefixed' with
     * the cpu_index.  This means an older CT would appear before a newer CT if
     * it had a greater cpu_index prefixed.
     *
     * At level 0 this is valid because inserts are disjoint (they go to a
     * specific CT based on the key->cpu_index hash).
     * At level 1 this is valid because CTs from a given cpu_index are still in
     * order, and for the same reasons it is valid at level 0.
     *
     * Skip ordering checks during init (we sort the tree afterwards). */
    if (!in_init && !list_empty(&da->levels[ct->level].trees))
    {
        struct list_head *l;

        /* RWCTs at level 0 are promoted to level 1 in a random order based on how
         * many keys get hashed to which CPU.  As a result for inserts at level 1
         * we search the list to find the correct place to insert these trees. */
        if (ct->level == 1)
        {
            list_for_each(l, &da->levels[ct->level].trees)
            {
                cmp_ct = list_entry(l, struct castle_component_tree, da_list);
                if (ct->seq > cmp_ct->seq)
                    break; /* list_for_each() */
                head = l;
            }
        }

        /* CT seq should be < head->next seq (skip if head is the last elephant) */
        if (!list_is_last(head, &da->levels[ct->level].trees))
        {
            cmp_ct = list_entry(head->next, struct castle_component_tree, da_list);
            BUG_ON(ct->seq <= cmp_ct->seq);
        }
    }

    list_add(&ct->da_list, head);
    da->levels[ct->level].nr_trees++;
    da->nr_trees++;

    if (ct->level > da->top_level)
    {
        BUG_ON(!in_init && (da->top_level + 1 != ct->level));
        da->top_level = ct->level;
        castle_printk(LOG_INFO, "DA: %d growing one level to %d, del_vers: %d\n",
                da->id, ct->level, atomic_read(&da->nr_del_versions));
    }
}

/**
 * Unlink ct from da->level[ct->level].trees list.
 */
static void castle_component_tree_del(struct castle_double_array *da,
                                      struct castle_component_tree *ct)
{
    BUG_ON(da->id != ct->da);
    BUG_ON(read_can_lock(&da->lock));
    BUG_ON(!CASTLE_IN_TRANSACTION);

    list_del(&ct->da_list);
    ct->da_list.next = NULL;
    ct->da_list.prev = NULL;
    if (ct->compacting)
        da->levels[ct->level].nr_compac_trees--;
    else
        da->levels[ct->level].nr_trees--;
    da->nr_trees--;
}

/**
 * Promote ct to next level.
 *
 * @param   da      Doubling array to promote ct in
 * @param   level   CT to promote
 * @param   in_init Set if we are adding a just demarshalled CT from disk
 */
static void castle_component_tree_promote(struct castle_double_array *da,
                                          struct castle_component_tree *ct,
                                          int in_init)
{
    castle_component_tree_del(da, ct);
    ct->level++;
    castle_component_tree_add(da, ct, NULL /* append */, in_init);
}

static void castle_ct_large_obj_writeback(struct castle_large_obj_entry *lo,
                                          struct castle_component_tree *ct)
{
    struct castle_lolist_entry mstore_entry;

    mstore_entry.ext_id = lo->ext_id;
    mstore_entry.length = lo->length;
    mstore_entry.ct_seq = ct->seq;

    castle_mstore_entry_insert(castle_lo_store, &mstore_entry);
}

static void __castle_ct_large_obj_remove(struct list_head *lh)
{
    struct castle_large_obj_entry *lo = list_entry(lh, struct castle_large_obj_entry, list);

    /* Remove LO from list. */
    list_del(&lo->list);

    /* Unlink LO extent from this CT. If it from merge, the output CT could hold a link to it. */
    castle_extent_unlink(lo->ext_id);

    /* Free memory. */
    castle_kfree(lo);
}

/* Remove a large object from list attached to the CT. This gets called only from T0 replaces.
 * This call could be inefficient due to linear search for LO through the complete list. But,
 * this gets called for only LO replaces.
 * Also this remove blocks new LO insertions as it is holding the mutex. Instead it might be a
 * good idea to not maintain LO list for T0. (that would make T0 persistence hard) */
int castle_ct_large_obj_remove(c_ext_id_t           ext_id,
                               struct list_head    *lo_list_head,
                               struct mutex        *mutex)
{
    struct list_head *lh, *tmp;
    int ret = -1;

    /* Get mutex on LO list. */
    if (mutex) mutex_lock(mutex);

    /* Search for LO we are trying to remove in the list. */
    list_for_each_safe(lh, tmp, lo_list_head)
    {
        struct castle_large_obj_entry *lo = list_entry(lh, struct castle_large_obj_entry, list);

        if (lo->ext_id == ext_id)
        {
            /* Remove LO from list. */
            __castle_ct_large_obj_remove(lh);
            ret = 0;
            break;
        }
    }
    if (mutex) mutex_unlock(mutex);

    return ret;
}

static void castle_ct_large_objs_remove(struct list_head *lo_list_head)
{
    struct list_head *lh, *tmp;

    /* no need of lock. Called from castle_ct_put. There shouldnt be any parallel operations. */
    list_for_each_safe(lh, tmp, lo_list_head)
        __castle_ct_large_obj_remove(lh);
}

int castle_ct_large_obj_add(c_ext_id_t              ext_id,
                            uint64_t                length,
                            struct list_head       *head,
                            struct mutex           *mutex)
{
    struct castle_large_obj_entry *lo;

    if (EXT_ID_INVAL(ext_id))
        return -EINVAL;

    lo = castle_malloc(sizeof(struct castle_large_obj_entry), GFP_KERNEL);
    if (!lo)
        return -ENOMEM;

    lo->ext_id = ext_id;
    lo->length = length;

    if (mutex) mutex_lock(mutex);
    list_add(&lo->list, head);
    if (mutex) mutex_unlock(mutex);

    return 0;
}

/**
 * Get a reference to the CT.
 *
 * @param ct    Component Tree to get bump reference count on
 * @param write True to get a write reference count
 *              False to get a read reference count
 *
 * NOTE: Caller should hold castle_da_lock.
 */
void castle_ct_get(struct castle_component_tree *ct, int write)
{
    atomic_inc(&ct->ref_count);
    if (write)
        atomic_inc(&ct->write_ref_count);
}

void castle_ct_put(struct castle_component_tree *ct, int write)
{
    BUG_ON(in_atomic());
    if(write)
        atomic_dec(&ct->write_ref_count);

    if(likely(!atomic_dec_and_test(&ct->ref_count)))
        return;

    BUG_ON(atomic_read(&ct->write_ref_count) != 0);

    debug("Ref count for ct id=%d went to 0, releasing.\n", ct->seq);
    /* If the ct still on the da list, this must be an error. */
    if(ct->da_list.next != NULL)
    {
        castle_printk(LOG_ERROR, "CT=%d, still on DA list, but trying to remove.\n", ct->seq);
        BUG();
    }
    /* Destroy the component tree */
    BUG_ON(TREE_GLOBAL(ct->seq) || TREE_INVAL(ct->seq));
    castle_ct_hash_remove(ct);

    debug("Releasing freespace occupied by ct=%d\n", ct->seq);
    /* Freeing all large objects. */
    castle_ct_large_objs_remove(&ct->large_objs);

    /* Free the extents. */
    castle_ext_freespace_fini(&ct->internal_ext_free);
    castle_ext_freespace_fini(&ct->tree_ext_free);
    castle_ext_freespace_fini(&ct->data_ext_free);

    if (ct->bloom_exists)
        castle_bloom_destroy(&ct->bloom);

    /* Poison ct (note this will be repoisoned by kfree on kernel debug build. */
    memset(ct, 0xde, sizeof(struct castle_component_tree));
    castle_kfree(ct);
}

/**
 * Promote level 0 RWCTs if they number differently to request-handling CPUS.
 *
 * @param   da  Doubling array to verify promotions for
 */
static int castle_da_level0_check_promote(struct castle_double_array *da, void *unused)
{
    write_lock(&da->lock);
    if (da->levels[0].nr_trees != castle_double_array_request_cpus())
    {
        struct castle_component_tree *ct;
        struct list_head *l, *tmp;

        castle_printk(LOG_INFO, "DA previously imported on system with different CPU "
                "count.  Promoting RWCTs at level 0 to level 1.\n");

        list_for_each_safe(l, tmp, &da->levels[0].trees)
        {
            ct = list_entry(l, struct castle_component_tree, da_list);
            castle_component_tree_promote(da, ct, 1);
        }
    }
    write_unlock(&da->lock);

    return 0;
}

/**
 * Promote modified level 0 RWCTs so the checkpoint thread writes them out.
 *
 * @param   work    Work structure (embedded in DA structure)
 *
 * @also castle_da_level0_modified_promote()
 */
static void __castle_da_level0_modified_promote(struct work_struct *work)
{
    struct castle_double_array *da;
    struct castle_component_tree *ct;
    int cpu_index;

    da = container_of(work, struct castle_double_array, work);

    /* Wait until *we* set the growing bit. */
    while (castle_da_growing_rw_test_and_set(da) != EXIT_SUCCESS)
        msleep_interruptible(1);

    for (cpu_index = 0; cpu_index < castle_double_array_request_cpus(); cpu_index++)
    {
        ct = castle_da_rwct_get(da, cpu_index);

        /* Promote level 0 CTs if they contain items.
         * CTs at level 1 will be written to disk by the checkpoint thread. */
        if (atomic64_read(&ct->item_count) != 0)
        {
            castle_printk(LOG_INFO, "Promote for DA 0x%x level 0 RWCT seq %u (has %ld items)\n",
                    da->id, ct->seq, atomic64_read(&ct->item_count));
            __castle_da_rwct_create(da, cpu_index, 0 /*in_tran*/, LFS_VCT_T_INVALID);
        }

        castle_ct_put(ct, 1 /*write*/);
    }

    castle_da_growing_rw_clear(da);

    /* Drop DA reference, adjust promoting DAs counter and signal caller. */
    castle_da_put(da);
    atomic_dec((atomic_t *)da->private);
    wake_up(&castle_da_promote_wq);
}

/**
 * Promote modified level 0 RWCTs so the checkpoint thread writes them out.
 *
 * @param   da      DA to search for modified level 0 RWCTs in
 * @param   counter To count outstanding DA promotes
 *
 * Schedule each DA promote on a workqueue as castle_da_rwct_create() might
 * sleep which is not safe while holding the da hash spinlock.
 *
 * @return  0       So hash iterator continues
 *
 * @also __castle_da_level0_modified_promote()
 * @also castle_double_arrays_writeback()
 */
static int castle_da_level0_modified_promote(struct castle_double_array *da, void *counter)
{
    atomic_inc((atomic_t *)counter);

    if (castle_da_deleted(da))
        return 0;

    /* Get DA reference and place on workqueue. */
    castle_da_get(da);
    da->private = counter;
    CASTLE_INIT_WORK(&da->work, __castle_da_level0_modified_promote);
    schedule_work(&da->work);

    return 0;
}

static int castle_da_trees_sort(struct castle_double_array *da, void *unused)
{
    int i;

    write_lock(&da->lock);
    for(i=0; i<MAX_DA_LEVEL; i++)
        list_sort(&da->levels[i].trees, castle_da_ct_dec_cmp);
    write_unlock(&da->lock);

    return 0;
}

void castle_da_ct_marshall(struct castle_clist_entry *ctm,
                           struct castle_component_tree *ct)
{
    int i;

    ctm->da_id             = ct->da;
    ctm->item_count        = atomic64_read(&ct->item_count);
    ctm->btree_type        = ct->btree_type;
    ctm->dynamic           = ct->dynamic;
    ctm->seq               = ct->seq;
    ctm->level             = ct->level;
    ctm->tree_depth        = ct->tree_depth;
    ctm->root_node         = ct->root_node;
    ctm->large_ext_chk_cnt = atomic64_read(&ct->large_ext_chk_cnt);
    for(i=0; i<MAX_BTREE_DEPTH; i++)
        ctm->node_sizes[i] = ct->node_sizes[i];

    castle_ext_freespace_marshall(&ct->internal_ext_free, &ctm->internal_ext_free_bs);
    castle_ext_freespace_marshall(&ct->tree_ext_free, &ctm->tree_ext_free_bs);
    castle_ext_freespace_marshall(&ct->data_ext_free, &ctm->data_ext_free_bs);

    ctm->bloom_exists = ct->bloom_exists;
    if (ct->bloom_exists)
        castle_bloom_marshall(&ct->bloom, ctm);
}

/**
 * Read an existing component tree from disk.
 *
 * - Prefetches btree extent for T0s.
 */
static c_da_t castle_da_ct_unmarshall(struct castle_component_tree *ct,
                                      struct castle_clist_entry *ctm)
{
    int i;

    ct->seq                 = ctm->seq;
    atomic_set(&ct->ref_count, 1);
    atomic_set(&ct->write_ref_count, 0);
    atomic64_set(&ct->item_count, ctm->item_count);
    ct->btree_type          = ctm->btree_type;
    ct->dynamic             = ctm->dynamic;
    ct->da                  = ctm->da_id;
    ct->level               = ctm->level;
    ct->tree_depth          = ctm->tree_depth;
    ct->root_node           = ctm->root_node;
    ct->new_ct              = 0;
    ct->compacting          = 0;
    atomic64_set(&ct->large_ext_chk_cnt, ctm->large_ext_chk_cnt);
    init_rwsem(&ct->lock);
    mutex_init(&ct->lo_mutex);
    for(i=0; i<MAX_BTREE_DEPTH; i++)
        ct->node_sizes[i] = ctm->node_sizes[i];
    castle_ext_freespace_unmarshall(&ct->internal_ext_free, &ctm->internal_ext_free_bs);
    castle_ext_freespace_unmarshall(&ct->tree_ext_free, &ctm->tree_ext_free_bs);
    castle_ext_freespace_unmarshall(&ct->data_ext_free, &ctm->data_ext_free_bs);
    castle_extent_mark_live(ct->internal_ext_free.ext_id, ct->da);
    castle_extent_mark_live(ct->tree_ext_free.ext_id, ct->da);
    castle_extent_mark_live(ct->data_ext_free.ext_id, ct->da);
    ct->da_list.next = NULL;
    ct->da_list.prev = NULL;
    INIT_LIST_HEAD(&ct->large_objs);
    ct->bloom_exists = ctm->bloom_exists;
    if (ctm->bloom_exists)
        castle_bloom_unmarshall(&ct->bloom, ctm);
    /* Pre-warm cache for T0 btree extents. */
    if (ct->level == 0)
    {
        /* CHUNK() will give us the offset of the last btree node (from chunk 0)
         * so bump it by 1 to get the number of chunks to prefetch. */
        uint64_t nr_bytes = atomic64_read(&ct->tree_ext_free.used);
        int chunks = (nr_bytes)? (CHUNK(nr_bytes - 1) + 1): 0;

        castle_cache_advise((c_ext_pos_t){ct->tree_ext_free.ext_id, 0},
                C2_ADV_EXTENT|C2_ADV_PREFETCH, chunks, -1, 0);
    }

    return ctm->da_id;
}

/**
 * Run fn() on each CT in the doubling array.
 *
 * @param da    Doubling array's CTs to enumerate
 * @param fn    Function to pass each of the da's CTs too
 * @param token ???
 */
static void __castle_da_foreach_tree(struct castle_double_array *da,
                                     int (*fn)(struct castle_double_array *da,
                                               struct castle_component_tree *ct,
                                               int level_cnt,
                                               void *token),
                                     void *token)
{
    struct castle_component_tree *ct;
    struct list_head *lh, *t;
    int i, j;

    for(i=0; i<MAX_DA_LEVEL; i++)
    {
        j = 0;
        list_for_each_safe(lh, t, &da->levels[i].trees)
        {
            ct = list_entry(lh, struct castle_component_tree, da_list);
            if(fn(da, ct, j, token))
                return;
            j++;
        }
    }
}

static void castle_da_foreach_tree(struct castle_double_array *da,
                                   int (*fn)(struct castle_double_array *da,
                                             struct castle_component_tree *ct,
                                             int level_cnt,
                                             void *token),
                                   void *token)
{
    write_lock(&da->lock);
    __castle_da_foreach_tree(da, fn, token);
    write_unlock(&da->lock);
}

static int castle_ct_hash_destroy_check(struct castle_component_tree *ct, void *ct_hash)
{
    struct list_head *lh, *t;
    int    err = 0;

    /* Only the global component tree should remain when we destroy DA hash. */
    if(((unsigned long)ct_hash > 0) && !TREE_GLOBAL(ct->seq))
    {
        castle_printk(LOG_WARN, "Error: Found CT=%d not on any DA's list, it claims DA=%d\n",
            ct->seq, ct->da);
        err = -1;
    }

   /* All CTs apart of global are expected to be on a DA list. */
   if(!TREE_GLOBAL(ct->seq) && (ct->da_list.next == NULL))
   {
       castle_printk(LOG_WARN, "Error: CT=%d is not on DA list, for DA=%d\n",
               ct->seq, ct->da);
       err = -2;
   }

   if(TREE_GLOBAL(ct->seq) && (ct->da_list.next != NULL))
   {
       castle_printk(LOG_WARN, "Error: Global CT=%d is on DA list, for DA=%d\n",
               ct->seq, ct->da);
       err = -3;
   }

   /* Ref count should be 1 by now. */
   if(atomic_read(&ct->ref_count) != 1)
   {
       castle_printk(LOG_WARN, "Error: Bogus ref count=%d for ct=%d, da=%d when exiting.\n",
               atomic_read(&ct->ref_count), ct->seq, ct->da);
       err = -4;
   }

   BUG_ON(err);

   /* Free large object structures. */
   list_for_each_safe(lh, t, &ct->large_objs)
   {
       struct castle_large_obj_entry *lo =
                list_entry(lh, struct castle_large_obj_entry, list);
       list_del(lh);
       castle_kfree(lo);
   }

    return 0;
}

static int castle_da_ct_dealloc(struct castle_double_array *da,
                                struct castle_component_tree *ct,
                                int level_cnt,
                                void *unused)
{
    castle_ct_hash_destroy_check(ct, (void*)0UL);
    list_del(&ct->da_list);
    list_del(&ct->hash_list);
    castle_kfree(ct);

    return 0;
}

static int castle_da_hash_dealloc(struct castle_double_array *da, void *unused)
{
    BUG_ON(!da);
    castle_sysfs_da_del(da);
    castle_da_foreach_tree(da, castle_da_ct_dealloc, NULL);
    list_del(&da->hash_list);
    castle_da_dealloc(da);

    return 0;
}

static void castle_da_hash_destroy(void)
{
    /* No need for the lock, end-of-day stuff. */
   __castle_da_hash_iterate(castle_da_hash_dealloc, NULL);
   castle_kfree(castle_da_hash);
}

static void castle_ct_hash_destroy(void)
{
    castle_ct_hash_iterate(castle_ct_hash_destroy_check, (void *)1UL);
    castle_kfree(castle_ct_hash);
}

/**
 * Flush CT's extents to disk and marshall CT structure.
 *
 * @note any changes here will require a review of the handling of incomplete cct checkpointing
 *       which is necessary for merge checkpointing.
 * @also castle_da_writeback
 */
static int castle_da_tree_writeback(struct castle_double_array *da,
                                    struct castle_component_tree *ct,
                                    int level_cnt,
                                    void *unused)
{
    struct castle_clist_entry mstore_entry;
    struct list_head *lh, *tmp;

    /* For periodic checkpoints flush component trees onto disk. */
    if (!castle_da_exiting)
    {
        /* Always writeback Global tree structure but, don't writeback. */
        /* Note: Global Tree is not Crash-Consistent. */
        if (TREE_GLOBAL(ct->seq))
            goto mstore_writeback;

        /* Don't write back T0. */
        if (ct->level == 0)
            return 0;

        /* Don't write back trees with outstanding writes. */
        if (atomic_read(&ct->write_ref_count) != 0)
            return 0;

        /* Mark new trees for flush. */
        if (ct->new_ct)
        {
            /* Schedule flush of new CT onto disk. */
            if(!EXT_ID_INVAL(ct->internal_ext_free.ext_id))
                castle_cache_extent_flush_schedule(ct->internal_ext_free.ext_id, 0,
                                               atomic64_read(&ct->internal_ext_free.used));
            castle_cache_extent_flush_schedule(ct->tree_ext_free.ext_id, 0,
                                               atomic64_read(&ct->tree_ext_free.used));
            castle_cache_extent_flush_schedule(ct->data_ext_free.ext_id, 0,
                                               atomic64_read(&ct->data_ext_free.used));
            if(ct->bloom_exists)
                castle_cache_extent_flush_schedule(ct->bloom.ext_id, 0, 0);

            ct->new_ct = 0;
        }
    }

mstore_writeback:
    /* Never writeback T0 in periodic checkpoints. */
    BUG_ON((ct->level == 0) && !castle_da_exiting);

    mutex_lock(&ct->lo_mutex);
    list_for_each_safe(lh, tmp, &ct->large_objs)
    {
        struct castle_large_obj_entry *lo =
                            list_entry(lh, struct castle_large_obj_entry, list);

        castle_ct_large_obj_writeback(lo, ct);
    }
    mutex_unlock(&ct->lo_mutex);

    castle_da_ct_marshall(&mstore_entry, ct);
    castle_mstore_entry_insert(castle_tree_store, &mstore_entry);

    return 0;
}

static int castle_da_hash_count(struct castle_double_array *da, void *_count)
{
    uint32_t *count = _count;

    (*count)++;
    return 0;
}

uint32_t castle_da_count(void)
{
    uint32_t count = 0;

    castle_da_hash_iterate(castle_da_hash_count, (void *)&count);

    return count;
}

/* assumes caller took serdes.mutex */
static void castle_da_merge_writeback(struct castle_double_array *da, unsigned int level)
{
    struct castle_dmserlist_entry *merge_mstore;
    struct castle_in_tree_merge_state_entry *in_tree_merge_mstore_arr;
    struct castle_component_tree *ct;
    c_merge_serdes_state_t current_state;
    unsigned int i;

    BUG_ON(!da);
    BUG_ON(level > MAX_DA_LEVEL);
    BUG_ON((level < MIN_DA_SERDES_LEVEL) && (level != BIG_MERGE));

    ct = da->levels[level].merge.serdes.out_tree;
    BUG_ON(!ct);

    merge_mstore = da->levels[level].merge.serdes.mstore_entry;
    BUG_ON(!merge_mstore);

    /* sanity check that there isn't a mismatch between the serdes state
       and where it's located in the da structure */
    BUG_ON(da->id != merge_mstore->da_id);
    BUG_ON(level  != merge_mstore->level);

    in_tree_merge_mstore_arr = da->levels[level].merge.serdes.in_tree_mstore_entry_arr;
    BUG_ON(!in_tree_merge_mstore_arr);

    /* sanity check each input tree state structure */
    for(i=0; i<merge_mstore->nr_trees; i++)
    {
        BUG_ON(da->id != in_tree_merge_mstore_arr[i].da_id);
        BUG_ON(level  != in_tree_merge_mstore_arr[i].level);
        BUG_ON(i      != in_tree_merge_mstore_arr[i].pos_in_merge_struct);
    }

    current_state = atomic_read(&da->levels[level].merge.serdes.valid);

    castle_printk(LOG_DEBUG, "%s::checkpointing merge on da %d, level %d\n",
            __FUNCTION__, da->id, level);

    /* writeback LOs */
    {
        struct list_head *lh, *tmp;
        mutex_lock(&ct->lo_mutex);
        list_for_each_safe(lh, tmp, &ct->large_objs)
        {
            struct castle_large_obj_entry *lo =
                list_entry(lh, struct castle_large_obj_entry, list);
            int lo_ref_cnt = castle_extent_link_count_get(lo->ext_id);
            /* input ct and/or output ct will have ref */
            BUG_ON(lo_ref_cnt < 1);
            debug("%s::writeback lo at ext %d\n", __FUNCTION__,
                    lo->ext_id);
            castle_ct_large_obj_writeback(lo, ct);
        }
        mutex_unlock(&ct->lo_mutex);
    }

    /* insert merge state into mstore */
    castle_mstore_entry_insert(castle_dmser_store, merge_mstore);
    for(i=0; i<merge_mstore->nr_trees; i++)
        castle_mstore_entry_insert(castle_dmser_in_tree_store, &in_tree_merge_mstore_arr[i]);

    /* flush extents if neccesary */
    if(current_state == VALID_AND_FRESH_DAM_SERDES)
    {
        c_merge_serdes_state_t new_state;
        /* we have fresh serialisation state, so flush output tree extents */
        struct castle_clist_entry *cl    = &merge_mstore->out_tree;

        /* make sure extents are valid */
        BUG_ON(EXT_ID_INVAL(cl->internal_ext_free_bs.ext_id));
        BUG_ON(EXT_ID_INVAL(cl->tree_ext_free_bs.ext_id));
        BUG_ON(EXT_ID_INVAL(cl->data_ext_free_bs.ext_id));

        /* make sure serialised extents match live extents */
        BUG_ON(ct->internal_ext_free.ext_id != cl->internal_ext_free_bs.ext_id);
        BUG_ON(ct->tree_ext_free.ext_id     != cl->tree_ext_free_bs.ext_id);
        BUG_ON(ct->data_ext_free.ext_id     != cl->data_ext_free_bs.ext_id);

        /* make sure we haven't somehow moved backwards on each extent */
        BUG_ON(atomic64_read(&ct->internal_ext_free.used) <
                cl->internal_ext_free_bs.used);
        BUG_ON(atomic64_read(&ct->tree_ext_free.used)     <
                cl->tree_ext_free_bs.used);
        BUG_ON(atomic64_read(&ct->data_ext_free.used)     <
                cl->data_ext_free_bs.used);

        debug("%s::flushing out_tree extents for ongoing merge on "
                "da %d, level %d:\n", __FUNCTION__, da->id, i);

        debug("%s::    internal_ext_free_bs ext_id = %lld.\n",
                __FUNCTION__, cl->internal_ext_free_bs.ext_id);
        /* always flush from 0, to account for the possibility that the castle_merges_checkpoint
           param may have been toggled from 0 to 1 halfway during some merges */
        castle_cache_extent_flush_schedule(
                cl->internal_ext_free_bs.ext_id, 0, cl->internal_ext_free_bs.used);

        debug("%s::    tree_ext_free_bs ext_id = %lld.\n",
                __FUNCTION__, cl->tree_ext_free_bs.ext_id);
        castle_cache_extent_flush_schedule(
                cl->tree_ext_free_bs.ext_id, 0, cl->tree_ext_free_bs.used);

        debug("%s::    data_ext_free_bs ext_id = %lld.\n",
                __FUNCTION__, cl->data_ext_free_bs.ext_id);
        castle_cache_extent_flush_schedule(
                cl->data_ext_free_bs.ext_id, 0, cl->data_ext_free_bs.used);

        if(cl->bloom_exists)
        {
            BUG_ON(EXT_ID_INVAL(cl->bloom_ext_id));
            BUG_ON(ct->bloom.ext_id != cl->bloom_ext_id);
            debug("%s::    bloom ext_id = %lld.\n",
                    __FUNCTION__, cl->bloom_ext_id);
            castle_cache_extent_flush_schedule(cl->bloom_ext_id, 0, 0);
        }
        new_state = VALID_AND_STALE_DAM_SERDES;
        atomic_set(&da->levels[level].merge.serdes.valid, (int)new_state);
    }

}

/**
 * Checkpoint function for DAs (including merges); calls mstore_insert.
 *
 * @param da [in] doubling array
 *
 * @note called through castle_da_hash_iterate from castle_double_arrays_writeback
 * @note blocks on serdes.mutex
 */
static int castle_da_writeback(struct castle_double_array *da, void *unused)
{
    struct castle_dlist_entry mstore_dentry;

    castle_da_marshall(&mstore_dentry, da);

    /* We get here with hash spinlock held. But since we're calling sleeping functions
       we need to drop it. Hash consitancy is guaranteed, because by this point
       noone should be modifying it anymore */
    read_unlock_irq(&castle_da_hash_lock);

    /* Writeback is happening under CASTLE_TRANSACTION LOCK, which guarentees no
     * addition/deletions to component tree list, no need of DA lock here. */
    __castle_da_foreach_tree(da, castle_da_tree_writeback, NULL);

    debug("Inserting a DA id=%d\n", da->id);
    castle_mstore_entry_insert(castle_da_store, &mstore_dentry);

    if(castle_merges_checkpoint)
    {
        int i; /* DA levels */
        for(i=0; i<MAX_DA_LEVEL-1; i++)
        {
            c_merge_serdes_state_t current_state;

            mutex_lock(&da->levels[i].merge.serdes.mutex);
            current_state = atomic_read(&da->levels[i].merge.serdes.valid);
            if( (current_state == VALID_AND_FRESH_DAM_SERDES) ||
                    (current_state == VALID_AND_STALE_DAM_SERDES) )
                castle_da_merge_writeback(da, i);
            mutex_unlock(&da->levels[i].merge.serdes.mutex);
        }/* rof each level */
    }

    read_lock_irq(&castle_da_hash_lock);
    return 0;
}

/**
 * Write double array structures to mstores.
 *
 * NOTE: Called within CASTLE_TRANSACTION.
 *
 * @also castle_double_arrays_pre_writeback()
 */
void castle_double_arrays_writeback(void)
{
    BUG_ON(castle_da_store || castle_tree_store || castle_lo_store
           || castle_dmser_store || castle_dmser_in_tree_store);

    castle_da_store   = castle_mstore_init(MSTORE_DOUBLE_ARRAYS,
                                         sizeof(struct castle_dlist_entry));
    castle_tree_store = castle_mstore_init(MSTORE_COMPONENT_TREES,
                                         sizeof(struct castle_clist_entry));
    castle_lo_store   = castle_mstore_init(MSTORE_LARGE_OBJECTS,
                                         sizeof(struct castle_lolist_entry));
    castle_dmser_store= castle_mstore_init(MSTORE_DA_MERGE,
                                         sizeof(struct castle_dmserlist_entry));
    castle_dmser_in_tree_store= castle_mstore_init(MSTORE_DA_MERGE_IN_TREE,
                                         sizeof(struct castle_in_tree_merge_state_entry));

    if(!castle_da_store || !castle_tree_store || !castle_lo_store
        || !castle_dmser_store || !castle_dmser_in_tree_store)
        goto out;

    castle_da_hash_iterate(castle_da_writeback, NULL);
    castle_da_tree_writeback(NULL, &castle_global_tree, -1, NULL);

out:
    if (castle_dmser_in_tree_store) castle_mstore_fini(castle_dmser_in_tree_store);
    if (castle_dmser_store) castle_mstore_fini(castle_dmser_store);
    if (castle_lo_store)    castle_mstore_fini(castle_lo_store);
    if (castle_tree_store)  castle_mstore_fini(castle_tree_store);
    if (castle_da_store)    castle_mstore_fini(castle_da_store);

    castle_da_store = castle_tree_store = castle_lo_store =
        castle_dmser_store = castle_dmser_in_tree_store = NULL;
}

/**
 * Perform any work prior to castle_double_arrays_writeback() outside of transaction lock.
 *
 * NOTE: Called outside of CASTLE_TRANSACTION.
 *
 * @also castle_double_arrays_writeback()
 */
void castle_double_arrays_pre_writeback(void)
{
    static int rwct_checkpoints = 0;
    atomic_t in_flight = ATOMIC(0);

    if (!castle_da_exiting && ++rwct_checkpoints >= castle_rwct_checkpoint_frequency)
    {
        /* Promote non-empty CTs in all DAs. */
        castle_da_hash_iterate(castle_da_level0_modified_promote, &in_flight);
        /* Wait for all promotes to complete. */
        wait_event(castle_da_promote_wq, atomic_read(&in_flight) == 0);

        rwct_checkpoints = 0;
    }
}

/**
 * Create one RWCT per strand for specified DA if they do not already exist.
 *
 * - Allocate one CT per CPU handling requests
 *
 * When any of these CTs subsequently get exhausted a new CT is allocated and
 * the old CT promoted in an atomic fashion (da->lock held).  This means we are
 * guaranteed to have none or all of the CTs at level 0.
 *
 * @param [inout] DA Double-Array structure
 * @param [in] type of the Low Free Space structure - Set to LFS_T_VCT_INVALID,
 *             if no need to handle low free space events.
 *
 * @also castle_double_array_start()
 * @also castle_da_rwct_create()
 */
static int castle_da_all_rwcts_create(struct castle_double_array *da, c_lfs_vct_type_t lfs_type)
{
    struct list_head *l, *p;
    LIST_HEAD(list);
    int cpu_index, had_to_wait = 0;

    /* Wait until *we* set the growing bit. */
    while (castle_da_growing_rw_test_and_set(da) != EXIT_SUCCESS)
    {
        had_to_wait = 1;
        /* Cannot sleep in interruptible, because this function is called from an IOCTL,
           and therefore in a user process context. This could be interrupted, and go into
           busy loop. This would cause a softlockup. */
        msleep(1);
    }

    /* We can return immediately if:
     * - the RWCTs already exist (yay!)
     * - we had to wait to get the growing lock (another thread just tried to
     *   create the RWCTs and failed; most likely we will also fail) */
    read_lock(&da->lock);
    if (!list_empty(&da->levels[0].trees))
    {
        BUG_ON(da->levels[0].nr_trees != castle_double_array_request_cpus());
        read_unlock(&da->lock);
        goto out;
    }
    else if (had_to_wait)
        goto err_out;
    else
        BUG_ON(da->levels[0].nr_trees != 0);
    read_unlock(&da->lock);

    /* No RWCTs at level 0 in this DA.  Create on per request-handling CPU. */
    for (cpu_index = 0; cpu_index < castle_double_array_request_cpus(); cpu_index++)
    {
        if (__castle_da_rwct_create(da, cpu_index, 1 /*in_tran*/, lfs_type) != EXIT_SUCCESS)
        {
            castle_printk(LOG_WARN, "Failed to create T0 %d for DA %u\n", cpu_index, da->id);
            goto err_out;
        }
    }

    /* Clear the growing bit and return success. */
    castle_printk(LOG_INFO, "Created %d CTs for DA %u T0\n", cpu_index, da->id);
out:
    castle_da_growing_rw_clear(da);
    return 0;

err_out:
    /* We were unable to allocate all of the T0s we need.  Free the ones we did
     * manage to allocate.  Splice them into a private list first. */
    write_lock(&da->lock);
    list_splice_init(&da->levels[0].trees, &list);
    write_unlock(&da->lock);
    list_for_each_safe(l, p, &list)
    {
        struct castle_component_tree *ct;
        list_del(l);
        l->next = NULL; /* for castle_ct_put() */
        l->prev = NULL; /* for castle_ct_put() */
        ct = list_entry(l, struct castle_component_tree, da_list);
        castle_ct_put(ct, 0);
    }

    /* Clear the growing bit and return failure. */
    castle_da_growing_rw_clear(da);
    return -EINVAL;
}

/**
 * Wrapper for castle_da_all_rwcts_create() to be called from castle_double_array_start().
 *
 * - Ignores errors
 * - Always returns 0 (to force iterator to continue)
 */
static int castle_da_rwct_init(struct castle_double_array *da, void *unused)
{
    castle_da_all_rwcts_create(da, LFS_VCT_T_INVALID);

    return 0;
}

/**
 * Start existing doubling arrays.
 *
 * - Called during module initialisation only
 *
 * @also castle_fs_init()
 * @also castle_double_array_read()
 */
int castle_double_array_start(void)
{
    /* Check all DAs to see whether any merges need to be done. */
    castle_da_hash_iterate(castle_da_merge_start, NULL);
    castle_da_hash_iterate(castle_da_merge_restart, NULL);

    return 0;
}

static int castle_da_ct_bloom_build_param_deserialise(struct castle_component_tree *ct,
                                                      struct castle_bbp_entry *bbpm)
{
    /* memory allocation and some sanity checking: */
    if(!ct->bloom_exists)
    {
        castle_printk(LOG_ERROR, "%s::no bloom filter attached to CT %d, "
                "yet we have build_params. Weird.\n", __FUNCTION__, ct->seq);
        BUG(); /* relax this if we might ever end up in this situation */
        return -ENXIO;
    }

    BUG_ON(ct->bloom.btree->magic != RO_VLBA_TREE_TYPE);
    ct->bloom.private = castle_zalloc(sizeof(struct castle_bloom_build_params), GFP_KERNEL);
    if(!ct->bloom.private)
    {
        castle_printk(LOG_ERROR, "%s::failed to deserialise bloom build parameters for CT %d; "
                "discarding bloom filter on this CT.", __FUNCTION__, ct->seq);
        castle_bloom_abort(&ct->bloom);
        castle_bloom_destroy(&ct->bloom);
        ct->bloom_exists=0;
        BUG(); /* Out of memory at init time? relax this if it's a possible valid situation */
        return -ENOMEM;
    }

    /* actual deserialisation work happens here: */
    castle_bloom_build_param_unmarshall(&ct->bloom, bbpm);
    return 0;
}

/**
 * Read doubling arrays and serialised component trees in from disk.
 *
 * - Called during module initialisation only
 *
 * @also castle_fs_init()
 */
int castle_double_array_read(void)
{
    struct castle_dlist_entry mstore_dentry;
    struct castle_clist_entry mstore_centry;
    struct castle_lolist_entry mstore_loentry;
    struct castle_mstore_iter *iterator = NULL;
    struct castle_component_tree *ct;
    struct castle_double_array *da;
    c_mstore_key_t key;
    c_da_t da_id;
    int ret = 0;
    debug("%s::start.\n", __FUNCTION__);

    castle_da_store   = castle_mstore_open(MSTORE_DOUBLE_ARRAYS,
                                         sizeof(struct castle_dlist_entry));
    castle_dmser_store = castle_mstore_open(MSTORE_DA_MERGE,
                                         sizeof(struct castle_dmserlist_entry));
    castle_dmser_in_tree_store = castle_mstore_open(MSTORE_DA_MERGE_IN_TREE,
                                         sizeof(struct castle_in_tree_merge_state_entry));
    castle_tree_store = castle_mstore_open(MSTORE_COMPONENT_TREES,
                                         sizeof(struct castle_clist_entry));
    castle_lo_store   = castle_mstore_open(MSTORE_LARGE_OBJECTS,
                                         sizeof(struct castle_lolist_entry));

    if(!castle_da_store || !castle_dmser_store || !castle_dmser_in_tree_store ||
            !castle_tree_store || !castle_lo_store)
        goto error_out;

    /* Read doubling arrays */
    iterator = castle_mstore_iterate(castle_da_store);
    if(!iterator)
        goto error_out;

    while(castle_mstore_iterator_has_next(iterator))
    {
        castle_mstore_iterator_next(iterator, &mstore_dentry, &key);
        da = castle_da_alloc(mstore_dentry.id);
        if(!da)
            goto error_out;
        castle_da_unmarshall(da, &mstore_dentry);
        castle_da_hash_add(da);
        debug("Read DA id=%d\n", da->id);
        castle_next_da_id = (da->id >= castle_next_da_id) ? da->id + 1 : castle_next_da_id;
    }
    castle_mstore_iterator_destroy(iterator);

    /* Merge deserialisation is roughly a 4-stage process:

       Stage 1) Read merge mstore_entry (corresponding to the output tree state and most of the
                state in the castle_da_merge structure, place in castle_double_array.
       Stage 2) Read the input tree state (mainly iterators), variable number of these, place in
                castle_double_array.
       Stage 3) LO handling
       Stage 4) Merge thread deserialises castle_da_merge structure, and initialises then
                "fast-forwards" iterator state to match serialised state.

       DA recovery must preceed all of this so Stage 1 has somewhere to put the deserialising merge
       state for merge thread to find. OTOH, Stage 1 must preceed LO recovery because we string our
       partially completed trees onto the ct hash for LO handling.

       Stage 1 will identify the number of trees in the merge, and will therefore allocate space
       for Stage 2.

       Stage 2 will find iterator state in a some arbitrary sequence (well maybe not, but we assume
       nothing about expected sequence), but each mstore entry will contain enough information
       (da id, level, and even the relative tree position within the merge) for the state to be
       inserted in the right place, provided place has been allocated. Clearly Stage 2 has to come
       after Stage 1. Having Stage 2 after CT recovery means we san do more sanity checking.

       Stage 3 is then just the normal LO recovery.

       Stage 4 (which happens in the merge thread) will then pull state off castle_double_array.

       IN SUMMARY, merge deserialisation is laid out in the following overall sequence:
           1) Recover DA
           2) Merge DES Stage 1
           3) Recover CTs
           4) Merge DES Stage 2
           5) Recover LOs (effectively Merge DES Stage 3)
            -- end of this func --
           6) Merge DES Stage 4 (merge thread)
    */
    /* Stage 1 merge DES */
    iterator = castle_mstore_iterate(castle_dmser_store);
    if(!iterator)
        goto error_out;
    while(castle_mstore_iterator_has_next(iterator))
    {
        int da_id, ct_da_id;
        int level;
        struct castle_double_array *des_da = NULL;
        struct castle_dmserlist_entry *mstore_dmserentry = NULL;

        mstore_dmserentry =
            castle_zalloc(sizeof(struct castle_dmserlist_entry), GFP_KERNEL);
        if(!mstore_dmserentry)
        {
            castle_printk(LOG_ERROR, "%s:: castle_malloc fail\n", __FUNCTION__);
            BUG();
        }

        castle_mstore_iterator_next(iterator, mstore_dmserentry, &key);
        da_id=mstore_dmserentry->da_id;
        level=mstore_dmserentry->level;
        BUG_ON((level < MIN_DA_SERDES_LEVEL) && (level != BIG_MERGE));

        castle_printk(LOG_DEBUG, "%s::deserialising merge on da %d level %d\n",
                __FUNCTION__, da_id, level);
        des_da=castle_da_hash_get(da_id);
        if(!des_da)
        {
            castle_printk(LOG_ERROR, "%s::could not find da %d\n", __FUNCTION__, da_id);
            BUG();
        }

        /* we know the da and the level, and we passed some sanity checking - so put the serdes
           state in the appropriate merge slot */
        des_da->levels[level].merge.serdes.mstore_entry = mstore_dmserentry;

        /* Recover partially complete output CT */
        des_da->levels[level].merge.serdes.out_tree = NULL;
        des_da->levels[level].merge.serdes.out_tree =
            castle_zalloc(sizeof(struct castle_component_tree), GFP_KERNEL);
        BUG_ON(!des_da->levels[level].merge.serdes.out_tree);
        ct_da_id = castle_da_ct_unmarshall(des_da->levels[level].merge.serdes.out_tree,
                                        &des_da->levels[level].merge.serdes.mstore_entry->out_tree);
        BUG_ON(da_id != ct_da_id);
        castle_ct_hash_add(des_da->levels[level].merge.serdes.out_tree);
        castle_printk(LOG_DEBUG, "%s::deserialising merge on da %d level %d with incomplete ct, seq %d\n",
                __FUNCTION__, da_id, level, des_da->levels[level].merge.serdes.out_tree->seq);
        /* the difference btwn unmarshalling a partially complete in-merge ct and a "normal" ct is
           unlike a normal ct (see code below), a partially complete in-merge ct does not get
           added to a DA through cct_add(da, ct, NULL, 1). */

        /* bloom_build_param recovery is left to merge thread (castle_da_merge_deserialise) */

        /* sanity check merge output tree state */
        castle_da_merge_serdes_out_tree_check(mstore_dmserentry, des_da, level);

        /* inc ct seq number if necessary */
        if (des_da->levels[level].merge.serdes.out_tree->seq >= atomic_read(&castle_next_tree_seq))
            atomic_set(&castle_next_tree_seq, des_da->levels[level].merge.serdes.out_tree->seq+1);

        /* allocate space required for Stage 2 DES */
        des_da->levels[level].merge.serdes.in_tree_mstore_entry_arr =
            castle_zalloc(sizeof(struct castle_in_tree_merge_state_entry)*mstore_dmserentry->nr_trees,
                    GFP_KERNEL);
        if(!des_da->levels[level].merge.serdes.in_tree_mstore_entry_arr)
        {
            castle_printk(LOG_ERROR, "%s:: castle_vmalloc fail\n", __FUNCTION__);
            BUG();
        }
        /* notify merge thread that there is a deserialising merge */
        des_da->levels[level].merge.serdes.des=1;

        /* set merge state as immediately re-checkpointable */
        atomic_set(&des_da->levels[level].merge.serdes.valid, VALID_AND_STALE_DAM_SERDES);

        if(level == BIG_MERGE)
        {
            castle_da_need_compaction_set(des_da);
            des_da->compaction_ct_seq = des_da->levels[level].merge.serdes.out_tree->seq;
        }

    }
    castle_mstore_iterator_destroy(iterator);

    /* Read component trees */
    iterator = castle_mstore_iterate(castle_tree_store);
    if(!iterator)
        goto error_out;

    while(castle_mstore_iterator_has_next(iterator))
    {
        uint32_t ct_seq;

        castle_mstore_iterator_next(iterator, &mstore_centry, &key);
        /* Special case for castle_global_tree, it doesn't have a da associated with it. */
        if(TREE_GLOBAL(mstore_centry.seq))
        {
            da_id = castle_da_ct_unmarshall(&castle_global_tree, &mstore_centry);
            BUG_ON(!DA_INVAL(da_id));
            castle_ct_hash_add(&castle_global_tree);
            continue;
        }
        /* Otherwise allocate a ct structure */
        ct = castle_malloc(sizeof(struct castle_component_tree), GFP_KERNEL);
        if(!ct)
            goto error_out;
        da_id = castle_da_ct_unmarshall(ct, &mstore_centry);
        castle_ct_hash_add(ct);
        da = castle_da_hash_get(da_id);
        if(!da)
            goto error_out;
        debug("Read CT seq=%d\n", ct->seq);
        write_lock(&da->lock);
        castle_component_tree_add(da, ct, NULL /*head*/, 1 /*in_init*/);
        write_unlock(&da->lock);
        /* Calculate maximum CT sequence number. Be wary of T0 sequence numbers, they prefix
         * CPU indexes. */
        ct_seq = ct->seq & ((1 << TREE_SEQ_SHIFT) - 1);
        if (ct_seq >= atomic_read(&castle_next_tree_seq))
            atomic_set(&castle_next_tree_seq, ct_seq+1);
    }
    castle_mstore_iterator_destroy(iterator);
    iterator = NULL;
    debug("castle_next_da_id = %d, castle_next_tree_id=%d\n",
            castle_next_da_id,
            atomic_read(&castle_next_tree_seq));

    /* Stage 2 merge DES */
    iterator = castle_mstore_iterate(castle_dmser_in_tree_store);
    if(!iterator)
        goto error_out;
    while(castle_mstore_iterator_has_next(iterator))
    {
        int da_id;
        int level;
        int pos;
        int seq;
        struct castle_double_array *da;
        struct castle_in_tree_merge_state_entry *mstore_in_tree_merge_state_entry;
        struct castle_component_tree *ct;

        mstore_in_tree_merge_state_entry = castle_zalloc(sizeof(struct castle_in_tree_merge_state_entry), GFP_KERNEL);
        if(!mstore_in_tree_merge_state_entry)
        {
            castle_printk(LOG_ERROR, "%s:: castle_malloc fail\n", __FUNCTION__);
            BUG();
        }

        castle_mstore_iterator_next(iterator, mstore_in_tree_merge_state_entry, &key);

        da_id = mstore_in_tree_merge_state_entry->da_id;
        level = mstore_in_tree_merge_state_entry->level;
        pos   = mstore_in_tree_merge_state_entry->pos_in_merge_struct;
        seq   = mstore_in_tree_merge_state_entry->seq;
        BUG_ON(TREE_GLOBAL(seq));
        BUG_ON(TREE_INVAL(seq));

        BUG_ON(da_id==0);
        BUG_ON((level < MIN_DA_SERDES_LEVEL) && (level != BIG_MERGE));
        BUG_ON(level > MAX_DA_LEVEL);

        da = castle_da_hash_get(da_id);
        BUG_ON(!da);

        /* there must already be a corresponding mstore_entry */
        BUG_ON(!da->levels[level].merge.serdes.mstore_entry);
        /* the array should already have been allocated */
        BUG_ON(!da->levels[level].merge.serdes.in_tree_mstore_entry_arr);
        /* if BUG on following, then there may be a mismatch between the merge state and input
           tree merge state, with the former expecting fewer trees and therefore Stage 1 having
           not allocated enough space. */
        BUG_ON(pos > da->levels[level].merge.serdes.mstore_entry->nr_trees);
        /* if BUG on the following, then we are trying to insert more than one in_tree_merge_state entry
           into the same slot. */
        BUG_ON(da->levels[level].merge.serdes.in_tree_mstore_entry_arr[pos].da_id != 0);

        ct = castle_ct_hash_get(mstore_in_tree_merge_state_entry->seq);
        BUG_ON(!ct);
        BUG_ON(ct->da    != da_id);
        if(level != BIG_MERGE)
        {
            castle_printk(LOG_DEBUG, "%s::ct level = %d, level = %d\n", __FUNCTION__, ct->level, level);
            BUG_ON(ct->level != level);
        }

        memcpy(&da->levels[level].merge.serdes.in_tree_mstore_entry_arr[pos],
                mstore_in_tree_merge_state_entry, sizeof(struct castle_in_tree_merge_state_entry));
        castle_free(mstore_in_tree_merge_state_entry);
        mstore_in_tree_merge_state_entry=NULL;

        castle_printk(LOG_DEBUG, "%s::recovered input tree (seq=%d) merge state for da %d level %d pos %d.\n",
                __FUNCTION__,
                da->levels[level].merge.serdes.in_tree_mstore_entry_arr[pos].seq,
                da->levels[level].merge.serdes.in_tree_mstore_entry_arr[pos].da_id,
                da->levels[level].merge.serdes.in_tree_mstore_entry_arr[pos].level,
                da->levels[level].merge.serdes.in_tree_mstore_entry_arr[pos].pos_in_merge_struct);
    }
    castle_mstore_iterator_destroy(iterator);

    /* Read all Large Objects lists. */
    /* (implicit Stage 3 merge DES) */
    iterator = castle_mstore_iterate(castle_lo_store);
    if(!iterator)
        goto error_out;

    while(castle_mstore_iterator_has_next(iterator))
    {
        struct castle_component_tree *ct;

        castle_mstore_iterator_next(iterator, &mstore_loentry, &key);
        ct = castle_component_tree_get(mstore_loentry.ct_seq);
        if (!ct)
        {
            castle_printk(LOG_ERROR, "Found zombi Large Object(%llu, %u)\n",
                    mstore_loentry.ext_id, mstore_loentry.ct_seq);
            BUG();
        }
        if (castle_ct_large_obj_add(mstore_loentry.ext_id,
                                    mstore_loentry.length,
                                    &ct->large_objs, NULL))
        {
            castle_printk(LOG_WARN, "Failed to add Large Object %llu to CT: %u\n",
                    mstore_loentry.ext_id,
                    mstore_loentry.ct_seq);
            goto error_out;
        }
        castle_extent_mark_live(mstore_loentry.ext_id, ct->da);
        debug("%s::Acquired Large Object %llu on CT: %u.\n",
                    __FUNCTION__, mstore_loentry.ext_id, mstore_loentry.ct_seq);
    }
    castle_mstore_iterator_destroy(iterator);
    iterator = NULL;

    /* Promote level 0 RWCTs if necessary. */
    castle_da_hash_iterate(castle_da_level0_check_promote, NULL);
    /* Sort all the tree lists by the sequence number */
    castle_da_hash_iterate(castle_da_trees_sort, NULL);

    /* Create T0 RWCTs for all DAs that don't have them (acquires lock).
     * castle_da_rwct_init() wraps castle_da_rwcts_create() for hash_iter. */
    __castle_da_hash_iterate(castle_da_rwct_init, NULL);

    /* Reset driver merge for all DAs. */
    castle_da_hash_iterate(__castle_da_driver_merge_reset, NULL);

    goto out;

error_out:
    /* The doubling arrays we've created so far should be destroyed by the module fini code. */
    ret = -EINVAL;
out:
    if (iterator)           castle_mstore_iterator_destroy(iterator);
    if (castle_da_store)    castle_mstore_fini(castle_da_store);
    if (castle_tree_store)  castle_mstore_fini(castle_tree_store);
    if (castle_lo_store)    castle_mstore_fini(castle_lo_store);
    if (castle_dmser_store) castle_mstore_fini(castle_dmser_store);
    if (castle_dmser_in_tree_store) castle_mstore_fini(castle_dmser_in_tree_store);
    castle_da_store = castle_dmser_store = castle_dmser_in_tree_store =
            castle_tree_store = castle_lo_store = NULL;

    debug("%s::end.\n", __FUNCTION__);
    return ret;
}

tree_seq_t castle_da_next_ct_seq(void)
{
    return atomic_inc_return(&castle_next_tree_seq);
}

/**
 * Allocate and initialise a CT.
 *
 * - Does not allocate extents
 *
 * @return NULL (CT could not be allocated) or pointer to new CT
 */
static struct castle_component_tree* castle_ct_alloc(struct castle_double_array *da,
                                                     btree_t type,
                                                     int level,
                                                     tree_seq_t seq)
{
    struct castle_component_tree *ct;

    BUG_ON((type != RO_VLBA_TREE_TYPE) && (type != RW_VLBA_TREE_TYPE));
    ct = castle_zalloc(sizeof(struct castle_component_tree), GFP_KERNEL);
    if(!ct)
        return NULL;

    /* Allocate an id for the tree, init the ct. */
    ct->seq             = (TREE_INVAL(seq)? castle_da_next_ct_seq(): seq);
    if(ct->seq >= (1U<<TREE_SEQ_SHIFT))
    {
        castle_printk(LOG_ERROR, "Could not allocate a CT because of sequence # overflow.\n");
        castle_kfree(ct);
        return NULL;
    }
    atomic_set(&ct->ref_count, 1);
    atomic_set(&ct->write_ref_count, 0);
    atomic64_set(&ct->item_count, 0);
    atomic64_set(&ct->large_ext_chk_cnt, 0);
    ct->btree_type      = type;
    ct->dynamic         = type == RW_VLBA_TREE_TYPE ? 1 : 0;
    ct->da              = da->id;
    ct->level           = level;
    ct->tree_depth      = -1;
    ct->root_node       = INVAL_EXT_POS;
    ct->new_ct          = 1;
    ct->compacting      = 0;
    init_rwsem(&ct->lock);
    mutex_init(&ct->lo_mutex);
    ct->da_list.next = NULL;
    ct->da_list.prev = NULL;
    INIT_LIST_HEAD(&ct->hash_list);
    INIT_LIST_HEAD(&ct->large_objs);
    castle_ct_hash_add(ct);
    ct->internal_ext_free.ext_id = INVAL_EXT_ID;
    ct->tree_ext_free.ext_id     = INVAL_EXT_ID;
    ct->data_ext_free.ext_id     = INVAL_EXT_ID;
    ct->bloom_exists    = 0;
#ifdef CASTLE_PERF_DEBUG
    ct->bt_c2bsync_ns   = 0;
    ct->data_c2bsync_ns = 0;
    ct->get_c2b_ns      = 0;
#endif

    return ct;
}

/**
 * Allocate and initialise a T0 component tree.
 *
 * @param da        DA to create new T0 for
 * @param cpu_index Offset within list to insert newly allocated CT
 * @param in_tran   Set if the caller is already within CASTLE_TRANSACTION
 * @param lfs_type  Type of the low free space event handler. Set it to LFS_VCT_T_INVALID.
 *
 * Holds the DA growing lock while:
 *
 * - Allocating a new CT
 * - Allocating data and btree extents
 * - Initialises root btree node
 * - Places allocated CT/extents onto DA list of level 0 CTs
 * - Restarts merges as necessary
 *
 * @also castle_ct_alloc()
 * @also castle_ext_fs_init()
 */
static int __castle_da_rwct_create(struct castle_double_array *da, int cpu_index, int in_tran,
                                   c_lfs_vct_type_t lfs_type)
{
    struct castle_component_tree *ct, *old_ct;
    struct castle_btree_type *btree;
    struct list_head *l = NULL;
    c2_block_t *c2b;
    int err;
#ifdef DEBUG
    static int t0_count = 0;
#endif
    c_ext_event_callback_t lfs_callback;
    void *lfs_data;
    struct castle_da_lfs_ct_t *lfs = &da->t0_lfs[cpu_index];

    if (castle_da_no_disk_space(da))
        return -ENOSPC;

    /* Caller must have set the DA's growing bit. */
    BUG_ON(!castle_da_growing_rw_test(da));

    ct = castle_ct_alloc(da, RW_VLBA_TREE_TYPE, 0 /* level */, INVAL_TREE);
    if (!ct)
        return -ENOMEM;

    btree = castle_btree_type_get(ct->btree_type);

    /* RWCTs are present only at levels 0,1 in the DA.
     * Prefix these CTs with cpu_index to preserve operation ordering when
     * inserting into the DA trees list at RWCT levels. */
    BUG_ON(sizeof(ct->seq) != 4);
    ct->seq = (cpu_index << TREE_SEQ_SHIFT) + ct->seq;

    /* Set callback based on LFS_VCT_T_ type. */
    if (lfs_type == LFS_VCT_T_T0)
    {
        lfs_callback = castle_da_lfs_rwct_callback;
        lfs_data = lfs;
    }
    else
    {
        BUG_ON(lfs_type != LFS_VCT_T_INVALID);
        lfs_callback = NULL;
        lfs_data = NULL;
    }


    /* If the space is not already reserved for the T0, allocate it from freespace. */
    if (!lfs->space_reserved)
    {
        /* Initialize the lfs structure with required extent sizes. */
        /* Note: Init this structure ahead so that, if allocation fails due to low free space
         * use this structure to register for notifications when more space is available. */
        castle_da_lfs_ct_init(lfs,
                              MAX_DYNAMIC_TREE_SIZE,
                              MAX_DYNAMIC_TREE_SIZE,
                              MAX_DYNAMIC_TREE_SIZE,
                              1 /* a T0. */);

        /* Allocate space from freespace. */
        err = castle_da_lfs_ct_space_alloc(lfs,
                                           0,   /* First allocation. */
                                           lfs_callback,
                                           lfs_data,
                                           0);  /* It's a T0. Dont use SSD. */

        /* If failed to allocate space, return error. lfs structure is already set.
         * Low freespace handler would allocate space, when more freespace is available. */
        if (err)    goto no_space;
    }

    /* Successfully allocated space. Initialize the component tree with alloced extents.
     * castle_da_lfs_ct_init_tree() would fail if the space reserved by lfs handler is not
     * enough for CT, but this could never happen for T0. */
    BUG_ON(castle_da_lfs_ct_init_tree(ct, lfs,
                                      MAX_DYNAMIC_TREE_SIZE,
                                      MAX_DYNAMIC_TREE_SIZE,
                                      MAX_DYNAMIC_TREE_SIZE));

    /* Done with lfs structure; reset it. */
    castle_da_lfs_ct_reset(lfs);

    /* Create a root node for this tree, and update the root version */
    ct->tree_depth = 0;
    c2b = castle_btree_node_create(ct,
                                   0 /* version */,
                                   0 /* level */,
                                   0 /* wasn't preallocated */);
    ct->root_node = c2b->cep;
    ct->tree_depth = 1;
    write_unlock_c2b(c2b);
    put_c2b(c2b);

    if (!in_tran) CASTLE_TRANSACTION_BEGIN;
    write_lock(&da->lock);

    /* Find cpu_index^th element from back and promote to level 1. */
    if (cpu_index < da->levels[0].nr_trees)
    {
        int index = 0;
        list_for_each_prev(l, &da->levels[0].trees)
        {
            if (index++ == cpu_index)
            {
                /* Found cpu_index^th element. */
                old_ct = list_entry(l, struct castle_component_tree, da_list);
                l = old_ct->da_list.prev; /* Position to insert new CT. */
                castle_component_tree_promote(da, old_ct, 0 /*in_init*/);
                /* Recompute merge driver. */
                castle_da_driver_merge_reset(da);
                break;
            }
        }
    }
    /* Insert new CT onto list.  l will be the previous element (from delete above) or NULL. */
    castle_component_tree_add(da, ct, l, 0 /* not in init */);

    debug("Added component tree seq=%d, root_node="cep_fmt_str
          ", it's threaded onto da=%p, level=%d\n",
            ct->seq, cep2str(c2b->cep), da, ct->level);

    FAULT(MERGE_FAULT);

    if (!in_tran) CASTLE_TRANSACTION_END;

    debug("Created T0: %d\n", ++t0_count);
    /* DA is attached, therefore we must be holding a ref, therefore it is safe to schedule
       the merge check. */
    write_unlock(&da->lock);
    castle_da_merge_restart(da, NULL);
    return 0;

no_space:
    if (ct)
        castle_ct_put(ct, 0);
    return err;
}

/**
 * Allocate and initialise a T0 component tree.
 *
 * - Attempts to set DA's growing bit
 * - Calls __castle_da_rwct_create() if it set the bit
 * - Otherwise waits for other thread to complete and then exits
 *
 * @param [inout] da - Double-Array.
 * @param [in] cpu_index - cpu index, for which we are creating T0.
 * @param [in] in_tran - is this fucntion called as a part of transaction.
 * @param [in] lfs_type  - Type of the low free space event handler. Set it to LFS_VCT_T_INVALID.
 *
 * @also __castle_da_rwct_create()
 */
static int castle_da_rwct_create(struct castle_double_array *da, int cpu_index, int in_tran,
                                 c_lfs_vct_type_t lfs_type)
{
    int ret;

    /* Serialise per-DA RWCT creation using the growing bit.
     * If it was already set then wait for whomever is already creating a new T0
     * RWCT to complete and return with EAGAIN.  Otherwise create a new T0. */
    if (castle_da_growing_rw_test_and_set(da))
    {
        debug("Racing RWCT make on da=%d\n", da->id);
        /* This look cannot use msleep_interruptible. Read comments in
           castle_da_all_rwcts_create() for more info. */
        while (castle_da_growing_rw_test(da))
            msleep(1); /* @TODO use out_of_line_wait_on_bit(_lock)() here instead */
        return -EAGAIN;
    }
    ret = __castle_da_rwct_create(da, cpu_index, in_tran, lfs_type);
    castle_da_growing_rw_clear(da);

    return ret;
}

/**
 * Allocate a new doubling array.
 *
 * - Called when userland creates a new doubling array
 *
 * @param da_id         id of doubling array (unique)
 * @param root_version  Root version
 *
 * @also castle_control_create()
 * @also castle_da_all_rwcts_create()
 */
int castle_double_array_make(c_da_t da_id, c_ver_t root_version)
{
    struct castle_double_array *da;
    int ret;

    debug("Creating doubling array for da_id=%d, version=%d\n", da_id, root_version);
    da = castle_da_alloc(da_id);
    if(!da)
        return -ENOMEM;
    /* Write out the id, and the root version. */
    da->id = da_id;
    da->root_version = root_version;
    /* Allocate all T0 RWCTs. */
    ret = castle_da_all_rwcts_create(da, LFS_VCT_T_INVALID);
    if (ret != EXIT_SUCCESS)
    {
        castle_printk(LOG_WARN, "Exiting from failed ct create.\n");
        castle_da_dealloc(da);

        return ret;
    }
    debug("Successfully made a new doubling array, id=%d, for version=%d\n",
        da_id, root_version);
    castle_da_hash_add(da);
    castle_sysfs_da_add(da);
    /* DA make succeeded, start merge threads. */
    castle_da_merge_start(da, NULL);

    return 0;
}

/**
 * Frees up the array of ct pointers used to perform gets.
 */
static void castle_da_cts_free(c_bvec_t *c_bvec)
{
    castle_free(c_bvec->trees);
    c_bvec->trees = NULL;
}

/**
 * Advaces current CT used by the bvec provided, to the next one in the trees array.
 * Releases the read reference on the now disused CT.
 *
 * Pointers to the released CTs are replaced with NULLs.
 *
 * @param ct   Current CT to use as basis for finding next CT
 */
void castle_da_ct_next(c_bvec_t *c_bvec)
{
    struct castle_component_tree *ct;
    int i;
    uint8_t compacting_tree = 0;

    ct = c_bvec->tree;
    /* Find the ct in the array of trees. */
    for(i=0; i<c_bvec->nr_trees; i++)
        if(c_bvec->trees[i] == ct)
            break;
    /* Tree must always be found. */
    BUG_ON(i >= c_bvec->nr_trees);

    compacting_tree = ct->compacting;

    /* Remove the tree from the array, and put the reference. */
    c_bvec->trees[i] = NULL;
    castle_ct_put(ct, 0);

    /* Advance to the next tree. */
    i++;
    if(i >= c_bvec->nr_trees)
    {
        c_bvec->tree = NULL;
        castle_da_cts_free(c_bvec);
    }
    else
    {
        c_bvec->tree = c_bvec->trees[i];
        BUG_ON(compacting_tree && !c_bvec->tree->compacting);
    }
}

/**
 * Return cpu_index^th T0 CT for da.
 *
 * - Does not take a reference
 *
 * NOTE: Caller must hold da read-lock.
 *
 * @return  cpu_index^th element from back of da->levels[0].trees list
 */
static struct castle_component_tree* __castle_da_rwct_get(struct castle_double_array *da,
                                                          int cpu_index)
{
    struct list_head *l;

    BUG_ON(cpu_index >= da->levels[0].nr_trees);
    BUG_ON(da->levels[0].nr_trees > num_online_cpus());
    list_for_each_prev(l, &da->levels[0].trees)
    {
        if (cpu_index == 0)
            /* Found cpu_index^th element. */
            return list_entry(l, struct castle_component_tree, da_list);
        else
            cpu_index--;
    }
    BUG_ON(cpu_index < 0);

    return NULL;
}

/**
 * Return cpu_index^th T0 CT for da with a reference held.
 *
 * NOTE: Always returns a valid CT pointer.
 *
 * @also __castle_da_rwct_get()
 */
static struct castle_component_tree* castle_da_rwct_get(struct castle_double_array *da,
                                                        int cpu_index)
{
    struct castle_component_tree *ct = NULL;

    read_lock(&da->lock);
    ct = __castle_da_rwct_get(da, cpu_index);
    BUG_ON(!ct);
    castle_ct_get(ct, 1 /*write*/);
    read_unlock(&da->lock);

    return ct;
}

/**
 * Acquires the write reference to a T0 (appropriate for the cpu_index provided), it dosen't
 * pre-allocate any freespace, but checks that at least 2 nodes worth of space is available.
 * If not, it'll try to allocate new T0.
 *
 * It should only be used if CT hasn't already been acquired through castle_da_reserve().
 * ATM all inserts do go through castle_da_reserve() therefore this function is effectively
 * unused
 */
static struct castle_component_tree* castle_da_rwct_acquire(struct castle_double_array *da,
                                                            int cpu_index)
{
    struct castle_component_tree *ct;
    struct castle_btree_type *btree;
    int ret;


again:
    if (castle_da_no_disk_space(da))
        return NULL;

    ct = castle_da_rwct_get(da, cpu_index);
    BUG_ON(!ct);

    /* Use this tree, but only if there is still some space left in it (otherwise
       we could get stuck in a loop where write fails, but we still use the same CT
       and try again). */
    btree = castle_btree_type_get(ct->btree_type);
    if(castle_ext_freespace_can_alloc(&ct->tree_ext_free,
                                      2 * btree->node_size(ct, 0) * C_BLK_SIZE))
        return ct;

    debug("Number of items in component tree %d, # items %ld. Trying to add a new rwct.\n",
            ct->seq, atomic64_read(&ct->item_count));
    /* Drop reference for old CT. */
    castle_ct_put(ct, 1 /*write*/);

    /* Try creating a new CT. */
    ret = castle_da_rwct_create(da, cpu_index, 0 /* in_tran */, LFS_VCT_T_T0);

    if((ret == 0) || (ret == -EAGAIN))
        goto again;

    castle_printk(LOG_INFO, "Warning: failed to create RWCT with errno=%d\n", ret);

    return NULL;
}

/**
 * Takes references to all CTs in the DA, stores pointers to them in a (malloced)
 * array, and returns it and its size.
 *
 * CT pointers are sorted from newest to oldest.
 */
static int castle_da_all_cts_get(struct castle_double_array *da,
                                 int *nr_cts_p,
                                 struct castle_component_tree ***cts_p)
{
    struct castle_component_tree** cts;
    struct list_head *l;
    int i, j, nr_cts;

again:
    /* Try to allocate the right amount of memory, but remember that nr_trees
       may change, because we are not holding the da lock (cannot kmalloc holding
       a spinlock). */
    nr_cts = da->nr_trees;
    if(nr_cts <= 0)
        return -EINVAL;

    cts = castle_zalloc(nr_cts * sizeof(struct castle_component_tree *), GFP_KERNEL);
    if(!cts)
        return -ENOMEM;

    read_lock(&da->lock);
    /* Check the number of trees under lock. Retry again if # changed. */
    if(nr_cts != da->nr_trees)
    {
        read_unlock(&da->lock);
        castle_printk(LOG_WARN,
                "Warning. Untested. # of cts changed while allocating memory for rq.\n");
        castle_free(cts);
        goto again;
    }
    /* Get refs to all the component trees, and release the lock */
    j=0;

    /* Get all trees that are not compacting. These trees definelty have latest data compared
     * to trees that are compacting, irrespective of their position in DA. */
    for(i=0; i<MAX_DA_LEVEL; i++)
    {
        list_for_each(l, &da->levels[i].trees)
        {
            struct castle_component_tree *ct;

            ct = list_entry(l, struct castle_component_tree, da_list);
            if (ct->compacting)
                continue;

            BUG_ON(j >= nr_cts);
            cts[j] = ct;
            castle_ct_get(ct, 0);
            BUG_ON((castle_btree_type_get(ct->btree_type)->magic != RW_VLBA_TREE_TYPE) &&
                   (castle_btree_type_get(ct->btree_type)->magic != RO_VLBA_TREE_TYPE));
            j++;
        }
    }

    /* Get all trees that are compacting. */
    for(i=0; i<MAX_DA_LEVEL; i++)
    {
        list_for_each(l, &da->levels[i].trees)
        {
            struct castle_component_tree *ct;

            ct = list_entry(l, struct castle_component_tree, da_list);
            if (!ct->compacting)
                continue;

            BUG_ON(j >= nr_cts);
            cts[j] = ct;
            castle_ct_get(ct, 0);
            BUG_ON((castle_btree_type_get(ct->btree_type)->magic != RW_VLBA_TREE_TYPE) &&
                   (castle_btree_type_get(ct->btree_type)->magic != RO_VLBA_TREE_TYPE));
            j++;
        }
    }

    read_unlock(&da->lock);
    BUG_ON(j != nr_cts);

    *nr_cts_p = nr_cts;
    *cts_p = cts;

    return 0;
}

/**
 * Releases references to all remaining CTs stored in c_bvec->trees array, and frees
 * that array.
 */
static void castle_da_cts_put(c_bvec_t *c_bvec)
{
    int i, cur_found;

    /* We expecting the trees array to looks as follows:
       NULL, ..., NULL, c_bvec->tree, ct_b, ct_c, ....
       Check for the layout for debugging purposes. */
    BUG_ON(!c_bvec->tree);
    for(i=0, cur_found=0; i<c_bvec->nr_trees; i++)
    {
        struct castle_component_tree *ct;

        ct = c_bvec->trees[i];
        if(ct == c_bvec->tree)
        {
            cur_found = 1;
            /* Skip c_bvec->tree. This reference must remain. */
            continue;
        }
        BUG_ON(!cur_found && (ct != NULL));
        BUG_ON( cur_found && (ct == NULL));
        if(ct)
        {
            BUG_ON(c_bvec->tree->compacting && !ct->compacting);
            castle_ct_put(ct, 0);
        }
    }
    castle_da_cts_free(c_bvec);
}

/**
 * Queue a write IO for later submission.
 *
 * @param da        Doubling array to queue IO for
 * @param c_bvec    IO to queue
 *
 * WARNING: Caller must hold c_bvec's wait queue lock.
 */
static void castle_da_bvec_queue(struct castle_double_array *da, c_bvec_t *c_bvec)
{
    struct castle_da_io_wait_queue *wq = &da->ios_waiting[c_bvec->cpu_index];

    BUG_ON(!spin_is_locked(&wq->lock));

    /* Queue the bvec. */
    list_add_tail(&c_bvec->io_list, &wq->list);

    /* Increment IO waiting counters. */
    wq->cnt++;
    atomic_inc(&da->ios_waiting_cnt);
}

/**
 * Submit write IOs queued on wait queue to btree.
 *
 * @param work  Embedded in struct castle_da_io_wait_queue
 *
 * - Remove pending IOs from the wait queue so long as ios_budget is positive
 * - Place pending IOs on a new list of IOs to be submitted to the appropriate
 *   btree
 * - We use an intermediate list to minimise the amount of time we hold the
 *   wait queue lock (although subsequent IOs should be hitting the same CPU)
 *
 * @also struct castle_da_io_wait_queue
 * @also castle_da_ios_budget_replenish()
 * @also castle_da_write_bvec_start()
 */
static void castle_da_queue_kick(struct work_struct *work)
{
    struct castle_da_io_wait_queue *wq = container_of(work, struct castle_da_io_wait_queue, work);
    struct list_head *l, *t;
    LIST_HEAD(submit_list);
    c_bvec_t *c_bvec;

    /* Get as many c_bvecs as we can and place them on the submit list.
       Take them all on module exit. */
    spin_lock(&wq->lock);
    while (((atomic_dec_return(&wq->da->ios_budget) >= 0)
                || castle_fs_exiting
                || castle_da_no_disk_space(wq->da))
            && !list_empty(&wq->list))
    {
        /* New IOs are queued at the end of the list.  Always pull from the
         * front of the list to preserve ordering. */
        c_bvec = list_first_entry(&wq->list, c_bvec_t, io_list);
        list_del(&c_bvec->io_list);
        list_add(&c_bvec->io_list, &submit_list);

        /* Decrement IO waiting counters. */
        BUG_ON(--wq->cnt < 0);
        BUG_ON(atomic_dec_return(&wq->da->ios_waiting_cnt) < 0);
    }
    spin_unlock(&wq->lock);

    /* Submit c_bvecs to the btree. */
    list_for_each_safe(l, t, &submit_list)
    {
        c_bvec = list_entry(l, c_bvec_t, io_list);
        list_del(&c_bvec->io_list);
        castle_da_reserve(wq->da, c_bvec);
    }
}

/**
 * Progresses read c_bvec to next CT, or if one is not available it terminates the requset,
 * by calling back to the client.
 */
static void castle_da_ct_read_next(c_bvec_t *c_bvec)
{
    void (*callback) (struct castle_bio_vec *c_bvec, int err, c_val_tup_t cvt);

    callback = c_bvec->orig_complete;

    castle_da_ct_next(c_bvec);

    /* No more trees left. Callback with invalid cvt. */
    if(!c_bvec->tree)
    {
        callback(c_bvec, 0, INVAL_VAL_TUP);
        return;
    }

    debug_verbose("Scheduling btree read in %s tree: %d.\n",
            c_bvec->tree->dynamic ? "dynamic" : "static", c_bvec->tree->seq);
    castle_bloom_submit(c_bvec);
}


/**
 * This is the callback used to complete a btree read. It either:
 * - calls back to the client if the key sought for has been found
 * - calls back to the client if there was an error
 * - calls back to the client if no more component trees are available
 * - otherwise it drops the reference to the last CT, takes reference to the next CT and
 *   submits the request to the bloom filter code (which will call this callback again,
 *   directly or indirectly).
 */
static void castle_da_ct_read_complete(c_bvec_t *c_bvec, int err, c_val_tup_t cvt)
{
    void (*callback) (struct castle_bio_vec *c_bvec, int err, c_val_tup_t cvt);

    callback = c_bvec->orig_complete;

    BUG_ON(c_bvec_data_dir(c_bvec) != READ);
    BUG_ON(atomic_read(&c_bvec->reserv_nodes));

    /* If there tree is null, it means that no more there are no more trees left
       to inspect, callback immediately. */
    if(!c_bvec->tree)
    {
        /* Trees array should have been deallocated by now. */
        BUG_ON(c_bvec->trees);
        callback(c_bvec, err, cvt);
        return;
    }

    /* Deal with counter adds (other component trees may have to be looked at). */
    if(!err && CVT_ADD_ALLV_COUNTER(cvt))
    {
        /* Callback (this should perform counter accumulation).
           NOTE: DA code retakes control after this callback is complete (after counter
           accumulation has been done). */
        callback(c_bvec, err, cvt);

        castle_da_ct_read_next(c_bvec);
        return;
    }

    /* If the key hasn't been found, or it is counter add, go to the next tree. */
    if(!err && CVT_INVALID(cvt))
    {
#ifdef CASTLE_BLOOM_FP_STATS
        if (c_bvec->tree->bloom_exists && c_bvec->bloom_positive)
        {
            atomic64_inc(&c_bvec->tree->bloom.false_positives);
            c_bvec->bloom_positive = 0;
        }
#endif
        debug_verbose("Checking next ct.\n");

        castle_da_ct_read_next(c_bvec);
        return;
    }

    /* Don't release the ct reference in order to hold on to medium objects array, etc.
       But release references to all other trees (those further on in the trees array). */
    castle_da_cts_put(c_bvec);
    callback(c_bvec, err, cvt);
}

/**
 * This function implements the btree write callback. At the moment it is expected that
 * space to do the btree write was preallocated and -ENOSPC error code will never happen
 * (this function is capable of handling this, but castle_da_write_bvec_start is not reentrant).
 *
 * Btree write is handled by releasing any unused preallocated btree extent space,
 * and calling back to the client.
 */
static void castle_da_ct_write_complete(c_bvec_t *c_bvec, int err, c_val_tup_t cvt)
{
    void (*callback) (struct castle_bio_vec *c_bvec, int err, c_val_tup_t cvt);
    struct castle_component_tree *ct;
    struct castle_double_array *da;

    callback = c_bvec->orig_complete;
    ct = c_bvec->tree;
    da = castle_da_hash_get(ct->da);

    /*
     * If the insert space failed, create a new ct, and continue.
     * At the moment we don't expect btree insert to fail, because space is always preallocated.
     */
    BUG_ON(err == -ENOSPC);
    if(err == -ENOSPC)
    {
        /* Release the reference to the tree. */
        castle_ct_put(ct, 1);
        /*
         * For now all inserts reserve space, and castle_da_write_bvec_start is not reentrant,
         * therefore err should never be -ENOSPC.
         */
        BUG();
        castle_da_write_bvec_start(da, c_bvec);
        return;
    }
    BUG_ON(c_bvec_data_dir(c_bvec) != WRITE);
    debug_verbose("Finished with DA, calling back.\n");
    castle_da_merge_budget_io_end(castle_da_hash_get(ct->da));
    /* Release the preallocated space in the btree extent. */
    castle_double_array_unreserve(c_bvec);
    BUG_ON(CVT_MEDIUM_OBJECT(cvt) && (cvt.cep.ext_id != c_bvec->tree->data_ext_free.ext_id));

    /* Don't release the ct reference in order to hold on to medium objects array, etc. */
    callback(c_bvec, err, cvt);
}

/**
 * Hand-off write request (bvec) to DA.
 *
 * - Fail request if there is no free space
 * - Get T0 CT for bvec
 * - Configure completion handlers
 * - Submit immediately to btree
 *
 * @also castle_da_read_bvec_start()
 * @also castle_btree_submit()
 */
static void castle_da_write_bvec_start(struct castle_double_array *da, c_bvec_t *c_bvec)
{
    int reserved;

    debug(LOG_DEBUG, "%s::Doing DA write for da_id=%d\n", __FUNCTION__, da->id);
    BUG_ON(c_bvec_data_dir(c_bvec) != WRITE);

    /* Medium, and only medium inserts must have reserved the space (in leaf nodes extent,
       and in the medium objects extent). */
    reserved = (c_bvec->tree != NULL);

    /* If space was reserved, start the insert immediately. */
    if(reserved)
        goto insert;

    /* If no disk space left, the best we can do is return an error. */
    if (castle_da_no_disk_space(da))
    {
        c_bvec->submit_complete(c_bvec, -ENOSPC, INVAL_VAL_TUP);
        return;
    }

    /* Get a reference to the current RW CT (a new one may be created). */
    c_bvec->tree = castle_da_rwct_acquire(da, c_bvec->cpu_index);
    if (!c_bvec->tree)
    {
        c_bvec->submit_complete(c_bvec, -ENOSPC, INVAL_VAL_TUP);
        return;
    }

insert:
    c_bvec->orig_complete   = c_bvec->submit_complete;
    c_bvec->submit_complete = castle_da_ct_write_complete;

    debug_verbose("Looking up in ct=%d\n", c_bvec->tree->seq);

    /* Submit directly to btree. */
    castle_btree_submit(c_bvec);
}

/**
 * Hand-off read request (bvec) to DA via bloom filter.
 *
 * - Get first CT for bvec (not necessarily a RWCT)
 * - Configure callback handlers
 * - Pass off to the bloom layer
 *
 * @also castle_da_write_bvec_start()
 * @also castle_bloom_submit()
 */
static void castle_da_read_bvec_start(struct castle_double_array *da, c_bvec_t *c_bvec)
{
    int err;

    debug_verbose("Doing DA read for da_id=%d\n", da_id);
    BUG_ON(c_bvec_data_dir(c_bvec) != READ);

    /* Get a reference to the first appropriate CT for this bvec. */
    err = castle_da_all_cts_get(da, &c_bvec->nr_trees, &c_bvec->trees);
    if (err)
    {
        c_bvec->submit_complete(c_bvec, err, INVAL_VAL_TUP);
        return;
    }

    /* Start with the first tree. */
    c_bvec->tree = c_bvec->trees[0];

    c_bvec->orig_complete   = c_bvec->submit_complete;
    c_bvec->submit_complete = castle_da_ct_read_complete;

    debug_verbose("Looking up in ct=%d\n", c_bvec->tree->seq);

    /* Submit via bloom filter. */
#ifdef CASTLE_BLOOM_FP_STATS
    c_bvec->bloom_positive = 0;
#endif
    castle_bloom_submit(c_bvec);
}

/**
 * Submit request to DA, queueing write IOs that are not within the DA ios_budget.
 *
 * Read requests:
 * - Processed immediately
 *
 * Write requests:
 * - Hold appropriate write queue spinlock to guarantee ordering
 * - If we're within ios_budget and there write queue is empty, queue the write
 *   IO immediately
 * - Otherwise queue write IO and wait for the ios_budget to be replenished
 *
 * @also castle_da_bvec_queue()
 * @also castle_da_read_bvec_start()
 * @also castle_da_write_bvec_start()
 */
void castle_double_array_submit(c_bvec_t *c_bvec)
{
    struct castle_attachment *att = c_bvec->c_bio->attachment;
    struct castle_double_array *da;
    c_da_t da_id;

    down_read(&att->lock);
    /* Since the version is attached, it must be found */
    BUG_ON(castle_version_read(att->version, &da_id, NULL, NULL, NULL, NULL));
    up_read(&att->lock);

    da = castle_da_hash_get(da_id);
    BUG_ON(!da);
    /* orig_complete should be null it is for our privte use */
    BUG_ON(c_bvec->orig_complete);

    /* Start the read bvecs without any queueing. */
    if(c_bvec_data_dir(c_bvec) == READ)
    {
        castle_da_read_bvec_start(da, c_bvec);
        return;
    }

    castle_da_write_bvec_start(da, c_bvec);
}

/**
 * Gets write reference to the appropriate T0 (for the cpu_index stored in c_bvec) and
 * reserves space in btree and medium object extents (for medium object writes).
 * It calls back to the client through queue_complete() callback, on success or failure.
 */
static void castle_da_reserve(struct castle_double_array *da, c_bvec_t *c_bvec)
{
    struct castle_component_tree *ct;
    struct castle_btree_type *btree;
    uint64_t value_len, req_btree_space, req_medium_space;
    int ret;

    if(castle_da_no_disk_space(da))
    {
        c_bvec->queue_complete(c_bvec, -ENOSPC);
        return;
    }

    value_len = c_bvec->c_bio->replace->value_len;
again:
    ct = castle_da_rwct_get(da, c_bvec->cpu_index);
    BUG_ON(!ct);

    /* Attempt to preallocate space in the btree and m-obj extents for writes. */
    btree = castle_btree_type_get(ct->btree_type);

    /* We may have to create up to 2 new leaf nodes in this write. Preallocate
       the space for this. */
    req_btree_space = 2 * btree->node_size(ct, 0) * C_BLK_SIZE;
    if (castle_ext_freespace_prealloc(&ct->tree_ext_free, req_btree_space) < 0)
        goto new_ct;
    /* Save how many nodes we've pre-allocated. */
    atomic_set(&c_bvec->reserv_nodes, 2);

    /* Preallocate (ceil to C_BLK_SIZE) space for the medium object. */
    req_medium_space = ((value_len - 1) / C_BLK_SIZE + 1) * C_BLK_SIZE;
    if ( is_medium(value_len) &&
        (castle_ext_freespace_prealloc(&ct->data_ext_free, req_medium_space) < 0))
    {
        /* We failed to preallocate space for the medium object. Free the space in btree extent. */
        castle_ext_freespace_free(&ct->tree_ext_free, req_btree_space);
        atomic_set(&c_bvec->reserv_nodes, 0);
        goto new_ct;
    }
    /* Save the CT in bvec. */
    c_bvec->tree = ct;

    c_bvec->queue_complete(c_bvec, 0);
    return;

new_ct:
    debug("Number of items in component tree %d, # items %ld. Trying to add a new rwct.\n",
            ct->seq, atomic64_read(&ct->item_count));
    /* Drop reference for old CT. */
    castle_ct_put(ct, 1 /*write*/);

    ret = castle_da_rwct_create(da, c_bvec->cpu_index, 0 /* in_tran */, LFS_VCT_T_T0);
    if((ret == 0) || (ret == -EAGAIN))
        goto again;

    BUG_ON(atomic_read(&c_bvec->reserv_nodes) != 0);

    c_bvec->queue_complete(c_bvec, ret);
}

/**
 * Unreserves any nodes reserved in the btree extent associated with the write request provided.
 */
void castle_double_array_unreserve(c_bvec_t *c_bvec)
{
    struct castle_component_tree *ct;
    struct castle_btree_type *btree;
    uint32_t reserv_nodes;

    /* Only works for write requests. */
    BUG_ON(c_bvec_data_dir(c_bvec) != WRITE);

    /* If no nodes are reserved, stop. */
    reserv_nodes = atomic_read(&c_bvec->reserv_nodes);
    if(reserv_nodes == 0)
        return;

    /* Free the nodes. */
    ct = c_bvec->tree;
    btree = castle_btree_type_get(ct->btree_type);
    castle_ext_freespace_free(&ct->tree_ext_free,
                               reserv_nodes * btree->node_size(ct, 0) * C_BLK_SIZE);
    /* Set the reservation back to 0. Don't use atomic_set() because this doesn't use
       locked prefix. */
    atomic_sub(reserv_nodes, &c_bvec->reserv_nodes);
}

/**
 * Submits write requests to this DA write queue which throttles inserts according to the
 * merge progress. Once the write is scheduled for processing (which could happen immediately
 * in the current thread's context) it reserves btree/medium object extent space, and
 * calls back to the user.
 *
 * @also castle_da_reserve()
 */
void castle_double_array_queue(c_bvec_t *c_bvec)
{
    struct castle_attachment *att = c_bvec->c_bio->attachment;
    struct castle_da_io_wait_queue *wq;
    struct castle_double_array *da;
    c_da_t da_id;

    BUG_ON(c_bvec_data_dir(c_bvec) != WRITE);

    down_read(&att->lock);
    /* Since the version is attached, it must be found */
    BUG_ON(castle_version_read(att->version, &da_id, NULL, NULL, NULL, NULL));
    up_read(&att->lock);

    da = castle_da_hash_get(da_id);
    BUG_ON(!da);
    BUG_ON(atomic_read(&c_bvec->reserv_nodes) != 0);

    /* Write requests must operate within the ios_budget but reads can be
     * scheduled immediately. */
    wq = &da->ios_waiting[c_bvec->cpu_index];
    spin_lock(&wq->lock);
    if ((atomic_dec_return(&da->ios_budget) >= 0) && list_empty(&wq->list))
    {
        /* We're within the budget and there are no other IOs on the queue,
         * schedule this write IO immediately. */
        spin_unlock(&wq->lock);
        castle_da_reserve(da, c_bvec);
    }
    else
    {
        /* Either the budget is exhausted or there are other IOs pending on
         * the write queue.  Queue this write IO.
         *
         * Don't do a manual queue kick as if/when ios_budget is replenished
         * kicks for all of the DA's write queues will be scheduled.  The
         * kick for 'our' write queue will block on the spinlock we hold.
         *
         * ios_budget will be replenished; save an atomic op and leave it
         * in a negative state. */
        castle_da_bvec_queue(da, c_bvec);
        spin_unlock(&wq->lock);
    }
}

/**************************************/
/* Double Array Management functions. */

int castle_double_array_create(void)
{
    /* Make sure that the global tree is in the ct hash */
    castle_ct_hash_add(&castle_global_tree);

    return 0;
}

/**
 * Initialise global doubling array state.
 */
int castle_double_array_init(void)
{
    int ret, cpu, i, j;
    int min_budget, budget;

    ret = -ENOMEM;

    /* Allocate merge workqueues. */
    for (i = 0; i < NR_CASTLE_DA_WQS; i++)
    {
        castle_da_wqs[i] = create_workqueue(castle_da_wqs_names[i]);
        if (!castle_da_wqs[i])
        {
            castle_printk(LOG_ERROR, KERN_ALERT "Error: Could not alloc wq\n");
            goto err0;
        }
    }

    /* Initialise modlist iter mergesort buffer based on cache size.
     * As a minimum we need to be able to merge two full T0s. */
    min_budget = 2 * MAX_DYNAMIC_TREE_SIZE * C_CHK_SIZE;            /* Two full T0s. */
    budget     = (castle_cache_size_get() * PAGE_SIZE) / 10;        /* 10% of cache. */
    if (budget < min_budget)
        budget = min_budget;
    castle_printk(LOG_INIT, "Allocating %lluMB for modlist iter byte budget.\n",
            budget / C_CHK_SIZE);
    atomic_set(&castle_ct_modlist_iter_byte_budget, budget);
    mutex_init(&castle_da_level1_merge_init);

    /* Populate request_cpus with CPU ids ready to handle requests. */
    request_cpus.cpus = castle_malloc(sizeof(int) * num_online_cpus(), GFP_KERNEL);
    if (!request_cpus.cpus)
        goto err0;
    request_cpus.cnt = 0;
    for_each_online_cpu(cpu)
    {
        request_cpus.cpus[request_cpus.cnt] = cpu;
        request_cpus.cnt++;
    }

    castle_da_hash = castle_da_hash_alloc();
    if(!castle_da_hash)
        goto err1;
    castle_ct_hash = castle_ct_hash_alloc();
    if(!castle_ct_hash)
        goto err2;

    castle_da_hash_init();
    castle_ct_hash_init();
    /* Start up the timer which replenishes merge and write IOs budget */
    castle_throttle_timer_fire(1);

    return 0;

err2:
    castle_kfree(castle_da_hash);
err1:
    castle_kfree(request_cpus.cpus);
err0:
    for (j = 0; j < i; j++)
        destroy_workqueue(castle_da_wqs[j]);
    BUG_ON(!ret);
    return ret;
}

void castle_double_array_merges_fini(void)
{
    int deleted_das;

    castle_da_exiting = 1;
    del_singleshot_timer_sync(&throttle_timer);
    /* This is happening at the end of execution. No need for the hash lock. */
    __castle_da_hash_iterate(castle_da_merge_stop, NULL);
    /* Also, wait for merges on deleted DAs. Merges will hold the last references to those DAs. */
    do {
        CASTLE_TRANSACTION_BEGIN;
        deleted_das = !list_empty(&castle_deleted_das);
        CASTLE_TRANSACTION_END;
        if(deleted_das)
            msleep(10);
    } while(deleted_das);
}

void castle_double_array_fini(void)
{
    int i;
    debug("%s::start.\n", __FUNCTION__);
    castle_da_hash_destroy();
    castle_ct_hash_destroy();

    castle_kfree(request_cpus.cpus);

    for (i = 0; i < NR_CASTLE_DA_WQS; i++)
        destroy_workqueue(castle_da_wqs[i]);
    debug("%s::end.\n", __FUNCTION__);
}

void castle_da_destroy_complete(struct castle_double_array *da)
{ /* Called with lock held. */
    int i;

    /* Sanity Checks. */
    BUG_ON(!castle_da_deleted(da));

    castle_printk(LOG_USERINFO, "Cleaning VerTree: %u\n", da->id);

    /* Destroy Component Trees. */
    for(i=0; i<MAX_DA_LEVEL; i++)
    {
        struct list_head *l, *lt;

        list_for_each_safe(l, lt, &da->levels[i].trees)
        {
            struct castle_component_tree *ct;

            ct = list_entry(l, struct castle_component_tree, da_list);
            /* No out-standing merges and active attachments. Componenet Tree
             * shouldn't be referenced any-where. */
            BUG_ON(atomic_read(&ct->ref_count) != 1);
            BUG_ON(atomic_read(&ct->write_ref_count));
            list_del(&ct->da_list);
            ct->da_list.next = ct->da_list.prev = NULL;
            castle_ct_put(ct, 0);
        }
    }

    /* Destroy Version and Rebuild Version Tree. */
    castle_version_tree_delete(da->root_version);

    /* Delete the DA from the list of deleted DAs. */
    list_del(&da->hash_list);

    /* Dealloc the DA. */
    castle_da_dealloc(da);
}

static void castle_da_get(struct castle_double_array *da)
{
    /* Increment ref count, it should never be zero when we get here. */
    BUG_ON(atomic_inc_return(&da->ref_cnt) <= 1);
}

static void castle_da_put(struct castle_double_array *da)
{
    if(atomic_dec_return(&da->ref_cnt) == 0)
    {
        /* Ref count dropped to zero -> delete. There should be no outstanding attachments. */
        BUG_ON(da->attachment_cnt != 0);
        BUG_ON(!castle_da_deleted(da));
        CASTLE_TRANSACTION_BEGIN;
        castle_da_destroy_complete(da);
        CASTLE_TRANSACTION_END;
    }
}

static void castle_da_put_locked(struct castle_double_array *da)
{
    BUG_ON(!CASTLE_IN_TRANSACTION);
    if(atomic_dec_return(&da->ref_cnt) == 0)
    {
        /* Ref count dropped to zero -> delete. There should be no outstanding attachments. */
        BUG_ON(da->attachment_cnt != 0);
        BUG_ON((da->hash_list.next != NULL) || (da->hash_list.prev != NULL));
        BUG_ON(!castle_da_deleted(da));
        castle_da_destroy_complete(da);
    }
}

int castle_double_array_alive(c_da_t da_id)
{
    BUG_ON(!CASTLE_IN_TRANSACTION);

    return (castle_da_hash_get(da_id)?1:0);
}

int castle_double_array_get(c_da_t da_id)
{
    struct castle_double_array *da;
    unsigned long flags;

    read_lock_irqsave(&castle_da_hash_lock, flags);
    da = __castle_da_hash_get(da_id);
    if(!da)
        goto out;
    castle_da_get(da);
    da->attachment_cnt++;
out:
    read_unlock_irqrestore(&castle_da_hash_lock, flags);

    return (da == NULL ? -EINVAL : 0);
}

void castle_double_array_put(c_da_t da_id)
{
    struct castle_double_array *da;

    /* We only call this for attached DAs which _must_ be in the hash. */
    da = castle_da_hash_get(da_id);
    BUG_ON(!da);
    /* DA allocated + our ref count on it. */
    BUG_ON(atomic_read(&da->ref_cnt) < 2);
    write_lock(&da->lock);
    da->attachment_cnt--;
    write_unlock(&da->lock);
    /* Put the ref cnt too. */
    castle_da_put(da);
}

/* TODO: Check what happens if ioctl functions or sysfs functions need to get reference on
 * resources. */
int castle_double_array_compact(c_da_t da_id)
{
    struct castle_double_array *da;

    da = castle_da_hash_get(da_id);
    if (!da)
        return -EINVAL;

    castle_printk(LOG_USERINFO, "Marking version tree %u for compaction.\n", da_id);
    castle_da_need_compaction_set(da);

    wake_up(&da->merge_waitq);

    return 0;
}

int castle_double_array_destroy(c_da_t da_id)
{
    struct castle_double_array *da;
    unsigned long flags;
    int ret;

    write_lock_irqsave(&castle_da_hash_lock, flags);
    da = __castle_da_hash_get(da_id);
    /* Fail if we cannot find the da in the hash. */
    if(!da)
    {
        castle_printk(LOG_USERINFO, "No Version Tree exists with id: %u\n", da_id);
        ret = -EINVAL;
        goto err_out;
    }
    BUG_ON(da->attachment_cnt < 0);
    /* Fail if there are attachments to the DA. */
    if(da->attachment_cnt > 0)
    {
        castle_printk(LOG_USERINFO, "Version Tree %u has %u outstanding attachments\n",
                      da_id,
                      da->attachment_cnt);
        ret = -EBUSY;
        goto err_out;
    }
    /* Now we are happy to delete the DA. Remove it from the hash. */
    BUG_ON(castle_da_deleted(da));
    __castle_da_hash_remove(da);
    da->hash_list.next = da->hash_list.prev = NULL;
    write_unlock_irqrestore(&castle_da_hash_lock, flags);

    castle_sysfs_da_del(da);

    castle_printk(LOG_USERINFO, "Marking DA %u for deletion\n", da_id);
    /* Set the destruction bit, which will stop further merges. */
    castle_da_deleted_set(da);
    /* Restart the merge threads, so that they get to exit, and drop their da refs. */
    castle_da_merge_restart(da, NULL);
    /* Add it to the list of deleted DAs. */
    list_add(&da->hash_list, &castle_deleted_das);
    /* Put the (usually) last reference to the DA. */
    castle_da_put_locked(da);

    return 0;

err_out:
    write_unlock_irqrestore(&castle_da_hash_lock, flags);
    return ret;
}

/**
 * Set nice value for all merge threads within a DA.
 */
static int __castle_da_threads_priority_set(struct castle_double_array *da, void *_value)
{
    int i;
    int nice_value = *((int *)_value);

    for (i=0; i<MAX_DA_LEVEL; i++)
    {
        if (da->levels[i].merge.thread)
            set_user_nice(da->levels[i].merge.thread, nice_value + 15);
    }

    return 0;
}

/**
 * Change the priority of merge threads for all doubling arrays.
 */
void castle_da_threads_priority_set(int nice_value)
{
    int i;

    castle_da_hash_iterate(__castle_da_threads_priority_set, &nice_value);

    for(i=0; i<NR_CASTLE_DA_WQS; i++)
        castle_wq_priority_set(castle_da_wqs[i]);
}
