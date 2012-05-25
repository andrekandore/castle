#include "castle_extent.h"

#ifndef __CASTLE_CACHE_H__
#define __CASTLE_CACHE_H__

struct castle_cache_block;
typedef void  (*c2b_end_io_t)(struct castle_cache_block *c2b, int did_io);

#define C2B_STATE_BITS_BITS         56          /**< Bits reserved for c2b state.                 */
#define C2B_STATE_PARTITION_OFFSET  C2B_STATE_BITS_BITS /**< Offset of c2b_state.partition.       */
#define C2B_STATE_PARTITION_BITS    4           /**< Bits reserved for c2b cache partition.       */
#define C2B_STATE_ACCESS_BITS       4           /**< Bits reserved for CLOCK access information.  */
#define C2B_STATE_ACCESS_MAX        ((1 << C2B_STATE_ACCESS_BITS) - 1)
STATIC_BUG_ON(C2B_STATE_BITS_BITS + C2B_STATE_PARTITION_BITS + C2B_STATE_ACCESS_BITS != 64);

/**
 * Extent dirtytree structure.
 *
 * ref_cnt: 1 reference held by the extent
 *          1 reference per dirty c2b
 */
typedef struct castle_cache_extent_dirtytree {
    c_ext_id_t          ext_id;             /**< Extent ID this dirtylist describes.            */
    c_ext_id_t          compr_ext_id;       /**< Extent ID of compressed extent.  Will be
                                                 INVAL_EXT_ID if ext_id !C_COMPR_VIRTUAL.       */
    spinlock_t          lock;               /**< Protects count, rb_root.                       */
    atomic_t            ref_cnt;            /**< References to this dirtylist.                  */
    struct rb_root      rb_root;            /**< RB-tree of dirty c2bs.                         */
    struct list_head    list;               /**< Position in a castle_cache_extent_dirtylist.   */
    uint8_t             flush_prio;         /**< Decides which dirtylist to use (low #, is
                                                 higher priority.                               */
    int                 nr_pages;           /**< Sum of c2b->nr_pages for c2bs in tree.
                                                 Protected by lock.                             */
    c_byte_off_t        flushed_off;        /**< Exclusive offset before which flush I/O has
                                                 been dispatched.                               */
    c_byte_off_t        compr_flushed_off;  /**< Exclusive offset before which flush I/O has
                                                 been dispatched in compressed extent.          */
    c_byte_off_t        compr_unit_size;    /**< Compression unit size.                         */
#ifdef CASTLE_PERF_DEBUG
    c_chk_cnt_t         ext_size;           /**< Size of extent when created (in chunks).       */
    c_ext_type_t        ext_type;           /**< Extent type when created.                      */
#endif
} c2_ext_dirtytree_t;

struct castle_cache_page;
typedef struct castle_cache_block {
    c_ext_pos_t                cep;
    atomic_t                   remaining;

    int                        nr_pages;        /**< Number of c2ps mapped by the block           */
    struct castle_cache_page **c2ps;            /**< Array of c2ps backing the buffer             */
    void                      *buffer;          /**< Linear mapping of the pages                  */

    struct hlist_node          hlist;           /**< Entry in castle_cache_block_hash[]           */
    struct list_head           clock;           /**< Position on castle_cache_block_clock.        */
    union {
        struct list_head       free;            /**< Position on castle_cache_block_freelist.     */
        struct list_head       reserve;         /**< Position on castle_cache_block_reservelist.  */
        struct list_head       evict;           /**< Position on castle_cache_block_evictlist.    */
    };
    struct rb_node             rb_dirtytree;    /**< Per-extent dirtytree RB-node.                */
    c2_ext_dirtytree_t        *dirtytree;       /**< Dirtytree c2b is a member of.                */

    struct c2b_state {
        unsigned long          bits:C2B_STATE_BITS_BITS;           /**< State bitfield            */
        unsigned long          partition:C2B_STATE_PARTITION_BITS; /**< Cache partition bitfield  */
        unsigned long          accessed:C2B_STATE_ACCESS_BITS;     /**< CLOCK access bitfield     */
    } state;
    atomic_t                   count;           /**< Count of active consumers                    */
    atomic_t                   lock_cnt;
    c2b_end_io_t               end_io;          /**< IO CB handler routine*/
    void                      *private;         /**< Can only be used if c2b is locked            */
#ifdef CASTLE_DEBUG
    char                      *file;
    int                        line;
#endif
} c2_block_t;

/**
 * Castle cache partition descriptors.
 */
typedef enum {
    IGNORE_PARTITION = -1,                  /**< Cache to ignore partition                        */
    USER = 0,                               /**< Direct user-accessed data                        */
    MERGE_IN,                               /**< Cache used for merge input                       */
    MERGE_OUT = MERGE_IN,                   /**< Cache used for merge output                      */
    NR_CACHE_PARTITIONS,                    /**< Number of cache partitions (must be last).       */
} c2_partition_id_t;
STATIC_BUG_ON(NR_CACHE_PARTITIONS >= C2B_STATE_PARTITION_BITS);

/**********************************************************************************************
 * Locking.
 */
void __lock_c2b                     (c2_block_t *c2b, int write, int first);
int  __trylock_c2b                  (c2_block_t *c2b, int write);
int  __trylock_node                 (c2_block_t *c2b, int write);
void __downgrade_write_c2b          (c2_block_t *c2b, int first);
void __write_unlock_c2b             (c2_block_t *c2b, int first);
void __read_unlock_c2b              (c2_block_t *c2b, int first);
//int write_trylock_c2b             (c2_block_t *c2b);
//int read_trylock_c2b              (c2_block_t *c2b);
//void write_lock_c2b               (c2_block_t *c2b);
//void read_lock_c2b                (c2_block_t *c2b);
#define downgrade_write_c2b(_c2b)   __downgrade_write_c2b(_c2b, 0)
#define write_unlock_c2b(_c2b)      __write_unlock_c2b(_c2b, 0)
#define read_unlock_c2b(_c2b)       __read_unlock_c2b(_c2b, 0)
//int write_trylock_node            (c2_block_t *c2b);
//int read_trylock_node             (c2_block_t *c2b);
//void write_lock_node              (c2_block_t *c2b);
//void read_lock_node               (c2_block_t *c2b);
#define downgrade_write_node(_c2b)  __downgrade_write_c2b(_c2b, 1)
#define write_unlock_node(_c2b)     __write_unlock_c2b(_c2b, 1)
#define read_unlock_node(_c2b)      __read_unlock_c2b(_c2b, 1)
int  c2b_read_locked                (c2_block_t *c2b);
int  c2b_write_locked               (c2_block_t *c2b);

static inline int write_trylock_c2b(c2_block_t *c2b)
{
    return __trylock_c2b(c2b, 1);
}
static inline int read_trylock_c2b(c2_block_t *c2b)
{
    return __trylock_c2b(c2b, 0);
}

static inline int write_trylock_node(c2_block_t *c2b)
{
    return __trylock_node(c2b, 1 /*write*/);
}
static inline int read_trylock_node(c2_block_t *c2b)
{
    return __trylock_node(c2b, 0 /*write*/);
}

/*
 * c2b locks that span all of the c2b->c2ps.
 */
#ifdef CASTLE_DEBUG
#define write_lock_c2b(_c2b)          \
{                                     \
    might_sleep();                    \
    __lock_c2b(_c2b, 1, 0);           \
    (_c2b)->file = __FILE__;          \
    (_c2b)->line = __LINE__;          \
}
#define read_lock_c2b(_c2b)           \
{                                     \
    might_sleep();                    \
    __lock_c2b(_c2b, 0, 0);           \
}
#else /* CASTLE_DEBUG */
#define write_lock_c2b(_c2b)          \
    __lock_c2b(_c2b, 1, 0);
#define read_lock_c2b(_c2b)           \
    __lock_c2b(_c2b, 0, 0);
#endif /* CASTLE_DEBUG */

/*
 * c2b locks that lock just c2b->c2ps[0] (e.g. first c2p).
 */
#ifdef CASTLE_DEBUG
#define write_lock_node(_c2b)         \
{                                     \
    might_sleep();                    \
    __lock_c2b(_c2b, 1, 1);           \
    (_c2b)->file = __FILE__;          \
    (_c2b)->line = __LINE__;          \
}
#define read_lock_node(_c2b)          \
{                                     \
    might_sleep();                    \
    __lock_c2b(_c2b, 0, 1);           \
}
#else /* CASTLE_DEBUG */
#define write_lock_node(_c2b)         \
    __lock_c2b(_c2b, 1, 1);
#define read_lock_node(_c2b)          \
    __lock_c2b(_c2b, 0, 1);
#endif /* CASTLE_DEBUG */

/**********************************************************************************************
 * Dirtying & up-to-date.
 */
int  c2b_dirty              (c2_block_t *c2b);
void dirty_c2b              (c2_block_t *c2b);
void clean_c2b              (c2_block_t *c2b);
int  c2b_uptodate           (c2_block_t *c2b);
void update_c2b             (c2_block_t *c2b);
int  c2b_bio_error          (c2_block_t *c2b);
void set_c2b_no_resubmit    (c2_block_t *c2b);
void clear_c2b_no_resubmit  (c2_block_t *c2b);
int  c2b_remap              (c2_block_t *c2b);
void set_c2b_remap          (c2_block_t *c2b);
void clear_c2b_remap        (c2_block_t *c2b);
void set_c2b_in_flight      (c2_block_t *c2b);
void set_c2b_eio            (c2_block_t *c2b);
void clear_c2b_eio          (c2_block_t *c2b);
int  c2b_eio                (c2_block_t *c2b);

/**********************************************************************************************
 * Refcounts.
 */
static inline void get_c2b(c2_block_t *c2b)
{
    atomic_inc(&c2b->count);
}

static inline void put_c2b(c2_block_t *c2b)
{
#ifdef DEBUG
    BUG_ON(c2b_write_locked(c2b));
#endif
    BUG_ON(atomic_read(&c2b->count) == 0);
    atomic_dec(&c2b->count);
}

#define check_and_put_c2b(_c2b)                                                     \
do {                                                                                \
    if (_c2b)                                                                       \
        put_c2b(_c2b);                                                              \
} while(0)

extern void put_c2b_and_demote(c2_block_t *c2b);


/**********************************************************************************************
 * Advising the cache.
 */
enum c2_advise_bits {
    C2_ADV_cep,
    C2_ADV_extent,  /** @FIXME needs to be folded into C2_ADV_cep */
    C2_ADV_prefetch,
    C2_ADV_hardpin,
    C2_ADV_static,
    C2_ADV_adaptive,
};

typedef uint32_t c2_advise_t;
#define C2_ADV_CEP          ((c2_advise_t) (1<<C2_ADV_cep))
#define C2_ADV_EXTENT       ((c2_advise_t) (1<<C2_ADV_extent))

#define C2_ADV_PREFETCH     ((c2_advise_t) (1<<C2_ADV_prefetch))
#define C2_ADV_HARDPIN      ((c2_advise_t) (1<<C2_ADV_hardpin))

#define C2_ADV_STATIC       ((c2_advise_t) (1<<C2_ADV_static))
#define C2_ADV_ADAPTIVE     ((c2_advise_t) (1<<C2_ADV_adaptive))

int  castle_cache_advise      (c_ext_pos_t cep, c2_advise_t advise, c2_partition_id_t partition, int chunks);
int  castle_cache_advise_clear(c_ext_pos_t cep, c2_advise_t advise, int chunks);
void castle_cache_prefetch_pin(c_ext_pos_t cep, c2_advise_t advise, c2_partition_id_t partition, int chunks);
void castle_cache_extent_flush(c_ext_id_t ext_id, uint64_t start, uint64_t size, unsigned int ratelimit);
void castle_cache_extent_evict(c2_ext_dirtytree_t *dirtytree, c_chk_cnt_t start, c_chk_cnt_t count);
void castle_cache_prefetches_wait(void);

/**********************************************************************************************
 * Misc.
 */
#define c2b_buffer(_c2b)    ((_c2b)->buffer)

int                        castle_stats_read               (void);

/**********************************************************************************************
 * The 'interesting' cache interface functions
 */
int         submit_c2b                (int rw, c2_block_t *c2b);
int         submit_c2b_sync           (int rw, c2_block_t *c2b);
int         submit_c2b_sync_barrier   (int rw, c2_block_t *c2b);
int         submit_c2b_rda            (int rw, c2_block_t *c2b);
int         submit_c2b_remap_rda      (c2_block_t *c2b, c_disk_chk_t *remap_chunks, int nr_remaps);
int         submit_direct_io          (int rw, struct block_device *bdev, sector_t sector,
                                       struct page **iopages, int nr_pages);

int         c2b_has_clean_pages       (c2_block_t *c2b);

int         _castle_cache_block_read  (c2_block_t *c2b, c2b_end_io_t end_io, void *private);
int         castle_cache_block_read   (c2_block_t *c2b, c2b_end_io_t end_io, void *private);
int         castle_cache_block_sync_read(c2_block_t *c2b);
#define     castle_cache_page_block_reserve(_partition) \
            castle_cache_block_get    ((c_ext_pos_t){RESERVE_EXT_ID, 0}, 1, _partition)
c2_block_t* castle_cache_block_get    (c_ext_pos_t cep,
                                       int nr_pages,
                                       c2_partition_id_t partition);
void        castle_cache_block_hardpin  (c2_block_t *c2b);
void        castle_cache_block_unhardpin(c2_block_t *c2b);
void        castle_cache_page_block_unreserve(c2_block_t *c2b);
int         castle_cache_extent_flush_schedule (c_ext_id_t ext_id, uint64_t start, uint64_t size);


int                        castle_checkpoint_init          (void);
void                       castle_checkpoint_fini          (void);
int                        castle_checkpoint_version_inc   (void);
void                       castle_checkpoint_ratelimit_set (unsigned long ratelimit);
void                       castle_checkpoint_wait          (void);
int                        castle_chk_disk                 (void);

void                       castle_cache_stats_print        (int verbose);
int                        castle_cache_size_get           (void);
int                        castle_cache_block_destroy      (c2_block_t *c2b);
void                       castle_cache_dirtytree_demote   (c2_ext_dirtytree_t *dirtytree);
/**********************************************************************************************
 * Cache init/fini.
 */
int  castle_cache_init(void);
void castle_cache_fini(void);

#ifdef CASTLE_DEBUG
void castle_cache_debug(void);
#endif

#define MIN_CHECKPOINT_PERIOD 5
#define MAX_CHECKPOINT_PERIOD 3600

#endif /* __CASTLE_CACHE_H__ */
