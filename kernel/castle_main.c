/* @TODO: when castle disk mounted, and then castle-fs-test run, later bd_claims on
          disk devices fail (multiple castle_fs_cli 'claim' cause problems?) */
#include <linux/bio.h>
#include <linux/kobject.h>
#include <linux/blkdev.h>
#include <linux/random.h>
#include <linux/crc32.h>
#include <linux/skbuff.h>
#include <linux/hardirq.h>
#include <linux/buffer_head.h>

#include "castle_public.h"
#include "castle_compile.h"
#include "castle.h"
#include "castle_utils.h"
#include "castle_da.h"
#include "castle_cache.h"
#include "castle_btree.h"
#include "castle_versions.h"
#include "castle_ctrl.h"
#include "castle_sysfs.h"
#include "castle_time.h"
#include "castle_debug.h"
#include "castle_events.h"
#include "castle_back.h"
#include "castle_extent.h"
#include "castle_freespace.h"
#include "castle_rebuild.h"
#include "castle_ctrl_prog.h"
#include "castle_unit_tests.h"

struct castle               castle;
struct castle_slaves        castle_slaves;

struct castle_attachments    castle_attachments = {
    .attachments    = {&castle_attachments.attachments, &castle_attachments.attachments}
};

static DEFINE_MUTEX(castle_sblk_lock);
struct castle_fs_superblock global_fs_superblock;
struct workqueue_struct     *castle_wqs[2*MAX_BTREE_DEPTH+1];
char                         castle_environment_block[NR_ENV_VARS * MAX_ENV_LEN];
char                        *castle_environment[NR_ENV_VARS]
                                = {[0] = &castle_environment_block[0 * MAX_ENV_LEN],
                                   [1] = &castle_environment_block[1 * MAX_ENV_LEN],
                                   [2] = &castle_environment_block[2 * MAX_ENV_LEN],
                                   [3] = &castle_environment_block[3 * MAX_ENV_LEN],
                                   [4] = &castle_environment_block[4 * MAX_ENV_LEN],
                                   [5] = &castle_environment_block[5 * MAX_ENV_LEN],
                                   [6] = &castle_environment_block[6 * MAX_ENV_LEN],
                                   [7] = &castle_environment_block[7 * MAX_ENV_LEN]};
int                          castle_fs_inited = 0;
int                          castle_fs_exiting = 0;
c_fault_t                    castle_fault = NO_FAULT;
uint32_t                     castle_fault_arg = 0;
c_state_t                    castle_fs_state = CASTLE_STATE_LOADING;

module_param(castle_checkpoint_period, int, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
MODULE_PARM_DESC(castle_checkpoint_period, "checkpoint_period,");

module_param(castle_extents_process_ratelimit, int, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
MODULE_PARM_DESC(castle_extents_process_ratelimit, "extproc_ratelimit,");

module_param(castle_rebuild_freespace_threshold, int, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
MODULE_PARM_DESC(castle_rebuild_freespace_threshold, "extproc_freesp_thresh,");

module_param(castle_meta_ext_compact_pct, int, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
MODULE_PARM_DESC(castle_meta_ext_compact_pct, "meta_ext_compact_pct,");

static DECLARE_WAIT_QUEUE_HEAD(castle_detach_waitq);

//#define DEBUG
#ifndef DEBUG
#define debug(_f, ...)  ((void)0)
#else
#define debug(_f, _a...)  (castle_printk(LOG_DEBUG, "%s:%.4d: " _f, __FILE__, __LINE__ , ##_a))
#endif

static void USED castle_fs_superblock_print(struct castle_fs_superblock *fs_sb)
{
    castle_printk(LOG_INIT, "Magic1: %.8x\n"
           "Magic2: %.8x\n"
           "Magic3: %.8x\n"
           "UUID: %x\n"
           "Version: %d\n"
           "Salt:   %x\n"
           "Pepper: %x\n",
           fs_sb->pub.magic1,
           fs_sb->pub.magic2,
           fs_sb->pub.magic3,
           fs_sb->pub.uuid,
           fs_sb->pub.version,
           fs_sb->pub.salt,
           fs_sb->pub.pepper);
}

static int castle_fs_superblock_validate(struct castle_fs_superblock *fs_sb)
{
    uint32_t checksum = fs_sb->pub.checksum;

    if(fs_sb->pub.magic1 != CASTLE_FS_MAGIC1) return -1;
    if(fs_sb->pub.magic2 != CASTLE_FS_MAGIC2) return -2;
    if(fs_sb->pub.magic3 != CASTLE_FS_MAGIC3) return -3;
    if(fs_sb->pub.version != CASTLE_FS_VERSION) return -4;
    if(fs_sb->fs_version == 0)                  return -5;

    fs_sb->pub.checksum = 0;
    if (fletcher32((uint16_t *)fs_sb, sizeof(struct castle_fs_superblock))
                        != checksum)
    {
        fs_sb->pub.checksum = checksum;
        return -6;
    }
    fs_sb->pub.checksum = checksum;

    return 0;
}

/*
 * Scans castle_slaves and updates the fs superblock with the slave states. This allows the
 * service state of slaves to be persistent across fs shutdowns and crashes.
 *
 * The fs superblock lock must by held on entry to this function.
 *
 * @param fs_sb  Pointer to the fs superblock.
 */
void castle_fs_superblock_slaves_update(struct castle_fs_superblock *fs_sb)
{
    struct castle_slave         *cs;
    struct list_head            *lh;
    int                         i;

    rcu_read_lock();
    list_for_each_rcu(lh, &castle_slaves.slaves)
    {
        cs = list_entry(lh, struct castle_slave, list);
        for (i=0; i<fs_sb->nr_slaves; i++)
        {
            /* Find the index in the fs superblock slave array with this uuid. */
            if (fs_sb->slaves[i] == cs->uuid)
            {
                if (test_bit(CASTLE_SLAVE_OOS_BIT, &cs->flags))
                {
                    debug("Setting slave 0x%x out-of-service bit in fs_sb slaves\n", cs->uuid);
                    set_bit(CASTLE_SLAVE_OOS_BIT, &fs_sb->slaves_flags[i]);
                }
                if (test_bit(CASTLE_SLAVE_EVACUATE_BIT, &cs->flags))
                {
                    debug("Setting slave 0x%x evacuating bit in fs_sb slaves\n", cs->uuid);
                    set_bit(CASTLE_SLAVE_EVACUATE_BIT, &fs_sb->slaves_flags[i]);
                } else
                {
                    debug("Clearing slave 0x%x evacuating bit in fs_sb slaves\n", cs->uuid);
                    clear_bit(CASTLE_SLAVE_EVACUATE_BIT, &fs_sb->slaves_flags[i]);
                }
                if (test_bit(CASTLE_SLAVE_REMAPPED_BIT, &cs->flags))
                {
                    debug("Setting slave 0x%x out-of-service bit in fs_sb slaves\n", cs->uuid);
                    set_bit(CASTLE_SLAVE_REMAPPED_BIT, &fs_sb->slaves_flags[i]);
                }
            }
        }
    }
    rcu_read_unlock();
}

static void castle_fs_superblock_init(struct castle_fs_superblock *fs_sb)
{
    int i;
    struct list_head *lh;
    struct castle_slave *cs;

    fs_sb->pub.magic1 = CASTLE_FS_MAGIC1;
    fs_sb->pub.magic2 = CASTLE_FS_MAGIC2;
    fs_sb->pub.magic3 = CASTLE_FS_MAGIC3;
    do {
        get_random_bytes(&fs_sb->pub.uuid,  sizeof(fs_sb->pub.uuid));
    } while (fs_sb->pub.uuid == 0);
    fs_sb->pub.version = CASTLE_FS_VERSION;
    get_random_bytes(&fs_sb->pub.salt,  sizeof(fs_sb->pub.salt));
    get_random_bytes(&fs_sb->pub.pepper, sizeof(fs_sb->pub.pepper));
    for(i=0; i<sizeof(fs_sb->mstore) / sizeof(c_ext_pos_t ); i++)
        fs_sb->mstore[i] = INVAL_EXT_POS;

    i = 0;
    rcu_read_lock();
    list_for_each_rcu(lh, &castle_slaves.slaves)
    {
        cs = list_entry(lh, struct castle_slave, list);
        fs_sb->slaves[i++] = cs->uuid;
    }
    rcu_read_unlock();
    fs_sb->nr_slaves = i;
}

static void castle_fs_superblocks_init(void)
{
    struct castle_fs_superblock *fs_sb;

    fs_sb = castle_fs_superblocks_get();
    castle_fs_superblock_init(fs_sb);
    castle_fs_superblocks_put(fs_sb, 1);
}

static void castle_fs_superblocks_load(struct castle_fs_superblock *fs_sb)
{
    memcpy(&global_fs_superblock, fs_sb, sizeof(struct castle_fs_superblock));
}

static inline struct castle_fs_superblock* castle_fs_superblock_get(struct castle_slave *cs)
{
    mutex_lock(&cs->sblk_lock);
    return &cs->fs_superblock;
}

static inline void castle_fs_superblock_put(struct castle_slave *cs, int dirty)
{
    mutex_unlock(&cs->sblk_lock);
}

/* Get all superblocks */
struct castle_fs_superblock* castle_fs_superblocks_get(void)
{
    mutex_lock(&castle_sblk_lock);
    return &global_fs_superblock;
}

/* Put all superblocks */
void castle_fs_superblocks_put(struct castle_fs_superblock *sb, int dirty)
{
    mutex_unlock(&castle_sblk_lock);
}

void castle_ext_freespace_size_update(c_ext_free_t *ext_free, int do_checks)
{
    c_chk_cnt_t start, end;

    castle_extent_latest_mask_read(ext_free->ext_id, &start, &end);

    /* This wrapper is written assuming that extent is not yet shrunk. */
    BUG_ON(do_checks && start);

    ext_free->ext_size = end * C_CHK_SIZE;
    BUG_ON(atomic64_read(&ext_free->used) > ext_free->ext_size);

    wmb();
}

/**
 * Initialises the freespace structure for an extent provided.
 *
 * @param ext_free  Pointer to the freespace structure.
 * @param ext_id    Id of the extent for which to initalise the freespace struct.
 */
void castle_ext_freespace_init(c_ext_free_t *ext_free,
                               c_ext_id_t    ext_id)
{
    /* Extent id must be valid. */
    BUG_ON(EXT_ID_INVAL(ext_id));

    /* Init the structure. */
    ext_free->ext_id = ext_id;

    atomic64_set(&ext_free->used, 0);
    atomic64_set(&ext_free->blocked, 0);

    /* Get it from extent layer and update. */
    castle_ext_freespace_size_update(ext_free, 1 /* Do checks. */);
}

/**
 * Allocates a new extent, and initialises the freespace structure for it.
 *
 * @param ext_free      [out]   Pointer to the freespace structure.
 * @param da_id         [in]    Which DA will the new extent belong to.
 * @param size          [in]    Size of the extent, in bytes.
 * @param in_tran       [in]    In extent transaction.
 * @param data          [in]    Data to be used in event handler.
 * @param callback      [in]    Extent Event handler. Current events are just low space events.
 *
 * @return  0       On success.
 * @return -ENOSPC  If extent could not be allocated.
 */
int castle_new_ext_freespace_init(c_ext_free_t           *ext_free,
                                  c_da_t                  da_id,
                                  c_ext_type_t            ext_type,
                                  c_byte_off_t            size,
                                  int                     in_tran)
{
    uint32_t nr_chunks;
    c_ext_id_t ext_id;

    /* Calculate the number of chunks required. */
    nr_chunks = ((size - 1) / C_CHK_SIZE) + 1;
    /* Try allocating the extent of the requested size. */
    ext_id = castle_extent_alloc(castle_get_rda_lvl(),
                                 da_id,
                                 ext_type,
                                 nr_chunks,
                                 in_tran ? CASTLE_EXT_FLAG_MUTEX_LOCKED: CASTLE_EXT_FLAGS_NONE);
    if(EXT_ID_INVAL(ext_id))
        return -ENOSPC;

    /* Initialise the freespace structure. */
    castle_ext_freespace_init(ext_free, ext_id);

    /* Success. */
    return 0;
}

/**
 * Unlink and reset extent if valid.
 */
void castle_ext_freespace_fini(c_ext_free_t *ext_free)
{
    if (EXT_ID_INVAL(ext_free->ext_id))
        /* Invalid extent, return immediately. */
        return;

    /* This is a valid extent, unlink and reset it. */
    castle_extent_unlink(ext_free->ext_id);
    ext_free->ext_id      = INVAL_EXT_ID;
    ext_free->ext_size    = 0;
    atomic64_set(&ext_free->used, 0);
    atomic64_set(&ext_free->blocked, 0);
}

int castle_ext_freespace_consistent(c_ext_free_t *ext_free)
{
    uint64_t used = atomic64_read(&ext_free->used);
    uint64_t blocked = atomic64_read(&ext_free->blocked);

    if (used == blocked)
        return 1;
    return 0;
}

int castle_ext_freespace_prealloc(c_ext_free_t *ext_free,
                                  c_byte_off_t  size)
{
    uint64_t ret;
    uint64_t used;
    uint64_t blocked;

    used = atomic64_read(&ext_free->used);
    barrier();
    blocked = atomic64_read(&ext_free->blocked);

    BUG_ON(blocked < used);
    BUG_ON(used > ext_free->ext_size);

    ret = atomic64_add_return(size, &ext_free->blocked);
    barrier();
    if (ret > ext_free->ext_size)
    {
        atomic64_sub(size, &ext_free->blocked);
        barrier();
        return -1;
    }

    return 0;
}

/**
 * Checks whether there is at least 'size' worth of freespace in the extent.
 */
int castle_ext_freespace_can_alloc(c_ext_free_t *ext_free,
                                   c_byte_off_t size)
{
    return (atomic64_read(&ext_free->blocked) + size <= ext_free->ext_size);
}

int castle_ext_freespace_free(c_ext_free_t *ext_free,
                              int64_t       size)
{
    atomic64_sub(size, &ext_free->blocked);
    barrier();

    return 0;
}

int castle_ext_freespace_get(c_ext_free_t *ext_free,
                             c_byte_off_t  size,
                             int           was_preallocated,
                             c_ext_pos_t  *cep)
{
    uint64_t used;
    uint64_t blocked;

    used = atomic64_read(&ext_free->used);
    barrier();
    blocked = atomic64_read(&ext_free->blocked);

    BUG_ON(blocked < used);
    BUG_ON(used > ext_free->ext_size);

    if (!was_preallocated)
    {
        int ret;

        ret = castle_ext_freespace_prealloc(ext_free, size);
        if (ret < 0)
        {
            castle_printk(LOG_ERROR, "%s [1]::Failed (%llu, %llu, %llu)\n",
                    __FUNCTION__, used, blocked, size);
            return ret;
        }
    }

    cep->ext_id = ext_free->ext_id;
    cep->offset = atomic64_add_return(size, &ext_free->used) - size;
    barrier();
    BUG_ON(EXT_ID_INVAL(cep->ext_id));

    used = atomic64_read(&ext_free->used);
    barrier();
    blocked = atomic64_read(&ext_free->blocked);

    if (blocked < used)
    {
        atomic64_sub(size, &ext_free->used);
        barrier();
        castle_printk(LOG_ERROR, "%s [2]:: Failed (%llu, %llu, %llu)\n",
                __FUNCTION__, used, blocked, size);
        return -2;
    }

    return 0;
}

void castle_ext_freespace_marshall(c_ext_free_t *ext_free, c_ext_free_bs_t *ext_free_bs)
{
    ext_free_bs->ext_id    = ext_free->ext_id;
    ext_free_bs->used      = atomic64_read(&ext_free->used);
    ext_free_bs->blocked   = atomic64_read(&ext_free->blocked);
}

void castle_ext_freespace_unmarshall(c_ext_free_t *ext_free, c_ext_free_bs_t *ext_free_bs)
{
    ext_free->ext_id    = ext_free_bs->ext_id;
    atomic64_set(&ext_free->used, ext_free_bs->used);
    atomic64_set(&ext_free->blocked, ext_free_bs->blocked);
    if (!EXT_ID_INVAL(ext_free->ext_id))
        castle_ext_freespace_size_update(ext_free, 0 /* No checks. */);
}

c_byte_off_t castle_ext_freespace_available(c_ext_free_t *ext_free)
{
    return (ext_free->ext_size - atomic64_read(&ext_free->used));
}

static int castle_slave_version_load(struct castle_slave *cs, uint32_t fs_version);
static void castle_slave_superblock_print(struct castle_slave_superblock *cs_sb);

static int slave_id = 0;

/**
 * Allocates a 'ghost' castle_slave and inserts it into the list of castle_slaves.
 * A 'ghost' entry is inserted for all slaves that used to be, but no longer are,
 * active members of the filesystem. The 'ghost' castle_slave is a light version
 * that is not fully initialised (e.g. no bdev). It allows the filesystem to determine
 * if a slave uuid it encounters (e.g. in a chunk mapping) is for a valid, but no longer
 * used, slave.
 *
 * @param uuid      The uuid for the slave to be added.
 *
 * @ret             A pointer to the newly created ghost castle_slave.
 */
static struct castle_slave *castle_slave_ghost_add(uint32_t uuid)
{
    struct castle_slave *slave;

    BUG_ON(!CASTLE_IN_TRANSACTION);
    if (!(slave = castle_zalloc(sizeof(struct castle_slave))))
        BUG_ON(!slave);
    slave->uuid = uuid;
    slave->id = slave_id++;
    slave->sup_ext = INVAL_EXT_ID;
    mutex_init(&slave->sblk_lock);
    set_bit(CASTLE_SLAVE_GHOST_BIT, &slave->flags);
    set_bit(CASTLE_SLAVE_OOS_BIT, &slave->flags);
    castle_freespace_slave_init(slave, 0);
    INIT_RCU_HEAD(&slave->rcu);
    list_add_rcu(&slave->list, &castle_slaves.slaves);

    if (castle_sysfs_slave_add(slave) != 0)
        debug("Could not add slave 0x%x to sysfs.\n", uuid);

    return slave;
}

extern atomic_t current_rebuild_seqno;

static int castle_unit_tests(void)
{
    int test_seq_id = 0;
    int err = 0;

    test_seq_id++; if (0 != (err = castle_slim_tree_unit_tests_do() ) ) goto fail;

    BUG_ON(err);
    castle_printk(LOG_INIT, "%s::%d tests passed.\n", __FUNCTION__, test_seq_id);
    return 0;
fail:
    castle_printk(LOG_ERROR, "%s::test %d failed with return code %d.\n",
            __FUNCTION__, test_seq_id, err);
    return err;
}

#define MAX_VERSION -1
int castle_fs_init(void)
{
#define FIRST_INIT_BUG_ON_ERROR(_fn)            \
    ({                                          \
        int _ret = 0;                           \
        if(first)                               \
            _ret = _fn;                         \
        BUG_ON(_ret);                           \
     })

#define NOT_FIRST_INIT_BUG_ON_ERROR(_fn)        \
    ({                                          \
        int _ret = 0;                           \
        if(!first)                              \
            _ret = _fn;                         \
        BUG_ON(_ret);                           \
     })

    struct   list_head *lh;
    struct   castle_slave *cs;
    struct   castle_fs_superblock fs_sb, *cs_fs_sb;
    int      first, prev_new_dev = -1;
    int      i, last, sync_checkpoint=0;
    uint32_t slave_count=0, nr_fs_slaves=0, nr_live_slaves=0, need_rebuild=0;
    uint32_t bcv=0, max=0, last_version_checked=MAX_VERSION;

    castle_printk(LOG_INIT, "Castle FS start.\n");
    if(castle_fs_inited)
    {
        castle_printk(LOG_WARN, "FS is already inited\n");
        return -EEXIST;
    }

    if(list_empty(&castle_slaves.slaves))
    {
        castle_printk(LOG_ERROR, "Found no slaves\n");
        return -ENOENT;
    }

    /*
     * Search for best common fs version. This is the greatest fs version that is supported
     * by preferably (1) all the slaves, or less preferably (2) all bar one of the slaves.
     */
    last = 0;
    while (!bcv)
    {
        uint32_t version_to_check=0;
        int nr_slaves = 0;
        int hits;

        /*
         * Each time we pass through this loop, we'll calculate the next-highest fs version on
         * any slave. Also not how many slaves we have found.
         */
        rcu_read_lock();
        list_for_each_rcu(lh, &castle_slaves.slaves)
        {
            cs = list_entry(lh, struct castle_slave, list);
            for (i=0; i<2; i++)
                if ((version_to_check <= cs->fs_versions[i]) &&
                    (cs->fs_versions[i] < last_version_checked))
                    version_to_check = cs->fs_versions[i];
            nr_slaves++;
        }
        rcu_read_unlock();

        if (!max) max = version_to_check;
        if (max && version_to_check < (max - 1))
        {
            castle_printk(LOG_INIT, "Error: could not find set of slaves to start filesystem using"
                                    " fs versions %d or %d.\n", max, max - 1);
            return -EINVAL;
        }

        /* No lower version found, so this is the lowest. */
        if (version_to_check == last_version_checked)
            last = 1;

        last_version_checked = version_to_check;

        /* Find how many slaves support this fs version. */
        hits = 0;
        rcu_read_lock();
        list_for_each_rcu(lh, &castle_slaves.slaves)
        {
            cs = list_entry(lh, struct castle_slave, list);
            for (i=0; i<2; i++)
            {
                if (cs->fs_versions[i] == version_to_check)
                {
                    hits++;
                    break;
                }
            }
        }
        rcu_read_unlock();

        if (hits == nr_slaves || (hits == nr_slaves-1))
        {
            /* Found a set of slaves to support this fs version - use it. */
            bcv = version_to_check;
            if (hits == nr_slaves)
                castle_printk(LOG_INIT, "Found Best Common Version %d on all live slaves.\n",
                              version_to_check);
            else
                castle_printk(LOG_INIT, "Found Best Common Version %d on quorum of live slaves.\n",
                              version_to_check);
            break;
        } else if (last)
        {
            castle_printk(LOG_INIT, "Error: could not find set of slaves to start filesystem.\n");
            return -EINVAL;
        }
    }

    /*
     * 1. Either all disks should be new or none.
     * 2. If there is a slave which did not support the version, mark it as out-of-service.
     */
    prev_new_dev = -1;
    rcu_read_lock();
    list_for_each_rcu(lh, &castle_slaves.slaves)
    {
        cs = list_entry(lh, struct castle_slave, list);

        if (prev_new_dev < 0)
            prev_new_dev = cs->new_dev;
        if (cs->new_dev != prev_new_dev)
        {
            castle_printk(LOG_ERROR, "Few disks are marked new and few are not\n");
            return -EINVAL;
        }

        if ((cs->fs_versions[0] != bcv) && (cs->fs_versions[1] != bcv))
        {
            castle_printk(LOG_INIT, "Slave 0x%x [%s] is not in quorum of live slaves. "
                          "Setting as out-of-service.\n",
                cs->uuid, cs->bdev_name);
            set_bit(CASTLE_SLAVE_OOS_BIT, &cs->flags);
            castle_release_device(cs);
        }
    }
    rcu_read_unlock();

    /* Load super blocks for the Best Common Version from all valid slaves. */
    rcu_read_lock();
    list_for_each_rcu(lh, &castle_slaves.slaves)
    {
        cs = list_entry(lh, struct castle_slave, list);
        if (test_bit(CASTLE_SLAVE_OOS_BIT, &cs->flags))
        {
            castle_freespace_slave_init(cs, 0);
            continue;
        }
        if ((cs->cs_superblock.fs_version != bcv) &&
                (castle_slave_version_load(cs, bcv)))
        {
            castle_printk(LOG_ERROR, "Couldn't find version %u on slave: 0x%x\n", bcv, cs->uuid);
            return -EINVAL;
        }
        if (castle_freespace_slave_init(cs, cs->new_dev))
        {
            castle_printk(LOG_ERROR, "Failed to initialise Freespace on slave: 0x%x\n", cs->uuid);
            return -EINVAL;
        }
        castle_slave_superblock_print(&cs->cs_superblock);
    }
    rcu_read_unlock();

    first = 1;
    /* Make sure that superblocks of the all non-new devices are
       the same, save the results */
    rcu_read_lock();
    list_for_each_rcu(lh, &castle_slaves.slaves)
    {
        cs = list_entry(lh, struct castle_slave, list);
        if (test_bit(CASTLE_SLAVE_OOS_BIT, &cs->flags))
            continue;
        slave_count++;

        if(cs->new_dev)
            continue;

        cs_fs_sb = castle_fs_superblock_get(cs);

        /* Save fs superblock if the first slave. */
        if(first)
        {
            memcpy(&fs_sb, cs_fs_sb, sizeof(struct castle_fs_superblock));
            first = 0;
        }
        else
        /* Check if fs superblock the same as the one we already know about */
        {
            if(memcmp(&fs_sb, cs_fs_sb,
                      sizeof(struct castle_fs_superblock)) != 0)
            {
                castle_printk(LOG_ERROR, "Castle fs superblocks do not match!\n");
                castle_fs_superblock_put(cs, 0);
                return -EINVAL;
            }
        }

        /* Check whether the slave is already in FS slaves list and it is not out-of-service. */
        for (i=0; i<fs_sb.nr_slaves; i++)
        {
            if (fs_sb.slaves[i] == cs->uuid)
            {
                nr_fs_slaves++;
                if (!test_bit(CASTLE_SLAVE_OOS_BIT, &fs_sb.slaves_flags[i]))
                    nr_live_slaves++;
                break;
            }
        }

        if (i == fs_sb.nr_slaves)
        {
            castle_printk(LOG_ERROR, "Slave 0x%x doesn't belong to this File system.\n", cs->uuid);
            return -EINVAL;
        }
        castle_fs_superblock_put(cs, 0);
    }
    rcu_read_unlock();

    debug("FS init found %d live slaves out of a total of %d slaves\n", nr_live_slaves, nr_fs_slaves);
    if (slave_count < 2)
    {
        castle_printk(LOG_ERROR, "Error: Need a minimum of two disks.\n");
        return -EINVAL;
    }

    castle_checkpoint_ratelimit_set(25 * 1024 * slave_count);

    if (!first)
    {
        if (nr_live_slaves == nr_fs_slaves - 1)
            castle_printk(LOG_WARN, "Warning: Starting filesystem with one slave missing.\n");
        else if (nr_live_slaves < nr_fs_slaves)
        {
            castle_printk(LOG_ERROR, "Error: could not find enough slaves to start the filesystem\n");
            return -EINVAL;
        }

        /*
         * Scan the list of slaves in the superblock, and mark any castle_slave which is
         * out-of-service or evacuating.
         */
        for (i=0; i<fs_sb.nr_slaves; i++)
        {
            cs = castle_slave_find_by_uuid(fs_sb.slaves[i]);
            if (!cs)
            {
                /* The slave used to exist as part of this filesystem but no longer. */
                castle_printk(LOG_WARN, "Warning: slave 0x%x is no longer live.\n",
                              fs_sb.slaves[i]);
                cs = castle_slave_ghost_add(fs_sb.slaves[i]);
                if (!test_bit(CASTLE_SLAVE_REMAPPED_BIT, &fs_sb.slaves_flags[i]))
                {
                    castle_printk(LOG_USERINFO, "Slave 0x%x is missing and has not been remapped.\n",
                                  fs_sb.slaves[i]);
                    need_rebuild++;
                } else
                    set_bit(CASTLE_SLAVE_REMAPPED_BIT, &cs->flags);
                /* Note. If slave was evacuating, we don't care. It is now oos. */
            } else
            {
                if ((cs->fs_versions[0] != bcv) && (cs->fs_versions[1] != bcv))
                {
                    /* This slave is not in the quorum. Check if it needs remapping. */
                    if (test_bit(CASTLE_SLAVE_REMAPPED_BIT, &fs_sb.slaves_flags[i]))
                    {
                        castle_printk(LOG_USERINFO, "Slave 0x%x has already been remapped.\n", fs_sb.slaves[i]);
                        set_bit(CASTLE_SLAVE_REMAPPED_BIT, &cs->flags);
                        /* We will not be performing I/O on this device. Release it. */
                        castle_release_device(cs);

                    } else
                    {
                        castle_printk(LOG_USERINFO, "Slave 0x%x is not in quorum and has not been remapped.\n",
                                      fs_sb.slaves[i]);
                        need_rebuild++;
                    }
                }
                if (test_bit(CASTLE_SLAVE_OOS_BIT, &fs_sb.slaves_flags[i]))
                {
                    set_bit(CASTLE_SLAVE_OOS_BIT, &cs->flags);
                    if (atomic_read(&cs->io_in_flight) == 0)
                        castle_release_device(cs);
                }

                /* If EVACUATE is set, but not OOS, then evacuation has not completed. */
                if ((test_bit(CASTLE_SLAVE_EVACUATE_BIT, &fs_sb.slaves_flags[i])) &&
                    (!test_bit(CASTLE_SLAVE_OOS_BIT, &fs_sb.slaves_flags[i])))
                {
                    castle_printk(LOG_USERINFO, "Slave 0x%x [%s] is still in evacuation\n",
                            cs->uuid, cs->bdev_name);
                    set_bit(CASTLE_SLAVE_EVACUATE_BIT, &cs->flags);
                }
            }
        }
    }

    /* We cannot handle more than one non-evacuating, not-remapped, slave. */
    if (need_rebuild > 1)
    {
        castle_printk(LOG_ERROR, "Error: too many out-of-service slaves need remapping. Cannot start filesystem\n");
        return -EINVAL;
    }

    /* Init the fs superblock */
    if(first) castle_fs_superblocks_init();
    else      castle_fs_superblocks_load(&fs_sb);

    /* Load extent structures of logical extents into memory */
    FIRST_INIT_BUG_ON_ERROR(castle_extents_create());
    NOT_FIRST_INIT_BUG_ON_ERROR(castle_extents_read());

    /* Load all extents into memory. */
    NOT_FIRST_INIT_BUG_ON_ERROR(castle_extents_read_complete(&sync_checkpoint));

    /* Now create the meta extent pool. */
    castle_extents_meta_pool_init();

    /* If first is still true, we've not found a single non-new cs.
       Init the fs superblock. */
    if(first) {
        /* Init version list */
        FIRST_INIT_BUG_ON_ERROR(castle_versions_zero_init());
        /* Make sure that fs_sb is up-to-date */
        cs_fs_sb = castle_fs_superblocks_get();
        memcpy(&fs_sb, cs_fs_sb, sizeof(struct castle_fs_superblock));
        castle_fs_superblocks_put(cs_fs_sb, 0);

        FAULT(FS_INIT_FAULT);
    }
    cs_fs_sb = castle_fs_superblocks_get();
    BUG_ON(!cs_fs_sb);
    /* This will initialise the fs superblock of all the new devices */
    memcpy(cs_fs_sb, &fs_sb, sizeof(struct castle_fs_superblock));
    castle_fs_superblocks_put(cs_fs_sb, 1);

    /* Read versions in. */
    NOT_FIRST_INIT_BUG_ON_ERROR(castle_versions_read());

    /* Read doubling arrays and component trees in. */
    NOT_FIRST_INIT_BUG_ON_ERROR(castle_double_array_read());

    /* Delete any orphan version trees. Note: If the system checkpoints DA deletion and crashes
     * before it completes the deletion, then version tree can exist with out DA. */
    castle_versions_orphans_check();

    /* Read Collection Attachments. */
    NOT_FIRST_INIT_BUG_ON_ERROR(castle_attachments_read());

    /* Read stats in. */
    NOT_FIRST_INIT_BUG_ON_ERROR(castle_stats_read());

    FAULT(FS_INIT_FAULT);

    NOT_FIRST_INIT_BUG_ON_ERROR(castle_chk_disk());

    castle_checkpoint_version_inc();

    FAULT(FS_RESTORE_FAULT);

    castle_events_init();

    BUG_ON(castle_unit_tests());

    castle_printk(LOG_INIT, "Castle FS started.\n");
    castle_fs_inited = 1;

    /*
     * Now the fs is started. We can add the sysfs tree for each non-GHOST slave.
     * Note that we don't rcu_read_lock protect this list scan as we might go to sleep
     * in castle_sysfs_slave_add. However, castle_fs_init and the slave_add functions
     * are initiated via control ioctls which are single-threaded through the
     * castle transaction lock, so this is safe to do.
     */
    list_for_each_rcu(lh, &castle_slaves.slaves)
    {
        cs = list_entry(lh, struct castle_slave, list);
        if (!test_bit(CASTLE_SLAVE_GHOST_BIT, &cs->flags) && castle_sysfs_slave_add(cs))
        {
            castle_printk(LOG_ERROR, "Could not add slave to sysfs.\n");
            BUG();
        }
        castle_events_slave_claim(cs->uuid);
    }

    if(sync_checkpoint)
    {
        /* Checkpoint should never be requested on the first startup.
           This way we can guarantee that global tree will not be created afterwards
           (which makes the rest of the startup process slightly more straightforward).
           FIXME: Global tree is gone do we still need this BUG_ON!! GM?*/
        BUG_ON(first);
        castle_printk(LOG_USERINFO, "Waiting for a checkpoint before completing the startup.\n");
        castle_ctrl_unlock();
        castle_checkpoint_wait();
        castle_ctrl_lock();
        castle_printk(LOG_USERINFO, "Waiting completed.\n");
    }

    castle_extents_start();

    BUG_ON(castle_double_array_start() < 0);

    castle_extents_rebuild_startup_check(need_rebuild);

    castle_fs_state = CASTLE_STATE_INITED;

    return 0;
}

static void castle_slave_superblock_print(struct castle_slave_superblock *cs_sb)
{
    castle_printk(LOG_INIT, "Magic1: %.8x\n"
           "Magic2: %.8x\n"
           "Magic3: %.8x\n"
           "Version:%x\n"
           "Uuid:   %x\n"
           "Used:   %x\n"
           "Size:   %llx\n",
           cs_sb->pub.magic1,
           cs_sb->pub.magic2,
           cs_sb->pub.magic3,
           cs_sb->pub.version,
           cs_sb->pub.uuid,
           cs_sb->pub.used,
           cs_sb->pub.size);
}

sector_t get_bd_capacity(struct block_device *bd)
{
    return bd->bd_contains == bd ? get_capacity(bd->bd_disk) : bd->bd_part->nr_sects;
}

static int castle_slave_superblock_validate(struct castle_slave *cs,
                                            struct castle_slave_superblock *cs_sb)
{
    uint32_t checksum = cs_sb->pub.checksum;

    if(cs_sb->pub.magic1 != CASTLE_SLAVE_MAGIC1) return -1;
    if(cs_sb->pub.magic2 != CASTLE_SLAVE_MAGIC2) return -2;
    if(cs_sb->pub.magic3 != CASTLE_SLAVE_MAGIC3) return -3;
    if(cs_sb->pub.version != CASTLE_SLAVE_VERSION) return -4;
    if(!(cs_sb->pub.flags & CASTLE_SLAVE_NEWDEV) && (cs_sb->fs_version == 0))
        return -5;

    /* Don't check checksum for new device. */
    if((cs_sb->pub.flags & CASTLE_SLAVE_NEWDEV) && (cs_sb->pub.checksum == 0))
        return 0;

    cs_sb->pub.checksum = 0;
    if(fletcher32((uint16_t *)cs_sb, sizeof(struct castle_slave_superblock))
                                != checksum)
    {
        cs_sb->pub.checksum = checksum;
        return -6;
    }
    cs_sb->pub.checksum = checksum;

    if((!(cs_sb->pub.flags & CASTLE_SLAVE_NEWDEV)) &&
        (cs_sb->pub.size != get_bd_capacity(cs->bdev) >> (C_BLK_SHIFT - 9)))
        return -7;

    /* This superblock has been invalidated, so it is not safe to use. */
    if((cs_sb->pub.flags & CASTLE_SLAVE_SB_INVALID))
        return -8;

    /* Claim after init is only valid for 'new' slaves. */
    if (castle_fs_inited && !cs->new_dev)
    {
        castle_printk(LOG_ERROR, "Cannot claim non-new slave after fs already initialised.\n");
        return -9;
    }
    return 0;
}

/**
 * Read directly from disk into a buffer up to C_BLK_SIZE size.
 *
 * @param bdev      [in]    The block device to read from.
 * @param sector    [in]    The sector to start reading from
 * @param size      [in]    The size of the buffer to read into (max C_BLK_SIZE)
 * @param buffer    [in]    The buffer
 *
 * @return 0:       On success.
 * @return -EINVAL: Invalid buffer size (> C_BLK_SIZE).
 */
static int castle_block_direct_read(struct block_device *bdev, sector_t sector,
                                    uint32_t size, char *buffer)
{
    struct page *iopage;
    int     ret;

    if (!bdev || (size > C_BLK_SIZE))
        return -1;

    iopage = alloc_page(GFP_KERNEL);
    BUG_ON(!iopage);

    /* We'll read a whole page, then copy up to the size requested into the passed buffer. */
    ret = submit_direct_io(READ, bdev, sector, &iopage, 1);
    if (ret)
    {
        castle_printk(LOG_ERROR, "Direct io submission failed with error %d\n", ret);
        return ret;
    }
    memcpy(buffer, pfn_to_kaddr(page_to_pfn(iopage)), size);

    __free_page(iopage);

    return EXIT_SUCCESS;
}

/**
 * Check whether a block device supports ordered writes. It does so by reading & writing
 * (in ordered mode) the very first block on the device. WARNING: not thread/cache safe.
 */
static int castle_block_ordered_supp_test(struct block_device *bdev)
{
    struct page *iopage;
    int         ret;

    iopage = alloc_page(GFP_KERNEL);
    BUG_ON(!iopage);

    ret = submit_direct_io(READ, bdev, 0, &iopage, 1);
    if (ret)
    {
        castle_printk(LOG_WARN, "Ordered support test: I/O read failed with error %d\n", ret);
        return 0;
    }

    ret = submit_direct_io(WRITE_BARRIER, bdev, 0, &iopage, 1);

    __free_page(iopage);

    return ret;
}

static void castle_slave_superblock_init(struct   castle_slave *cs,
                                         struct   castle_slave_superblock *cs_sb,
                                         uint32_t uuid)
{
    castle_printk(LOG_INIT, "Initing slave superblock.\n");

    cs_sb->pub.magic1 = CASTLE_SLAVE_MAGIC1;
    cs_sb->pub.magic2 = CASTLE_SLAVE_MAGIC2;
    cs_sb->pub.magic3 = CASTLE_SLAVE_MAGIC3;
    cs_sb->pub.version= CASTLE_SLAVE_VERSION;
    cs_sb->pub.used   = 2; /* Two blocks used for the superblocks */
    cs_sb->pub.uuid   = uuid;
    cs_sb->pub.size   = get_bd_capacity(cs->bdev) >> (C_BLK_SHIFT - 9);
    cs_sb->pub.flags  = 0;
    castle_slave_superblock_print(cs_sb);

    castle_printk(LOG_INIT, "Done.\n");
}

static int castle_slave_superblock_read(struct castle_slave *cs)
{
    struct castle_slave_superblock *cs_sb = NULL;
    struct castle_fs_superblock *fs_sb = NULL;
    int err = 0, errs[2];
    int fs_version;
    int i;

    BUG_ON(sizeof(struct castle_slave_superblock) > C_BLK_SIZE);
    BUG_ON(sizeof(struct castle_fs_superblock) > C_BLK_SIZE);

    cs_sb = castle_alloc(sizeof(struct castle_slave_superblock) * 2);
    fs_sb = castle_alloc(sizeof(struct castle_fs_superblock) * 2);
    if (!cs_sb || !fs_sb)
    {
        castle_printk(LOG_ERROR, "Failed to allocate memory for superblocks\n");
        goto error_out;
    }

    for (i=0; i<2; i++)
    {
        if ((err = castle_block_direct_read(cs->bdev, (i*16),
                        sizeof(struct castle_slave_superblock),
                        (char *)&cs_sb[i])) < 0)
            goto error_out;
        if ((err = castle_block_direct_read(cs->bdev, (i*16+8),
                        sizeof(struct castle_fs_superblock),
                        (char *)&fs_sb[i])) < 0)
            goto error_out;

        errs[i] = castle_slave_superblock_validate(cs, &cs_sb[i]);
        if (errs[i])
        {
            debug("Slave superblock %u is not valid: %d\n", i, errs[i]);
            continue;
        }

        /* Sanity check on version number. */
        if ((cs_sb[i].fs_version % 2) != i)
        {
            errs[i] = -11;
            continue;
        }

        /* No need to check for FS superblock, if the first superblock is marked
         * with new device flag. */
        if ((i==0) && (cs_sb[i].pub.flags & CASTLE_SLAVE_NEWDEV))
            continue;

        /* Validate FS superblock. */
        errs[i] = castle_fs_superblock_validate(&fs_sb[i]);
        if (errs[i])
            debug("FS superblock %u is not valid: %d\n", i, errs[i]);
        else if (fs_sb[i].fs_version != cs_sb[i].fs_version)
            errs[i] = -22;
    }

    /* Only first disk is valid and new device flag is set - Initialize new disk. */
    if ((!errs[0] && errs[1]) && (cs_sb[0].pub.flags & CASTLE_SLAVE_NEWDEV))
    {
        castle_slave_superblock_init(cs, &cs->cs_superblock, cs_sb[0].pub.uuid);
        /* keep the SSD flag */
        cs->cs_superblock.pub.flags |= cs_sb[0].pub.flags & CASTLE_SLAVE_SSD;
        fs_version = 0;
        goto out;
    }

    /* Both are invalid superblocks - Return error. */
    if (errs[0] && errs[1])
    {
        castle_printk(LOG_ERROR, "Superblock is invalid and not a new device: (%d, %d)\n",
                errs[0], errs[1]);
        err = errs[0];
        goto error_out;
    }

    fs_version = 0;
    if (!errs[0])  fs_version = cs_sb[0].fs_version;
    if (!errs[1])  fs_version = (fs_version < cs_sb[1].fs_version)? cs_sb[1].fs_version: fs_version;

    if (fs_version == 0)
    {
        err = -EINVAL;
        goto error_out;
    }

    fs_version = fs_version % 2;

    castle_printk(LOG_INIT, "Disk 0x%x has FS versions - ", cs_sb[fs_version].pub.uuid);
    if (!errs[0])    castle_printk(LOG_INIT, "[%u]", cs_sb[0].fs_version);
    if (!errs[1])    castle_printk(LOG_INIT, "[%u]", cs_sb[1].fs_version);
    castle_printk(LOG_INIT, "\n");

    cs->fs_versions[0] = cs_sb[0].fs_version;
    cs->fs_versions[1] = cs_sb[1].fs_version;

    /* Check for the uuids of both versions to match. */
    if ((!errs[0] && !errs[1]) && (cs_sb[0].pub.uuid != cs_sb[1].pub.uuid))
    {
        castle_printk(LOG_ERROR, "Found versions with different uuids 0x%x:0x%x\n",
                cs_sb[0].pub.uuid, cs_sb[1].pub.uuid);
#ifdef DEBUG
        BUG();
#endif
        err = -EINVAL;
        goto error_out;
    }

    castle_printk(LOG_INIT, "Disk superblock found.\n");
    memcpy(&cs->cs_superblock, &cs_sb[fs_version], sizeof(struct castle_slave_superblock));
    memcpy(&cs->fs_superblock, &fs_sb[fs_version], sizeof(struct castle_fs_superblock));

out:
    /* Save the uuid and exit */
    cs->uuid        = cs_sb[fs_version].pub.uuid;
    cs->new_dev     = cs_sb[fs_version].pub.flags & CASTLE_SLAVE_NEWDEV;
    BUG_ON(err);

error_out:
    castle_check_free(cs_sb);
    castle_check_free(fs_sb);

    return err;
}

static int castle_slave_version_load(struct castle_slave *cs, uint32_t fs_version)
{
    int ret = -EINVAL;
    sector_t sector = (fs_version % 2) * 16;

    mutex_lock(&cs->sblk_lock);

    /* Return, if version is already loaded. */
    if (cs->cs_superblock.fs_version == fs_version)
    {
        ret = 0;
        goto out;
    }

    /* If the version is not previous version return error. */
    if (fs_version != (cs->cs_superblock.fs_version - 1))
        goto out;

    if (castle_block_direct_read(cs->bdev, sector, sizeof(struct castle_slave_superblock),
                                (char *)&cs->cs_superblock))
        goto out;
    if (castle_block_direct_read(cs->bdev, sector+8, sizeof(struct castle_fs_superblock),
                                (char *)&cs->fs_superblock))
        goto out;
    if (castle_slave_superblock_validate(cs, &cs->cs_superblock))
        goto out;

    /* If the version doesn't match, return error. */
    if (cs->cs_superblock.fs_version != fs_version)
        goto out;

    /* Initialize the superblock, if the version is 0. */
    if (!fs_version)
    {
        cs->new_dev = 1;
        castle_slave_superblock_init(cs, &cs->cs_superblock,
                                     cs->cs_superblock.pub.uuid);
    }
    else
    {
        if (castle_fs_superblock_validate(&cs->fs_superblock))
            goto out;
        if (cs->cs_superblock.fs_version != cs->fs_superblock.fs_version)
            goto out;
        cs->new_dev = 0;
    }

    cs->uuid = cs->cs_superblock.pub.uuid;

    ret = 0;

out:
    mutex_unlock(&cs->sblk_lock);
    return ret;
}

struct castle_slave_superblock* castle_slave_superblock_get(struct castle_slave *cs)
{
    mutex_lock(&cs->sblk_lock);
    return &cs->cs_superblock;
}

void castle_slave_superblock_put(struct castle_slave *cs, int dirty)
{
    mutex_unlock(&cs->sblk_lock);
}

int castle_slave_superblocks_writeback(struct castle_slave *cs, uint32_t version)
{
    c2_block_t *c2b;
    c_ext_pos_t cep;
    char       *buf;
    int         slot = version % 2;
    int         length = (2 * C_BLK_SIZE);
    int         ret;
    struct castle_slave_superblock *cs_sb;
    struct castle_fs_superblock *fs_sb;

    cep.ext_id = cs->sup_ext;
    cep.offset = length * slot;

    c2b = castle_cache_block_get(cep, 2, USER);
    if (!c2b)
        return -ENOMEM;

    cs_sb = castle_slave_superblock_get(cs);
    fs_sb = castle_fs_superblocks_get();

    BUG_ON(cs_sb->fs_version != version);

    write_lock_c2b(c2b);
    update_c2b(c2b);
    buf = c2b_buffer(c2b);

    /* Calculate checksum for superblocks - with checksum bytes set to 0. */
    cs_sb->pub.checksum = fs_sb->pub.checksum = 0;
    cs_sb->pub.checksum = fletcher32((uint16_t *)cs_sb,
                                     sizeof(struct castle_slave_superblock));
    fs_sb->pub.checksum = fletcher32((uint16_t *)fs_sb,
                                     sizeof(struct castle_fs_superblock));

    /* Copy superblocks with valid checksums. */
    memcpy(buf, cs_sb, sizeof(struct castle_slave_superblock));
    memcpy(buf + C_BLK_SIZE, fs_sb, sizeof(struct castle_fs_superblock));

    dirty_c2b(c2b);

    debug("Free chunks: %u|%u|%u\n", cs_sb->freespace.free_chk_cnt,
           cs_sb->freespace.prod,
           cs_sb->freespace.cons);

    ret = submit_c2b_sync_barrier(WRITE, c2b);
    write_unlock_c2b(c2b);
    put_c2b(c2b);

    if(ret)
    {
        castle_printk(LOG_ERROR, "Could not write superblocks out, for cs uuid=0x%x\n", cs->uuid);
        castle_slave_superblock_put(cs, 1);
        castle_fs_superblocks_put(fs_sb, 1);
        return -EIO;
    }

    castle_slave_superblock_put(cs, 1);
    castle_fs_superblocks_put(fs_sb, 1);

    return 0;
}

int castle_superblocks_writeback(uint32_t version)
{
    struct list_head *lh;
    struct castle_slave *slave;

    rcu_read_lock();
    list_for_each_rcu(lh, &castle_slaves.slaves)
    {
        slave = list_entry(lh, struct castle_slave, list);

        /* Do not attempt writeback to out-of-service slaves. */
        if ((test_bit(CASTLE_SLAVE_OOS_BIT, &slave->flags)))
            continue;

        if (castle_slave_superblocks_writeback(slave, version))
            return -EIO;
    }
    rcu_read_unlock();

    return 0;
}

struct castle_slave* castle_slave_find_by_id(uint32_t id)
{
    struct list_head *lh;
    struct castle_slave *slave;

    rcu_read_lock();
    list_for_each_rcu(lh, &castle_slaves.slaves)
    {
        slave = list_entry(lh, struct castle_slave, list);
        if(slave->id == id)
            return slave;
    }
    rcu_read_unlock();

    return NULL;
}

struct castle_slave* castle_slave_find_by_uuid(uint32_t uuid)
{
    struct list_head *lh;
    struct castle_slave *slave;

    rcu_read_lock();
    list_for_each_rcu(lh, &castle_slaves.slaves)
    {
        slave = list_entry(lh, struct castle_slave, list);
        if(slave->uuid == uuid)
            return slave;
    }
    rcu_read_unlock();

    return NULL;
}

struct castle_slave* castle_slave_find_by_bdev(struct block_device *bdev)
{
    struct list_head *lh;
    struct castle_slave *slave;

    rcu_read_lock();
    list_for_each_rcu(lh, &castle_slaves.slaves)
    {
        slave = list_entry(lh, struct castle_slave, list);
        if(slave->bdev == bdev)
            return slave;
    }
    rcu_read_unlock();

    return NULL;
}

struct castle_slave* castle_slave_find_by_block(c_ext_pos_t  cep)
{
    return castle_slave_find_by_uuid(cep.ext_id);
}

static int castle_slave_add(struct castle_slave *cs)
{
    struct list_head *l;
    struct castle_slave *s;

    rcu_read_lock();
    list_for_each_rcu(l, &castle_slaves.slaves)
    {
        s = list_entry(l, struct castle_slave, list);
        if(s->uuid == cs->uuid)
        {
            castle_printk(LOG_ERROR, "Uuid of two slaves match (uuid=0x%x, id1=%d, id2=%d)\n",
                    cs->uuid, s->id, cs->id);
            return -EINVAL;
        }
    }
    rcu_read_unlock();

    /* If no UUID collision, add to the list */
    BUG_ON(!CASTLE_IN_TRANSACTION);
    list_add_rcu(&cs->list, &castle_slaves.slaves);
    return 0;
}

#define MIN_SLAVE_CAPACITY  (5120)  /* In chunks. */
STATIC_BUG_ON(MIN_LIVE_SLAVES *  MIN_SLAVE_CAPACITY <=                              \
              MIN_LIVE_SLAVES * (FREE_SPACE_START + MSTORE_SPACE_SIZE) +            \
              MAX_NR_SLAVES * META_SPACE_SIZE +                                     \
              /* Fudge: there are extra overheads since the allocator               \
                        allocates in CHKS_PER_SLOT granularity. */                  \
              CHKS_PER_SLOT * 2 /* 2-RDA */ * MIN_LIVE_SLAVES /* per-disk */ *      \
              5 /* number of extents allocated */ * 2 /* just in case */);

struct castle_slave* castle_claim(uint32_t new_dev)
{
    dev_t dev;
    struct block_device *bdev = NULL;
    int cs_added = 0;
    int err;
    char b[BDEVNAME_SIZE];
    struct castle_slave *cs = NULL;
    struct castle_fs_superblock *fs_sb;
    struct castle_slave_superblock *cs_sb;

    debug("Claiming: in_atomic=%d.\n", in_atomic());

    if (slave_id >= MAX_NR_SLAVES)
    {
        castle_printk(LOG_ERROR, "Could not add slave 0x%x. Maximum number of slaves has already "
                                 "been reached,\n", cs->uuid);
        goto err_out;
    }

    if(!(cs = castle_zalloc(sizeof(struct castle_slave))))
        goto err_out;
    cs->id          = slave_id++;
    cs->last_access = jiffies;
    cs->sup_ext     = INVAL_EXT_ID;
    mutex_init(&cs->sblk_lock);
    INIT_RCU_HEAD(&cs->rcu);
    atomic_set(&cs->io_in_flight, 0);

    dev = new_decode_dev(new_dev);
    bdev = open_by_devnum(dev, FMODE_READ|FMODE_WRITE);
    if (IS_ERR(bdev))
    {
        castle_printk(LOG_ERROR, "Could not open %s.\n", __bdevname(dev, b));
        bdev = NULL;
        goto err_out;
    }
    cs->bdev = bdev;

    /* Keep track of the bdev name, so that it can be accessed after the bdev has been released. */
    bdevname(bdev, cs->bdev_name);

    if(castle_slave_superblock_read(cs))
    {
        castle_printk(LOG_ERROR, "Invalid superblock.\n");
        goto err_out;
    }

    /* Make sure that the disk is at least FREE_SPACE_START big. */
    if(castle_freespace_slave_capacity_get(cs) < MIN_SLAVE_CAPACITY)
    {
        castle_printk(LOG_ERROR, "Disk %s capacity too small. Must be at least %dM, got %uM\n",
                 __bdevname(dev, b),
                 MIN_SLAVE_CAPACITY,
                 castle_freespace_slave_capacity_get(cs));
        goto err_out;
    }

    err = bd_claim(bdev, &castle);
    if (err)
    {
        castle_printk(LOG_ERROR, "Could not bd_claim %s, err=%d.\n", cs->bdev_name, err);
        goto err_out;
    }
    set_bit(CASTLE_SLAVE_BDCLAIMED_BIT, &cs->flags);

    /* Check whether the block device supports ordered writes. */
    err = castle_block_ordered_supp_test(bdev);
    if (err)
    {
        castle_printk(LOG_WARN,
                      "Block device %s doesn't support ordered writes. "
                      "Crash consistency cannot be guaranteed.\n",
                      __bdevname(dev, b));
    }
    else
    {
        set_bit(CASTLE_SLAVE_ORDERED_SUPP_BIT, &cs->flags);
    }

    cs->sup_ext = castle_extent_sup_ext_init(cs);
    if (cs->sup_ext == INVAL_EXT_ID)
    {
        castle_printk(LOG_ERROR, "Could not initialize super extent for slave 0x%x\n", cs->uuid);
        goto err_out;
    }

    FAULT(CLAIM_FAULT);

    if (castle_fs_inited)
    {
        /*
         * This is the initial part of 'disk claim after init'.
         * Set up the relevant superblocks and extents.
         */

        /* Indicate that slave cannot yet be used for allocations. */
        set_bit(CASTLE_SLAVE_CLAIMING_BIT, &cs->flags);

        fs_sb = castle_fs_superblocks_get();
        fs_sb->slaves[fs_sb->nr_slaves++] = cs->uuid;
        cs_sb = castle_slave_superblock_get(cs);

        /* Slave is initialised with current fs version. */
        cs_sb->fs_version = fs_sb->fs_version;

        castle_slave_superblock_put(cs, 1);
        castle_fs_superblocks_put(fs_sb, 1);
    }

    err = castle_slave_add(cs);
    if(err)
    {
        castle_printk(LOG_ERROR, "Could not add slave to the list.\n");
        goto err_out;
    }
    cs_added = 1;

    if (castle_fs_inited)
    {
        /*
         * The final part of 'disk claim after init'.
         * Freespace initialisation must occur after the slave has been added to castle_slaves,
         * otherwise the c2b submits it generates will not find the slave.
         */
        castle_freespace_slave_init(cs, cs->new_dev);

        /* Update the micro map to include the new slave. */
        castle_extent_micro_ext_update(cs);

        /* Mark the slave as available for general use. */
        clear_bit(CASTLE_SLAVE_CLAIMING_BIT, &cs->flags);

        castle_printk(LOG_USERINFO, "Disk 0x%x [%s] has been successfully added.\n",
                      cs->uuid, cs->bdev_name);

        err = castle_sysfs_slave_add(cs);
        if(err)
        {
            castle_printk(LOG_ERROR, "Could not add slave to sysfs.\n");
            goto err_out;
        }
        castle_events_slave_claim(cs->uuid);
    }

    return cs;
err_out:
    if (!EXT_ID_INVAL(cs->sup_ext))
        castle_extent_sup_ext_close(cs);
    if(cs_added)     list_del(&cs->list);
    if(test_bit(CASTLE_SLAVE_BDCLAIMED_BIT, &cs->flags))
    {
        bd_release(bdev);
        clear_bit(CASTLE_SLAVE_BDCLAIMED_BIT, &cs->flags);
    }
#if LINUX_VERSION_CODE <= KERNEL_VERSION(2,6,24)
    if(bdev) blkdev_put(bdev);
#else
    if(bdev) blkdev_put(bdev, FMODE_READ|FMODE_WRITE);
#endif
    castle_check_free(cs);
    slave_id--;

    return NULL;
}

void castle_release_oos_slave(struct work_struct *work)
{
    struct castle_slave *cs = container_of(work, struct castle_slave, work);

    BUG_ON(!cs);
    BUG_ON(!test_bit(CASTLE_SLAVE_OOS_BIT, &cs->flags));
    BUG_ON(atomic_read(&cs->io_in_flight));
    castle_release_device(cs);
}

void castle_release_device(struct castle_slave *cs)
{
    BUG_ON(!cs);
    BUG_ON(atomic_read(&cs->io_in_flight));
    if (test_and_clear_bit(CASTLE_SLAVE_BDCLAIMED_BIT, &cs->flags))
    {
        sysfs_remove_link(&cs->kobj, "dev");
        bd_release(cs->bdev);
#if LINUX_VERSION_CODE <= KERNEL_VERSION(2,6,24)
        blkdev_put(cs->bdev);
#else
        blkdev_put(cs->bdev, FMODE_READ|FMODE_WRITE);
#endif
        castle_printk(LOG_USERINFO, "Device 0x%x [%s] has been released.\n",
                      cs->uuid, cs->bdev_name);
    }
}

void castle_release(struct castle_slave *cs)
{
    BUG_ON(!cs);
    if (test_bit(CASTLE_SLAVE_SYSFS_BIT, &cs->flags))
        castle_sysfs_slave_del(cs);
    /* Ghost slaves are only partially initialised, and have no bdev. */
    if (!test_bit(CASTLE_SLAVE_GHOST_BIT, &cs->flags))
        castle_release_device(cs);

    list_del_rcu(&cs->list);
    synchronize_rcu();
    castle_free(cs);
}

void castle_bio_get(c_bio_t *c_bio)
{
    /* This should never called in race with the last _put */
    BUG_ON(atomic_read(&c_bio->count) == 0);
    atomic_inc(&c_bio->count);
}

/* Do not declare this as static because castle_debug calls this directly.
   But this is the only external reference */
void castle_bio_put(c_bio_t *c_bio)
{
    struct bio *bio = c_bio->bio;
    int finished, err = c_bio->err;

    finished = atomic_dec_and_test(&c_bio->count);
    if(!finished)
        return;

    castle_debug_bio_deregister(c_bio);

    castle_utils_bio_free(c_bio);

#if LINUX_VERSION_CODE <= KERNEL_VERSION(2,6,18)
    bio_endio(bio, bio->bi_size, err);
#else
    bio_endio(bio, err);
#endif
}

struct castle_attachment* castle_attachment_get(c_collection_id_t col_id, int rw)
{
    struct castle_attachment *ca, *result = NULL;
    struct list_head *lh;

    spin_lock(&castle_attachments.lock);

    list_for_each(lh, &castle_attachments.attachments)
    {
        ca = list_entry(lh, struct castle_attachment, list);
        if(ca->col.id == col_id)
        {
            if (rw == WRITE && castle_collection_is_rdonly(ca))
            {
                castle_printk(LOG_USERINFO, "Collection %u is read only\n", col_id);
                result = NULL;
            }
            else
            {
                result = ca;
                ca->ref_cnt++;
            }
            break;
        }
    }

    spin_unlock(&castle_attachments.lock);

    return result;
}

void castle_attachment_put(struct castle_attachment *ca)
{
    int to_free = 0;

    BUG_ON(in_atomic());
    spin_lock(&castle_attachments.lock);

    ca->ref_cnt--;

    BUG_ON(ca->ref_cnt < 0);

    if (ca->ref_cnt == 0)
        to_free = 1;

    spin_unlock(&castle_attachments.lock);

    if (to_free)
    {
        c_ver_t version = ca->version;

        castle_version_detach(version);
        castle_double_array_put(ca->col.da);
        castle_printk(LOG_USERINFO, "Attachment %u is completely removed\n", ca->col.id);

        set_bit(CASTLE_ATTACH_DEAD, &ca->col.flags);

        wake_up(&castle_detach_waitq);
    }
}

void castle_attachment_free(struct castle_attachment *ca)
{
    /* Drop the attachment from list, to prevent new references on attachment. */
    spin_lock(&castle_attachments.lock);
    list_del(&ca->list);
    spin_unlock(&castle_attachments.lock);

    castle_sysfs_collection_del(ca);

    /* Release the reference taken at init. */
    castle_attachment_put(ca);
}

void castle_attachment_free_complete(struct castle_attachment *ca)
{
    /* Wait for the attachment to get freed. */
    wait_event(castle_detach_waitq, test_bit(CASTLE_ATTACH_DEAD, &ca->col.flags));

    BUG_ON(ca->ref_cnt != 0);

    /* Free collection. */
    castle_free(ca->col.name);
    castle_free(ca);
}

EXPORT_SYMBOL(castle_attachment_get);
EXPORT_SYMBOL(castle_attachment_put);

static struct castle_attachment* castle_attachment_init(c_ver_t version,
                                                        c_da_t *da_id,
                                                        c_byte_off_t *size,
                                                        int *leaf)
{
    struct castle_attachment *attachment = NULL;

    if(castle_version_attach(version))
        return NULL;
    BUG_ON(castle_version_read(version, da_id, NULL, NULL, size, leaf));

    attachment = castle_alloc(sizeof(struct castle_attachment));
    if(!attachment)
    {
        castle_version_detach(version);
        return NULL;
    }
    init_rwsem(&attachment->lock);
    attachment->ref_cnt = 1; /* Use double put on detach */
    attachment->version = version;

    atomic64_set(&attachment->get.ios, 0);
    atomic64_set(&attachment->get.bytes, 0);
    atomic64_set(&attachment->put.ios, 0);
    atomic64_set(&attachment->put.bytes, 0);
    atomic64_set(&attachment->big_get.ios, 0);
    atomic64_set(&attachment->big_get.bytes, 0);
    atomic64_set(&attachment->big_put.ios, 0);
    atomic64_set(&attachment->big_put.bytes, 0);
    atomic64_set(&attachment->rq.ios, 0);
    atomic64_set(&attachment->rq.bytes, 0);
    atomic64_set(&attachment->rq_nr_keys, 0);

    return attachment;
}

int castle_collection_is_rdonly(struct castle_attachment *ca)
{
    return test_bit(CASTLE_ATTACH_RDONLY, &ca->col.flags);
}

struct castle_attachment* castle_collection_init(c_ver_t version, uint32_t flags, char *name)
{
    struct castle_attachment *collection = NULL;
    static c_collection_id_t collection_id = 0;
    c_da_t da_id;
    struct castle_double_array *da = NULL;
    int err;

    BUG_ON(strlen(name) > MAX_NAME_SIZE);

    collection = castle_attachment_init(version, &da_id, NULL, NULL);
    if (!collection)
        goto error_out;

    if (DA_INVAL(da_id))
    {
        castle_printk(LOG_WARN,
                      "Could not attach collection: %s, version: %d, because no DA found.\n",
                      name, version);
        goto error_out;
    }
    if (!(da = castle_double_array_get(da_id)))
        goto error_out;

    collection->col.id    = collection_id++;
    collection->col.name  = name;
    collection->col.flags = flags;
    collection->col.da    = da;
    spin_lock(&castle_attachments.lock);
    list_add(&collection->list, &castle_attachments.attachments);
    spin_unlock(&castle_attachments.lock);
    INIT_LIST_HEAD(&collection->stateful_ops);
    spin_lock_init(&collection->sop_lock);

    err = castle_sysfs_collection_add(collection);
    if(err)
    {
        spin_lock(&castle_attachments.lock);
        list_del(&collection->list);
        spin_unlock(&castle_attachments.lock);
        goto error_out;
    }

    castle_events_collection_attach(collection->col.id, version);

    return collection;

error_out:
    castle_free(name);
    if (collection)
    {
        castle_version_detach(version);
        castle_free(collection);
    }
    if (da)
        castle_double_array_put(da);
    castle_printk(LOG_USERINFO, "Failed to init collection.\n");
    return NULL;
}

static char *wq_names[2*MAX_BTREE_DEPTH+1];
static void castle_wqs_fini(void)
{
    int i;

    for(i=0; i<=2*MAX_BTREE_DEPTH; i++)
    {
        castle_check_free(wq_names[i]);
        if(castle_wqs[i])
            destroy_workqueue(castle_wqs[i]);
    }
}

static int castle_wqs_init(void)
{
    int i;

    /* Init the castle workqueues */
    memset(wq_names  , 0, sizeof(char *) * (2*MAX_BTREE_DEPTH+1));
    memset(castle_wqs, 0, sizeof(struct workqueue_struct *) * (2*MAX_BTREE_DEPTH+1));
    for(i=0; i<=2*MAX_BTREE_DEPTH; i++)
    {
        /* At most two characters for the number */
        BUG_ON(i > 99);
        wq_names[i] = castle_alloc(strlen("castle_wq")+3);
        if(!wq_names[i])
            goto err_out;
        sprintf(wq_names[i], "castle_wq%d", i);
        castle_wqs[i] = create_workqueue(wq_names[i]);
        if(!castle_wqs[i])
            goto err_out;
    }

    return 0;

err_out:
    castle_printk(LOG_ERROR, "Could not create workqueues.\n");

    return -ENOMEM;
}

static void castle_slaves_free(void)
{
    struct list_head *lh, *th;
    struct castle_slave *slave;

    rcu_read_lock();
    list_for_each_safe_rcu(lh, th, &castle_slaves.slaves)
    {
        slave = list_entry(lh, struct castle_slave, list);
        castle_release(slave);
    }
    rcu_read_unlock();
}

static int castle_slaves_init(void)
{
    /* Validate superblock size. */
    BUG_ON(sizeof(struct castle_slave_superblock) % 2);
    BUG_ON(sizeof(struct castle_fs_superblock) % 2);

    /* Init the slaves structures */
    /* Note: kobject is already is initialised. Don't try to memset. */
    INIT_LIST_HEAD(&castle_slaves.slaves);

    return 0;
}

static int castle_attachments_init(void)
{
    int major;

    /* Note: kobject is already is initialised. Don't try to memset. */
    INIT_LIST_HEAD(&castle_attachments.attachments);

    /* Allocate a major for this device */
    if((major = register_blkdev(0, "castle-fs")) < 0)
    {
        castle_printk(LOG_ERROR, "Couldn't register castle device\n");
        return -ENOMEM;
    }
    spin_lock_init(&castle_attachments.lock);

    castle_attachments.major = major;

    return 0;
}

static void castle_attachments_free(void)
{
    struct list_head *lh, *th;
    struct castle_attachment *ca;

    list_for_each_safe(lh, th, &castle_attachments.attachments)
    {
        ca = list_entry(lh, struct castle_attachment, list);

        castle_attachment_free(ca);
        castle_attachment_free_complete(ca);
    }

    if (castle_attachments.major)
        unregister_blkdev(castle_attachments.major, "castle-fs");
}

static int __init castle_init(void)
{
    int ret;

    printk("Castle FS load (build: %s).\n", CASTLE_COMPILE_CHANGESET);

    castle_fs_inited = 0;
    if((ret = castle_sysfs_init()))             goto err_out0;
    if((ret = castle_printk_init()))            goto err_out1;
    if((ret = castle_debug_init()))             goto err_out2;
    if((ret = castle_time_init()))              goto err_out3;
    if((ret = castle_wqs_init()))               goto err_out5;
    if((ret = castle_slaves_init()))            goto err_out6;
    if((ret = castle_extents_init()))           goto err_out7;
    if((ret = castle_resubmit_init()))          goto err_out8;
    if((ret = castle_extents_process_init()))   goto err_out9;
    if((ret = castle_cache_init()))             goto err_out10;
    if((ret = castle_versions_init()))          goto err_out11;
    if((ret = castle_btree_init()))             goto err_out12;
    if((ret = castle_double_array_init()))      goto err_out13;
    if((ret = castle_attachments_init()))       goto err_out14;
    if((ret = castle_checkpoint_init()))        goto err_out15;
    if((ret = castle_control_init()))           goto err_out16;
    if((ret = castle_netlink_init()))           goto err_out17;
    if((ret = castle_ctrl_prog_init()))         goto err_out18;
    if((ret = castle_back_init()))              goto err_out19;

    castle_printk(LOG_INIT, "Castle FS load done.\n");
    castle_fs_state = CASTLE_STATE_UNINITED;

    return 0;

    castle_back_fini(); /* Unreachable */
err_out19:
    castle_ctrl_prog_fini();
err_out18:
    castle_netlink_fini();
err_out17:
    castle_control_fini();
err_out16:
    castle_checkpoint_fini();
err_out15:
    castle_attachments_free();
err_out14:
    castle_double_array_merges_fini();
    castle_double_array_fini();
err_out13:
    castle_btree_free();
err_out12:
    castle_versions_fini();
err_out11:
    BUG_ON(!list_empty(&castle_slaves.slaves));
    castle_cache_fini();
err_out10:
    castle_extents_process_fini();
err_out9:
    castle_resubmit_fini();
err_out8:
    castle_extents_fini();
err_out7:
    castle_slaves_free();
err_out6:
    castle_wqs_fini();
err_out5:
    castle_time_fini();
err_out3:
    castle_debug_fini();
err_out2:
    castle_printk_fini();
err_out1:
    castle_sysfs_fini();
err_out0:

    return ret;
}

void castle_rda_slaves_free(void);

static void __exit castle_exit(void)
{
    castle_printk(LOG_INIT, "Castle FS exit.\n");

    castle_fs_exiting = 1;

    /* Note: Doesn't seem like clean design. Revisit - BM.*/
    castle_double_array_inserts_enable();

    /* Remove externally visible interfaces. Starting with the control file
       (so that no new connections can be created). */
    castle_ctrl_prog_fini();
    castle_netlink_fini();
    castle_control_fini();
    castle_back_fini();
    FAULT(FINI_FAULT);
    /* Now, make sure no more IO can be made, internally or externally generated */
    castle_double_array_merges_fini();  /* Completes all internal i/o - merges. */
    castle_extents_last_chkpt_prepare();
    castle_extents_process_fini();
    castle_checkpoint_fini();
    /* Note: Changes from here are not persistent. */
    castle_attachments_free();
    /* Cleanup/writeout all metadata */
    castle_double_array_fini();
    castle_btree_free();
    castle_versions_fini();
    /* Make sure all resubmitted I/O is handled before exit */
    castle_resubmit_fini();
    /* Flush the cache, free the slaves. */
    castle_cache_fini();
    castle_extents_fini();
    castle_slaves_free();
    castle_wqs_fini();
    /* All finished, stop the debuggers */
    castle_time_fini();
    castle_debug_fini();
    castle_printk_fini();

    castle_sysfs_fini();

    printk("Castle FS exit done.\n");
}

module_init(castle_init);
module_exit(castle_exit);

MODULE_LICENSE("GPL");

