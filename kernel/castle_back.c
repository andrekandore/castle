#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/completion.h>
#include <linux/wait.h>
#include <linux/workqueue.h>
#include <linux/sched.h>
#include <linux/kthread.h>
#include <linux/mm.h>
#include <linux/miscdevice.h>
#include <linux/poll.h>
#include <linux/rbtree.h>
#include <linux/list.h>
#include <linux/vmalloc.h>
#include <linux/delay.h>
#include <asm/pgtable.h>

#include "castle_public.h"
#include "castle_defines.h"
#include "castle.h"
#include "castle_objects.h"
#include "castle_cache.h"
#include "castle_da.h"
#include "castle_utils.h"
#include "castle_debug.h"
#include "castle_back.h"
#include "castle_ring.h"
#include "castle_systemtap.h"
#include "castle_da.h"

DEFINE_RING_TYPES(castle, castle_request_t, castle_response_t);

#define MAX_BUFFER_PAGES  (256)
#define MAX_BUFFER_SIZE   (MAX_BUFFER_PAGES << PAGE_SHIFT)

#define MAX_STATEFUL_OPS  (CASTLE_STATEFUL_OPS)

#define CASTLE_BACK_NAME  "castle-back"

#define USED              __attribute__((used))
#define WARN_UNUSED_RET   __attribute__((warn_unused_result))
#define error(_f, _a...)  castle_printk(LOG_ERROR, KERN_ERR "%s:%.4d: " _f, __FILE__, __LINE__ , ##_a)

//#define DEBUG
#ifndef DEBUG
#define debug(_f, ...)              ((void)0)
#define stateful_debug(_f, _a...)   ((void)0)
#else
#define debug(_f, _a...)            (castle_printk(LOG_DEBUG, "%s:%.4d: " _f, __FILE__, __LINE__ , ##_a))
#define stateful_debug(_f, _a...)   (castle_printk(LOG_DEBUG, "%s:%.4d:%s: " _f, __FILE__, __LINE__, __FUNCTION__, ##_a))
#endif

struct workqueue_struct        *castle_back_wq;
static wait_queue_head_t        conn_close_wait;
atomic_t                        castle_back_conn_count; /**< Number of active castle_back_conns */
spinlock_t                      conns_lock;         /**< Protects castle_back_conns list        */
static                LIST_HEAD(castle_back_conns); /**< List of all active castle_back_conns   */
int                             castle_back_inited = 0;
atomic_t                        castle_req_seq_id = ATOMIC_INIT(0); /**< Unique ID for tracing */

struct castle_back_op;

#define CASTLE_BACK_CONN_INITIALISED_BIT    (0)
#define CASTLE_BACK_CONN_INITIALISED_FLAG   (1 << CASTLE_BACK_CONN_INITIALISED_BIT)
#define CASTLE_BACK_CONN_NOTIFY_BIT         (1)
#define CASTLE_BACK_CONN_NOTIFY_FLAG        (1 << CASTLE_BACK_CONN_NOTIFY_BIT)
#define CASTLE_BACK_CONN_DEAD_BIT           (2)
#define CASTLE_BACK_CONN_DEAD_FLAG          (1 << CASTLE_BACK_CONN_DEAD_BIT)

struct castle_back_conn
{
    unsigned long           flags;

    /* details of the shared ring buffer */
    unsigned long           rings_vstart;   /**< Where is the ring mapped in?       */
    castle_back_ring_t      back_ring;
    wait_queue_head_t       wait;
    struct task_struct     *work_thread;
    struct list_head        list;           /**< Position on castle_back_conns list */
    spinlock_t              response_lock;
    atomic_t                ref_count;
    int                     cpu;            /**< CPU id for this conn               */
    int                     cpu_index;      /**< CPU index for this conn            */

    /*
     * in kernel state for each operation
     * should be RING_SIZE(back_ring) of these
     * and a free list to get new ones
     */
    struct castle_back_op           *ops;
    struct list_head                 free_ops;
    struct castle_back_stateful_op  *stateful_ops;
    struct list_head                 free_stateful_ops;
    struct timer_list                stateful_op_timeout_check_timer;
    spinlock_t                       restart_timer_lock;
    int                              restart_timer;
    struct workqueue_struct         *timeout_check_wq;
    struct work_struct               timeout_check_work;

    /* details of the shared buffers */
    rwlock_t                         buffers_lock;  /**< Protects buffers_rb                */
    struct rb_root                   buffers_rb;    /**< RB tree for castle_back_buffers    */
};

struct castle_back_buffer
{
    struct rb_node     rb_node;
    unsigned long      user_addr;   /**< Userland address (@TODO should this be void*?)     */
    uint32_t           size;
    atomic_t           ref_count;   /**< Ref count                                          */
    void              *buffer;      /**< Pointer to buffer in kernel address space          */
};

struct castle_back_op
{
    struct list_head                 list;
    struct work_struct               work;
    int                              cpu;           /**< CPU id for this op                 */
    int                              cpu_index;     /**< CPU index for this op              */

    castle_request_t                 req;           /**< Contains call_id etc.              */
    struct castle_back_conn         *conn;
    struct castle_back_buffer       *buf;
    struct castle_attachment        *attachment;

    /* used by castle_back_request_process() to pass the key to the ops */
    c_vl_bkey_t                     *key;

    /* used for assembling a get and partial writes in puts */
    uint64_t value_length;
    uint32_t buffer_offset;

    union
    {
        struct castle_object_replace replace;
        struct castle_object_get     get;
    };
};

struct castle_back_iterator
{
    c_collection_id_t             collection_id;    /**< Collection ID.                     */
    int                           get_all;          /**< If set, iterator will return all   */
                                                    /**< keys, with all # dims.             */
                                                    /**< start/end_key below are be both    */
                                                    /**< invaild.                           */
    c_vl_bkey_t                  *start_key;        /**< Iterator start key.                */
    c_vl_bkey_t                  *end_key;          /**< Iterator end key.                  */
    c_vl_bkey_t                  *saved_key;        /**< Saved key when buffer filled up.   */
    c_val_tup_t                   saved_val;        /**< Saved value when buffer filled up. */
    castle_object_iterator_t     *iterator;         /**< Iterator structure.                */
    c_buf_constructor_t           buf_con;          /**< Buffer constructor structure.      */
    uint32_t                      buf_len;          /**< Bytes in buffer kv_list can fill.  */
    uint64_t                      nr_keys;          /**< Stats: number of keys.             */
    uint64_t                      nr_bytes;         /**< Stats: number of Bytes.            */
};

struct castle_back_stream_in
{
    c_collection_id_t     collection_id;           /**< Collection ID.                            */
    uint64_t              expected_entries;        /**< How many entries the user said to expect. */
    c_chk_cnt_t           expected_dataext_chunks; /**< How many chunks the user said we would need
                                                        for medium objects extent.                */
    uint64_t              received_entries;        /**< Entries provided so far.                  */
    c_byte_off_t          received_mobj_off;       /**< Last offset used in medium object extent. */
    struct castle_immut_tree_construct *da_stream; /**< DA in_stream structure.                   */
};

#define stateful_op_fmt_str     "conn=%p stateful_op=%p curr_op=%p in_use=%d "                  \
                                "cancel_on_op_complete=%d tag=%u token=0x%x"
#define stateful_op2str(_s_op)  (_s_op)->conn, (_s_op), (_s_op)->curr_op, (_s_op)->in_use,      \
                                (_s_op)->cancel_on_op_complete, (_s_op)->tag, (_s_op)->token

typedef void (*castle_back_stateful_op_expire_t) (struct castle_back_stateful_op *stateful_op);

struct castle_back_stateful_op
{
    struct list_head                    attachment_list;
    struct list_head                    list;
    /* token is calculated by (index + use_count * MAX_STATEFUL_OPS) | 0x80000000 where index is the
     * index in the stateful_ops array in the connection. So the index is calculated by
     * token % MAX_STATEFUL_OPS.  This means tokens are reused at most every 2^31/MAX_STATEFUL_OPS
     * calls so shouldn't get collisions.
     */
    castle_interface_token_t            token;
    /* use_count counts how many times this stateful op has been reused.
     * Wraps around on overflow.
     */
    uint32_t                            use_count;
    int                                 in_use;             /**< Boolean                        */
    uint32_t                            tag;
    uint8_t                             flags;              /**< From userland request          */
    int                                 cpu;                /**< CPU all ops should run on      */
    int                                 cpu_index;          /**< CPU index all ops should use   */
    int                                 seq_id;             /**< Unique ID for tracing          */

    struct list_head                    op_queue;
    spinlock_t                          lock;
    struct castle_back_conn            *conn;

    uint64_t                            queued_size;        /**< Sum size of all queued buffers */
    struct castle_back_op              *curr_op;
    struct work_struct                  work[2];

    unsigned long                       last_used_jiffies;
    struct work_struct                  expire_work;
    struct castle_attachment           *attachment;
    castle_back_stateful_op_expire_t    expire;
    int                                 expire_enabled;     /**< expire() called only iff
                                                                 expire_enabled != 0            */
    int                                 cancelled;          /**< #3006 debug                    */

    /* set when this stateful_op is expiring. No further operations
     * on the stateful_op are valid while expiring. */
    int                                 expiring;
    /* Set this to non-zero to disable any more ops to be added to op_queue
     * and to cause the stateful_op to be put when curr_op completes. If changed
     * to non-zero when curr_op==NULL will just disable new ops to be added
     * to the queue.
     */
    int                                 cancel_on_op_complete;

    union
    {
        struct castle_back_iterator     iterator;
        struct castle_back_stream_in    stream_in;
        struct castle_object_replace    replace;
        struct castle_object_get        get;
        struct castle_object_pull       pull;
    };
};

/******************************************************************
 * Utilities to reserve ("pin"?) & map some vmalloc'd pages etc
 */

static inline void ReservePages(void *buffer, unsigned long size)
{
    int i, pages = size >> PAGE_SHIFT;
    for (i = 0; i < pages; i++)
        SetPageReserved(vmalloc_to_page(buffer + (i * PAGE_SIZE)));
}

static inline void UnReservePages(void *buffer, unsigned long size)
{
    int i, pages = size >> PAGE_SHIFT;
    for (i = 0; i < pages; i++)
        ClearPageReserved(vmalloc_to_page(buffer + (i * PAGE_SIZE)));
}

static inline int castle_vma_map(struct vm_area_struct *vma, void *buffer, unsigned long size)
{
    int i, err, offset, pages = size >> PAGE_SHIFT;

    for (i=0; i<pages; i++)
    {
        offset = i << PAGE_SHIFT;
        err = remap_pfn_range(vma, vma->vm_start + offset,
            vmalloc_to_pfn(buffer + offset),
            PAGE_SIZE, vma->vm_page_prot);
        if (err)
        {
            error("castle_back: mapping failed!\n");
            err = -ENOMEM; /* @TODO should I do this or just return remap_pfn_range? */
            goto err_out;
        }
    }

    return 0;

err_out:
   zap_page_range(vma, vma->vm_start, vma->vm_end - vma->vm_start, NULL);
   return err;
}

static USED void castle_back_print_page(char *buff, int length)
{
    int i=0;
    while(i<length)
    {
        castle_printk(LOG_DEBUG, " [%d]=", i);

        while(i<length)
        {
            castle_printk(LOG_DEBUG, "%2x, ", (unsigned char)buff[i]);
            i++;
            if (i % 8 == 0)
                break;
        }

        castle_printk(LOG_DEBUG, "\n");
    }
}

/******************************************************************
 * Functions to deal with getting and putting buffers, inserting
 * new ones into tree
 */

#define castle_back_user_to_kernel(__buffer, __user_addr) \
    (__buffer->buffer + ((unsigned long)__user_addr - __buffer->user_addr))

#define castle_back_kernel_to_user(__buffer, __kernel_addr) \
    (__buffer->user_addr + ((unsigned long)__kernel_addr - (unsigned long)__buffer->buffer))

/**
 * Return whether a buffer satisfying start-end exists in conn's RB tree.
 *
 * NOTE: Caller must hold a read-lock on the conn's buffers_lock.
 *
 * NOTE: See comment for castle_back_buffer_get() regarding potential race.
 *
 * @also castle_back_buffer_get()
 */
static inline int __castle_back_buffer_exists(struct castle_back_conn *conn,
                                              unsigned long start, unsigned long end)
{
    struct castle_back_buffer *buffer;
    struct rb_node *node;

    node = conn->buffers_rb.rb_node;

    while (node)
    {
        buffer = rb_entry(node, struct castle_back_buffer, rb_node);

        //debug("Considering buffer (%lx, %ld, %p)\n", buffer->user_addr,
        //    buffer->size, buffer->buffer);

        if (end <= buffer->user_addr)
            node = node->rb_left;
        else if (start >= buffer->user_addr + buffer->size)
            node = node->rb_right;
        else
        {
            /* Found a matching buffer.
             *
             * It's only valid if it has a non-zero ref_count. */
            if (atomic_read(&buffer->ref_count) > 0)
                return 1; /* Found buffer between start and end */
            else
                return 0;
        }
    }

    return 0;
}

/**
 * Look up buffer matching user_addr in conn's RB tree.
 *
 * @param conn          Connection to search for user buffers
 * @param user_addr     Userland buffer address to find
 * @param user_len      Space we're supposed to use in user_addr
 *
 * - Hold a read-lock on conn->buffers_lock to prevent the tree changing beneath
 *   us during our search
 *
 * NOTE: Because we hold a read-lock we could potentially race with a thread
 *       calling castle_back_buffer_put() who has decremented a buffer's
 *       reference count to 0.  If we come across a matching buffer in the tree
 *       with a 0 ref_count, pretend we never saw it.
 *
 * @also castle_back_buffer_get()
 * @also castle_back_buffer_put()
 */
static inline struct castle_back_buffer *castle_back_buffer_get(struct castle_back_conn *conn,
                                                                unsigned long user_addr,
                                                                uint32_t user_len)
{
    struct rb_node *node;
    struct castle_back_buffer *buffer, *ret = NULL;

    read_lock(&conn->buffers_lock);
    node = conn->buffers_rb.rb_node;
    while (node)
    {
        buffer = rb_entry(node, struct castle_back_buffer, rb_node);

        //debug("Considering buffer (%lx, %ld, %p)\n", buffer->user_addr,
        //    buffer->size, buffer->buffer);

        if (user_addr < buffer->user_addr)
            node = node->rb_left;
        else if (user_addr >= buffer->user_addr + buffer->size)
            node = node->rb_right;
        else
        {
            /* We found a buffer that matches the UAS buffer pointer. */
            if (user_addr + user_len <= buffer->user_addr + buffer->size)
            {
                /* User-supplied buffer length fits within the kernel buffer.
                 *
                 * If ref_count is 0, ignore it, otherwise increment by 1 and
                 * return the buffer pointer to the caller. */
                if (atomic_add_unless(&buffer->ref_count, 1, 0))
                {
                    debug("castle_back_buffer_get ref_count is now %d\n",
                            atomic_read(&buffer->ref_count));
                    ret = buffer;
                }
            }

            break;
        }
    }
    read_unlock(&conn->buffers_lock);

    return ret;
}

/**
 * Put a reference to one of conn's buffers (buf), freeing it if necessary.
 *
 * NOTE: See comment for castle_back_buffer_get() regarding potential race.
 *
 * @also castle_back_buffer_get()
 */
static void castle_back_buffer_put(struct castle_back_conn *conn,
                                   struct castle_back_buffer *buf)
{
    int use_cnt;

    BUG_ON(buf == NULL);

    use_cnt = atomic_sub_return(1, &buf->ref_count);
    debug("castle_back_buffer_put ref_count=%i, buf=%p\n", use_cnt, buf);

    if (use_cnt > 0)
        /* Other references exist, return now. */
        return;

    /* We just put the last reference.
     *
     * Hold the write lock while removing it from the RB-tree.  Once removed we
     * are safe to free it lock-free. */
    write_lock(&conn->buffers_lock);
    rb_erase(&buf->rb_node, &conn->buffers_rb);
    write_unlock(&conn->buffers_lock);

    debug("castle_back_buffer_put freeing buffer %lx\n", buf->user_addr);

    UnReservePages(buf->buffer, buf->size);
    castle_vfree(buf->buffer);
    castle_free(buf);
}

static inline struct castle_back_buffer
*__castle_back_buffers_rb_insert(struct castle_back_conn *conn,
                                 unsigned long user_addr,
                                 struct rb_node *node)
{
    struct rb_node **p;
    struct rb_node *parent = NULL;
    struct castle_back_buffer *buffer;

    BUG_ON(read_can_lock(&conn->buffers_lock)); /* can't be write-locked if readers can lock */
    p = &conn->buffers_rb.rb_node;

    while (*p)
    {
        parent = *p;
        buffer = rb_entry(parent, struct castle_back_buffer, rb_node);

        if (user_addr < buffer->user_addr)
            p = &(*p)->rb_left;
        else if (user_addr >= buffer->user_addr + buffer->size)
            p = &(*p)->rb_right;
        else
            return buffer; /* user_addr is in this buffer */
    }

    rb_link_node(node, parent, p);

    return NULL;
}

static inline struct castle_back_buffer
*castle_back_buffers_rb_insert(struct castle_back_conn *conn,
                               unsigned long user_addr,
                               struct rb_node *node)
{
    struct castle_back_buffer *buffer;
    if ((buffer = __castle_back_buffers_rb_insert(conn, user_addr, node)))
        goto out;
    rb_insert_color(node, &conn->buffers_rb);
out:
    return buffer;
}

/******************************************************************
 * ops for dealing with the stateful ops pool
 */

static inline void castle_back_conn_get(struct castle_back_conn *conn);
static inline void castle_back_conn_put(struct castle_back_conn *conn);

static struct castle_back_stateful_op *castle_back_find_stateful_op(struct castle_back_conn *conn,
                                                                    castle_interface_token_t token,
                                                                    uint32_t tag)
{
    struct castle_back_stateful_op *stateful_op = conn->stateful_ops + (token % MAX_STATEFUL_OPS);

    spin_lock(&stateful_op->lock);
    if (stateful_op->in_use
            && stateful_op->token == token
            && stateful_op->tag == tag
            && !stateful_op->expiring)
    {
        debug("castle_back_find_stateful_op returning: token = 0x%x, use_count = %u, index = %ld\n",
                    stateful_op->token, stateful_op->use_count, stateful_op - conn->stateful_ops);
        spin_unlock(&stateful_op->lock);
        return stateful_op;
    }
    spin_unlock(&stateful_op->lock);

    return NULL;
}

static void castle_back_stateful_op_expire(struct work_struct *work);

/* *op_ptr is NULL if there are no free ones */
static castle_interface_token_t
castle_back_stateful_op_get(struct castle_back_conn *conn,
                            struct castle_back_stateful_op **op_ptr,
                            int cpu,
                            int cpu_index,
                            castle_back_stateful_op_expire_t expire)
{
    struct castle_back_stateful_op *stateful_op;

    BUG_ON(!expire);

    spin_lock(&conn->response_lock);
    if (list_empty(&conn->free_stateful_ops))
    {
        spin_unlock(&conn->response_lock);
        *op_ptr = NULL;
        return 0;
    }
    stateful_op = list_entry(conn->free_stateful_ops.next, struct castle_back_stateful_op, list);
    list_del(&stateful_op->list);
    spin_unlock(&conn->response_lock);

    debug("castle_back_stateful_op_get got op: in_use = %d, "
            "token = %u, use_count = %u, index = %ld\n",
            stateful_op->in_use, stateful_op->token,
            stateful_op->use_count, stateful_op - conn->stateful_ops);

    spin_lock(&stateful_op->lock);
    BUG_ON(stateful_op->in_use);
    stateful_op->cpu = cpu;
    stateful_op->cpu_index = cpu_index;
    stateful_op->seq_id = atomic_inc_return(&castle_req_seq_id);
    stateful_op->last_used_jiffies = jiffies;
    stateful_op->expire = expire;
    stateful_op->expire_enabled = 0;
    stateful_op->expiring = 0;
    stateful_op->cancel_on_op_complete = 0;
    stateful_op->cancelled = 0;
    CASTLE_INIT_WORK(&stateful_op->expire_work, castle_back_stateful_op_expire);
    /* see def of castle_back_stateful_op */
    stateful_op->token = ((stateful_op - conn->stateful_ops) + (stateful_op->use_count * MAX_STATEFUL_OPS)) | 0x80000000;
    stateful_op->use_count++;
    stateful_op->conn = conn;
    stateful_op->attachment = NULL;
    INIT_LIST_HEAD(&stateful_op->op_queue);
    spin_unlock(&stateful_op->lock);

    *op_ptr = stateful_op;

    castle_back_conn_get(conn);

    return stateful_op->token;
}

static void castle_back_put_stateful_op(struct castle_back_conn *conn,
                                        struct castle_back_stateful_op *stateful_op)
{
    debug("castle_back_put_stateful_op putting: token = 0x%x, use_count = %u, index = %ld\n",
            stateful_op->token, stateful_op->use_count, stateful_op - conn->stateful_ops);

    BUG_ON(!spin_is_locked(&stateful_op->lock));
    BUG_ON(!list_empty(&stateful_op->op_queue));
    BUG_ON(stateful_op->curr_op != NULL); /* Remember to put the current op! */
    BUG_ON(stateful_op->attachment != NULL); /* Remember to put the current attachment */

    stateful_op->in_use = 0;
    spin_unlock(&stateful_op->lock);

    /* If the expire_work work struct for this stateful op has already been queued,
       wait for it to(start getting) processed. Otherwise, there is nothing that stops this
       stateful op being reallocated. This overwrites expire_work, and corrupts the
       list of work items on a workqueue in the process. */
    while(test_bit(0, &stateful_op->expire_work.pending))
        msleep(1);

    /* Put stateful_op back on freelist, safe to do this without the lock
     * since nothing else will modify this stateful_op with in_use = 0
     */
    spin_lock(&conn->response_lock);
    list_add_tail(&stateful_op->list, &conn->free_stateful_ops);
    spin_unlock(&conn->response_lock);

    castle_back_conn_put(conn);
}

#define STATEFUL_OP_TIMEOUT_CHECK_INTERVAL 1 * HZ
#define STATEFUL_OP_TIMEOUT 60 * HZ

static void castle_back_stateful_op_timeout_check(unsigned long data);

/**
 * The timer doesn't inc conn->ref_count, so del_timer_sync is used to ensure the callback
 * is not being executed when conn is freed.
 */
static void castle_back_start_stateful_op_timeout_check_timer(struct castle_back_conn *conn)
{
    struct timer_list *timer = &conn->stateful_op_timeout_check_timer;

    debug("Scheduling stateful_op_timeout_check_timer for conn %p.\n", conn);

    setup_timer(timer, castle_back_stateful_op_timeout_check, (unsigned long)conn);
    mod_timer(timer, jiffies + STATEFUL_OP_TIMEOUT_CHECK_INTERVAL);
}

/**
 *
 * NOTE: Called from a timer (soft interrupt).
 */
static void castle_back_stateful_op_timeout_check(unsigned long data)
{
    struct castle_back_conn *conn = (struct castle_back_conn *)(data);
    unsigned long flags;

    debug("castle_back_stateful_op_timeout_check for conn = %p\n", conn);

    queue_work(conn->timeout_check_wq, &conn->timeout_check_work);

    /*
     * Reschedule ourselves. The lock is needed to avoid this race.
     * restart_timer is initially 1, then after checking is set to 0. Then timer deleted,
     * which then blocks until we leave i.e. the timer is rescheduled.  Then the timer is running, but
     * the connection is freed.
     */
    spin_lock_irqsave(&conn->restart_timer_lock, flags);
    if (conn->restart_timer)
        castle_back_start_stateful_op_timeout_check_timer(conn);
    spin_unlock_irqrestore(&conn->restart_timer_lock, flags);
}

static void _castle_back_stateful_op_timeout_check(void *data)
{
    struct castle_back_conn *conn = (struct castle_back_conn*)data;
    struct castle_back_stateful_op *stateful_ops = conn->stateful_ops;
    uint32_t i;

    debug("%s::conn = %p\n", __FUNCTION__, conn);

    for (i = 0; i < MAX_STATEFUL_OPS; i++)
    {
        /*
         * Get the lock, to ensure that the stateful_op didn't complete after we test, and
         * we schedule expiry after the connection is potentially freed.
         */
        spin_lock(&stateful_ops[i].lock);
        if (stateful_ops[i].in_use && stateful_ops[i].expire_enabled &&
                jiffies - stateful_ops[i].last_used_jiffies > STATEFUL_OP_TIMEOUT &&
                !stateful_ops[i].expiring)
        {
            castle_printk(LOG_WARN, "stateful_op index %u, token %u has expired.\n",
                    i, stateful_ops[i].token);
            /*
             * We may have already queued up this stateful_op to expire. Be sure to not
             * take a reference more than once. It is safe to increment the reference count
             * after queueing since the expire can't have started because we still have the lock.
             */
            if (queue_work(castle_back_wq, &stateful_ops[i].expire_work))
                castle_back_conn_get(conn);
        }
        spin_unlock(&stateful_ops[i].lock);
    }
}

static inline void castle_back_stateful_op_enable_expire(struct castle_back_stateful_op *stateful_op)
{
    BUG_ON(!spin_is_locked(&stateful_op->lock));

    /* only reset last_used_jiffies if was disabled before */
    if (!stateful_op->expire_enabled)
    {
        stateful_op->last_used_jiffies = jiffies;
        stateful_op->expire_enabled = 1;
    }
}

static inline void castle_back_stateful_op_disable_expire(struct castle_back_stateful_op *stateful_op)
{
    BUG_ON(!spin_is_locked(&stateful_op->lock));
    stateful_op->expire_enabled = 0;
}

static void castle_back_stateful_op_expire(struct work_struct *work)
{
    struct castle_back_stateful_op *stateful_op;
    struct castle_back_conn *conn;

    stateful_op = container_of(work, struct castle_back_stateful_op, expire_work);
    conn = stateful_op->conn;

    debug("castle_back_stateful_op_expire for stateful_op = %p\n", stateful_op);

    spin_lock(&stateful_op->lock);

    /* check it hasn't been used since expire was queued up */
    if (stateful_op->in_use && stateful_op->expire_enabled &&
            jiffies - stateful_op->last_used_jiffies > STATEFUL_OP_TIMEOUT &&
            !stateful_op->expiring) /* possible have been here before */
    {
        BUG_ON(!stateful_op->expire);
        BUG_ON(!list_empty(&stateful_op->op_queue));

        stateful_op->expiring = 1;

        spin_unlock(&stateful_op->lock);

        castle_printk(LOG_INFO, "Stateful operation with token 0x%x has expired.\n",
                stateful_op->token);
        stateful_op->expire(stateful_op);
    }
    else
        spin_unlock(&stateful_op->lock);

    castle_back_conn_put(conn);
}

void castle_attachment_stateful_ops_expire(struct castle_attachment *ca)
{
    struct castle_back_stateful_op *sop;
    struct list_head *lh;

    BUG_ON(in_atomic());

    spin_lock(&ca->sop_lock);
    list_for_each(lh, &ca->stateful_ops)
    {
        sop = list_entry(lh, struct castle_back_stateful_op, attachment_list);
        spin_lock(&sop->lock);
        BUG_ON(sop->attachment != ca);
        if (sop->in_use)
        {
            /* Simply nudge the normal process of expiry to work it's "magic". */
            castle_printk(LOG_USERINFO, "%s::[col %u] forcing expiry of stateful op 0x%x\n",
                    __FUNCTION__, ca->col.id, sop->token);
            sop->last_used_jiffies = 0;
        }
        spin_unlock(&sop->lock);
    }
    spin_unlock(&ca->sop_lock);
}

/**
 * Checks the token is still valid before queueing in case it has finished in between the find
 * and getting here.
 *
 * @also castle_back_release()
 * @also stateful_op->expire()
 */
static int castle_back_stateful_op_queue_op(struct castle_back_stateful_op *stateful_op,
                                            castle_interface_token_t token,
                                            struct castle_back_op *op)
{
    BUG_ON(!spin_is_locked(&stateful_op->lock));
    if (!stateful_op->in_use
            || stateful_op->token != token
            || stateful_op->cancel_on_op_complete)
    {
        error("Token expired 0x%x\n", token);
        return -EBADFD;
    }

    if (op->req.tag == CASTLE_RING_ITER_FINISH || op->req.tag == CASTLE_RING_ITER_FINISH_FASTPATH)
        /* Don't process any queued ITER_NEXT ops if we have been informed that
         * the iterator is to terminate (either by ITER_NEXT or userspace). */
        list_add(&op->list, &stateful_op->op_queue);
    else
        list_add_tail(&op->list, &stateful_op->op_queue);

    return 0;
}

/**
 * Call this when an op for a stateful op has been completed. Checks to see
 * if the stateful_op should now expire.
 *
 * @return 0 means carry on processing the next op; non-zero means the stateful op has been
 * cancelled and is now invalid. The lock is dropped on non-zero return.
 */
static int WARN_UNUSED_RET castle_back_stateful_op_completed_op
                                        (struct castle_back_stateful_op *stateful_op)
{
    BUG_ON(!spin_is_locked(&stateful_op->lock));
    if (stateful_op->cancel_on_op_complete)
    {
        BUG_ON(stateful_op->expiring);
        BUG_ON(!stateful_op->expire);
        stateful_op->expiring = 1;
        spin_unlock(&stateful_op->lock);
        stateful_op->expire(stateful_op);
        return 1;
    }
    return 0;
}

/*
 * Take an op from Queue and set it as curr_op.
 *
 * @return non-zero if the next op should be called, 0 otherwise
 */
static int castle_back_stateful_op_prod(struct castle_back_stateful_op *stateful_op)
{
    BUG_ON(!spin_is_locked(&stateful_op->lock));
    BUG_ON(stateful_op->cancel_on_op_complete);
    BUG_ON(!stateful_op->in_use);

    /* If there is a ongoing operation, don't go further. */
    if (stateful_op->curr_op != NULL)
    {
        /* Should never expire with a ongoing operation. */
        BUG_ON(stateful_op->expire_enabled);
        return 0;
    }

    /* Queue is empty. */
    if (list_empty(&stateful_op->op_queue))
    {
        /* there is no ongoing op and nothing in the queue - set to expire */
        castle_back_stateful_op_enable_expire(stateful_op);
        return 0;
    }

    /* Queue is not empty. Few outstanding(no ongoing) ops. Shouldn't expire. */
    castle_back_stateful_op_disable_expire(stateful_op);

    /* take an op off the queue and process it */
    stateful_op->curr_op = list_first_entry(&stateful_op->op_queue, struct castle_back_op, list);
    list_del(&stateful_op->curr_op->list);

    return 1;
}

static int castle_back_reply(struct castle_back_op *op, int err,
                             castle_interface_token_t token,
                             uint64_t length,
                             castle_user_timestamp_t user_timestamp,
                             castle_resp_flags_t flags);

/**
 * Finish all ops on the stateful op queue, giving them err & closing their buffers
 */
static void castle_back_stateful_op_finish_all(struct castle_back_stateful_op *stateful_op, int err)
{
    struct list_head *pos, *tmp;
    struct castle_back_op *op;
#ifdef DEBUG
    int cancelled = 0;
#endif

    BUG_ON(!spin_is_locked(&stateful_op->lock));

    /* Prevent any further ops from being queued. */
    stateful_op->cancel_on_op_complete = 1;

    /* Return err for all ops already queued. */
    list_for_each_safe(pos, tmp, &stateful_op->op_queue)
    {
        op = list_entry(pos, struct castle_back_op, list);
        list_del(pos);
        if (op->buf)
            castle_back_buffer_put(op->conn, op->buf);
        /* even though we have the lock, this is safe since conn reference count cannot be
         * decremented to 0 since the stateful_op has a count */
        castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
#ifdef DEBUG
        cancelled++;
#endif
    }

    stateful_debug("stateful_op=%p cancelled %i ops.\n",
            stateful_op, cancelled);
}

/******************************************************************
 * Castle VM ops for buffers etc
 */

/**
 * This should only be call on a partial munmap, when
 * the vma is split.  We use it to up the ref count on
 * the buffer, to stop the buffer going away when you
 * do multiple, partial munmaps.
 */
static void castle_back_vm_open(struct vm_area_struct *vma)
{
    struct castle_back_conn *conn = NULL;
    struct castle_back_buffer *buf = NULL;

    debug("castle_back_vm_open vm_start=%lx vm_end=%lx\n", vma->vm_start, vma->vm_end);

    if (vma->vm_file != NULL)
        conn = vma->vm_file->private_data;

    if (conn == NULL)
    {
        error("castle_back_vm_open: no connection!\n");
        return;
    }

    buf = castle_back_buffer_get(conn, vma->vm_start, 0 /*user_len*/);

    if (buf == NULL)
    {
        error("castle_back_vm_open: could not find buffer!\n");
        return;
    }

    debug("castle_back_vm_open buf=%p, size=%d, vm_start=%lx, vm_end=%lx\n",
        buf, buf->size, vma->vm_start, vma->vm_end);

    atomic_inc(&buf->ref_count);

    castle_back_buffer_put(conn, buf);
}

static void castle_back_vm_close(struct vm_area_struct *vma)
{
    struct castle_back_conn *conn = NULL;
    struct castle_back_buffer *buf = NULL;

    debug("castle_back_vm_close vm_start=%lx vm_end=%lx\n", vma->vm_start, vma->vm_end);
    debug("castle_back_vm_close mm->mmap_sem.activity=%d\n", vma->vm_mm->mmap_sem.activity);

    if (vma->vm_file != NULL)
        conn = vma->vm_file->private_data;

    if (conn == NULL)
    {
        error("castle_back_vm_close: no connection!\n");
        return;
    }

    buf = castle_back_buffer_get(conn, vma->vm_start, 0 /*user_len*/);

    if (buf == NULL)
    {
        error("castle_back_vm_close: could not find buffer!\n");
        return;
    }

    debug("castle_back_vm_close buf=%p, size=%d, vm_start=%lx, vm_end=%lx\n",
        buf, buf->size, vma->vm_start, vma->vm_end);

    /* Double put - This is the reverse of the ref_count=1 in buffer_map */
    castle_back_buffer_put(conn, buf);
    castle_back_buffer_put(conn, buf);
}

static struct vm_operations_struct castle_back_vm_ops = {
    open:     castle_back_vm_open,
    close:    castle_back_vm_close,
};

/******************************************************************
 * High(er) level rpc callbacks
 */

/**
 * @also castle_back_poll()
 */
static int castle_back_reply(struct castle_back_op *op,
                             int err,
                             castle_interface_token_t token,
                             uint64_t length,
                             castle_user_timestamp_t user_timestamp,
                             castle_resp_flags_t flags)
{
    struct castle_back_conn *conn = op->conn;
    castle_back_ring_t *back_ring = &conn->back_ring;
    castle_response_t resp;
    int notify;

    resp.call_id = op->req.call_id;
    resp.err = err;
    resp.token = token;
    resp.length = length;
    resp.user_timestamp = user_timestamp;
    resp.flags = flags;

    debug("castle_back_reply op=%p, call_id=%d, err=%d, token=0x%x, length=%llu, "
        "timestamp = %llu, flags=%u\n",
        op, op->req.call_id, err, token, length, user_timestamp, flags);

    spin_lock(&conn->response_lock);

    memcpy(RING_GET_RESPONSE(back_ring, back_ring->rsp_prod_pvt), &resp, sizeof(resp));
    back_ring->rsp_prod_pvt++;

    RING_PUSH_RESPONSES_AND_CHECK_NOTIFY(back_ring, notify);

    /* Put op at the back of the freelist. */
    list_add_tail(&op->list, &conn->free_ops);

    spin_unlock(&conn->response_lock);

    /* @TODO if (notify) ? */
    debug(">>>notifying user\n");
    set_bit(CASTLE_BACK_CONN_NOTIFY_BIT, &conn->flags);
    wake_up(&conn->wait);

    castle_back_conn_put(conn);

    return 0;
}

/**
 * Copy userland key from one buffer to another.
 *
 * @param   src_key Pointer to key
 * @param   key_len Size of src_key
 * @param   dst_key Where to copy src_key
 *
 * NOTE: Caller must ensure that src_key is in a buffer that will not go away.
 */
static int _castle_back_key_copy_get(c_vl_bkey_t *src_key,
                                     uint32_t key_len,
                                     c_vl_bkey_t **dst_key)
{
    c_vl_bkey_t *bkey;
    int err, i;

    if (key_len < sizeof(c_vl_bkey_t) || key_len > VLBA_TREE_MAX_KEY_SIZE)
    {
        error("Bad key length %u\n", key_len);
        return -ENAMETOOLONG;
    }

    /* Perform checks on a temporary copy of the key to ensure the key is valid
     * and that the userspace doesn't modify the key during checks. */
    bkey = castle_dup_or_copy(src_key, key_len, NULL, NULL);

    if (key_len != (bkey->length + 4))
    {
        error("Buffer length(%u) doesn't match with key length(%u)\n", key_len, bkey->length+4);
        err = -EINVAL;
        goto err;
    }

    if (*((uint64_t *)bkey->_unused) != 0)
    {
        error("Unused bits need to be set to 0\n");
        err = -EINVAL;
        goto err;
    }

    /* Check if the key length is smaller than space needed for all dim_heads. */
    if ((sizeof(c_vl_bkey_t) + (bkey->nr_dims * 4)) > key_len)
    {
        error("Too many dimensions %d\n", bkey->nr_dims);
        err = -EINVAL;
        goto err;
    }

    if (bkey->nr_dims == 0)
    {
        error("Zero-dimensional key\n");
        err = -EINVAL;
        goto err;
    }

    debug("Original key pointer %p\n", user_key);

    /* Check if all the key dimensions or sane. */
    for (i=0; i < bkey->nr_dims; i++)
    {
        uint32_t dim_len   = castle_object_btree_key_dim_length(bkey, i);
        uint8_t  dim_flags = castle_object_btree_key_dim_flags_get(bkey, i);
        uint8_t *dim_data  = castle_object_btree_key_dim_get(bkey, i);

        /* Flags other than INFINITY flags are not supported. */
        if (dim_flags & (KEY_DIMENSION_FLAGS_MASK ^ KEY_DIMENSION_INFINITY_FLAGS_MASK))
        {
            error("Found flags other than INFINITY %u\n", dim_flags);
            err = -EINVAL;
            goto err;
        }

        /* Only one kind of infinity is possible. */
        if ((dim_flags & KEY_DIMENSION_MINUS_INFINITY_FLAG) &&
            (dim_flags & KEY_DIMENSION_PLUS_INFINITY_FLAG))
        {
            error("Found both PLUS_INFINITY and MINUS_INFINITY for the same dimension.\n");
            err = -EINVAL;
            goto err;
        }

        /* Length should be zero, if the dimension is infinity. */
        if ((dim_flags & KEY_DIMENSION_INFINITY_FLAGS_MASK) && (dim_len != 0))
        {
            error("Found mis-match for INFINITY flags and dimension length.\n");
            err = -EINVAL;
            goto err;
        }

        /* Dimension payload shouldn't cross key boundaries. */
        if ((dim_data + dim_len) > (((uint8_t *)bkey) + key_len))
        {
            error("Dimension payload going beyond the key boundaries [%p, %u] - [%p, %u]\n",
                  dim_data, dim_len, bkey, key_len);
            err = -EINVAL;
            goto err;
        }
    }

    *dst_key = bkey;

#ifdef DEBUG
    vl_bkey_print(LOG_DEBUG, bkey);
#endif

    return 0;

err:
    castle_free(bkey);
    return err;
}

/**
 * Copy userland key from uas_key to dst_key (size key_len).
 *
 * @param conn      Connection to get buffer for
 * @param uas_key   Source key pointer (UAS)
 * @param key_len   Size of user_key
 * @param dst_key   Destination pointer (KAS)
 */
static int castle_back_key_copy_get(struct castle_back_conn *conn,
                                    c_vl_bkey_t *uas_key,
                                    uint32_t key_len,
                                    c_vl_bkey_t **dst_key)
{
    struct castle_back_buffer *buf;
    c_vl_bkey_t *src_key;
    int err;

    /* Take a reference to the usrspace buffer and work out start (inclusive)
     * and end point (exclusive) of the key block in user space. */
    buf = castle_back_buffer_get(conn, (unsigned long)uas_key, key_len);
    if (!buf)
    {
        error("Bad user pointer %p\n", uas_key);
        return -EINVAL;
    }

    src_key = castle_back_user_to_kernel(buf, uas_key);
    err = _castle_back_key_copy_get(src_key, key_len, dst_key);


    castle_back_buffer_put(conn, buf);

    return err;
}

static void castle_back_replace_complete(struct castle_object_replace *replace, int err)
{
    struct castle_back_op *op = container_of(replace, struct castle_back_op, replace);

    debug("castle_back_replace_complete\n");

    if (op->replace.value_len > 0)
        castle_back_buffer_put(op->conn, op->buf);

    /* Update stats. */
    if (!err)
    {
        atomic64_inc(&op->attachment->put.ios);
        atomic64_add(op->replace.value_len, &op->attachment->put.bytes);
    }

    castle_free(replace->key);

    castle_attachment_put(op->attachment);

    /* If we receive -EEXIST here, it must be because we tried to insert an object into a T0 that
       already contains an object with the same key but newer timestamp (see c_o_replace_cvt_get);
       the entry is dropped, and we choose to be quiet about it by pretending there was no error. */
    if(err==-EEXIST)
        err=0;

    castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

static uint32_t castle_back_replace_data_length_get(struct castle_object_replace *replace)
{
    struct castle_back_op *op = container_of(replace, struct castle_back_op, replace);

    return op->req.replace.value_len;
}

static void castle_back_replace_data_copy(struct castle_object_replace *replace,
                                          void *buffer, uint32_t buffer_length, int not_last)
{
    struct castle_back_op *op = container_of(replace, struct castle_back_op, replace);

    debug("%s:: buffer=%p, buffer_length=%u, not_last=%d, value_len=%u\n",
        __FUNCTION__, buffer, buffer_length, not_last, op->req.replace.value_len);

    if (op->req.replace.value_len == 0)
        return;

    /* @TODO: actual zero copy! */

    BUG_ON(op->buffer_offset + buffer_length > op->req.replace.value_len);

    memcpy(buffer, castle_back_user_to_kernel(op->buf, op->req.replace.value_ptr) + op->buffer_offset,
        buffer_length);

    op->buffer_offset += buffer_length;
}

/**
 * Insert/replace value at specified key,version in DA.
 *
 * @also castle_object_replace()
 * @also castle_back_remove()
 */
static void castle_back_replace(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_conn *conn = op->conn;
    int err;

    op->attachment = castle_attachment_get(op->req.replace.collection_id, WRITE);
    if (op->attachment == NULL)
    {
        error("Collection not found id=0x%x\n", op->req.replace.collection_id);
        err = -ENOTCONN;
        goto err0;
    }

    /*
     * Get buffer with value in it and save it
     */
    if (op->req.replace.value_len > 0)
    {
        op->buf = castle_back_buffer_get(conn,
                                         (unsigned long) op->req.replace.value_ptr,
                                         op->req.replace.value_len);
        if (op->buf == NULL)
        {
            error("Couldn't get buffer for pointer=%p length=%u\n",
                    op->req.replace.value_ptr, op->req.replace.value_len);
            err = -EINVAL;
            goto err1;
        }
    }
    else op->buf = NULL;

    op->buffer_offset = 0;

    op->replace.value_len = op->req.replace.value_len;
    op->replace.replace_continue = NULL;
    op->replace.complete = castle_back_replace_complete;
    op->replace.data_length_get = castle_back_replace_data_length_get;
    op->replace.data_copy = castle_back_replace_data_copy;
    op->replace.counter_type = CASTLE_OBJECT_NOT_COUNTER;
    op->replace.has_user_timestamp = 0;
    op->replace.key = op->key;  /* key will be freed by replace_complete() */

    err = castle_object_replace(&op->replace, op->attachment, op->cpu_index, 0);
    if (err)
        goto err2;

    return;

err2: if (op->buf) castle_back_buffer_put(conn, op->buf);
err1: castle_attachment_put(op->attachment);
err0: castle_free(op->key);
      castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

static void castle_back_timestamped_replace(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_conn *conn = op->conn;
    int err;

    op->attachment = castle_attachment_get(op->req.timestamped_replace.collection_id, WRITE);
    if (op->attachment == NULL)
    {
        error("Collection not found id=0x%x\n", op->req.timestamped_replace.collection_id);
        err = -ENOTCONN;
        goto err0;
    }

    /*
     * Get buffer with value in it and save it
     */
    if (op->req.timestamped_replace.value_len > 0)
    {
        op->buf = castle_back_buffer_get(conn,
                                         (unsigned long) op->req.timestamped_replace.value_ptr,
                                         op->req.timestamped_replace.value_len);
        if (op->buf == NULL)
        {
            error("Couldn't get buffer for pointer=%p length=%u\n",
                    op->req.timestamped_replace.value_ptr,
                    op->req.timestamped_replace.value_len);
            err = -EINVAL;
            goto err1;
        }
    }
    else op->buf = NULL;

    op->buffer_offset = 0;

    op->replace.value_len = op->req.timestamped_replace.value_len;
    op->replace.replace_continue = NULL;
    op->replace.complete = castle_back_replace_complete;
    op->replace.data_length_get = castle_back_replace_data_length_get;
    op->replace.data_copy = castle_back_replace_data_copy;
    op->replace.counter_type = CASTLE_OBJECT_NOT_COUNTER;
    op->replace.has_user_timestamp = 1;
    op->replace.user_timestamp = op->req.timestamped_replace.user_timestamp;
    op->replace.key = op->key;  /* key will be freed by replace_complete() */

    err = castle_object_replace(&op->replace, op->attachment, op->cpu_index, 0);
    if (err)
        goto err2;

    return;

err2: if (op->buf) castle_back_buffer_put(conn, op->buf);
err1: castle_attachment_put(op->attachment);
err0: castle_free(op->key);
      castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

static void castle_back_counter_replace(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_conn *conn = op->conn;
    int err;

    op->attachment = castle_attachment_get(op->req.counter_replace.collection_id, WRITE);
    if (op->attachment == NULL)
    {
        error("Collection not found id=0x%x\n", op->req.counter_replace.collection_id);
        err = -ENOTCONN;
        goto err0;
    }

    /*
     * Get buffer with value in it and save it
     */
    if (op->req.counter_replace.value_len > 0)
    {
        op->buf = castle_back_buffer_get(conn,
                                         (unsigned long) op->req.counter_replace.value_ptr,
                                         op->req.counter_replace.value_len);
        if (op->buf == NULL)
        {
            error("Couldn't get buffer for pointer=%p length=%u\n",
                    op->req.counter_replace.value_ptr,
                    op->req.counter_replace.value_len);
            err = -EINVAL;
            goto err1;
        }
    }
    else op->buf = NULL;

    op->buffer_offset = 0;

    op->replace.value_len = op->req.counter_replace.value_len;
    op->replace.replace_continue = NULL;
    op->replace.complete = castle_back_replace_complete;
    op->replace.data_length_get = castle_back_replace_data_length_get;
    op->replace.data_copy = castle_back_replace_data_copy;
    op->replace.counter_type = op->req.counter_replace.add == CASTLE_COUNTER_TYPE_SET ?
        CASTLE_OBJECT_COUNTER_SET : CASTLE_OBJECT_COUNTER_ADD;
    op->replace.has_user_timestamp = 0;
    op->replace.key = op->key;  /* key will be freed by replace_complete() */

    err = castle_object_replace(&op->replace, op->attachment, op->cpu_index, 0);
    if (err)
        goto err2;

    return;

err2: if (op->buf) castle_back_buffer_put(conn, op->buf);
err1: castle_attachment_put(op->attachment);
err0: castle_free(op->key);
      castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

static void castle_back_remove_complete(struct castle_object_replace *replace, int err)
{
    struct castle_back_op *op = container_of(replace, struct castle_back_op, replace);

    debug("castle_back_remove_complete\n");

    /* Update stats. */
    if (!err)
    {
        atomic64_inc(&op->attachment->put.ios);
        /* Replacing with Tomb Stone. Don't increment bytes. */
    }

    castle_free(replace->key);

    castle_attachment_put(op->attachment);

    /* If we receive -EEXIST here, it must be because we tried to remove an object from a T0 that
       contained an object with the same key but newer timestamp than the tombstone (see
       c_o_replace_cvt_get); the tombstone is dropped, and we choose to be quiet about it by
       pretending there was no error. */
    if(err==-EEXIST)
        err=0;

    castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

/**
 * Remove (tombstone) value at specified key,version in DA.
 *
 * @also castle_object_replace()
 * @also castle_back_replace()
 */
static void castle_back_remove(void *data)
{
    struct castle_back_op *op = data;
    int err;

    op->attachment = castle_attachment_get(op->req.remove.collection_id, WRITE);
    if (op->attachment == NULL)
    {
        error("Collection not found id=0x%x\n", op->req.remove.collection_id);
        err = -ENOTCONN;
        goto err0;
    }

    op->buf = NULL;
    op->replace.value_len = 0;
    op->replace.replace_continue = NULL;
    op->replace.complete = castle_back_remove_complete;
    op->replace.data_length_get = NULL;
    op->replace.data_copy = NULL;
    op->replace.counter_type = CASTLE_OBJECT_NOT_COUNTER;
    op->replace.has_user_timestamp = 0;
    op->replace.key = op->key;  /* key will be freed by remove_complete() */

    err = castle_object_replace(&op->replace, op->attachment, op->cpu_index, 1 /*tombstone*/);
    if (err)
        goto err2;

    return;

err2: castle_attachment_put(op->attachment);
err0: castle_free(op->key);
      castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

static void castle_back_timestamped_remove(void *data)
{
    struct castle_back_op *op = data;
    int err;

    op->attachment = castle_attachment_get(op->req.timestamped_remove.collection_id, WRITE);
    if (op->attachment == NULL)
    {
        error("Collection not found id=0x%x\n", op->req.timestamped_remove.collection_id);
        err = -ENOTCONN;
        goto err0;
    }

    op->buf = NULL;
    op->replace.value_len = 0;
    op->replace.replace_continue = NULL;
    op->replace.complete = castle_back_remove_complete;
    op->replace.data_length_get = NULL;
    op->replace.data_copy = NULL;
    op->replace.counter_type = CASTLE_OBJECT_NOT_COUNTER;
    op->replace.has_user_timestamp = 1;
    op->replace.user_timestamp = op->req.timestamped_replace.user_timestamp;
    op->replace.key = op->key;  /* key will be freed by remove_complete() */

    err = castle_object_replace(&op->replace, op->attachment, op->cpu_index, 1 /*tombstone*/);
    if (err)
        goto err2;

    return;

err2: castle_attachment_put(op->attachment);
err0: castle_free(op->key);
      castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

int castle_back_get_reply_continue(struct castle_object_get *get,
                                   int err,
                                   void *buffer,
                                   uint32_t buffer_len,
                                   int last)
{
    struct castle_back_op *op = container_of(get, struct castle_back_op, get);
    void *dest = castle_back_user_to_kernel(op->buf, op->req.get.value_ptr + op->buffer_offset);
    uint32_t to_copy = min(buffer_len, (uint32_t) (op->req.get.value_len - op->buffer_offset));

    if (err)
    {
        castle_back_buffer_put(op->conn, op->buf);
        castle_free(get->key);
        castle_attachment_put(op->attachment);
        castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);

        return 1;
    }

    BUG_ON(!buffer);

    if (to_copy > 0)
    {
        memcpy(dest, buffer, to_copy);
        op->buffer_offset += to_copy;
    }

    BUG_ON(op->req.get.value_len < op->buffer_offset);
    /* We should finish if that's the last bit of data we are going to get,
       or if there is no more space in the shared buffer. */
    last = last || (op->req.get.value_len == op->buffer_offset);

    if (last)
    {
        uint32_t get_value_len = op->req.get.value_len;
        castle_user_timestamp_t u_ts = ULLONG_MAX;
        castle_resp_flags_t resp_flags = CASTLE_RESPONSE_FLAG_NONE;

        if (get->flags & CASTLE_RING_FLAG_RET_TIMESTAMP)
            u_ts = get->cvt.user_timestamp;

        if (CVT_TOMBSTONE(get->cvt))
        {
            /* if we got this far, it must be because the objects layer saw the flag: */
            BUG_ON(!(get->flags & CASTLE_RING_FLAG_RET_TOMBSTONE));
            resp_flags |= CASTLE_RESPONSE_FLAG_TOMBSTONE;
        }

        castle_back_buffer_put(op->conn, op->buf);

        /* Update stats. */
        if (!err)
        {
            atomic64_inc(&op->attachment->get.ios);
            atomic64_add(get_value_len, &op->attachment->get.bytes);
        }

        castle_free(get->key);
        castle_attachment_put(op->attachment);
        castle_back_reply(op, err, 0, op->value_length, u_ts, resp_flags);
    }

    return last;
}

int castle_back_get_reply_start(struct castle_object_get *get,
                                int err,
                                uint64_t data_length,
                                void *buffer,
                                uint32_t buffer_length)
{
    struct castle_back_op *op = container_of(get, struct castle_back_op, get);
    int err_prime;

    BUG_ON(buffer_length > data_length);

    if (err)
    {
        err_prime = err;
        goto err;
    }

    if (!buffer)
    {
        BUG_ON((data_length != 0) || (buffer_length != 0));
        err_prime = -ENOENT;
        goto err;
    }

    op->value_length = data_length;
    op->buffer_offset = 0;

    return castle_back_get_reply_continue(get,
                                          0,
                                          buffer,
                                          buffer_length,
                                          buffer_length == data_length);

err:
    castle_free(get->key);
    castle_back_buffer_put(op->conn, op->buf);
    castle_attachment_put(op->attachment);
    castle_back_reply(op, err_prime, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);

    /* Return value ignored if there was an error. */
    return 0;
}

/**
 * Look for specified key,version in DA.
 *
 * @also castle_object_get()
 */
static void castle_back_get(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_conn *conn = op->conn;
    int err;

    op->attachment = castle_attachment_get(op->req.get.collection_id, READ);
    if (op->attachment == NULL)
    {
        error("Collection not found id=0x%x\n", op->req.get.collection_id);
        err = -ENOTCONN;
        goto err0;
    }

    /*
     * Get buffer with value in it and save it
     */
    op->buf = castle_back_buffer_get(conn,
                                     (unsigned long) op->req.get.value_ptr,
                                     op->req.get.value_len);
    if (op->buf == NULL)
    {
        error("Couldn't get buffer for pointer=%p length=%u\n",
                op->req.get.value_ptr, op->req.get.value_len);
        err = -EINVAL;
        goto err1;
    }

    op->get.reply_start = castle_back_get_reply_start;
    op->get.reply_continue = castle_back_get_reply_continue;
    op->get.key = op->key;
    op->get.flags = op->req.flags;

    if ((op->req.flags & CASTLE_RING_FLAG_RET_TIMESTAMP) &&
        !castle_attachment_user_timestamping_check(op->attachment))
    {
        error("User requested timestamp return on a non-timestamped collection, id=0x%x\n",
              op->req.get.collection_id);
        err = -EINVAL;
        goto err2;
    }

    err = castle_object_get(&op->get, op->attachment, op->cpu_index);
    if (err)
        goto err2;

    return;

err2: castle_back_buffer_put(conn, op->buf);
err1: castle_attachment_put(op->attachment);
err0: castle_free(op->key);
      castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

/**** ITERATORS ****/

/*
 * CASTLE_BACK_ITER_START
 *      Initialises an iterator and immediately begins returning values.
 *      In the case where the total set of results returned by the iterator
 *      fits into the provided buffer the caller needs to issue no further
 *      ring operations.
 *
 *      castle_back_iter_start()
 *          Gets, initialises and takes a stateful_op reference.  Enables
 *          expiry and decodes the relevant keys to be passed to the
 *          castle_objects function (castle_object_iter_init()) that
 *          initialises the underlying iterator structures.
 *
 *          Fakes up a userland iter_next structure and calls directly into
 *          fastpath _castle_back_iter_next() (takes a stateful_op structure
 *          instead of searching based on provided iter_next token).
 *
 *          Overflows into CASTLE_BACK_ITER_NEXT
 *
 * CASTLE_BACK_ITER_NEXT
 *      Requests results from castle_objects iterator and places them into
 *      the userland-provided buffer.
 *
 *      For range queries where the results do not fit into a single buffer
 *      the userland code is expected to make multiple requests.
 *
 *      castle_back_iter_next()
 *          Only called when the request came from the ring.  Looks up the
 *          stateful_op structure based on iter_next request token.  There is
 *          no need to take a further stateful_op reference - it has not been
 *          dropped since castle_back_iter_start().
 *
 *      _castle_back_iter_next()
 *          Takes a reference to the userland-provided output buffer and
 *          requeues __castle_back_iter_next() which does the heavy lifting.
 *
 *      __castle_back_iter_next()
 *          If this is not the first ring request there may be a saved KVP
 *          in the stateful_op structure.  Before the iterator is restarted
 *          this value is saved to the buffer.
 *
 *          castle_object_iter_next() is called with callback function
 *          castle_back_iter_next_callback() to request more results.
 *
 *      castle_back_iter_next_callback()
 *          Inserts castle_objects-provided KVP into the userland buffer using
 *          castle_back_save_key_value_to_list().
 *
 *          In the common case we insert a valid KVP into the buffer and
 *          return.
 *
 *          The other two cases are more interesting:
 *
 *          1. castle_back_save_key_value_to_list() returns 0 to indicate that
 *          the KVP will not fit in the buffer.  In this case we store the
 *          KVP in the stateful_op (to be inserted into the next buffer) and
 *          set the final KVP list->next ptr to the start of the userspace
 *          buffer indicating to userspace that the iterator has more values
 *          to provide.  Puts a response on the ring for the userland to
 *          consume.
 *
 *          2. The passed key was NULL, indicating that the underlying
 *          iterator has been exhausted.  We set the final KVP list->next ptr
 *          to NULL indicating to userspace that the iterator has terminated.
 *          Fakes up a userland iter_finish structure and calls directly into
 *          fastpath _castle_back_iter_finish() (takes a stateful_op
 *          structure instead of searching based on provided iter_finish token).
 *
 *          Case (2) overflows into CASTLE_BACK_ITER_FINISH
 *
 * CASTLE_BACK_ITER_FINISH
 *      Tears down underlying iterator state and frees up the stateful_op.
 *
 *      castle_back_iter_finish()
 *          Only called when the request came from the ring.  Looks up the
 *          stateful_op structure based on iter_finish request token.  There
 *          is no need to take a further stateful_op reference - it has not
 *          been dropped since castle_back_iter_start().
 *
 *      _castle_back_iter_finish()
 *          Requeues __castle_back_iter_finish().
 *
 *      __castle_back_iter_finish()
 *          Calls castle_object_iter_finish() to inform underlying iterator
 *          to tear down state.  Puts a response on the ring for the userland
 *          to consume.  Calls castle_back_stateful_op_finish_all() to finish
 *          all outstanding ops on the stateful_op queue.
 *
 *          Finally calls castle_back_iter_clean() to free any remaining
 *          back iterator state and puts the stateful_op.
 *
 * In case of early termination, e.g. by userland closing the ring, we call
 * into castle_back_release() which calls each stateful_op's->expire().  For
 * iterators the expire function is castle_back_iter_expire() which does a
 * similar job to __castle_back_iter_finish() (terminating any queued ops
 * on the stateful_op op queue).  However, in the case that there is a
 * running op (stateful_op->curr_op) we set
 * stateful_op->cancel_on_op_complete indicating that as soon as the running
 * op is completed, it should not terminate.  For this reason it is essential
 * to always call castle_back_iter_reply() (which checks the cancel bit)
 * instead of castle_back_reply() directly.
 */

static void castle_back_stateful_op_attach(struct castle_back_stateful_op *stateful_op,
                                           struct castle_attachment *attachment)
{
    BUG_ON(!attachment);
    BUG_ON(!stateful_op);
    BUG_ON(in_atomic());

    stateful_op->attachment = attachment;
    spin_lock(&stateful_op->attachment->sop_lock);
    list_add_tail(&stateful_op->attachment_list, &attachment->stateful_ops);
    spin_unlock(&stateful_op->attachment->sop_lock);
}

static void __castle_back_iter_next(void *data);
DEFINE_WQ_TRACE_FN(__castle_back_iter_next, struct castle_back_stateful_op);

static void _castle_back_iter_next(struct castle_back_op *op,
                                   struct castle_back_stateful_op *stateful_op,
                                   int fastpath);
static void __castle_back_iter_finish(void *data);
DEFINE_WQ_TRACE_FN(__castle_back_iter_finish, struct castle_back_stateful_op);

static void _castle_back_iter_finish(struct castle_back_op *op,
                                     struct castle_back_stateful_op *stateful_op,
                                     int fastpath);
static void castle_back_iter_cleanup(struct castle_back_stateful_op *stateful_op);

static void castle_back_iter_expire(struct castle_back_stateful_op *stateful_op)
{
    debug("castle_back_iter_expire token=%u.\n", stateful_op->token);

    BUG_ON(!stateful_op->expiring);
    BUG_ON(!list_empty(&stateful_op->op_queue));
    BUG_ON(stateful_op->curr_op != NULL);

    castle_object_iter_finish(stateful_op->iterator.iterator, -ETIMEDOUT);

    spin_lock(&stateful_op->attachment->sop_lock);
    list_del_init(&stateful_op->attachment_list);
    spin_unlock(&stateful_op->attachment->sop_lock);

    spin_lock(&stateful_op->lock);
    castle_back_iter_cleanup(stateful_op); /* drops stateful_op->lock */
}

/**
 * Execute the next stateful_op op, if available.
 *
 * @also castle_back_stateful_op_queue_op()
 * @also castle_back_stateful_op_prod()
 */
static void castle_back_iter_call_queued(struct castle_back_stateful_op *stateful_op)
{
    BUG_ON(!spin_is_locked(&stateful_op->lock));

    if (castle_back_stateful_op_prod(stateful_op))
    {
        switch (stateful_op->curr_op->req.tag)
        {
            case CASTLE_RING_ITER_NEXT:
                /* Requeue ITER_NEXTs to prevent a stack overflow when called
                 * from castle_back_iter_reply(). */
                stateful_debug(stateful_op_fmt_str" ITER_NEXT\n",
                        stateful_op2str(stateful_op));
                spin_unlock(&stateful_op->lock);
                BUG_ON(!queue_work_on(stateful_op->cpu, castle_back_wq, &stateful_op->work[0]));
                break;

            case CASTLE_RING_ITER_FINISH:
            case CASTLE_RING_ITER_FINISH_FASTPATH:
                /* ITER_FINISH can only ever occur once for a stateful op, so
                 * don't requeue allowing a no-requeue fastpath iterator. */
                stateful_debug(stateful_op_fmt_str" ITER_FINISH\n",
                        stateful_op2str(stateful_op));
                spin_unlock(&stateful_op->lock);
                __castle_back_iter_finish((void *)stateful_op);
                break;

            default:
                error("Invalid tag %d in castle_back_iter_call_queued.\n",
                        stateful_op->curr_op->req.tag);
                BUG();
        }
    }
    else
        spin_unlock(&stateful_op->lock);
}

/**
 * Iterator wrapper for castle_back_reply().
 *
 * Iterator functions that have either a valid token or a token that has expired
 * during the course of the function (e.g. castle_back_stateful_op_queue_op()
 * returned an error) should always call castle_back_iter_reply().
 *
 * Iterator functions that failed to get a stateful_op or have a token that
 * expired prior to calling castle_back_find_stateful_op() should call
 * castle_back_reply() directly.
 *
 * NOTE: In error cases op ! = stateful_op->curr_op.
 */
static void castle_back_iter_reply(struct castle_back_stateful_op *stateful_op,
                                   struct castle_back_op *op,
                                   int err)
{
    stateful_debug(stateful_op_fmt_str"\n", stateful_op2str(stateful_op));

    castle_back_reply(op, err, stateful_op->token, 0, 0, CASTLE_RESPONSE_FLAG_NONE);

    spin_lock(&stateful_op->lock);

    stateful_op->curr_op = NULL;

    /* drops the lock if return non-zero */
    if (castle_back_stateful_op_completed_op(stateful_op))
        return;

    castle_back_iter_call_queued(stateful_op); // drops stateful_op->lock
}

/**
 * Complete initialisation of stateful op range query.
 *
 * @param   private     stateful_op pointer
 * @param   err         Error code from castle_object_iter_init()
 *
 * Called asynchronously from castle_object_iter_init() once the object iterator
 * has been initialised.
 *
 * @also castle_back_iter_start()
 * @also castle_object_iter_init()
 */
void _castle_back_iter_start(void *private, int err)
{
    struct castle_back_stateful_op *stateful_op = private;
    struct castle_back_op *op = stateful_op->curr_op;
    struct castle_attachment *attachment = stateful_op->attachment;
    struct castle_back_conn *conn = op->conn;
    uint32_t buffer_len;
    char *buffer_ptr;

    /* Objects code iterator returned an error. */
    if (err)
        goto err;

    /* Check CASTLE_BACK_CONN_DEAD_FLAG under the stateful_op lock to prevent
     * racing with castle_back_release().  _op_enable_expire() also needs it. */
    spin_lock(&stateful_op->lock);

    if (stateful_op->conn->flags & CASTLE_BACK_CONN_DEAD_FLAG)
    {
        /* See castle_back_iter_start() comment for why we reset curr_op. */
        stateful_op->curr_op  = NULL;
        stateful_op->expiring = 1;
        spin_unlock(&stateful_op->lock);
        stateful_op->expire(stateful_op);

        return;
    }
    else
        stateful_op->in_use = 1;
    castle_back_stateful_op_enable_expire(stateful_op);

    spin_unlock(&stateful_op->lock);

    /* Iterator has been initialised.  Fake up an iter_next request and pass
     * it to castle_back_iter_next() to return the first buffer of results.
     * See also: libcastle.hg:castle_iter_next_prepare() */
    buffer_len = op->req.iter_start.buffer_len;
    buffer_ptr = op->req.iter_start.buffer_ptr;
    op->req.tag = CASTLE_RING_ITER_NEXT;
    op->req.iter_next.token      = stateful_op->token;
    op->req.iter_next.buffer_ptr = buffer_ptr;
    op->req.iter_next.buffer_len = buffer_len;

    /* Start the iterator.  iter_next() handles castle_back(_iter)_reply(). */
    _castle_back_iter_next(op, stateful_op, 1 /*fastpath*/);

    return;

err:
    spin_lock(&stateful_op->attachment->sop_lock);
    list_del_init(&stateful_op->attachment_list);
    spin_unlock(&stateful_op->attachment->sop_lock);
    castle_attachment_put(attachment);
    /* See castle_back_iter_start() comment for why we reset curr_op. */
    spin_lock(&stateful_op->lock);
    stateful_op->curr_op    = NULL;
    stateful_op->attachment = NULL;
    castle_back_put_stateful_op(conn, stateful_op);
    castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

/**
 * Begin stateful op iterating for values in specified key,version range in DA.
 *
 * @also _castle_back_iter_start()
 * @also castle_object_iter_init()
 * @also castle_back_iter_next()
 */
static void castle_back_iter_start(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_conn *conn = op->conn;
    struct castle_back_stateful_op *stateful_op;
    struct castle_attachment *attachment;
    c_vl_bkey_t *start_key;
    c_vl_bkey_t *end_key;
    int err, get_all;

    stateful_debug("conn=%p op=%p\n", conn, op);

    castle_back_stateful_op_get(conn,
                                &stateful_op,
                                op->cpu,
                                op->cpu_index,
                                castle_back_iter_expire);
    if (!stateful_op)
    {
        error("castle_back: no more free stateful ops!\n");
        err = -EAGAIN;
        goto err0;
    }

    attachment = castle_attachment_get(op->req.iter_start.collection_id, READ);
    if (attachment == NULL)
    {
        error("Collection not found id=0x%x\n", op->req.iter_start.collection_id);
        err = -ENOTCONN;
        goto err1;
    }

    if ((op->req.flags & CASTLE_RING_FLAG_RET_TIMESTAMP) &&
        !castle_attachment_user_timestamping_check(attachment))
    {
        error("User requested timestamp return on a non-timestamped collection, id=0x%x\n",
              op->req.get.collection_id);
        err = -EINVAL;
        goto err2;
    }


    /* Special case: if both start and end_key are NULL, the RQ will be over
       entire dataset, without any hypercube filtering. */
    if (unlikely((op->req.iter_start.start_key_ptr == NULL) &&
                 (op->req.iter_start.end_key_ptr == NULL)))
    {
        start_key = end_key = NULL;
        get_all = 1;
    }
    else
    {
        /* start_key and end_key are freed by castle_object_iter_finish */
        err = castle_back_key_copy_get(conn, op->req.iter_start.start_key_ptr,
            op->req.iter_start.start_key_len, &start_key);
        if (err)
            goto err2;

        err = castle_back_key_copy_get(conn, op->req.iter_start.end_key_ptr,
            op->req.iter_start.end_key_len, &end_key);
        if (err)
            goto err3;

        get_all = 0;
#ifdef DEBUG
        stateful_debug("start_key: \n");
        vl_bkey_print(LOG_DEBUG, start_key);

        stateful_debug("end_key: \n");
        vl_bkey_print(LOG_DEBUG, end_key);
#endif
    }

    stateful_op->tag = CASTLE_RING_ITER_START;
    stateful_op->flags = op->req.flags;
    /* Here we abuse the curr_op field to store *this* op.  This is necessary
     * as we use the stateful_op as our private data to be passed back from the
     * asynchronous castle_object_iter_init() callback.
     * There is no need to hold the stateful_op->lock while we set curr_op as
     * in_use is 0 (set by castle_back_stateful_op_get()) so this stateful_op
     * will never be handled by castle_back_release().  In addition we have not
     * set a timeout for this stateful_op yet. */
    stateful_op->curr_op = op;

    stateful_op->iterator.collection_id = op->req.iter_start.collection_id;
    stateful_op->iterator.get_all = get_all;
    stateful_op->iterator.saved_key = NULL;
    stateful_op->iterator.start_key = start_key;
    stateful_op->iterator.end_key = end_key;
    stateful_op->iterator.nr_keys = 0;
    stateful_op->iterator.nr_bytes = 0;
    castle_back_stateful_op_attach(stateful_op, attachment);

    CASTLE_INIT_WORK_AND_TRACE(&stateful_op->work[0], __castle_back_iter_next, stateful_op);
    CASTLE_INIT_WORK_AND_TRACE(&stateful_op->work[1], __castle_back_iter_finish, stateful_op);

    trace_CASTLE_REQUEST_BEGIN(stateful_op->seq_id, stateful_op->tag);

    /* For incremental backup we want to return tombstones also. */
    if (stateful_op->flags & CASTLE_RING_FLAG_INC_BACKUP)
        stateful_op->flags |= CASTLE_RING_FLAG_RET_TOMBSTONE;

    err = castle_object_iter_init(attachment,
                                  get_all,
                                  start_key,
                                  end_key,
                                  &stateful_op->iterator.iterator,
                                  stateful_op->seq_id,
                                  stateful_op->flags,
                                  _castle_back_iter_start, /*async_cb*/
                                  stateful_op /*private*/);
    if (err)
        goto err4;

    /* castle_object_iter_init() has gone asynchronous, iter_start will be
     * continued via callback handler, _castle_back_iter_start(). */

    stateful_debug("op=%p "stateful_op_fmt_str"\n", op, stateful_op2str(stateful_op));

    trace_CASTLE_REQUEST_RELEASE(stateful_op->seq_id);

    return;

err4: stateful_op->curr_op = NULL; /* revert the abuse performed above */
      castle_free(end_key);
err3: castle_free(start_key);
err2: spin_lock(&stateful_op->attachment->sop_lock);
      list_del_init(&stateful_op->attachment_list);
      spin_unlock(&stateful_op->attachment->sop_lock);
      castle_attachment_put(attachment);
      stateful_op->attachment = NULL;
err1: /* No one could have added another op to queue as we haven't returns token yet */
      spin_lock(&stateful_op->lock);
      castle_back_put_stateful_op(conn, stateful_op);
err0: castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

/**
 * Free any memory allocated by castle_buffer_kvp_get().
 *
 * @also castle_buffer_kvp_get()
 */
void castle_buffer_kvp_free(c_buf_user_kv_hdr_t *user_hdr)
{
    castle_free(user_hdr->key);
}

/**
 * IMPORTANT: This function is implemented (textualy identical) in fs.hg & libcastle.hg.
 *            Sync any changes in both repos.
 *
 * Get key value pair from buffer.
 *
 * @param   buf_con     Pointer to buffer consumer structure
 * @param   kv_index    Get kv_index^th key from buffer
 * @param   user_hdr    Return structure
 *
 * NOTE: Consumer must call castle_buffer_kvp_free() on success (kernel only).
 *
 * @return -EINVAL  Buffer sanity checking failed
 * @return  0       Success
 *
 * @also castle_buffer_kvp_free()
 */
int castle_buffer_kvp_get(c_buf_consumer_t *buf_con,
                          int kv_index,
                          c_buf_user_kv_hdr_t *user_hdr)
{
    c_buf_kv_hdr_t kv_hdr;
    uint32_t key_len;
    int err;

    /* Get copy of key value header.  kv_hdrs[] already sanity checked. */
    memcpy(&kv_hdr, &buf_con->kv_hdrs[kv_index], sizeof(c_buf_kv_hdr_t));

    /* Sanity check key in buffer and set key pointer. */
    if (kv_hdr.key_off + sizeof(c_vl_bkey_t) > buf_con->buf_len)
        return -EINVAL;
    user_hdr->key = buf_con->buf + kv_hdr.key_off;
    key_len = user_hdr->key->length + 4;
    if (kv_hdr.key_off + key_len > buf_con->buf_len)
        return -EINVAL;
#ifdef __KERNEL__
    if ((err = _castle_back_key_copy_get(user_hdr->key, key_len, &user_hdr->key)))
        return err;
#endif

    /* Sanity check value in buffer and set val pointer. */
    if (kv_hdr.val_type == CASTLE_VALUE_TYPE_INVALID)
    {
        err = -EINVAL;
        goto err;
    }
    else if (kv_hdr.val_type < CASTLE_VALUE_TYPE_OUT_OF_LINE)
    {
        if (kv_hdr.val_off + kv_hdr.val_len > buf_con->buf_len)
        {
            err = -EINVAL;
            goto err;
        }
#ifdef __KERNEL__
        if (kv_hdr.val_len > MEDIUM_OBJECT_LIMIT)
        {
            err = -EINVAL;
            goto err;
        }
#endif
        user_hdr->val = buf_con->buf + kv_hdr.val_off;
    }
    else if (kv_hdr.val_type == CASTLE_VALUE_TYPE_OUT_OF_LINE)
        user_hdr->val           = NULL;
    else
    {
        err = -EINVAL;
        goto err;
    }

    user_hdr->val_type       = kv_hdr.val_type;
    user_hdr->val_len        = kv_hdr.val_len;
    user_hdr->user_timestamp = kv_hdr.user_timestamp;

    return 0;

err:
#ifdef __KERNEL__
    castle_buffer_kvp_free(user_hdr);
#endif

    return err;
}

/**
 * IMPORTANT: This function is implemented (textualy identical) in fs.hg & libcastle.hg.
 *            Sync any changes in both repos.
 *
 * Initialise buffer consumer and return number of key and value pairs.
 */
int castle_buffer_consumer_init(c_buf_consumer_t *buf_con,
                                void *buf,
                                uint32_t buf_len)
{
    c_buf_hdr_t *buf_hdr;
    uint16_t nr_entries;
    uint32_t index_off;

    /* Sanity check buffer header fits in buffer and set pointer. */
    if (buf_len < sizeof(c_buf_hdr_t))
    {
        castle_printk(LOG_ERROR, "%s::failed because buf_len (%lu) is too small.\n",
            __FUNCTION__, buf_len);
        return -EINVAL;
    }
    buf_hdr = buf;

    /* Sanity check buffer version. */
    if (buf_hdr->version != CASTLE_BUFFER_VERSION)
    {
        castle_printk(LOG_ERROR, "%s::failed because of header version mismatch (want %u got %u).\n",
            __FUNCTION__, buf_hdr->version, CASTLE_BUFFER_VERSION);
        return -EINVAL;
    }

    /* Sanity check all key value headers fit in buffer.
     *
     * Store copies of nr_entries and index_off as we can't guarantee that they
     * won't be changed after we check them. */
    nr_entries = buf_hdr->nr_entries;
    index_off  = buf_hdr->index_off;
    if (index_off + nr_entries * sizeof(c_buf_kv_hdr_t) > buf_len)
    {
        castle_printk(LOG_ERROR, "%s::failed because buf %p apparently can't hold all it's values.\n",
            __FUNCTION__, buf);
        return -EINVAL;
    }

    buf_con->buf     = buf;
    buf_con->buf_len = buf_len;
    buf_con->buf_hdr = buf_hdr;
    buf_con->kv_hdrs = buf + index_off;

    return nr_entries;
}

/**
 * Prefetch an out-of-line object based on CVT and copy value into buf.
 *
 * @param   dest    Where to copy the value described by cvt_src
 * @param   cvt_src Describes location and size of out-of-line object
 */
static void castle_back_buffer_ool_memcpy(void *dest, c_val_tup_t *cvt_src)
{
    c2_block_t *c2b;
    int nr_blocks;

    debug("%s: cvt->cep="cep_fmt_str", len=%d\n", cep2str(cvt_src->cep), cvt_src->length);

    nr_blocks = (cvt_src->length - 1) / C_BLK_SIZE + 1;
    c2b = castle_cache_block_get(cvt_src->cep, nr_blocks, USER);
    castle_cache_advise(c2b->cep, C2_ADV_PREFETCH, USER, 0);
    BUG_ON(castle_cache_block_sync_read(c2b));
    read_lock_c2b(c2b);
    memcpy(dest, c2b->buffer, cvt_src->length);
    read_unlock_c2b(c2b);
    put_c2b(c2b);
}

/**
 * Converts an in-kernel CVT, to userspace type field.
 *
 * @param cvt   In-kernel CVT to convert.
 */
static uint8_t castle_back_buffer_cvt_type_to_user(c_val_tup_t cvt)
{
    /* We should never be returning these to userspace. */
    BUG_ON(CVT_NODE(cvt));

    /* Check for counters before checking for inline (which will also be true). */
    if(CVT_COUNTER_SET(cvt) || CVT_COUNTER_LOCAL_SET(cvt))
        return CASTLE_VALUE_TYPE_COUNTER;

    if(CVT_COUNTER_ADD(cvt) || CVT_COUNTER_LOCAL_ADD(cvt))
        return CASTLE_VALUE_TYPE_COUNTER_DELTA;

    if(CVT_INLINE(cvt))
        return CASTLE_VALUE_TYPE_INLINE;

    if(CVT_ON_DISK(cvt))
        return CASTLE_VALUE_TYPE_OUT_OF_LINE;

    if(CVT_TOMBSTONE(cvt))
        return CASTLE_VALUE_TYPE_TOMBSTONE;

    /* All types should have been dealt with by now. */
    BUG();
}

/**
 * Finalise a buffer.
 *
 * @param   buf_con Buffer constructor pointer
 * @param   status  Status to assign to completed buffer
 *
 * @also c_buf_status_t
 */
static void castle_back_buffer_fini(c_buf_constructor_t *buf_con, uint8_t status)
{
    buf_con->buf_hdr->status     = status;
    buf_con->buf_hdr->nr_entries = buf_con->nr_entries;
}

/**
 * Add a key and value pair to buffer.
 *
 * @return  0       KVP added successfully
 * @return -ENOSPC  Not enough space remaining in buffer
 */
static int castle_back_buffer_kvp_add(c_buf_constructor_t *buf_con,
                                      c_vl_bkey_t *key,
                                      c_val_tup_t *val,
                                      c_collection_id_t collection_id)
{
#define MIN_RESERVE_BYTES   (sizeof(c_buf_hdr_t) + sizeof(c_buf_kv_hdr_t))
    c_buf_kv_hdr_t *kv_hdr;
    int key_len, val_len;
    uint32_t total_kvp_len;
    uint8_t val_type;

    /* Only incremental backup should return counter adds. */
    BUG_ON(!(buf_con->flags & CASTLE_RING_FLAG_INC_BACKUP) && CVT_COUNTER_ADD(*val));

    key_len = key->length + 4;
    if (CVT_INLINE(*val)
            || (CVT_ON_DISK(*val) && buf_con->flags & CASTLE_RING_FLAG_ITER_GET_OOL
                && MIN_RESERVE_BYTES + key_len + val->length <= buf_con->buf_len))
        val_len = val->length;
    else
        val_len = 0;
    if (buf_con->flags & CASTLE_RING_FLAG_ITER_NO_VALUES)
        val_len = 0;

    /* Return immediately if the buffer is too small. */
    total_kvp_len = sizeof(c_buf_kv_hdr_t) + key_len + val_len;
    if (unlikely(total_kvp_len > buf_con->buf_rem))
        return -ENOSPC;

    /* Get current key value header. */
    kv_hdr = buf_con->buf + buf_con->cur_hdr_off;
    buf_con->cur_hdr_off += sizeof(c_buf_kv_hdr_t);

    /* Copy key into buffer. */
    buf_con->cur_kv_off -= key_len;
    kv_hdr->key_off      = buf_con->cur_kv_off;
    memcpy(buf_con->buf + buf_con->cur_kv_off, key, key_len);

    /* Copy value into buffer. */
    val_type = castle_back_buffer_cvt_type_to_user(*val);
    if (likely(val_len))
    {
        buf_con->cur_kv_off -= val_len;
        kv_hdr->val_off      = buf_con->cur_kv_off;
        if (CVT_INLINE(*val))
            memcpy(buf_con->buf + buf_con->cur_kv_off, CVT_INLINE_VAL_PTR(*val), val_len);
        else
        {
            castle_back_buffer_ool_memcpy(buf_con->buf + buf_con->cur_kv_off, val);
            val_type = CASTLE_VALUE_TYPE_INLINE;
        }
    }

    /* With the exception of iterators for incremental backup we will have
     * accumulated counters from all trees relevant to the requested range so
     * return a counter "set" type in the buffer.  By its nature, incremental
     * backup may have seen a subset of trees, so return the "delta" type. */
    if (!(buf_con->flags & CASTLE_RING_FLAG_INC_BACKUP)
            && val_type == CASTLE_VALUE_TYPE_COUNTER_DELTA)
        kv_hdr->val_type   = CASTLE_VALUE_TYPE_COUNTER;
    else
        kv_hdr->val_type   = val_type;
    kv_hdr->val_len        = val->length;
    kv_hdr->user_timestamp = val->user_timestamp;

    /* Update buffer constructor (kernel). */
    buf_con->buf_rem -= total_kvp_len;
    buf_con->nr_entries++;
    if (val->length && val_len)
        buf_con->buf_hdr->flags |= CASTLE_BUFFER_FLAG_HAS_OOL;

    return 0;
}

/**
 * Initialise buffer and buffer constructor.
 */
static void castle_back_buffer_init(c_buf_constructor_t *buf_con,
                                    void *buf,
                                    uint32_t buf_len,
                                    uint8_t flags)
{
    c_buf_hdr_t *buf_hdr;

    /* Caller should have verified buffer is at least PAGE_SIZE. */
    BUG_ON(buf_len < PAGE_SIZE);

    /* Initialise buffer constructor (kernel). */
    buf_con->flags       = flags;
    buf_con->nr_entries  = 0;
    buf_con->buf         = buf;
    buf_con->buf_hdr     = buf;
    buf_con->cur_hdr_off = sizeof(c_buf_hdr_t);
    buf_con->cur_kv_off  = buf_len;
    buf_con->buf_len     = buf_len;
    buf_con->buf_rem     = buf_len - buf_con->cur_hdr_off;

    /* Initialise buffer header (user). */
    buf_hdr = buf_con->buf_hdr;
    buf_hdr->flags      = 0;
    buf_hdr->nr_entries = 0;
    buf_hdr->index_off  = buf_con->cur_hdr_off;
    buf_hdr->version    = CASTLE_BUFFER_VERSION;
}

/**
 * Callback to add an iterated KVP to user-supplied buffer.
 *
 * Primary return point for iterator results.
 *
 * @return  1   Continue iterating
 * @return  0   Error or buffer full
 *
 * @also castle_back_reply()
 * @also __castle_back_iter_next()
 */
static int castle_back_iter_next_callback(struct castle_object_iterator *iterator,
                                          c_vl_bkey_t *key,
                                          c_val_tup_t *val,
                                          int err,
                                          void *data)
{
    struct castle_back_stateful_op *stateful_op;
    struct castle_back_conn *conn;
    struct castle_back_op *op;
    int ret;

    stateful_op = (struct castle_back_stateful_op *)data;
    BUG_ON(!stateful_op);
    BUG_ON(!stateful_op->in_use);
    BUG_ON(!stateful_op->curr_op);
    conn = stateful_op->conn;
    op   = stateful_op->curr_op;

    stateful_debug("op=%p "stateful_op_fmt_str" key=%p err=%d\n",
            op, stateful_op2str(stateful_op), key, err);

    if (err)
        goto err0;

    /* If we've been passed a NULL key, the iterator has completed. */
    if (key == NULL)
    {
        /* Finalise buffer, inform consumer the iterator has terminated. */
        castle_back_buffer_fini(&stateful_op->iterator.buf_con, CASTLE_BUFFER_STATUS_COMPLETE);

        /* Iterator has finished.  Fake up an iter_finish request and pass it
         * to castle_back_iter_finish() to end the iterator.
         * See also: libcastle.hg:castle_iter_finish_prepare() */
        spin_lock(&stateful_op->lock);
        BUG_ON(!stateful_op->curr_op);

        op->req.tag = CASTLE_RING_ITER_FINISH_FASTPATH;
        op->req.iter_finish.token = stateful_op->token;
        spin_unlock(&stateful_op->lock);

        /* End the iterator. */
        castle_back_buffer_put(conn, op->buf);
        _castle_back_iter_finish(op, stateful_op, 1 /*fastpath*/);

        return 0;
    }

    if ( CVT_TOMBSTONE(*val) && /* got a tombstone */
        !(stateful_op->flags & CASTLE_RING_FLAG_RET_TOMBSTONE) ) /* didn't ask for them back */
        return 1; /* skip, and tell caller to continue iterating */

    /* The iterator has returned a key.  Try and add it to the list. */
    ret = castle_back_buffer_kvp_add(&stateful_op->iterator.buf_con,
                                     key,
                                     val,
                                     stateful_op->iterator.collection_id);
    if (ret)
    {
        /* The buffer was too small to save the key-value pair.
         * Finalise buffer, inform consumer the iterator has more keys. */
        BUG_ON(ret != -ENOSPC);
        castle_back_buffer_fini(&stateful_op->iterator.buf_con, CASTLE_BUFFER_STATUS_HAS_MORE);

        /* key->length + 4 because key->length does not include overheads. */
        stateful_op->iterator.saved_key = castle_dup_or_copy(key, key->length + 4, NULL, NULL);
        if (!stateful_op->iterator.saved_key)
        {
            err = -ENOMEM;
            goto err0;
        }
        stateful_op->iterator.saved_val = *val;
        /* Copy the value since it may get removed from the cache.
           There is no need to memcpy local counters. */
        if (CVT_INLINE(*val) && !CVT_LOCAL_COUNTER(*val))
        {
            stateful_op->iterator.saved_val.val_p = castle_alloc(val->length);
            memcpy(stateful_op->iterator.saved_val.val_p,
                   CVT_INLINE_VAL_PTR(*val),
                   val->length);
        }

        stateful_debug("op=%p "stateful_op_fmt_str" key=%p err=%d cur_len=0\n",
                op, stateful_op2str(stateful_op), key, err);
        castle_back_buffer_put(conn, op->buf);
        castle_back_iter_reply(stateful_op, op, 0);

        return 0;
    }

    /* We were successful, inform caller to continue iterating. */
    return 1;

err0:
    castle_back_buffer_put(conn, op->buf);
    castle_back_iter_reply(stateful_op, op, err);

    return 0;
}

/**
 *
 * @also castle_back_iter_next_callback()
 */
static void __castle_back_iter_next(void *data)
{
    struct castle_back_stateful_op *stateful_op;
    struct castle_back_conn        *conn;
    struct castle_back_op          *op;
    castle_object_iterator_t       *iterator;

    stateful_op = (struct castle_back_stateful_op *)data;
    BUG_ON(!stateful_op);
    BUG_ON(!stateful_op->in_use);
    BUG_ON(!stateful_op->curr_op);
    conn     = stateful_op->conn;
    op       = stateful_op->curr_op;
    iterator = stateful_op->iterator.iterator;

    stateful_debug("op=%p "stateful_op_fmt_str" iterator=%p iterator.saved_key=%p\n",
            op, stateful_op2str(stateful_op), iterator, stateful_op->iterator.saved_key);

    /* Initialise buffer and buffer constructor. */
    castle_back_buffer_init(&stateful_op->iterator.buf_con,
                            op->buf->buffer,
                            op->req.iter_next.buffer_len,
                            stateful_op->flags);

#ifdef DEBUG
    stateful_debug("iter_next start_key\n");
    vl_bkey_print(LOG_DEBUG, iterator->start_key);

    stateful_debug("iter_next end_key\n");
    vl_bkey_print(LOG_DEBUG, iterator->end_key);
#endif

    /* if we have a saved key and value from the last call, add them to the buffer */
    if (stateful_op->iterator.saved_key != NULL)
    {
        int ret;

        ret = castle_back_buffer_kvp_add(&stateful_op->iterator.buf_con,
                                         stateful_op->iterator.saved_key,
                                        &stateful_op->iterator.saved_val,
                                         stateful_op->iterator.collection_id);

        if (unlikely(ret))
        {
            BUG_ON(ret != -ENOSPC);
            error("iterator buffer too small\n");
            castle_back_buffer_fini(&stateful_op->iterator.buf_con, CASTLE_BUFFER_STATUS_HAS_MORE);
            castle_back_buffer_put(conn, op->buf);
            castle_back_iter_reply(stateful_op, op, -EINVAL);

            return;
        }

        castle_free(stateful_op->iterator.saved_key);
        /* we copied it so free it */
        CVT_INLINE_FREE(stateful_op->iterator.saved_val);

        stateful_op->iterator.saved_key = NULL;
    }

    castle_object_iter_next(iterator, castle_back_iter_next_callback, stateful_op);
}

/**
 * Continue iterator and return (up to) another buffer's-worth of results.
 *
 * Called from _castle_back_iter_start() and castle_back_iter_next().
 *
 * @param   op          Op to queue
 * @param   stateful_op Stateful op to queue on
 * @param   fastpath    Whether this a fastpath iter_next
 *
 * @also castle_back_iter_start()
 * @also castle_back_iter_next()
 */
static void _castle_back_iter_next(struct castle_back_op *op,
                                   struct castle_back_stateful_op *stateful_op,
                                   int fastpath)
{
    int err;

    if (op->req.iter_next.buffer_len < PAGE_SIZE)
    {
        error("castle_back_iter_next buffer_len smaller than a page\n");
        err = -ENOBUFS;
        goto err;
    }

    /* Get buffer with value in and save it. */
    op->buf = castle_back_buffer_get(op->conn,
                                     (unsigned long)op->req.iter_next.buffer_ptr,
                                     op->req.iter_next.buffer_len);
    if (op->buf == NULL)
    {
        error("Couldn't get buffer for pointer=%p length=%u\n",
                op->req.iter_next.buffer_ptr, op->req.iter_next.buffer_len);
        err = -EINVAL;
        goto err;
    }

    spin_lock(&stateful_op->lock);

    if (fastpath)
    {
        BUG_ON(!stateful_op->curr_op);
        stateful_op->curr_op = NULL;
    }

    /* Put this op on the queue for the iterator */
    stateful_debug("op=%p "stateful_op_fmt_str" fastpath=%d\n",
            op, stateful_op2str(stateful_op), fastpath);
    err = castle_back_stateful_op_queue_op(stateful_op, op->req.iter_next.token, op);
    if (err)
    {
        spin_unlock(&stateful_op->lock);
        /* NOTE: No need to castle_back_buffer_put() as this has been done as
         * part of stateful_op expiry in castle_back_stateful_op_finish_all() */
        goto err;
    }

    castle_back_iter_call_queued(stateful_op); // drops stateful_op->lock

    return;

err:
    /* Call the stateful reply if we're fastpath.  If we're not fastpath then
     * we do not have a stateful_op so reply directly. */
    if (fastpath)
        castle_back_iter_reply(stateful_op, op, err);
    else
        castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

/**
 * Find stateful op then continue iterating.
 *
 * Called from castle_back_request_process()
 *
 * @also castle_back_request_process()
 * @also _castle_back_iter_next()
 */
static void castle_back_iter_next(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_stateful_op *stateful_op;

    stateful_op = castle_back_find_stateful_op(op->conn,
                                               op->req.iter_next.token,
                                               CASTLE_RING_ITER_START);
    if (!stateful_op)
    {
        castle_printk(LOG_INFO, "%s Token not found 0x%x\n",
                __FUNCTION__, op->req.iter_next.token);
        castle_back_reply(op, -EBADFD, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);

        return;
    }

    _castle_back_iter_next(op, stateful_op, 0 /*fastpath*/);
}

/**
 * Tear down iterator state.
 *
 * @also castle_back_iter_expire()
 * @also __castle_back_iter_finish()
 */
static void castle_back_iter_cleanup(struct castle_back_stateful_op *stateful_op)
{
    struct castle_attachment *attachment;

    BUG_ON(!spin_is_locked(&stateful_op->lock));
    BUG_ON(stateful_op->tag != CASTLE_RING_ITER_START);
    BUG_ON(!list_empty(&stateful_op->op_queue));
    BUG_ON(stateful_op->curr_op != NULL);

    if (stateful_op->iterator.saved_key != NULL)
    {
        castle_free(stateful_op->iterator.saved_key);
        CVT_INLINE_FREE(stateful_op->iterator.saved_val);
    }

    if (!stateful_op->iterator.get_all)
    {
        castle_free(stateful_op->iterator.start_key);
        castle_free(stateful_op->iterator.end_key);
    }
    attachment = stateful_op->attachment;
    stateful_op->attachment = NULL;

    castle_back_put_stateful_op(stateful_op->conn, stateful_op); /* drops stateful_op->lock */

    castle_attachment_put(attachment);
}

static void __castle_back_iter_finish(void *data)
{
    struct castle_back_stateful_op *stateful_op = data;
    struct castle_back_op *op = stateful_op->curr_op;
    int err;

    /* Verify this iter hasn't been finished twice. */
    BUG_ON(stateful_op->cancelled);
    stateful_op->cancelled++;

    /* CASTLE_RING_ITER_FINISH_FASTPATH: Finished after returning all keys.
     *
     * CASTLE_RING_ITER_FINISH: Explicit call for finish from userspace, which means iterator
     * hasn't yet returned all the keys. If it did, iterator should already be closed by
     * CASTLE_RING_ITER_FINISH_FASTPATH before. */
    err = castle_object_iter_finish(stateful_op->iterator.iterator,
                          (op->req.tag == CASTLE_RING_ITER_FINISH_FASTPATH)? 0: -ECONNABORTED);

    stateful_debug(stateful_op_fmt_str" err=%d\n",
            stateful_op2str(stateful_op), err);

    castle_back_reply(stateful_op->curr_op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);

    spin_lock(&stateful_op->attachment->sop_lock);
    list_del_init(&stateful_op->attachment_list);
    spin_unlock(&stateful_op->attachment->sop_lock);

    spin_lock(&stateful_op->lock);
    stateful_op->curr_op = NULL;

    castle_back_stateful_op_finish_all(stateful_op, -EINVAL);

    /* Update stats. */
    if (!err)
    {
        struct castle_attachment *attachment = stateful_op->attachment;

        atomic64_inc(&attachment->rq.ios);
        atomic64_add(stateful_op->iterator.nr_bytes, &attachment->rq.bytes);
        atomic64_add(stateful_op->iterator.nr_keys, &attachment->rq_nr_keys);
    }

    castle_back_iter_cleanup(stateful_op); /* drops stateful_op->lock */
}

/**
 * Prepare to terminate the iterator.
 *
 * Called from castle_back_iter_finish() and castle_back_iter_next_callback().
 *
 * @param   op          Op to queue
 * @param   stateful_op Stateful op to queue on
 * @param   fastpath    Whether this is a fastpath iter_finish
 *
 * fastpath is introduced for #3731 to prevent a race between fastpath iterators
 * and castle_back_release().  If we are called from fastpath we expect that
 * stateful_op->curr_op is set and we must unset it before queuing up this
 * iter finish request.
 *
 * @also castle_back_iter_finish()
 * @also castle_back_iter_next_callback()
 */
static void _castle_back_iter_finish(struct castle_back_op *op,
                                     struct castle_back_stateful_op *stateful_op,
                                     int fastpath)
{
    int err;

    /*
     * Put this op on the queue for the iterator
     */
    spin_lock(&stateful_op->lock);

    if (fastpath)
    {
        BUG_ON(!stateful_op->curr_op);
        stateful_op->curr_op = NULL;
    }

    err = castle_back_stateful_op_queue_op(stateful_op, op->req.iter_finish.token, op);
    if (err)
    {
        spin_unlock(&stateful_op->lock);
        /* NOTE: No need to castle_back_buffer_put() as this has been done as
         * part of stateful_op expiry in castle_back_stateful_op_finish_all() */
        goto err;
    }

    castle_back_iter_call_queued(stateful_op); // drops stateful_op->lock

    return;

err:
    /* Call the stateful reply if we're fastpath.  If we're not fastpath then
     * we do not have a stateful_op so reply directly. */
    if (fastpath)
        castle_back_iter_reply(stateful_op, op, err);
    else
        castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

/**
 * Find stateful op then terminate iterator.
 *
 * Called from castle_back_request_process()
 *
 * @also castle_back_request_process()
 * @also _castle_back_iter_finish()
 */
static void castle_back_iter_finish(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_stateful_op *stateful_op;

    stateful_op = castle_back_find_stateful_op(op->conn,
                                               op->req.iter_finish.token,
                                               CASTLE_RING_ITER_START);
    if (!stateful_op)
    {
        stateful_debug("op=%p stateful_op=%p token=0x%x not found\n",
                op, stateful_op, op->req.iter_finish.token);
        castle_back_reply(op, -EBADFD, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);

        return;
    }

    stateful_debug("op=%p "stateful_op_fmt_str"\n", op, stateful_op2str(stateful_op));

    _castle_back_iter_finish(op, stateful_op, 0 /*fastpath*/);
}

/**** BIG PUT ****/

static void castle_back_big_put_expire(struct castle_back_stateful_op *stateful_op)
{
    struct castle_attachment *attachment;

    debug("castle_back_big_put_expire token=%u.\n", stateful_op->token);

    BUG_ON(!stateful_op->expiring);
    BUG_ON(!list_empty(&stateful_op->op_queue));
    BUG_ON(stateful_op->curr_op != NULL);

    castle_object_replace_cancel(&stateful_op->replace);
    castle_free(stateful_op->replace.key);

    spin_lock(&stateful_op->attachment->sop_lock);
    list_del_init(&stateful_op->attachment_list);
    spin_unlock(&stateful_op->attachment->sop_lock);

    spin_lock(&stateful_op->lock);
    attachment = stateful_op->attachment;
    stateful_op->attachment = NULL;

    castle_back_put_stateful_op(stateful_op->conn, stateful_op); /* drops stateful_op->lock */


    castle_attachment_put(attachment);
}

static void castle_back_stream_in_expire(struct castle_back_stateful_op *stateful_op)
{
    struct castle_attachment *attachment;

    debug("%s::token=%u.\n", __FUNCTION__, stateful_op->token);

    BUG_ON(!stateful_op->expiring);
    BUG_ON(!list_empty(&stateful_op->op_queue));
    stateful_op->curr_op = NULL;

    castle_da_in_stream_complete(stateful_op->stream_in.da_stream, 1 /*abort*/);

    spin_lock(&stateful_op->attachment->sop_lock);
    list_del_init(&stateful_op->attachment_list);
    spin_unlock(&stateful_op->attachment->sop_lock);

    spin_lock(&stateful_op->lock);
    attachment = stateful_op->attachment;
    stateful_op->attachment = NULL;

    castle_back_put_stateful_op(stateful_op->conn, stateful_op); /* drops stateful_op->lock */

    castle_attachment_put(attachment);
}


static void castle_back_put_chunk_continue(void *data);

static void castle_back_stateful_call_queued(struct castle_back_stateful_op *stateful_op)
{
    BUG_ON(!spin_is_locked(&stateful_op->lock));

    /* Take an op from queue and schedule work for it. */
    if (castle_back_stateful_op_prod(stateful_op))
        BUG_ON(!queue_work_on(stateful_op->cpu, castle_back_wq, &stateful_op->work[0]));
}

static void castle_back_big_put_continue(struct castle_object_replace *replace)
{
    struct castle_back_stateful_op *stateful_op =
        container_of(replace, struct castle_back_stateful_op, replace);

    spin_lock(&stateful_op->lock);

    if (!stateful_op->in_use)
        stateful_op->in_use = 1;

    if (stateful_op->curr_op != NULL)
    {
        struct castle_back_op *op = stateful_op->curr_op;

        /* Just completed data transfer for PUT_CHUNK, release the buffer. */
        if (op->req.tag == CASTLE_RING_PUT_CHUNK && op->buf)
            /* This may free the buffer with stateful_op->lock held; this is
             * safe (but not ideal) since the free functions don't sleep. */
            castle_back_buffer_put(stateful_op->conn, op->buf);

        /* Respond back to client. */
        castle_back_reply(op, 0, stateful_op->token, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
        stateful_op->curr_op = NULL;
    }

    /* Check if stateful_op is expired, if so no need to handle anymore ops. Return back. */
    /* drops the lock if return non-zero */
    if (castle_back_stateful_op_completed_op(stateful_op))
        return;

    /* Look for more queued ops (put_chunks in this case). */
    castle_back_stateful_call_queued(stateful_op);

    spin_unlock(&stateful_op->lock);

    /* To prevent #3144. */
    might_resched();
}

static void castle_back_big_put_complete(struct castle_object_replace *replace, int err)
{
    struct castle_back_stateful_op *stateful_op =
        container_of(replace, struct castle_back_stateful_op, replace);
    struct castle_attachment *attachment;
    c_vl_bkey_t *key = replace->key;

    debug("castle_back_big_put_complete err=%d\n", err);

    spin_lock(&stateful_op->attachment->sop_lock);
    list_del_init(&stateful_op->attachment_list);
    spin_unlock(&stateful_op->attachment->sop_lock);

    spin_lock(&stateful_op->lock);

    if (stateful_op->curr_op != NULL)
    {
        struct castle_back_op *op = stateful_op->curr_op;
        if (op->req.tag == CASTLE_RING_PUT_CHUNK && op->buf)
            castle_back_buffer_put(stateful_op->conn, op->buf);

        /* If we receive -EEXIST here, it must be because we tried to insert an object into a T0 that
           already contains an object with the same key but newer timestamp (see c_o_replace_cvt_get);
           the entry is dropped, and we choose to be quiet about it by pretending there was no error. */
        if(err==-EEXIST)
            err=0;

        castle_back_reply(op, err, stateful_op->token, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
        stateful_op->curr_op = NULL;
    }

    /* Responds back to all outstanding ops with error. */
    castle_back_stateful_op_finish_all(stateful_op, err);
    attachment = stateful_op->attachment;
    stateful_op->attachment = NULL;

    /* Update stats. */
    if (!err)
    {
        atomic64_inc(&attachment->big_put.ios);
        atomic64_add(stateful_op->replace.value_len, &attachment->big_put.bytes);
    }

    /* Will drop stateful_op->lock. */
    castle_back_put_stateful_op(stateful_op->conn, stateful_op);
    castle_free(key);

    castle_attachment_put(attachment);
}

static uint32_t castle_back_big_put_data_length_get(struct castle_object_replace *replace)
{
    struct castle_back_stateful_op *stateful_op =
        container_of(replace, struct castle_back_stateful_op, replace);
    uint32_t length = 0;

    spin_lock(&stateful_op->lock);
    /* curr_op could be the BIG_PUT */
    if (stateful_op->curr_op != NULL && stateful_op->curr_op->req.tag == CASTLE_RING_PUT_CHUNK)
        length = stateful_op->curr_op->req.put_chunk.buffer_len;
    spin_unlock(&stateful_op->lock);

    BUG_ON(stateful_op->curr_op != NULL && (length == 0 && stateful_op->curr_op->req.tag == CASTLE_RING_PUT_CHUNK));

    return length;
}

static void castle_back_big_put_data_copy(struct castle_object_replace *replace,
                                          void *buffer, uint32_t buffer_length, int not_last)
{
    struct castle_back_stateful_op *stateful_op =
        container_of(replace, struct castle_back_stateful_op, replace);
    struct castle_back_op *op;

    spin_lock(&stateful_op->lock);

    op = stateful_op->curr_op;

    BUG_ON(op == NULL);

    debug("castle_back_big_put_data_copy buffer=%p, buffer_length=%u,"
        "not_last=%d, value_len=%u\n, buffer_offset=%u\n",
        buffer, buffer_length, not_last, op->req.put_chunk.buffer_len, op->buffer_offset);

    BUG_ON(op->req.tag != CASTLE_RING_PUT_CHUNK);
    BUG_ON(op->buffer_offset + buffer_length > op->req.put_chunk.buffer_len);

    /* @TODO: actual zero copy! */

    memcpy(buffer, castle_back_user_to_kernel(op->buf, op->req.put_chunk.buffer_ptr)
        + op->buffer_offset, buffer_length);

    op->buffer_offset += buffer_length;

    spin_unlock(&stateful_op->lock);
}

/**
 * Call flow:
 *
 *  castle_back functions                            castle_objects functions
 *
 *  castle_back_big_put()
 *                                      ->          castle_object_replace()
 *  data_copy()                         <-
 *  replace_continue()                  <-
 *
 *
 *  castle_back_put_chunk()
 *      - queue the op
 *      - castle_back_stateful_call_queued()
 *      - Schedule castle_back_put_chunk_continue()
 *                                      ->          castle_object_replace_continue
 *        data_copy()                   <-
 *        replace_continue()            <-
 *          - castle_back_stateful_call_queued()
 */

/**
 * Begin stateful op put of big value at specified key,version in DA.
 *
 * @also castle_object_replace()
 * @also castle_back_put_chunk()
 */
static void castle_back_big_put(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_conn *conn = op->conn;
    int err;
    struct castle_attachment *attachment;
    struct castle_back_stateful_op *stateful_op;

    debug("castle_back_big_put\n");

    /* @TODO: 0 indicates we don't know the length, but not supported yet */
    if (op->req.big_put.value_len <= MAX_INLINE_VAL_SIZE)
    {
        err = -EINVAL;
        goto err0;
    }

    /* Get a new stateful op to handle big_put. */
    castle_back_stateful_op_get(conn,
                                &stateful_op,
                                op->cpu,
                                op->cpu_index,
                                castle_back_big_put_expire);

    /* Couldn't find a free stateful op. */
    if (!stateful_op)
    {
        error("castle_back: no more free stateful ops!\n");
        err = -EAGAIN;
        goto err0;
    }

    /* Get reference on attachment - Consequently on DA. */
    attachment = castle_attachment_get(op->req.big_put.collection_id, WRITE);
    if (attachment == NULL)
    {
        error("Collection not found id=0x%x\n", op->req.big_put.collection_id);
        err = -ENOTCONN;
        goto err1;
    }

#ifdef DEBUG
    debug("key: \n");
    vl_bkey_print(LOG_DEBUG, op->key);
#endif
    /* Initialize stateful op. */
    stateful_op->tag = CASTLE_RING_BIG_PUT;
    stateful_op->queued_size = 0;
    /* big_put is the first op, followed by series of put_chunks. */
    stateful_op->curr_op = op;
    castle_back_stateful_op_attach(stateful_op, attachment);

    /* Length of the complete value. */
    stateful_op->replace.value_len = op->req.big_put.value_len;

    /* Call on completion of an operation in castle_object and to continue with next
     * operation. Gets called before copying data. */
    stateful_op->replace.replace_continue = castle_back_big_put_continue;

    /* Call on completion of big_put, including data copy. */
    stateful_op->replace.complete = castle_back_big_put_complete;

    /* Length of current put_chunk. */
    stateful_op->replace.data_length_get = castle_back_big_put_data_length_get;

    /* Copy data from interface buffers into given cache buffers(C2B). */
    stateful_op->replace.data_copy = castle_back_big_put_data_copy;

    /* Store key and free it after completion of operation. */
    stateful_op->replace.key = op->key;

    /* We would never big_put counters. */
    stateful_op->replace.counter_type = CASTLE_OBJECT_NOT_COUNTER;

    stateful_op->replace.has_user_timestamp = 0;

    /* Work structure to run every queued op. Every put_chunk gets queued. */
    INIT_WORK(&stateful_op->work[0], castle_back_put_chunk_continue, stateful_op);

    /* Call castle_object layer to insert (k,v) pair. */
    err = castle_object_replace(&stateful_op->replace,
                                attachment,
                                op->cpu_index,
                                0 /*tombstone*/);
    if (err)
        goto err2;

    /* To prevent #3144. */
    might_resched();

    return;

err2: spin_lock(&stateful_op->attachment->sop_lock);
      list_del_init(&stateful_op->attachment_list);
      spin_unlock(&stateful_op->attachment->sop_lock);
      castle_attachment_put(attachment);
      stateful_op->attachment = NULL;
err1: /* Safe as no-one could have queued up an op - we have not returned token */
      spin_lock(&stateful_op->lock);
      stateful_op->curr_op = NULL;
      /* will drop stateful_op->lock */
      castle_back_put_stateful_op(conn, stateful_op);
err0: castle_free(op->key);
      castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
      /* To prevent #3144. */
      might_resched();
}

static c_chk_cnt_t castle_back_stream_in_tree_ext_size_wc_estimate(struct castle_back_stateful_op *stateful_op)
{
    struct castle_btree_type *btree;
    int nodes;
    c_chk_cnt_t tree_ext_size;
    int wc_entries_per_node;

    btree = castle_double_array_btree_type_get(stateful_op->attachment);
    wc_entries_per_node = btree->max_entries(HDD_RO_TREE_LEAF_NODE_SIZE);
    nodes = DIV_ROUND_UP(stateful_op->stream_in.expected_entries, wc_entries_per_node);
    tree_ext_size = DIV_ROUND_UP(nodes*HDD_RO_TREE_LEAF_NODE_SIZE*PAGE_SIZE, C_CHK_SIZE);

    return tree_ext_size;
    //TODO@tr: fix this! atm we are basically ignoring it!
    //TODO@tr: from LT: I changed this to HDD_RO_TREE_LEAF_NODE_SIZE, you may need to adjust
}

static c_chk_cnt_t castle_back_stream_in_internal_ext_size_wc_estimate(struct castle_back_stateful_op *stateful_op,
                                                                  c_chk_cnt_t tree_ext_size)
{
    struct castle_btree_type *btree;
    c_chk_cnt_t internal_ext_size;

    btree = castle_double_array_btree_type_get(stateful_op->attachment);
    /* The following "inspired" by castle_da_merge_output_size, a true story */
    internal_ext_size = tree_ext_size;
    internal_ext_size /= (HDD_RO_TREE_INTERNAL_NODE_SIZE * C_BLK_SIZE);
    internal_ext_size /= btree->max_entries(SSD_RO_TREE_INTERNAL_NODE_SIZE);
    internal_ext_size ++;
    internal_ext_size *= (SSD_RO_TREE_INTERNAL_NODE_SIZE * C_BLK_SIZE);
    internal_ext_size  = MASK_CHK_OFFSET(internal_ext_size + C_CHK_SIZE);
    internal_ext_size *= 2;
    return internal_ext_size;
}

static void castle_back_stream_in_continue(void *data);
static void castle_back_stream_in_start(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_conn *conn = op->conn;
    struct castle_back_stateful_op *stateful_op;
    c_chk_cnt_t tree_ext_size;
    c_chk_cnt_t internal_ext_size;
    struct castle_attachment *attachment;
    struct castle_immut_tree_construct *constr;
    int err = 0;

    if(!op->req.stream_in_start.entries_count)
    {
        error("castle_back: rejecting request for stream_in of 0 objects.\n");
        err = -ECANCELED;
        goto err0;
    }

    /* Get a new stateful op to handle stream_in. */
    castle_back_stateful_op_get(conn,
                                &stateful_op,
                                op->cpu,
                                op->cpu_index,
                                castle_back_stream_in_expire);

    /* Couldn't find a free stateful op. */
    if (!stateful_op)
    {
        error("castle_back: no more free stateful ops!\n");
        err = -EAGAIN;
        goto err0;
    }

    /* Get reference on attachment - Consequently on DA. */
    attachment = castle_attachment_get(op->req.stream_in_start.collection_id, WRITE);
    if (attachment == NULL)
    {
        error("Collection not found id=0x%x\n", op->req.stream_in_start.collection_id);
        err = -ENOTCONN;
        goto err1;
    }

    /* Initialize stateful op. */
    stateful_op->tag = CASTLE_RING_STREAM_IN_START;
    stateful_op->flags = op->req.flags;
    stateful_op->queued_size = 0;
    castle_back_stateful_op_attach(stateful_op, attachment);

    stateful_op->stream_in.collection_id
                                    = op->req.stream_in_start.collection_id;
    stateful_op->stream_in.expected_entries
                                    = op->req.stream_in_start.entries_count;
    stateful_op->stream_in.expected_dataext_chunks
                                    = op->req.stream_in_start.medium_object_chunks;
    stateful_op->stream_in.received_entries
                                    = 0;
    stateful_op->stream_in.received_mobj_off
                                    = 0;

    tree_ext_size     = castle_back_stream_in_tree_ext_size_wc_estimate(stateful_op);
    internal_ext_size = castle_back_stream_in_internal_ext_size_wc_estimate(stateful_op, tree_ext_size);
    internal_ext_size = tree_ext_size; /* TODO@tr get rid of this! */

    castle_printk(LOG_DEBUG, "%s:: stream_in op on cpu %u, expected_entries: %lld, "
                "expected_dataext_chunks: %ld, stateful_op:%p\n",
                __FUNCTION__,
                op->cpu,
                stateful_op->stream_in.expected_entries,
                stateful_op->stream_in.expected_dataext_chunks,
                stateful_op);

    /* FIXME: Do we need transaction lock here? */
    CASTLE_TRANSACTION_BEGIN;
    constr = castle_da_in_stream_start(stateful_op->attachment->col.da,
                                       stateful_op->stream_in.expected_entries,
                                       internal_ext_size,
                                       tree_ext_size,
                                       stateful_op->stream_in.expected_dataext_chunks);
    CASTLE_TRANSACTION_END;

    if (!constr)
    {
        castle_printk(LOG_ERROR, "%s::castle_da_in_stream_start failed for "
                "collection id 0x%x, expected entries %lld, expected MO chunks %lld\n",
                __FUNCTION__,
                stateful_op->stream_in.collection_id,
                stateful_op->stream_in.expected_entries,
                stateful_op->stream_in.expected_dataext_chunks);
        err = -ENOSPC;
        goto err2;
    }
    /* Work structure to run every queued op. Every stream_in_next gets queued. */
    INIT_WORK(&stateful_op->work[0], castle_back_stream_in_continue, stateful_op);

    spin_lock(&stateful_op->lock);
    stateful_op->stream_in.da_stream = constr;
    castle_back_stateful_op_enable_expire(stateful_op);
    spin_unlock(&stateful_op->lock);

    /* stream_in_start is the first op, but we already finished it now. */
    stateful_op->curr_op = NULL;
    stateful_op->in_use = 1;
    castle_back_reply(op, 0, stateful_op->token, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
    /* To prevent #3144. */
    might_resched();
    return;

err2:
    spin_lock(&stateful_op->attachment->sop_lock);
    list_del_init(&stateful_op->attachment_list);
    spin_unlock(&stateful_op->attachment->sop_lock);
    castle_attachment_put(attachment);
    stateful_op->attachment = NULL;
err1:
    /* Safe as no-one could have queued up an op - we have not returned token */
    spin_lock(&stateful_op->lock);
    stateful_op->curr_op = NULL;
    /* will drop stateful_op->lock */
    castle_back_put_stateful_op(conn, stateful_op);
err0:
    castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
    /* To prevent #3144. */
    might_resched();
}

static void castle_back_stream_in_next(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_conn *conn = op->conn;
    struct castle_back_stateful_op *stateful_op;
    int err;

    castle_printk(LOG_DEBUG, "%s::start, with token %llu on conn op->conn %p\n",
            __FUNCTION__, op->req.stream_in_finish.token, op->conn);
    stateful_op = castle_back_find_stateful_op(op->conn,
                                               op->req.stream_in_next.token,
                                               CASTLE_RING_STREAM_IN_START);

    if (!stateful_op)
    {
        stateful_debug("op=%p stateful_op=%p token=0x%x not found\n",
                op, stateful_op, op->req.stream_in_next.token);
        err = -EBADFD;
        castle_printk(LOG_ERROR, "%s:: failed to get stateful op, err:%d\n", __FUNCTION__, err);
        goto err0;
    }

    /* Get buffer with batch in it and save it. */
    op->buf = castle_back_buffer_get(conn,
                                     (unsigned long) op->req.stream_in_next.buffer_ptr,
                                     op->req.stream_in_next.buffer_len);
    if (op->buf == NULL)
    {
        error("Couldn't get buffer for pointer=%p length=%u\n",
                op->req.stream_in_next.buffer_ptr, op->req.stream_in_next.buffer_len);
        err = -EINVAL;
        goto err0;
    }
    op->buffer_offset = 0;

    /*
     * Put this op on the queue for the stream_in
     */
    spin_lock(&stateful_op->lock);
    err = castle_back_stateful_op_queue_op(stateful_op, op->req.stream_in_next.token, op);
    if (err)
    {
        castle_printk(LOG_ERROR, "%s:: failed to queue, err:%u\n", __FUNCTION__, err);
        spin_unlock(&stateful_op->lock);
        goto err0;
    }

    /* Go through Q and handle ops. */
    castle_back_stateful_call_queued(stateful_op);

    spin_unlock(&stateful_op->lock);

    /* To prevent #3144. */
    might_resched();

    return;

err0:
    castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
    /* To prevent #3144. */
    might_resched();
}

static void castle_back_stream_in_finish(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_stateful_op *stateful_op;
    int err;

    stateful_op = castle_back_find_stateful_op(op->conn,
                                               op->req.stream_in_finish.token,
                                               CASTLE_RING_STREAM_IN_START);

    if (!stateful_op)
    {
        stateful_debug("op=%p stateful_op=%p token=0x%x not found\n",
                op, stateful_op, op->req.stream_in_finish.token);
        err = -EBADFD;
        castle_printk(LOG_ERROR, "%s:: failed to get stateful op, err:%d\n", __FUNCTION__, err);
        goto err0;
    }
    castle_printk(LOG_INFO, "%s::stateful op %llx\n", __FUNCTION__, stateful_op->token);
    /*
     * Put this op on the queue for the stream_in
     */
    spin_lock(&stateful_op->lock);
    err = castle_back_stateful_op_queue_op(stateful_op, op->req.stream_in_finish.token, op);
    if (err)
    {
        castle_printk(LOG_ERROR, "%s:: failed to queue, err:%u\n", __FUNCTION__, err);
        spin_unlock(&stateful_op->lock);
        goto err0;
    }

    /* Go through Q and handle ops. */
    castle_back_stateful_call_queued(stateful_op);
    stateful_op->in_use = 0;

    castle_back_stateful_op_disable_expire(stateful_op);
    spin_unlock(&stateful_op->lock);

    /* To prevent #3144. */
    might_resched();

    return;

err0:
    castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
    /* To prevent #3144. */
    might_resched();
}

/**
 * Begin stateful op put of big value at specified key,version in DA, with a timestamp.
 *
 * @also castle_object_replace()
 * @also castle_back_put_chunk()
 */
static void castle_back_timestamped_big_put(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_conn *conn = op->conn;
    int err;
    struct castle_attachment *attachment;
    struct castle_back_stateful_op *stateful_op;

    debug("castle_back_timestamped_big_put\n");

    /* @TODO: 0 indicates we don't know the length, but not supported yet */
    if (op->req.timestamped_big_put.value_len <= MAX_INLINE_VAL_SIZE)
    {
        err = -EINVAL;
        goto err0;
    }

    /* Get a new stateful op to handle timestamped_big_put. */
    castle_back_stateful_op_get(conn,
                                &stateful_op,
                                op->cpu,
                                op->cpu_index,
                                castle_back_big_put_expire);

    /* Couldn't find a free stateful op. */
    if (!stateful_op)
    {
        error("castle_back: no more free stateful ops!\n");
        err = -EAGAIN;
        goto err0;
    }

    /* Get reference on attachment - Consequently on DA. */
    attachment = castle_attachment_get(op->req.timestamped_big_put.collection_id, WRITE);
    if (attachment == NULL)
    {
        error("Collection not found id=0x%x\n", op->req.timestamped_big_put.collection_id);
        err = -ENOTCONN;
        goto err1;
    }

#ifdef DEBUG
    debug("key: \n");
    vl_bkey_print(LOG_DEBUG, op->key);
#endif

    /* Initialize stateful op. */
    stateful_op->tag = CASTLE_RING_BIG_PUT;
    stateful_op->queued_size = 0;
    /* big_put is the first op, followed by series of put_chunks. */
    stateful_op->curr_op = op;
    castle_back_stateful_op_attach(stateful_op, attachment);

    /* Length of the complete value. */
    stateful_op->replace.value_len = op->req.timestamped_big_put.value_len;

    /* Call on completion of an operation in castle_object and to continue with next
     * operation. Gets called before copying data. */
    stateful_op->replace.replace_continue = castle_back_big_put_continue;

    /* Call on completion of big_put, including data copy. */
    stateful_op->replace.complete = castle_back_big_put_complete;

    /* Length of current put_chunk. */
    stateful_op->replace.data_length_get = castle_back_big_put_data_length_get;

    /* Copy data from interface buffers into given cache buffers(C2B). */
    stateful_op->replace.data_copy = castle_back_big_put_data_copy;

    /* Store key and free it after completion of operation. */
    stateful_op->replace.key = op->key;

    /* We would never big_put counters. */
    stateful_op->replace.counter_type = CASTLE_OBJECT_NOT_COUNTER;

    stateful_op->replace.has_user_timestamp = 1;
    stateful_op->replace.user_timestamp     =
            op->req.timestamped_big_put.user_timestamp;

    /* Work structure to run every queued op. Every put_chunk gets queued. */
    INIT_WORK(&stateful_op->work[0], castle_back_put_chunk_continue, stateful_op);

    /* Call castle_object layer to insert (k,v) pair. */
    err = castle_object_replace(&stateful_op->replace,
                                attachment,
                                op->cpu_index,
                                0 /*tombstone*/);
    if (err)
        goto err2;

    return;

err2: spin_lock(&stateful_op->attachment->sop_lock);
      list_del_init(&stateful_op->attachment_list);
      spin_unlock(&stateful_op->attachment->sop_lock);
      castle_attachment_put(attachment);
      stateful_op->attachment = NULL;
err1: /* Safe as no-one could have queued up an op - we have not returned token */
      spin_lock(&stateful_op->lock);
      stateful_op->curr_op = NULL;
      /* will drop stateful_op->lock */
      castle_back_put_stateful_op(conn, stateful_op);
err0: castle_free(op->key);
      castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

static void castle_back_put_chunk(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_conn *conn = op->conn;
    struct castle_back_stateful_op *stateful_op;
    int err;

    stateful_op = castle_back_find_stateful_op(conn,
            op->req.put_chunk.token, CASTLE_RING_BIG_PUT);

    if (!stateful_op)
    {
        castle_printk(LOG_INFO, "%s Token not found 0x%x\n",
                __FUNCTION__, op->req.put_chunk.token);
        err = -EBADFD;
        goto err0;
    }

    /* Get buffer with value in it and save it. */
    op->buf = castle_back_buffer_get(conn,
                                     (unsigned long) op->req.put_chunk.buffer_ptr,
                                     op->req.put_chunk.buffer_len);
    if (op->buf == NULL)
    {
        error("Couldn't get buffer for pointer=%p length=%u\n",
                op->req.put_chunk.buffer_ptr, op->req.put_chunk.buffer_len);
        err = -EINVAL;
        goto err0;
    }
    op->buffer_offset = 0;

    /*
     * Put this op on the queue for the big put
     */
    spin_lock(&stateful_op->lock);

    /* Check, if we are trying to put more than value size. */
    if (op->req.put_chunk.buffer_len + stateful_op->queued_size > stateful_op->replace.value_len ||
            op->req.put_chunk.buffer_len == 0)
    {
        spin_unlock(&stateful_op->lock);
        error("Invalid buffer length %u (ptr=%p)\n", op->req.put_chunk.buffer_len, op->req.put_chunk.buffer_ptr);
        err = -EINVAL;
        goto err1;
    }

    stateful_op->queued_size += op->req.put_chunk.buffer_len;

    /* Add put_chunk into queue. */
    err = castle_back_stateful_op_queue_op(stateful_op, op->req.put_chunk.token, op);
    if (err)
    {
        spin_unlock(&stateful_op->lock);
        goto err1;
    }

    /* Go through Q and handle ops. */
    castle_back_stateful_call_queued(stateful_op);

    spin_unlock(&stateful_op->lock);

    /* To prevent #3144. */
    might_resched();

    return;

err1: castle_back_buffer_put(conn, op->buf);
err0: castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
    /* To prevent #3144. */
    might_resched();
}

static void castle_back_put_chunk_continue(void *data)
{
    struct castle_back_stateful_op *stateful_op = data;

    castle_object_replace_continue(&stateful_op->replace);

    /* To prevent #3144. */
    might_resched();
}

/**
 * Perform stream in on stateful_op->curr_op->buf.
 */
static int castle_back_stream_in_buf_process(struct castle_back_stateful_op *stateful_op)
{
    struct castle_back_op *op = stateful_op->curr_op;
    struct castle_immut_tree_construct *da_stream;
    struct castle_attachment *attachment;
    struct castle_btree_type *btree;
    c_buf_user_kv_hdr_t kv_hdr;
    c_buf_consumer_t buf_con;
    int err, nr_keys, kvp;
    struct timeval now;
    c_val_tup_t cvt;
    void *key;

    /* Basic init and checks. */
    BUG_ON(!stateful_op->attachment);
    da_stream  = stateful_op->stream_in.da_stream;
    attachment = stateful_op->attachment;
    btree      = castle_double_array_btree_type_get(attachment);
    do_gettimeofday(&now);

    /* Initialise buffer consumer, get number of keys. */
    nr_keys    = castle_buffer_consumer_init(&buf_con,
                                             op->buf->buffer,
                                             op->buf->size);
    if (unlikely(nr_keys < 0))
    {
        castle_printk(LOG_ERROR, "%s::[op %llu] nr_keys %d\n",
            __FUNCTION__, stateful_op->token, nr_keys);
        return -EINVAL;
    }

    if (unlikely(stateful_op->stream_in.received_entries
                + nr_keys > stateful_op->stream_in.expected_entries))
    {
        castle_printk(LOG_ERROR, "%s::[op %llu] Attempt to insert %d keys would overflow "
                "expected %lu (already received %lu).\n",
                __FUNCTION__,
                stateful_op->token,
                nr_keys,
                stateful_op->stream_in.expected_entries,
                stateful_op->stream_in.received_entries);
        return -ENOSPC;
    }

    /* Iterate over all key value pairs in the buffer. */
    for (kvp = 0; kvp < nr_keys; kvp++)
    {
        CVT_INVALID_INIT(cvt);
        err = castle_buffer_kvp_get(&buf_con, kvp, &kv_hdr);
        if (err)
            goto err1;

        /* Construct key. */
        key = btree->key_pack(kv_hdr.key, NULL, NULL);
        if (!key)
        {
            err = -ENOMEM;
            goto err2;
        }
#if 0
        castle_printk(LOG_DEVEL, "%s::key: \n", __FUNCTION__);
        btree->key_print(LOG_DEVEL, key);
#endif

        /* Construct the CVT. */
        switch (kv_hdr.val_type)
        {
            case CASTLE_VALUE_TYPE_INLINE:
                /* Buffer inline vlaue could be inline or a medium object. */
                if (kv_hdr.val_len <= MAX_INLINE_VAL_SIZE)
                {
                    CVT_INLINE_INIT(cvt, kv_hdr.val_len, kv_hdr.val);
                }
                else
                {
                    c_byte_off_t ext_space_needed;
                    c_ext_pos_t mobj_ext_cep;
                    int total_blocks;
                    void *val_ptr;
                    uint64_t rem_bytes_to_copy;
                    int rem_blocks_to_copy;

                    BUG_ON(kv_hdr.val_len > MEDIUM_OBJECT_LIMIT);
                    if(!stateful_op->stream_in.expected_dataext_chunks)
                    {
                        castle_printk(LOG_ERROR, "%s::[op %llx] user requested 0 MO chunks on this op; giving up on value of size %llu bytes.\n",
                            __FUNCTION__, stateful_op->token, kv_hdr.val_len);
                        err = -EFBIG;
                        goto err2;
                    }

                    /* Allocate space for the new copy. */
                    BUG_ON(EXT_ID_INVAL(da_stream->tree->data_ext_free.ext_id));
                    total_blocks = (kv_hdr.val_len - 1) / C_BLK_SIZE + 1;
                    ext_space_needed = total_blocks * C_BLK_SIZE;
                    debug("%s::total_blocks = %u, ext_space_needed = %lu\n",
                        __FUNCTION__, total_blocks, ext_space_needed);
                    if ((err = castle_ext_freespace_get(&da_stream->tree->data_ext_free,
                                                        ext_space_needed,
                                                        0, /* was_preallocated */
                                                        &mobj_ext_cep)) < 0)
                    {
                        castle_printk(LOG_ERROR, "%s::[op %llu] Failed to get medium object freespace, "
                                "err=%d.\n", __FUNCTION__, stateful_op->token, err);
                        err = -ENOSPC;
                        goto err2;
                    }

                    stateful_op->stream_in.received_mobj_off = mobj_ext_cep.offset;

                    /* Copy buffer value into medium object extent. */
                    val_ptr = kv_hdr.val;

                    /* Init the cvt _before_ inserting the value, since this will modify
                       mobj_ext_cep. */
                    CVT_MEDIUM_OBJECT_INIT(cvt, kv_hdr.val_len, mobj_ext_cep);

                    rem_blocks_to_copy = total_blocks;
                    rem_bytes_to_copy = kv_hdr.val_len;
                    while (rem_bytes_to_copy)
                    {
                        c2_block_t *c_c2b;
                        uint64_t bytes_to_copy;
                        /* copy a chk at a time if possible */
                        int blocks_to_copy =
                            (rem_blocks_to_copy < BLKS_PER_CHK) ? rem_blocks_to_copy
                            : BLKS_PER_CHK;
                        int last_copy = (blocks_to_copy == rem_blocks_to_copy);

                        if(last_copy)
                        {
                            BUG_ON(((rem_bytes_to_copy -1) / C_BLK_SIZE + 1) != blocks_to_copy);
                            bytes_to_copy = rem_bytes_to_copy;
                        }
                        else
                            bytes_to_copy = blocks_to_copy * C_BLK_SIZE;

                        BUG_ON(bytes_to_copy > rem_bytes_to_copy);

                        c_c2b = castle_cache_block_get(mobj_ext_cep, blocks_to_copy, MERGE_OUT);
                        write_lock_c2b(c_c2b);
                        update_c2b(c_c2b);
                        memcpy(c2b_buffer(c_c2b), val_ptr, bytes_to_copy);

                        if (last_copy)
                        {
                            int mod_cp = bytes_to_copy % C_BLK_SIZE;
                            if(mod_cp)
                            {
                                debug("%s::zero padding %u bytes.\n",
                                        __FUNCTION__, (C_BLK_SIZE - mod_cp));
                                memset((c2b_buffer(c_c2b) + bytes_to_copy),
                                        0,
                                        (C_BLK_SIZE - mod_cp));
                            }
                        }

                        dirty_c2b(c_c2b);
                        write_unlock_c2b(c_c2b);
                        set_c2b_immutable(c_c2b);
                        put_c2b(c_c2b);
                        mobj_ext_cep.offset += C_BLK_SIZE;
                        val_ptr += bytes_to_copy;
                        rem_bytes_to_copy -= bytes_to_copy;
                        rem_blocks_to_copy -= blocks_to_copy;
                    }
                }
                break;
            case CASTLE_VALUE_TYPE_COUNTER:
                CVT_COUNTER_SET_INIT(cvt, kv_hdr.val_len, kv_hdr.val);
                break;
            case CASTLE_VALUE_TYPE_COUNTER_DELTA:
                CVT_COUNTER_ADD_INIT(cvt, kv_hdr.val_len, kv_hdr.val);
                break;
            case CASTLE_VALUE_TYPE_TOMBSTONE:
                CVT_TOMBSTONE_INIT(cvt, (uint64_t)now.tv_sec);
                break;
            case CASTLE_VALUE_TYPE_OUT_OF_LINE:
                castle_printk(LOG_WARN, "%s::[op 0x%llx] skipping OoL.\n",
                    __FUNCTION__, stateful_op->token);
                BUG_ON(!CVT_INVALID(cvt)); /* should still be INVAL; let's leave it that way. */
                break;
            default:
                BUG(); /* trust castle_buffer_kvp_get() */
                break;
        }
        cvt.user_timestamp = kv_hdr.user_timestamp;

        BUG_ON(CVT_INVALID(cvt) && (kv_hdr.val_type != CASTLE_VALUE_TYPE_OUT_OF_LINE));
        if (!CVT_INVALID(cvt) &&
            unlikely(err = castle_da_in_stream_entry_add(da_stream,
                                                         key,
                                                         attachment->version,
                                                         cvt)))
            goto err2;

        castle_free(key);
        castle_buffer_kvp_free(&kv_hdr);
    }

    stateful_op->stream_in.received_entries += nr_keys;

    return 0;

err2:
    if (key)
        castle_free(key);
    castle_buffer_kvp_free(&kv_hdr);
err1:
    stateful_op->stream_in.received_entries += kvp + 1;
    return err;
}

static void castle_back_stream_in_continue(void *data)
{
    struct castle_back_stateful_op *stateful_op = data;
    struct castle_attachment *attachment;
    int ret = 0;
    castle_interface_token_t token = stateful_op->token;
    struct castle_back_op *op = stateful_op->curr_op;
    int abort_stream = 0;

    spin_lock(&stateful_op->lock);
    stateful_op->in_use = 1;

    /* Check if stateful_op is expired, if so no need to handle anymore ops. Return back. */
    /* drops the lock if return non-zero */
    if (castle_back_stateful_op_completed_op(stateful_op))
        return;
    attachment = stateful_op->attachment;

    switch (stateful_op->curr_op->req.tag)
    {
        case CASTLE_RING_STREAM_IN_NEXT:
            spin_unlock(&stateful_op->lock);
            if ((ret = castle_back_stream_in_buf_process(stateful_op)))
                castle_printk(LOG_ERROR, "%s::stateful op %llx failed with error code %d.\n",
                        __FUNCTION__, stateful_op->token, ret);
            spin_lock(&stateful_op->lock);
            castle_back_buffer_put(stateful_op->conn, op->buf);
            castle_back_reply(op, ret, token, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
            stateful_op->curr_op = NULL;
            castle_back_stateful_op_enable_expire(stateful_op);
            spin_unlock(&stateful_op->lock);
            break;
        case CASTLE_RING_STREAM_IN_FINISH:
            abort_stream = stateful_op->curr_op->req.stream_in_finish.abort;

            castle_printk(LOG_USERINFO, "%s::finishing stream in for op %llx, abort=%u\n",
                    __FUNCTION__,
                    stateful_op->token,
                    abort_stream);

            spin_unlock(&stateful_op->lock);

            /* Takes transaction lock. */
            castle_da_in_stream_complete(stateful_op->stream_in.da_stream,
                    abort_stream);

            spin_lock(&stateful_op->attachment->sop_lock);
            list_del_init(&stateful_op->attachment_list);
            spin_unlock(&stateful_op->attachment->sop_lock);

            spin_lock(&stateful_op->lock);

            castle_back_stateful_op_finish_all(stateful_op, 0);
            stateful_op->curr_op = NULL;
            stateful_op->attachment = NULL;
            /* Will drop stateful_op->lock. */
            castle_back_put_stateful_op(stateful_op->conn, stateful_op);
            castle_back_reply(op, ret, token, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
            castle_attachment_put(attachment);
            break;
        default:
            castle_printk(LOG_ERROR, "%s::Invalid tag %d.\n",
                    __FUNCTION__, stateful_op->curr_op->req.tag);
            BUG();
    }

    /* To prevent #3144. */
    might_resched();
}

/*
 * BIG GET
 */
static void castle_back_big_get_expire(struct castle_back_stateful_op *stateful_op)
{
    struct castle_attachment *attachment;
    debug("castle_back_big_get_expire token=%u.\n", stateful_op->token);

    BUG_ON(!stateful_op->expiring);
    BUG_ON(!list_empty(&stateful_op->op_queue));
    BUG_ON(stateful_op->curr_op != NULL);
    BUG_ON(stateful_op->tag != CASTLE_RING_BIG_GET);

    castle_object_pull_finish(&stateful_op->pull);

    castle_free(stateful_op->pull.key);
    stateful_op->pull.key = NULL;

    spin_lock(&stateful_op->attachment->sop_lock);
    list_del_init(&stateful_op->attachment_list);
    spin_unlock(&stateful_op->attachment->sop_lock);

    spin_lock(&stateful_op->lock);

    attachment = stateful_op->attachment;
    stateful_op->attachment = NULL;

    castle_back_put_stateful_op(stateful_op->conn, stateful_op); /* drops stateful_op->lock */

    castle_attachment_put(attachment);
}

static void castle_back_big_get_do_chunk(void *data)
{
    struct castle_back_stateful_op *stateful_op = data;
    struct castle_back_op *op = stateful_op->curr_op;

    BUG_ON(stateful_op->tag != CASTLE_RING_BIG_GET);
    BUG_ON(op == NULL);
    BUG_ON(op->req.tag != CASTLE_RING_GET_CHUNK);

    castle_object_chunk_pull(&stateful_op->pull, castle_back_user_to_kernel(op->buf,
        op->req.get_chunk.buffer_ptr), op->req.get_chunk.buffer_len);
}

static void castle_back_big_get_call_queued(struct castle_back_stateful_op *stateful_op)
{
    BUG_ON(!spin_is_locked(&stateful_op->lock));
    if (castle_back_stateful_op_prod(stateful_op))
        BUG_ON(!queue_work_on(stateful_op->cpu, castle_back_wq, &stateful_op->work[0]));
}

/**
 *
 * @also castle_back_big_get_do_chunk()
 */
static void castle_back_big_get_continue(struct castle_object_pull *pull,
                                         int err,
                                         uint64_t length,
                                         int done)
{
    struct castle_back_stateful_op *stateful_op =
        container_of(pull, struct castle_back_stateful_op, pull);
    struct castle_attachment *attachment;
    int not_found;

    not_found = CVT_INVALID(pull->cvt);
    debug("castle_back_big_get_continue stateful_op=%p err=%d length=%llu not_found=%d, done=%d\n",
        stateful_op, err, length, not_found, done);

    BUG_ON(stateful_op->tag != CASTLE_RING_BIG_GET);
    BUG_ON(stateful_op->curr_op == NULL);
    BUG_ON(stateful_op->curr_op->req.tag != CASTLE_RING_GET_CHUNK
        && stateful_op->curr_op->req.tag != CASTLE_RING_BIG_GET);
    BUG_ON(stateful_op->curr_op->req.tag == CASTLE_RING_GET_CHUNK && !stateful_op->in_use);

    if (stateful_op->curr_op->req.tag == CASTLE_RING_GET_CHUNK)
        castle_back_buffer_put(stateful_op->conn, stateful_op->curr_op->buf);

    spin_lock(&stateful_op->lock);

    castle_back_reply(stateful_op->curr_op,
                      err ? err : (not_found ? -ENOENT : 0),
                      stateful_op->token,
                      length,
                      pull->cvt.user_timestamp,
                      CASTLE_RESPONSE_FLAG_NONE);

    if (err || done)
    {
        castle_back_stateful_op_finish_all(stateful_op, err);
        spin_unlock(&stateful_op->lock);

        attachment = stateful_op->attachment;
        stateful_op->attachment = NULL;

        if (!err)
        {
            /* May sleep so do not hold the spinlock. Safe because curr_op is still not null */
            castle_object_pull_finish(&stateful_op->pull);

            /* Update stats. */
            atomic64_inc(&attachment->big_get.ios);
            atomic64_add(stateful_op->pull.cvt.length, &attachment->big_get.bytes);
        }

        castle_free(pull->key);

        spin_lock(&attachment->sop_lock);
        list_del_init(&stateful_op->attachment_list);
        spin_unlock(&attachment->sop_lock);

        spin_lock(&stateful_op->lock);

        stateful_op->curr_op = NULL;

        /* This drops the spinlock. */
        castle_back_put_stateful_op(stateful_op->conn, stateful_op);

        castle_attachment_put(attachment);

        return;
    }

    if (!stateful_op->in_use)
        stateful_op->in_use = 1;

    stateful_op->curr_op = NULL;

    /* drops the lock if return non-zero */
    if (castle_back_stateful_op_completed_op(stateful_op))
        return;

    castle_back_big_get_call_queued(stateful_op); // castle_back_big_get_do_chunk()

    spin_unlock(&stateful_op->lock);
}

static void castle_back_big_get(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_conn *conn = op->conn;
    int err;
    struct castle_attachment *attachment;
    struct castle_back_stateful_op *stateful_op;

    debug("castle_back_big_get\n");

    castle_back_stateful_op_get(conn,
                                &stateful_op,
                                op->cpu,
                                op->cpu_index,
                                castle_back_big_get_expire);
    if (!stateful_op)
    {
        error("castle_back: no more free stateful ops!\n");
        err = -EAGAIN;
        goto err0;
    }

    attachment = castle_attachment_get(op->req.big_get.collection_id, READ);
    if (attachment == NULL)
    {
        error("Collection not found id=0x%x\n", op->req.big_get.collection_id);
        err = -ENOTCONN;
        goto err1;
    }

    stateful_op->tag = CASTLE_RING_BIG_GET;
    stateful_op->curr_op = op;
    castle_back_stateful_op_attach(stateful_op, attachment);

    stateful_op->pull.pull_continue = castle_back_big_get_continue;

    INIT_WORK(&stateful_op->work[0], castle_back_big_get_do_chunk, stateful_op);

#ifdef DEBUG
    stateful_debug("key: \n");
    vl_bkey_print(LOG_DEBUG, op->key);
#endif

    stateful_op->pull.key = op->key;

    err = castle_object_pull(&stateful_op->pull, attachment, op->cpu_index);
    if (err)
        goto err2;

    return;

err2: spin_lock(&stateful_op->attachment->sop_lock);
      list_del_init(&stateful_op->attachment_list);
      spin_unlock(&stateful_op->attachment->sop_lock);
      castle_attachment_put(attachment);
      stateful_op->attachment = NULL;
err1: /* Safe as no one will have queued up a op - we haven't returned token yet */
      spin_lock(&stateful_op->lock);
      stateful_op->curr_op = NULL;
      castle_back_put_stateful_op(conn, stateful_op);
err0: castle_free(op->key);
      castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

static void castle_back_get_chunk(void *data)
{
    struct castle_back_op *op = data;
    struct castle_back_conn *conn = op->conn;
    struct castle_back_stateful_op *stateful_op;
    int err;

    stateful_op = castle_back_find_stateful_op(conn,
        op->req.get_chunk.token, CASTLE_RING_BIG_GET);
    if (!stateful_op)
    {
        castle_printk(LOG_INFO, "%s Token not found 0x%x\n",
                __FUNCTION__, op->req.get_chunk.token);
        err = -EBADFD;
        goto err0;
    }

    /*
     * Get buffer with value in it and save it
     */
    op->buf = castle_back_buffer_get(conn,
                                     (unsigned long) op->req.get_chunk.buffer_ptr,
                                     op->req.get_chunk.buffer_len);
    if (op->buf == NULL)
    {
        error("Couldn't get buffer for pointer=%p length=%u\n",
                op->req.get_chunk.buffer_ptr, op->req.get_chunk.buffer_len);
        err = -EINVAL;
        goto err0;
    }

    if (((unsigned long) op->req.get_chunk.buffer_ptr) % PAGE_SIZE)
    {
        error("Invalid ptr, not page aligned (ptr=%p)\n", op->req.put_chunk.buffer_ptr);
        err = -EINVAL;
        goto err1;
    }

    if ((op->req.get_chunk.buffer_len) % PAGE_SIZE)
    {
        error("Invalid len, not page aligned (len=%u)\n", op->req.put_chunk.buffer_len);
        err = -EINVAL;
        goto err1;
    }

    /*
     * Put this op on the queue for the get chunk
     */
    spin_lock(&stateful_op->lock);

    err = castle_back_stateful_op_queue_op(stateful_op, op->req.get_chunk.token, op);
    if (err)
    {
        spin_unlock(&stateful_op->lock);
        goto err1;
    }

    castle_back_big_get_call_queued(stateful_op);

    spin_unlock(&stateful_op->lock);

    return;

err1: castle_back_buffer_put(conn, op->buf);
err0: castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

/******************************************************************
 * Castle device functions
 */

/**
 * Blocks userland until a response is available on the ring.
 */
unsigned int castle_back_poll(struct file *file, poll_table *wait)
{
    struct castle_back_conn *conn = file->private_data;
    if (conn == NULL)
    {
        error("castle_back: poll, retrieving connection failed\n");
        return -EINVAL;
    }

    debug(">>>castle_back_poll\n");

    poll_wait(file, &conn->wait, wait);

    debug("castle_back_poll done\n");

    if (conn->flags & CASTLE_BACK_CONN_NOTIFY_FLAG)
    {
        clear_bit(CASTLE_BACK_CONN_NOTIFY_BIT, &conn->flags);
        RING_PUSH_RESPONSES(&conn->back_ring);

        return POLLIN | POLLRDNORM;
    }

    debug("castle_back_poll nothing to say\n");

    return 0;
}

/**
 * Get cpu_index for a given stateful op.
 */
static int castle_back_stateful_op_cpu_index_get(struct castle_back_conn *conn,
                                                 castle_interface_token_t token,
                                                 uint32_t tag)
{
    struct castle_back_stateful_op *stateful_op;

    stateful_op = castle_back_find_stateful_op(conn, token, tag);
    if (!stateful_op)
        /* Error later, queue on current conn CPU for now. */
        return conn->cpu_index;
    else
        return stateful_op->cpu_index;
}

/**
 * Queue request on key-hash specified CPU with appropriate op function.
 *
 * - Hash okey and select appropriate CPU to queue request onto
 * - Stateful ops maintain CPU affinity
 */
static void castle_back_request_process(struct castle_back_conn *conn, struct castle_back_op *op)
{
    int err;
    uint64_t val_len = 0;
    int8_t   counter_add_flag = -1;

    debug("Got a request call=%d tag=%d\n", op->req.call_id, op->req.tag);

    op->key = NULL;

    /* Required in case castle_back_key_copy_get() fails to return a key.
     * It won't matter that the op ends up on the wrong CPU because it will
     * return before hitting the DA. */
    op->cpu_index = conn->cpu_index;

    CVT_INVALID_INIT(op->replace.cvt);
    switch (op->req.tag)
    {
        /* Point ops
         *
         * Have CPU affinity based on hash of the key.  They must hit the
         * correct CPU (op->cpu) and CT (op->cpu_index). */

        case CASTLE_RING_STREAM_IN_START: /* iterator, round-robin CPU selection */
            op->cpu_index = conn->cpu_index;
            INIT_WORK(&op->work, castle_back_stream_in_start, op);
            break;

        case CASTLE_RING_STREAM_IN_NEXT:
            op->cpu_index = castle_back_stateful_op_cpu_index_get(conn,
                                                                  op->req.stream_in_next.token,
                                                                  CASTLE_RING_STREAM_IN_START);
            INIT_WORK(&op->work, castle_back_stream_in_next, op);
            break;

        case CASTLE_RING_STREAM_IN_FINISH:
            op->cpu_index = castle_back_stateful_op_cpu_index_get(conn,
                                                                  op->req.stream_in_finish.token,
                                                                  CASTLE_RING_STREAM_IN_START);
            INIT_WORK(&op->work, castle_back_stream_in_finish, op);
            break;

        case CASTLE_RING_TIMESTAMPED_REMOVE:
            if ((err = castle_back_key_copy_get(conn, op->req.timestamped_remove.key_ptr,
                                                op->req.timestamped_remove.key_len, &op->key)))
                goto err;
            INIT_WORK(&op->work, castle_back_timestamped_remove, op);
            break;

        case CASTLE_RING_REMOVE:
            if ((err = castle_back_key_copy_get(conn, op->req.remove.key_ptr,
                                                op->req.remove.key_len, &op->key)))
                goto err;
            INIT_WORK(&op->work, castle_back_remove, op);
            break;

        case CASTLE_RING_TIMESTAMPED_REPLACE:
            if ((err = castle_back_key_copy_get(conn, op->req.timestamped_replace.key_ptr,
                                                op->req.timestamped_replace.key_len, &op->key)))
                goto err;
            INIT_WORK(&op->work, castle_back_timestamped_replace, op);
            break;

        case CASTLE_RING_REPLACE:
            if ((err = castle_back_key_copy_get(conn, op->req.replace.key_ptr,
                                                op->req.replace.key_len, &op->key)))
                goto err;
            INIT_WORK(&op->work, castle_back_replace, op);
            break;

        case CASTLE_RING_COUNTER_REPLACE:
            err = -EINVAL;

            val_len = op->req.counter_replace.value_len;
            if (val_len != sizeof(int64_t))
            {
                error("Counter value len %lld is invalid; must be 8 bytes.\n", val_len);
                goto err;
            }

            /* Note: the following sanity check has never been tested */
            counter_add_flag = op->req.counter_replace.add;
            if (counter_add_flag != CASTLE_COUNTER_TYPE_SET &&
                counter_add_flag != CASTLE_COUNTER_TYPE_ADD)
            {
                error("Counter op flag (add) must be either 0 (SET) or 1 (ADD); value %d not recognized.\n",
                      counter_add_flag);
                goto err;
            }

            if ((err = castle_back_key_copy_get(conn, op->req.counter_replace.key_ptr,
                                                op->req.counter_replace.key_len, &op->key)))
                goto err;
            INIT_WORK(&op->work, castle_back_counter_replace, op);
            break;

        case CASTLE_RING_GET:
            if ((err = castle_back_key_copy_get(conn, op->req.get.key_ptr,
                                                op->req.get.key_len, &op->key)))
                goto err;
            INIT_WORK(&op->work, castle_back_get, op);
            break;

        /* Stateful op initialisers
         *
         * Initialise CPU affinity but are broken down into two categories:
         *
         * 1. Ops that require CPU affinity (as per point ops)
         * 2. Ops that pick a CPU via a round-robin method (e.g. iterators)
         *    - Hashing the key could result in a poor balance of ops as iters
         *      are more likely to start/end on a frequently-used key
         *
         * Regardless of type, subsequent stateful ops will use the same CPU. */

        case CASTLE_RING_TIMESTAMPED_BIG_PUT: /* put, key-hash CPU-affinity */
            if ((err = castle_back_key_copy_get(conn, op->req.big_put.key_ptr,
                                                op->req.timestamped_big_put.key_len, &op->key)))
                goto err;
            INIT_WORK(&op->work, castle_back_timestamped_big_put, op);
            break;

        case CASTLE_RING_BIG_PUT: /* put, key-hash CPU-affinity */
            if ((err = castle_back_key_copy_get(conn, op->req.big_put.key_ptr,
                                                op->req.big_put.key_len, &op->key)))
                goto err;
            INIT_WORK(&op->work, castle_back_big_put, op);
            break;

        case CASTLE_RING_BIG_GET: /* get, key-hash CPU-affinity */
            if ((err = castle_back_key_copy_get(conn, op->req.big_get.key_ptr,
                                                op->req.big_get.key_len, &op->key)))
                goto err;
            INIT_WORK(&op->work, castle_back_big_get, op);
            break;

        case CASTLE_RING_ITER_START: /* iterator, round-robin CPU selection */
            op->cpu_index = conn->cpu_index;
            INIT_WORK(&op->work, castle_back_iter_start, op);
            break;

        /* Stateful op continuations
         *
         * Maintain existing CPU affinity. */

        case CASTLE_RING_ITER_NEXT:
            op->cpu_index = castle_back_stateful_op_cpu_index_get(conn,
                                                                  op->req.iter_next.token,
                                                                  CASTLE_RING_ITER_START);
            INIT_WORK(&op->work, castle_back_iter_next, op);
            break;

        case CASTLE_RING_ITER_FINISH:
            op->cpu_index = castle_back_stateful_op_cpu_index_get(conn,
                                                                  op->req.iter_finish.token,
                                                                  CASTLE_RING_ITER_START);
            INIT_WORK(&op->work, castle_back_iter_finish, op);
            break;

        case CASTLE_RING_PUT_CHUNK:
            op->cpu_index = castle_back_stateful_op_cpu_index_get(conn,
                                                                  op->req.put_chunk.token,
                                                                  CASTLE_RING_BIG_PUT);
            INIT_WORK(&op->work, castle_back_put_chunk, op);
            break;

        case CASTLE_RING_GET_CHUNK:
            op->cpu_index = castle_back_stateful_op_cpu_index_get(conn,
                                                                  op->req.get_chunk.token,
                                                                  CASTLE_RING_BIG_GET);
            INIT_WORK(&op->work, castle_back_get_chunk, op);
            break;

        default:
            error("Unknown request tag %d\n", op->req.tag);
            err = -ENOSYS;
            goto err;
    }

    /* Hash key for cpu_index. */
    if (op->key != NULL)
        op->cpu_index = castle_double_array_key_cpu_index(op->key);

    /* Get CPU and queue work. */
    op->cpu = castle_double_array_request_cpu(op->cpu_index);
    queue_work_on(op->cpu, castle_back_wq, &op->work);

    /* Bump conn cpu_index/cpu for next op (might be used by stateful ops). */
    if (++conn->cpu_index >= castle_double_array_request_cpus())
        conn->cpu_index = 0;
    conn->cpu = castle_double_array_request_cpu(conn->cpu_index);

    return;

err:
    castle_back_reply(op, err, 0, 0, 0, CASTLE_RESPONSE_FLAG_NONE);
}

/**
 * This is called once per connection and lives for as long as the connection is alive.
 */
static int castle_back_work_do(void *data)
{
    struct castle_back_conn *conn = data;
    castle_back_ring_t *back_ring = &conn->back_ring;
    int should_stop, more, items = 0;
    RING_IDX cons, rp;
    struct castle_back_op *op;
    uint32_t ring_size = __RING_SIZE(back_ring->sring, CASTLE_RING_SIZE);

    debug("castle_back: doing work for conn = %p.\n", conn);

    while(1)
    {
        rp = back_ring->sring->req_prod;
        xen_rmb();

        //debug("castle_back: rp=%d\n", rp);

        while ((cons = back_ring->req_cons) != rp)
        {
            if (rp - cons > ring_size)
            {
                castle_printk(LOG_WARN, "Detected ring corruption, skipping ring contents.\n");
                back_ring->req_cons = rp;
                break;
            }
            spin_lock(&conn->response_lock);
            BUG_ON(list_empty(&conn->free_ops));
            op = list_entry(conn->free_ops.next, struct castle_back_op, list);
            list_del(&op->list);
            spin_unlock(&conn->response_lock);

            op->buf = NULL;
            memcpy(&op->req, RING_GET_REQUEST(back_ring, cons), sizeof(castle_request_t));

            back_ring->req_cons++;

            /* this is put in castle_back_reply */
            castle_back_conn_get(conn);

            castle_back_request_process(conn, op);
            items++;
        }

        /* this ensures that if we get an ioctl in between checking the ring
         * for more and calling schedule, we don't sleep and miss it
         */
        preempt_disable();
        set_current_state(TASK_INTERRUPTIBLE);
        xen_rmb();
        RING_FINAL_CHECK_FOR_REQUESTS(back_ring, more);

        should_stop = kthread_should_stop();
        if (more || should_stop)
            set_current_state(TASK_RUNNING);
        if (should_stop)
            break;
        preempt_enable();
        if (!more)
        {
            trace_CASTLE_BACK_WORK_DO(conn, items);
            items = 0;
            schedule();
        }
    }

    debug("castle_back: done work for conn = %p.\n", conn);

    return 0;
}

long castle_back_unlocked_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
    struct castle_back_conn *conn = file->private_data;

    if (conn == NULL)
    {
        error("castle_back: ioctl, retrieving connection failed\n");
        return -EINVAL;
    }

    debug("castle_back_ioctl\n");

    switch (cmd)
    {
        case CASTLE_IOCTL_POKE_RING:
            castle_wake_up_task(conn->work_thread, 1 /*inhibit_cs*/);
            break;

        default:
            return -ENOIOCTLCMD;
    }

    return 0;
}

/**
 * Handle a new connection by creating a castle_back_conn.
 *
 * This is the only place we allocate castle_back_conn structures.
 *
 * @also castle_back_cleanup_conn()
 */
int castle_back_open(struct inode *inode, struct file *file)
{
    castle_sring_t *sring;
    struct castle_back_conn *conn;
    int i, err = 0;
    static atomic_t castle_next_conn_cpu_index = ATOMIC_INIT(0);

    debug("castle_back_dev_open\n");

    /* Do nothing if the castle_init hasn't yet completed. */
    if(!castle_back_inited)
    {
        err = -EAGAIN;
        goto err0;
    }

    conn = castle_alloc(sizeof(struct castle_back_conn));
    if (conn == NULL)
    {
        error("castle_back: failed to malloc new connection\n");
        err = -ENOMEM;
        goto err0;
    }

    conn->flags = 0;
    conn->cpu_index = castle_atomic_inc_cycle(castle_double_array_request_cpus(),
                                              &castle_next_conn_cpu_index);
    conn->cpu = castle_double_array_request_cpu(conn->cpu_index);
    atomic_set(&conn->ref_count, 1);

    init_waitqueue_head(&conn->wait);
    spin_lock_init(&conn->response_lock);
    rwlock_init(&conn->buffers_lock);
    spin_lock_init(&conn->restart_timer_lock);
    conn->buffers_rb = RB_ROOT;

    /* Structure is mapped in userspace, vmalloc() for page alignment. */
    sring = (castle_sring_t *)castle_vmalloc(CASTLE_RING_SIZE);
    if (sring == NULL)
    {
        error("castle_back: failed to vmalloc shared ring\n");
        err = -ENOMEM;
        goto err1;
    }

    ReservePages(sring, CASTLE_RING_SIZE);

    SHARED_RING_INIT(sring);
    BACK_RING_INIT(&conn->back_ring, sring, CASTLE_RING_SIZE);

    /* init the ops pool */
    conn->ops = castle_vmalloc(sizeof(struct castle_back_op) * RING_SIZE(&conn->back_ring));
    if (conn->ops == NULL)
    {
        error("castle_back: failed to vmalloc mirror buffer for ops\n");
        err = -ENOMEM;
        goto err2;
    }

    INIT_LIST_HEAD(&conn->free_ops);

    for (i=0; i<RING_SIZE(&conn->back_ring); i++)
    {
        conn->ops[i].conn = conn;
        list_add(&conn->ops[i].list, &conn->free_ops);
    }

    /* init the stateful ops pool */
    conn->stateful_ops = castle_vmalloc(sizeof(struct castle_back_stateful_op) * MAX_STATEFUL_OPS);
    if (conn->stateful_ops == NULL)
    {
        error("castle_back: failed to vmalloc buffer for stateful_ops\n");
        err = -ENOMEM;
        goto err3;
    }
    memset(conn->stateful_ops, 0, sizeof(struct castle_back_stateful_op) * MAX_STATEFUL_OPS);

    INIT_LIST_HEAD(&conn->free_stateful_ops);

    for (i=0; i<MAX_STATEFUL_OPS; i++)
    {
        list_add_tail(&conn->stateful_ops[i].list, &conn->free_stateful_ops);
        spin_lock_init(&conn->stateful_ops[i].lock);
        /* calls to this stateful op before a start stateful op call (such as iternext) will
         * be scheduled on this cpu
         */
        conn->stateful_ops[i].cpu = first_cpu(cpu_online_map);
    }

    file->private_data = conn;

    /* Don't increase the reference count here, since we hold a reference count
     * and won't release it until kthread_stop has returned. */
    conn->work_thread = kthread_create(castle_back_work_do, conn, "castle_client");
    if (!conn->work_thread)
    {
        error("Could not create work thread\n");
        goto err4;
    }

    INIT_WORK(&conn->timeout_check_work, _castle_back_stateful_op_timeout_check, conn);
    conn->timeout_check_wq = create_workqueue("castle_back_timeout");

    conn->restart_timer = 1;
    castle_back_start_stateful_op_timeout_check_timer(conn);

    atomic_inc(&castle_back_conn_count);
    spin_lock(&conns_lock);
    list_add_tail(&conn->list, &castle_back_conns);
    spin_unlock(&conns_lock);

    debug("castle_back_open for conn = %p returning.\n", conn);

    return 0;

err4:
    castle_vfree(conn->stateful_ops);
err3:
    castle_vfree(conn->ops);
err2:
    UnReservePages(conn->back_ring.sring, CASTLE_RING_SIZE);
    castle_vfree(sring);
err1:
    castle_free(conn);
err0:
    return err;
}

/**
 * Clean-up and free the conn structure.
 *
 * Except for conn's that don't get fully initialised in castle_back_open()
 * this is the only place where conns are freed.
 *
 * @also castle_back_open().
 */
static void castle_back_cleanup_conn(struct castle_back_conn *conn)
{
    struct castle_back_stateful_op *stateful_ops = conn->stateful_ops;
    uint32_t i;

    debug("castle_back_cleanup_conn for conn = %p\n", conn);

    BUG_ON(atomic_read(&conn->ref_count) > 0);

    /* shouldn't be any stateful_ops running here for ref_count to be 0 */
    for (i = 0; i < MAX_STATEFUL_OPS; i++)
        BUG_ON(stateful_ops[i].in_use);

    /* del the timer and wait until it has finished the callback */
    spin_lock_irq(&conn->restart_timer_lock);
    conn->restart_timer = 0;
    spin_unlock_irq(&conn->restart_timer_lock);
    del_timer_sync(&conn->stateful_op_timeout_check_timer);

    destroy_workqueue(conn->timeout_check_wq);

    debug("castle_back_cleanup_conn for conn = %p cleaned up and freeing\n", conn);

    UnReservePages(conn->back_ring.sring, CASTLE_RING_SIZE);
    castle_vfree(conn->back_ring.sring);
    castle_vfree(conn->ops);
    castle_vfree(conn->stateful_ops);

    spin_lock(&conns_lock);
    list_del(&conn->list);
    spin_unlock(&conns_lock);
    atomic_dec(&castle_back_conn_count);

    /* _buffer_put() cleans up buffers and they should now all have been freed
     * as conn->ref_count has reached 0. */
    castle_free(conn);

    wake_up(&conn_close_wait);
}

static inline void castle_back_conn_get(struct castle_back_conn *conn)
{
    atomic_inc(&conn->ref_count);
}

static inline void castle_back_conn_put(struct castle_back_conn *conn)
{
    if (atomic_dec_and_test(&conn->ref_count))
        castle_back_cleanup_conn(conn);
}

int castle_back_release(struct inode *inode, struct file *file)
{
    struct castle_back_conn *conn = file->private_data;
    struct castle_back_stateful_op *stateful_ops;
    uint32_t i;

    debug("castle_back_release\n");

    if (conn == NULL)
    {
        error("castle_back: release, retrieving connection failed\n");
        return -EINVAL;
    }

    set_bit(CASTLE_BACK_CONN_DEAD_BIT, &conn->flags);
    file->private_data = NULL;
    kthread_stop(conn->work_thread);
    wake_up(&conn->wait);

    stateful_ops = conn->stateful_ops;

    for (i = 0; i < MAX_STATEFUL_OPS; i++)
    {
        struct castle_back_stateful_op *stateful_op = &stateful_ops[i];

        spin_lock(&stateful_op->lock);

        /* if it's in use but already expiring we don't need to do anything here */
        if (stateful_op->in_use && !stateful_op->expiring)
        {
            castle_back_stateful_op_finish_all(stateful_op, -EINVAL);
            stateful_op->cancel_on_op_complete = 1;

            if (!stateful_op->curr_op)
            {
                stateful_op->expiring = 1;
                spin_unlock(&stateful_op->lock);
                stateful_op->expire(stateful_op);
            }
            else
            {
                debug("Trying to release conn %p, but ongoing op %p for stateful op %p.\n",
                        conn, stateful_op->curr_op, stateful_op);
                spin_unlock(&stateful_op->lock);
            }
        }
        else
            spin_unlock(&stateful_op->lock);
    }

    castle_back_conn_put(conn);

    return 0;
}

static int castle_buffer_map(struct castle_back_conn *conn, struct vm_area_struct *vma)
{
    int err;
    unsigned long size;
    struct castle_back_buffer *buffer;

    size = vma->vm_end - vma->vm_start;
    if (size > MAX_BUFFER_SIZE)
    {
        error("castle_back: you tried to map %ld bytes, max is %d!\n", size, MAX_BUFFER_SIZE);
        err = -EINVAL;
        goto err1;
    }
    else if (vma->vm_start % PAGE_SIZE)
    {
        error("castle_back: you tried to map at addr %ld, not page aligned!\n", vma->vm_start);
        err = -EINVAL;
        goto err1;
    }
    else if (size % PAGE_SIZE)
    {
        error("castle_back: you tried to map %ld bytes, not multiple of page size!\n", size);
        err = -EINVAL;
        goto err1;
    }

    buffer = castle_zalloc(sizeof(struct castle_back_buffer));
    if (!buffer)
    {
        error("castle_back: failed to alloc memory for rb entry\n");
        err = -ENOMEM;
        goto err1;
    }

    debug("castle_buffer_map buffer=%p, size=%ld, vm_start=%lx, vm_end=%lx\n", buffer, size, vma->vm_start, vma->vm_end);

    vma->vm_flags |= VM_RESERVED;
    vma->vm_ops = &castle_back_vm_ops;

    buffer->user_addr = vma->vm_start;
    buffer->size = size;
    atomic_set(&buffer->ref_count, 1);
    buffer->buffer = castle_vmalloc(size);
    if (!buffer->buffer)
    {
        error("castle_back: failed to alloc memory for buffer\n");
        err = -ENOMEM;
        goto err2;
    }

    /*
     * Add entry to our rb tree so we can find the buffer later
     * there is only one mmap at once, so no worries about concurrency here
     */
    read_lock(&conn->buffers_lock);
    if (__castle_back_buffer_exists(conn, vma->vm_start, vma->vm_end))
    {
        read_unlock(&conn->buffers_lock);
        error("castle_back: mapping exists!\n");
        err = -EEXIST;
        goto err3;
    }
    read_unlock(&conn->buffers_lock);

    ReservePages(buffer->buffer, size);

    vma->vm_flags |= VM_DONTCOPY;

    err = castle_vma_map(vma, buffer->buffer, size);
    if (err)
    {
        error("castle_back: mapping failed!\n");
        goto err4;
    }

    write_lock(&conn->buffers_lock);
    BUG_ON(castle_back_buffers_rb_insert(conn, vma->vm_start, &buffer->rb_node) != NULL);
    write_unlock(&conn->buffers_lock);

    debug("Create shared buffer kernel=%p, user=%lx, size=%u\n",
        buffer->buffer, buffer->user_addr, buffer->size);

    return 0;

err4:
    UnReservePages(buffer->buffer, buffer->size);
err3:
    castle_vfree(buffer->buffer);
err2:
    castle_free(buffer);
err1:
    return err;
}

static int castle_ring_map(struct castle_back_conn *conn, struct vm_area_struct *vma)
{
    unsigned long size;
    int err;

    size = vma->vm_end - vma->vm_start;
    if (size != CASTLE_RING_SIZE)
    {
        error("castle_back: you _must_ map exactly %d bytes (you asked for %ld)!\n",
            CASTLE_RING_SIZE, size);
        return -EINVAL;
    }
    else if (vma->vm_start % PAGE_SIZE)
    {
        error("castle_back: you tried to map at addr %ld, not page aligned!\n", vma->vm_start);
        return -EINVAL;
    }

    vma->vm_flags |= VM_RESERVED;
    /* ring doesn't really need our VM ops */
    // vma->vm_ops = &castle_vm_ops;

    conn->rings_vstart = vma->vm_start;

    err = castle_vma_map(vma, conn->back_ring.sring, CASTLE_RING_SIZE);
    if (err)
    {
        error("castle_back: mapping failed!\n");
        return err;
    }

    vma->vm_flags |= VM_DONTCOPY;

    return 0;
}

int castle_back_mmap(struct file *file, struct vm_area_struct *vma)
{
    int err;
    struct castle_back_conn *conn = file->private_data;
    if (conn == NULL)
    {
        error("castle_back: castle, retrieving connection failed\n");
        return -EINVAL;
    }

    debug("castle_back_mmap mm->mmap_sem.activity=%d\n", vma->vm_mm->mmap_sem.activity);

    if(!test_and_set_bit(CASTLE_BACK_CONN_INITIALISED_BIT, &conn->flags))
    {
        err = castle_ring_map(conn, vma);
        if (err)
        {
            clear_bit(CASTLE_BACK_CONN_INITIALISED_BIT, &conn->flags);
            goto err_out;
        }
    }
    else
    {
        err = castle_buffer_map(conn, vma);
        if (err)
            goto err_out;
    }

    return 0;

err_out:
    return err;
}

int castle_back_init(void)
{
    int err;

    debug("castle_back initing...");

    castle_back_wq = create_workqueue("castle_back");
    if (!castle_back_wq)
    {
        error(KERN_ALERT "Error: Could not alloc wq\n");
        err = -ENOMEM;
        goto err1;
    }

    init_waitqueue_head(&conn_close_wait);
    spin_lock_init(&conns_lock);
    atomic_set(&castle_back_conn_count, 0);

    castle_back_inited = 1;

    debug("done!\n");

    return 0;

    destroy_workqueue(castle_back_wq); /* unreachable */
err1:
    return err;
}

void castle_back_fini(void)
{
    /* wait for all connections to be closed */
    wait_event(conn_close_wait, (atomic_read(&castle_back_conn_count) == 0));

    BUG_ON(!list_empty(&castle_back_conns));

    destroy_workqueue(castle_back_wq);

    debug("done!\n");
}
