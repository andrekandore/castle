#ifndef __CASTLE_UTILS_H__
#define __CASTLE_UTILS_H__

#include <linux/log2.h>
#include <linux/skbuff.h>
#include "castle_public.h"
#include "castle.h"
#include "castle_debug.h"
#include "castle_cache.h"

#define ATOMIC(_i)  ((atomic_t)ATOMIC_INIT(_i))

/* Uses RW spinlock for synchronization. Use iterate_exclusive() for exclusive
 * access while iterating over the hash table. */
#define DEFINE_HASH_TBL(_prefix, _tab, _tab_size, _struct, _list_mbr, _key_t, _key)            \
                                                                                               \
static DEFINE_RWLOCK(_prefix##_hash_lock);                                                     \
static int _prefix##_nr_entries;                                                               \
                                                                                               \
static inline int _prefix##_hash_idx(_key_t key)                                               \
{                                                                                              \
    unsigned long hash = 0UL;                                                                  \
                                                                                               \
    memcpy(&hash, &key, sizeof(_key_t) > 8 ? 8 : sizeof(_key_t));                              \
                                                                                               \
    return (int)(hash % _tab_size);                                                            \
}                                                                                              \
                                                                                               \
static inline _struct* __##_prefix##_hash_get(_key_t key);                                     \
                                                                                               \
static inline void _prefix##_hash_add(_struct *v)                                              \
{                                                                                              \
    int idx = _prefix##_hash_idx(v->_key);                                                     \
    unsigned long flags;                                                                       \
                                                                                               \
    write_lock_irqsave(&_prefix##_hash_lock, flags);                                           \
    BUG_ON(__##_prefix##_hash_get(v->_key));                                                   \
    list_add(&v->_list_mbr, &_tab[idx]);                                                       \
    _prefix##_nr_entries++;                                                                    \
    write_unlock_irqrestore(&_prefix##_hash_lock, flags);                                      \
}                                                                                              \
                                                                                               \
static inline void __##_prefix##_hash_remove(_struct *v)                                       \
{                                                                                              \
    list_del(&v->_list_mbr);                                                                   \
    _prefix##_nr_entries--;                                                                    \
}                                                                                              \
                                                                                               \
static inline void _prefix##_hash_remove(_struct *v)                                           \
{                                                                                              \
    unsigned long flags;                                                                       \
                                                                                               \
    write_lock_irqsave(&_prefix##_hash_lock, flags);                                           \
    list_del(&v->_list_mbr);                                                                   \
    BUG_ON(__##_prefix##_hash_get(v->_key));                                                   \
    write_unlock_irqrestore(&_prefix##_hash_lock, flags);                                      \
}                                                                                              \
                                                                                               \
static inline _struct* __##_prefix##_hash_get(_key_t key)                                      \
{                                                                                              \
    _struct *v;                                                                                \
    struct list_head *l;                                                                       \
    int idx = _prefix##_hash_idx(key);                                                         \
                                                                                               \
    list_for_each(l, &_tab[idx])                                                               \
    {                                                                                          \
        v = list_entry(l, _struct, _list_mbr);                                                 \
        if(memcmp(&v->_key, &key, sizeof(_key_t)) == 0)                                        \
            return v;                                                                          \
    }                                                                                          \
                                                                                               \
    return NULL;                                                                               \
}                                                                                              \
                                                                                               \
static inline _struct* _prefix##_hash_get(_key_t key)                                          \
{                                                                                              \
    _struct *v;                                                                                \
    unsigned long flags;                                                                       \
                                                                                               \
    read_lock_irqsave(&_prefix##_hash_lock, flags);                                            \
    v = __##_prefix##_hash_get(key);                                                           \
    read_unlock_irqrestore(&_prefix##_hash_lock, flags);                                       \
                                                                                               \
    return v;                                                                                  \
}                                                                                              \
                                                                                               \
static inline int __##_prefix##_nr_entries_get()                                               \
{                                                                                              \
    return _prefix##_nr_entries;                                                               \
}                                                                                              \
                                                                                               \
static inline int _prefix##_nr_entries_get()                                                   \
{                                                                                              \
    int nr_entries;                                                                            \
    unsigned long flags;                                                                       \
                                                                                               \
    read_lock_irqsave(&_prefix##_hash_lock, flags);                                            \
    nr_entries = __##_prefix##_nr_entries_get();                                               \
    read_unlock_irqrestore(&_prefix##_hash_lock, flags);                                       \
                                                                                               \
    return nr_entries;                                                                         \
}                                                                                              \
                                                                                               \
static inline void __##_prefix##_hash_iterate(int (*fn)(_struct*, void*), void *arg)           \
{                                                                                              \
    struct list_head *l, *t;                                                                   \
    _struct *v;                                                                                \
    int i;                                                                                     \
                                                                                               \
    if(!_tab) goto out;                                                                        \
    for(i=0; i<_tab_size; i++)                                                                 \
    {                                                                                          \
        list_for_each_safe(l, t, &_tab[i])                                                     \
        {                                                                                      \
            v = list_entry(l, _struct, hash_list);                                             \
            if(fn(v, arg)) goto out;                                                           \
        }                                                                                      \
    }                                                                                          \
out:                                                                                           \
   return;                                                                                     \
}                                                                                              \
                                                                                               \
static inline void _prefix##_hash_iterate(int (*fn)(_struct*, void*), void *arg)               \
{                                                                                              \
    read_lock_irq(&_prefix##_hash_lock);                                                       \
    __##_prefix##_hash_iterate(fn, arg);                                                       \
    read_unlock_irq(&_prefix##_hash_lock);                                                     \
}                                                                                              \
                                                                                               \
static inline void _prefix##_hash_iterate_exclusive(int (*fn)(_struct*, void*), void *arg)     \
{                                                                                              \
    write_lock_irq(&_prefix##_hash_lock);                                                      \
    __##_prefix##_hash_iterate(fn, arg);                                                       \
    write_unlock_irq(&_prefix##_hash_lock);                                                    \
}                                                                                              \
                                                                                               \
static inline struct list_head* _prefix##_hash_alloc(void)                                     \
{                                                                                              \
    return castle_malloc(sizeof(struct list_head) * _tab_size, GFP_KERNEL);                    \
}                                                                                              \
                                                                                               \
static inline void _prefix##_hash_init(void)                                                   \
{                                                                                              \
    int i;                                                                                     \
    for(i=0; i<_tab_size; i++)                                                                 \
        INIT_LIST_HEAD(&_tab[i]);                                                              \
}

/**
 * list_for_each_from - iterate over list of given type from the current point
 * @from:   curren point to start from
 * @pos:    the &struct list_head to use as a loop cursor.
 * @head:   the head for your list.
 *
 * Iterate over list of given type, continuing from current position.
 */
#define list_for_each_from(from, pos, head)                                                    \
    for (pos = (from); prefetch(pos->next), pos != (head); pos = pos->next)

void list_append(struct list_head *head1, struct list_head *head2);

static inline uint32_t BUF_L_GET(const char *buf)
{
    __be32 word;

    memcpy(&word, buf, 4);

    return ntohl(word);
}

static inline uint32_t SKB_L_GET(struct sk_buff *skb)
{
    __be32 word;

    BUG_ON(skb_copy_bits(skb, 0, &word, 4) < 0);
    BUG_ON(!pskb_pull(skb, 4));

    return ntohl(word);
}

static inline uint64_t SKB_LL_GET(struct sk_buff *skb)
{
    __be64 qword;

    BUG_ON(skb == NULL);
    BUG_ON(skb_copy_bits(skb, 0, &qword, 8) < 0);
    BUG_ON(!pskb_pull(skb, 8));

    return be64_to_cpu(qword);
}

static inline char* SKB_STR_GET(struct sk_buff *skb, int max_len)
{
    uint32_t str_len = SKB_L_GET(skb);
    char *str;

    if((str_len > max_len) || (str_len > skb->len))
        return NULL;

    if(!(str = castle_zalloc(str_len+1, GFP_KERNEL)))
        return NULL;

    BUG_ON(skb_copy_bits(skb, 0, str, str_len) < 0);
    str_len += (str_len % 4 == 0 ? 0 : 4 - str_len % 4);
    BUG_ON(!pskb_pull(skb, str_len));

    return str;
}

static inline void SKB_STR_CPY(struct sk_buff *skb, void *dst, int str_len, int round)
{
    uint32_t *dst32 = (uint32_t *)dst;

    BUG_ON(str_len > skb->len);
    BUG_ON(skb_copy_bits(skb, 0, dst32, str_len) < 0);
    if(round)
        str_len += (str_len % 4 == 0 ? 0 : 4 - str_len % 4);
    BUG_ON(!pskb_pull(skb, str_len));
}

static inline c_bio_t* castle_utils_bio_alloc(int nr_bvecs)
{
    c_bio_t *c_bio;
    c_bvec_t *c_bvecs;
    int i;

    /* Allocate bio & bvec structures in one memory block */
    c_bio = castle_malloc(sizeof(c_bio_t) + nr_bvecs * sizeof(c_bvec_t), GFP_KERNEL);
    if(!c_bio)
        return NULL;
    c_bvecs = (c_bvec_t *)(c_bio + 1);
    for (i = 0; i < nr_bvecs; i++)
    {
        c_bvecs[i].cpu       = -1;
        c_bvecs[i].c_bio     = c_bio;
        c_bvecs[i].tree      = NULL;
        c_bvecs[i].cts_proxy = NULL;
        c_bvecs[i].cts_index = -1;
#ifdef CASTLE_PERF_DEBUG
        c_bvecs[i].timeline  = NULL;
#endif
    }
    c_bio->c_bvecs = c_bvecs;
    /* Single reference taken out, the user decides how many more to take */
    c_bio->count   = ATOMIC(1);
    c_bio->err     = 0;

    return c_bio;
}

static inline void castle_utils_bio_free(c_bio_t *bio)
{
    castle_kfree(bio);
}

static inline int list_length(struct list_head *head)
{
    struct list_head *l;
    int length = 0;

    list_for_each(l, head)
        length++;

    return length;
}

/**
 * Checks whether there is an overlap between two pointer ranges. Check is inclusive,
 * that is function will return true, even if the ranges only have one point in common.
 *
 * @arg min1    Start of range1
 * @arg max1    End of range1
 * @arg min2    Start of range2
 * @arg max2    End of range2
 * @return true if there is an overlap in at least one point
 */
static inline int overlap(void *min1, void *max1, void *min2, void *max2)
{
    return (max1 >= min2) && (min1 <= max2);
}

/**
 * Checks whether a pointer is in the specified range.
 */
static inline int ptr_in_range(void *ptr, void *range_start, size_t range_size)
{
    return overlap(ptr, ptr, range_start, range_start + range_size);
}

#ifdef DEBUG
#include <linux/sched.h>
static USED void check_stack_usage(void)
{
    unsigned long *n = end_of_stack(current) + 1;
    unsigned long free;

    while (*n == 0)
        n++;
    free = (unsigned long)n - (unsigned long)end_of_stack(current);

    castle_printk(LOG_DEBUG, "%s used greatest stack depth: %lu bytes left, currently left %lu\n",
                current->comm,
                free,
                (unsigned long)&free - (unsigned long)end_of_stack(current));
}
#endif

/****** Stack implementation, designed to hold array indices ******/
typedef struct castle_uint32t_stack_t{
    uint32_t *_stack;
    uint32_t top; /* can't be bothered to provide an 'is_empty'; just check this! */
    uint32_t _max_top;
} c_uint32_stack;
int      castle_uint32_stack_construct(c_uint32_stack *stack, uint32_t size);
void     castle_uint32_stack_destroy(c_uint32_stack *stack);
void     castle_uint32_stack_push(c_uint32_stack *stack, uint32_t new_element);
uint32_t castle_uint32_stack_top_val_ret(c_uint32_stack *stack);
uint32_t castle_uint32_stack_pop(c_uint32_stack *stack);
void     castle_uint32_stack_reset(c_uint32_stack *stack);
/******************************************************************/


void castle_key_ptr_destroy(struct castle_key_ptr_t *key_ptr);
void castle_key_ptr_ref_cp(struct castle_key_ptr_t *dest, struct castle_key_ptr_t *src);
void * castle_alloc_func(size_t size);
void castle_free_func(void *ptr);

void *castle_alloc_maybe_func(size_t len, void *dst, size_t *dst_len);
void *castle_dup_or_copy_func(const void *src, size_t src_len, void *dst, size_t *dst_len);

/**
 * Store per-level castle_printk() ratelimit state.
 */
struct castle_printk_state {
    int             ratelimit_jiffies;
    int             ratelimit_burst;
    int             missed;             /**< Number of missed printk() since last message.  */
    unsigned long   toks;
    unsigned long   last_msg;
};

/**
 * Defines castle_printk() log levels.
 *
 * @TODO Level ordering and MIN_CONS_LEVEL will need revising before release.
 */
typedef enum {
    LOG_DEBUG = 0,  /**< Debug-related messages                    */
    LOG_INFO,       /**< Filesystem informational messages         */
    LOG_PERF,       /**< Performance related messages              */
    LOG_DEVEL,      /**< Ephemeral development messages            */
    LOG_USERINFO,   /**< Information messages aimed at the user    */
    LOG_WARN,       /**< Filesystem warnings                       */
    LOG_INIT,       /**< Init()/fini() messages                    */
    LOG_ERROR,      /**< Major error messages                      */
    MAX_CONS_LEVEL  /**< Counts number of levels (has to be last). */
} c_printk_level_t;
#define MIN_CONS_LEVEL  LOG_PERF    /**< Minimum log level to hit the system console.   */

void castle_printk(c_printk_level_t level, const char *fmt, ...);
int castle_printk_init(void);
void castle_printk_fini(void);

void        castle_counter_accumulating_reduce(c_val_tup_t *accumulator,
                                               c_val_tup_t delta_cvt,
                                               int delta_ancestoral);
int         castle_counter_simple_reduce(c_val_tup_t *accumulator, c_val_tup_t delta_cvt);

void        castle_component_tree_prefetch(struct castle_component_tree *ct);

inline void list_swap(struct list_head *t1, struct list_head *t2);
void        list_sort(struct list_head *list,
                      int (*compare)(struct list_head *l1, struct list_head *l2));

void        skb_print(struct sk_buff *skb);
void        vl_bkey_print(c_printk_level_t level, const c_vl_bkey_t *key);

c_val_tup_t convert_to_cvt(uint8_t type, uint64_t length, c_ext_pos_t cep, void *inline_ptr,
                           castle_user_timestamp_t user_timestamp);

int         castle_from_user_copy(const char __user *from, int len, int max_len, char **to);

void        castle_wake_up_task(struct task_struct *task, int inhibit_cs);

void        castle_unmap_vm_area(void *addr_p, int nr_pages);
int         castle_map_vm_area(void *addr_p, struct page **pages, int nr_pages, pgprot_t prot);

uint32_t    murmur_hash_32(const void *key, int len, uint32_t seed);
uint64_t    murmur_hash_64(const void *key, int len, uint32_t seed);
void        castle_atomic64_max(uint64_t new_val, atomic64_t *v);
void        castle_atomic64_min(uint64_t new_val, atomic64_t *v);

#endif /* __CASTLE_UTILS_H__ */
