#include <linux/list.h>
#include <asm/tlbflush.h>
#include <linux/vmalloc.h>

#include "castle_public.h"
#include "castle_utils.h"
#include "castle_cache.h"
#include "castle_btree.h"
#include "castle_objects.h"
#include "castle.h"

/**
 * Reinit and drop ref on a castle_key_ptr_t.
 */
void castle_key_ptr_destroy(struct castle_key_ptr_t *key_ptr)
{
    BUG_ON(!key_ptr);
    BUG_ON(key_ptr->node_c2b == NULL);
    BUG_ON(key_ptr->key      == NULL);

    put_c2b(key_ptr->node_c2b);
    key_ptr->node_c2b   = NULL;
    key_ptr->key        = NULL;
    key_ptr->node_size  = 0;
}
/**
 * Reference copy a castle_key_ptr_t.
 */
void castle_key_ptr_ref_cp(struct castle_key_ptr_t *dest, struct castle_key_ptr_t *src)
{
    BUG_ON(!src);
    BUG_ON(!dest);
    BUG_ON(src->node_c2b == NULL);
    BUG_ON(src->key      == NULL);
    dest->node_c2b   = src->node_c2b;
    dest->key        = src->key;
    dest->node_size = src->node_size;
    get_c2b(dest->node_c2b);
}

/**
 * Allocate bytes using malloc(), falling back to vmalloc() as appropriate.
 */
void * castle_alloc_func(size_t size)
{
    void *buf;

    if (likely(size <= MAX_KMALLOC_SIZE))
    {
        buf = kmalloc(size, GFP_KERNEL);
        if (likely(!buf))
            buf = vmalloc(size);
    }
    else
        buf = vmalloc(size);

    return buf;
}

/**
 * Deallocate buffer previously allocated via castle_alloc_func().
 */
void castle_free_func(void *ptr)
{
    unsigned long addr = (unsigned long) ptr;

    if (likely(addr >= VMALLOC_START && addr < VMALLOC_END))
        vfree(ptr);
    else
        kfree(ptr);
}

/*****
 * castle_printk()
 ****/

struct castle_printk_buffer     printk_buf;
struct castle_printk_state     *castle_printk_states;

/**
 * Determines what CVT type should be used for counter composed of two sub-counters.
 * The type of sub-counters (add/set) are provided.
 */
static inline uint8_t castle_counter_accumulating_type_get(int counter1_set, int counter2_set)
{
    if(counter1_set && counter2_set)
        return CVT_TYPE_COUNTER_ACCUM_SET_SET;

    if(!counter1_set && counter2_set)
        return CVT_TYPE_COUNTER_ACCUM_ADD_SET;

    if(!counter1_set && !counter2_set)
        return CVT_TYPE_COUNTER_ACCUM_ADD_ADD;

    BUG();
}

/**
 * Accumulates delta CVT (which may or may not be a counter) into the accumulator.
 *
 * Accumulation accounts for whether the delta is ancestral, or in precisely the same
 * version as the accumulator.
 *
 * The semantics is:
 * - if delta isn't a counter, the accumulator becomes SET/SET. Values of both
 *   sub-counters remain unchanged.
 * - otherwise delta is a counter. If delta is ancestral (i.e. not in the same
 *   version as the accumulator) only the all-v sub-counter will be accumulated.
 *   Otherwise both are accumulated.
 * - individual sub-counters are accumulated separately, according to the standard
 *   semantics, as per castle_counter_simple_reduce().
 *
 * NB: Its expected that accumulator is a composite counter, with the first sub-counter
 * (accumulating just one version) being an add. Next, if delta is a counter, it
 * must be a composite counter.
 */
void castle_counter_accumulating_reduce(c_val_tup_t *accumulator,
                                        c_val_tup_t delta_cvt,
                                        int delta_ancestoral)
{
    int64_t delta_counter1, delta_counter2, accumulator_counter1, accumulator_counter2;
    int counter1_set, counter2_set;

    /* Accumulator must be an accumulating counter. */
    BUG_ON(!CVT_ACCUM_COUNTER(*accumulator));
    /* If delta is a counter it must be an accumulating counter. */
    BUG_ON(CVT_ANY_COUNTER(delta_cvt) && !CVT_ACCUM_COUNTER(*accumulator));

    /* Set/set accumulator shouldn't be reduced any more. Also, it shouldn't really be used
       with this function. Warn. */
    if(CVT_COUNTER_ACCUM_SET_SET(*accumulator))
    {
        castle_printk(LOG_WARN, "SET/SET accumulator used in reductions.\n");
        return;
    }

    /* Work out the types of subcounters in the accumulator. */
    counter1_set = 0;
    counter2_set = CVT_COUNTER_ACCUM_ADD_SET(*accumulator);

    /* If delta isn't a counter, finish reduction early. */
    if(!CVT_ANY_COUNTER(delta_cvt))
    {
         /* Change the type to SET/SET.
           NOTE: this must be a SET/SET because in the case of replacing a entry in T0
                 non-counters will be lost. Therefore _both_ sub-counters need to become
                 sets.
         */
        accumulator->type = castle_counter_accumulating_type_get(1, 1);
        return;
    }

    /* Proper reduction neccessary. Work out what both of the subcounts of both counters are. */
    memcpy(&delta_counter1, CVT_INLINE_VAL_PTR(delta_cvt),   8);
    memcpy(&delta_counter2, CVT_INLINE_VAL_PTR(delta_cvt)+8, 8);
    memcpy(&accumulator_counter1, CVT_INLINE_VAL_PTR(*accumulator)  , 8);
    memcpy(&accumulator_counter2, CVT_INLINE_VAL_PTR(*accumulator)+8, 8);

    /* Merge the first sub-counter only if the delta is precisely the same version. */
    if(!delta_ancestoral)
    {
        /* First counter must be an add. */
        BUG_ON(counter1_set);
        /* Accumulate. */
        accumulator_counter1 += delta_counter1;
        memcpy(CVT_INLINE_VAL_PTR(*accumulator), &accumulator_counter1, 8);
        /* The first counter inherits the type from the delta counter. */
        counter1_set = CVT_COUNTER_ACCUM_SET_SET(delta_cvt);
    }
    /* Merge the second sub-counter only if the second sub-counter in the accumulator isn't
       already a set. */
    if(!counter2_set)
    {
        accumulator_counter2 += delta_counter2;
        memcpy(CVT_INLINE_VAL_PTR(*accumulator)+8, &accumulator_counter2, 8);
        /* The second counter becomes a set if the second counter in delta is also a set. */
        counter2_set = CVT_COUNTER_ACCUM_SET_SET(delta_cvt) ||
                       CVT_COUNTER_ACCUM_ADD_SET(delta_cvt);
    }
    /* Accumulator values have already been updated, update the type. */
    accumulator->type = castle_counter_accumulating_type_get(counter1_set, counter2_set);
}

/**
 * Accumulates delta_cvt (which may or may not be a counter) into the accumulator.
 * The accumulator is expected to be a local add counter.
 *
 * Reduction semantics is:
 * - if delta_cvt is not a counter, its effect is identical to set_0 counter,
 * - accumulator value is incremented by delta
 * - type of the accumulator is changed from add to a set if the delta is a set
 *
 * @return True if accumulator bacame a set (reduction terminated).
 */
int castle_counter_simple_reduce(c_val_tup_t *accumulator, c_val_tup_t delta_cvt)
{
    int64_t delta_counter;

    if(!CVT_ANY_COUNTER(delta_cvt))
    {
        /* Set the accumulator type to LOCAL_SET. */
        CVT_COUNTER_LOCAL_SET_INIT(*accumulator, accumulator->counter)
        return 1;
    }

    /* Delta counter shouldn't be an accumulating counter, because we don't know which
       of the two sub-counters to use. */
    BUG_ON(CVT_ACCUM_COUNTER(delta_cvt));

    /* Accumulator should be an add (otherwise we shouldn't be accumulating any more). */
    BUG_ON(!CVT_ADD_ALLV_COUNTER(*accumulator));

    /* The value length should be 8 bytes. */
    BUG_ON(delta_cvt.length != 8);

    /* Get the counter out of the CVT. */
    memcpy(&delta_counter, CVT_INLINE_VAL_PTR(delta_cvt), 8);
    /* Add it to the accumulator. */
    accumulator->counter += delta_counter;

    /* Stop accumulating if we reached a set. */
    if(CVT_SET_COUNTER(delta_cvt))
    {
        /* Set the accumulator type to LOCAL_SET. */
        CVT_COUNTER_LOCAL_SET_INIT(*accumulator, accumulator->counter)
        return 1;
    }

    return 0;
}

/**
 * Prefetch extents associated with CT.
 *
 * NOTE: Caller must hold a reference on CT.
 */
void castle_component_tree_prefetch(struct castle_component_tree *ct)
{
    c_ext_pos_t cep;
    int chunks;

    castle_printk(LOG_DEVEL, "Prefetching ct=%p da_id=0x%x\n", ct, ct->da);

    /* Prefetch all extents from beginning. */
    cep.offset = 0;

    cep.ext_id = ct->internal_ext_free.ext_id;
    chunks = (atomic64_read(&ct->internal_ext_free.used) / C_CHK_SIZE) + 1;
    castle_cache_prefetch_pin(cep, chunks, C2_ADV_PREFETCH);

    cep.ext_id = ct->tree_ext_free.ext_id;
    chunks = (atomic64_read(&ct->tree_ext_free.used) / C_CHK_SIZE) + 1;
    castle_cache_prefetch_pin(cep, chunks, C2_ADV_PREFETCH);

    cep.ext_id = ct->data_ext_free.ext_id;
    chunks = (atomic64_read(&ct->data_ext_free.used) / C_CHK_SIZE) + 1;
    castle_cache_prefetch_pin(cep, chunks, C2_ADV_PREFETCH);

    /* Waits for all outstanding prefetch IOs to complete. */
    castle_cache_prefetches_wait();
}

/**
 * Determine whether to ratelimit printks at specified level.
 *
 * @param   level   castle_printk() level
 *
 * NOTE: Adapted from 2.6.18 __printk_ratelimit() code.
 *
 * @return  0       Caller should not issue printk() call
 * @return  1       Caller can issue printk() call
 *
 * @also castle_printk()
 */
static int castle_printk_ratelimit(c_printk_level_t level)
{
    struct castle_printk_state *state = &castle_printk_states[level];
    unsigned long now = jiffies;

    state->toks    += now - state->last_msg;
    state->last_msg = now;
    if (state->toks > (state->ratelimit_burst * state->ratelimit_jiffies))
        state->toks  = state->ratelimit_burst * state->ratelimit_jiffies;
    if (state->toks >= state->ratelimit_jiffies)
    {
        int lost = state->missed;

        state->missed = 0;
        state->toks -= state->ratelimit_jiffies;
        if (lost)
            printk(KERN_WARNING "printk: %d level %d messages suppressed.\n", lost, level);
        return 1;
    }
    state->missed++;
    return 0;
}

/**
 * Print to dmesg and castle ring buffer.
 *
 * @param msg   Message to print
 *
 * @also castle_printk_init()
 * @also castle_printk_fini()
 * @also castle_printk_ratelimit()
 *
 * @TODO timestamp
 * @TODO castle-trace handler
 */
void castle_printk(c_printk_level_t level, const char *fmt, ...)
{
    c_byte_off_t len, rem, pos = 0;
    unsigned long flags;
    char tmp_buf[1024];
    va_list args;

    BUG_ON(PRINTK_BUFFER_SIZE < sizeof(tmp_buf));

    va_start(args, fmt);
    len = (c_byte_off_t) vscnprintf(tmp_buf, sizeof(tmp_buf), fmt, args) + 1; /* +1 for '\0'$ */
    va_end(args);

    /* Serialise access to the ring buffer. */
    spin_lock_irqsave(&printk_buf.lock, flags);

    rem = PRINTK_BUFFER_SIZE - printk_buf.off;
    if (unlikely(len > rem))
    {
        /* Write up to the end of the buffer if it cannot handle a whole write.
         * We'll update the various pointers and continue the write outside of
         * this 'overflow' handler (below). */

        memcpy(&printk_buf.buf[printk_buf.off], &tmp_buf[pos], rem);

        pos += rem;
        len -= rem;

        printk_buf.off = 0;
        printk_buf.wraps++;

        printk("Wrapping castle_printk() ring buffer.\n");
    }

    /* Copy message to the printk buffer. */
    memcpy(&printk_buf.buf[printk_buf.off], &tmp_buf[pos], len);
    printk_buf.off += len - 1; /* -1 for '\0'$ */

    spin_unlock_irqrestore(&printk_buf.lock, flags);

    // @TODO castle-trace output here

    /* Only print warnings, errors and testing messages to the console. */
    if (level >= MIN_CONS_LEVEL)
    {
        /* and then only printk() if we're within the ratelimit. */
        if (castle_printk_ratelimit(level))
            printk("%s", tmp_buf);
    }
}

/**
 * Dump CASTLE_DMESG_DUMP_SIZE worth of the Castle dmesg ring-buffer.
 */
#define CASTLE_DMESG_DUMP_SIZE  (1*1024*1024)   /**< How much of the printk buffer to dump (1MB). */
void castle_dmesg(void)
{
    c_byte_off_t read_off, write_off, size;
    char *buf, line[1024];
    unsigned long flags;
    int wraps;

    /* Allocate a buffer to store CASTLE_DMESG_DUMP_SIZE of buffer. */
    buf = castle_alloc(CASTLE_DMESG_DUMP_SIZE);
    if (!buf)
        printk("Couldn't allocate lines buffer to print castle_dmesg().\n");

    /* Populate the local buffer under printk_buf lock. */
    spin_lock_irqsave(&printk_buf.lock, flags);

    /* Fill at end, in case we wrapped and need to push more at the front. */
    if (printk_buf.off >= CASTLE_DMESG_DUMP_SIZE)
        size = CASTLE_DMESG_DUMP_SIZE;
    else
        size = printk_buf.off;
    read_off = printk_buf.off - size;
    write_off = CASTLE_DMESG_DUMP_SIZE - size;
    memcpy(&buf[write_off], &printk_buf.buf[read_off], size);

    /* Fill the front of the local buffer from the end of the printk_buf if
     * we wrapped. */
    if (write_off && printk_buf.wraps)
    {
        size = write_off;
        read_off = printk_buf.size - size;
        write_off = 0;

        memcpy(&buf[write_off], &printk_buf.buf[read_off], size);
    }

    wraps = printk_buf.wraps;
    read_off = write_off;

    spin_unlock_irqrestore(&printk_buf.lock, flags);

    printk("================================================================================\n");
    printk("DUMPING %lld BYTES OF CASTLE PRINTK BUFFER WRAPPED %d TIME(S)\n",
            CASTLE_DMESG_DUMP_SIZE - read_off, wraps);
    printk("================================================================================\n");

    /* Dump the buffer in lines as large as possible (1024). */
    while (read_off < CASTLE_DMESG_DUMP_SIZE - 1)
    {
        if (likely(read_off + 1024 < CASTLE_DMESG_DUMP_SIZE))
            size = 1024;
        else
            size = CASTLE_DMESG_DUMP_SIZE - read_off;

        memset(&line, 0, 1024);
        memcpy(&line, &buf[read_off], size);
        read_off += size - 1;

        printk("%s", line);
    }

    printk("================================================================================\n");
    printk("END OF CASTLE PRINTK BUFFER\n");
    printk("================================================================================\n");

    castle_free(buf);
}

/**
 * Initialise ring buffer and level states for castle_printk().
 *
 * NOTE: We must call vmalloc() directly as we are the first subsystem
 *       that gets initialised on start-up.
 */
int castle_printk_init(void)
{
    int i;

    BUG_ON(printk_buf.buf);
    BUG_ON(castle_printk_states);

    /* castle_printk() buffer. */
    printk_buf.off      = 0;
    printk_buf.wraps    = 0;
    printk_buf.size     = PRINTK_BUFFER_SIZE;
    printk_buf.buf      = vmalloc(printk_buf.size);
    if (!printk_buf.buf)
        goto err1;
    spin_lock_init(&printk_buf.lock);

    /* castle_printk_ratelimit() level states. */
    castle_printk_states = vmalloc(sizeof(struct castle_printk_state) * MAX_CONS_LEVEL);
    if (!castle_printk_states)
        goto err2;
    for (i = 0; i < MAX_CONS_LEVEL; i++)
    {
        castle_printk_states[i].ratelimit_jiffies   = HZ/PRINTKS_PER_SEC_STEADY_STATE;
        castle_printk_states[i].ratelimit_burst     = PRINTKS_IN_BURST;
        castle_printk_states[i].missed              = 0;
        castle_printk_states[i].toks                = 10 * 5 * HZ;
        castle_printk_states[i].last_msg            = 0;
    }

    castle_printk(LOG_INIT, "Initialised Castle printk ring buffer.\n");

    return 0;

err2:
    vfree(printk_buf.buf);
err1:
    return -ENOMEM;
}

/**
 * Free ring buffer and level states for printk.
 */
void castle_printk_fini(void)
{
    BUG_ON(!castle_printk_states);
    BUG_ON(!printk_buf.buf);

    castle_printk(LOG_INIT, "Freeing Castle printk ring buffer.\n");

    vfree(castle_printk_states);
    castle_printk_states = NULL;
    vfree(printk_buf.buf);
    printk_buf.buf = NULL;
}



inline void  __list_swap(struct list_head *p,
                         struct list_head *t1,
                         struct list_head *t2,
                         struct list_head *n)
{
    p->next  = t2;
    t2->prev = p;
    t2->next = t1;
    t1->prev = t2;
    t1->next = n;
    n->prev  = t1;
}

inline void list_swap(struct list_head *t1, struct list_head *t2)
{
    __list_swap(t1->prev, t1, t2, t2->next);
}


/* Implements O(n^2) list sort using externally provided comparator */
void list_sort(struct list_head *list,
               int (*compare)(struct list_head *l1, struct list_head *l2))
{
    struct list_head *t1, *t2;
    int length;
    int i, j;

    /* Length of the list */
    for(length=0, t1=list->next; t1 != list; length++, t1=t1->next);

    /* 0 & 1 long lists are already sorted */
    if(length <= 1)
        return;

    /* Bubble sort */
    for(i=0; i<length-1; i++)
    {
        t1 = list->next;
        for(j=length; j>i+1; j--)
        {
            t2 = t1->next;
            /* Potentially swap */
            if(compare(t1, t2) > 0)
                /* t1 should remain unchanged (it's going to be moved forward) */
                list_swap(t1, t2);
            else
                t1 = t2;
        }
    }
}

void skb_print(struct sk_buff *skb)
{
    int i;
    uint8_t byte;

    castle_printk(LOG_DEBUG, "\nPacket length=%d\n", skb->len);
    for(i=0; i<skb->len; i++)
    {
        BUG_ON(skb_copy_bits(skb, i, &byte, 1) < 0);
        if((byte >= 32) && (byte <= 126))
            castle_printk(LOG_DEBUG, " [%d]=%d (%c)\n", i, byte, byte);
        else
            castle_printk(LOG_DEBUG, " [%d]=%d\n", i, byte);
    }
    castle_printk(LOG_DEBUG, "\n");
}

#if 0
void vl_key_print(c_printk_level_t level, c_vl_key_t *vl_key)
{
    castle_printk(level, " key len=%d: ", vl_key->length);
    print_hex_dump_bytes("", DUMP_PREFIX_NONE, vl_key->key, vl_key->length);
}

EXPORT_SYMBOL(vl_okey_print);

void vl_bkey_print(c_printk_level_t level, c_vl_bkey_t *key)
{
    c_vl_okey_t *okey;

    okey = castle_object_btree_key_convert(key);
    if(!okey)
    {
        castle_printk(level, "Couldn't convert btree key for printing.\n");
        return;
    }
    castle_printk(level, "Btree key, length=%d\n", key->length);
    vl_okey_print(level, okey);
    castle_object_okey_free(okey);
}
#endif
/**
 * Parse vl_okey as a string in form "[dim,dim,...dim]".
 *
 * Suitable for piping straight into castle-cli.
 */
void vl_bkey_print(c_printk_level_t level, c_vl_bkey_t *key)
{
    int i, j;
    static char _buf[1024];
    char *buf = _buf;

    if (!key || key->length == 0) /* e.g. for big_put */
    {
        buf = "[null]";
        goto print_and_exit;
    }
    if (key->length == 0xFFFFFFFE)
    {
        buf = "[max key]";
        goto print_and_exit;
    }
    if (key->length == 0xFFFFFFFF)
    {
        buf = "[inval key]";
        goto print_and_exit;
    }

    *buf++ = '[';
    for(i=0; i<key->nr_dims; i++)
    {
        const uint8_t *dim = castle_object_btree_key_dim_get(key, i);

        *buf++ = '0';
        *buf++ = 'x';
        for(j=0; j<castle_object_btree_key_dim_length(key, i); j++)
        {
            sprintf(buf, "%.2x", dim[j]);
            buf += 2;
        }
        *buf++ = ',';
    }
    *(buf-1) = ']';
    *buf++ = '\0';
print_and_exit:
    castle_printk(level, "%s, len=%d\n", _buf, key->length);
}


/**
 * Copies a string out of the userspace, performing checks to verify that string
 * isn't too long or malformed.
 *
 * @param from    Userspace string pointer
 * @param len     String length
 * @param max_len Maximum length allowed
 * @param to      Pointer to the return char pointer
 */
int castle_from_user_copy(const char __user *from, int len, int max_len, char **to)
{
    char *out_str;
    int ret;

    ret = 0;
    /* Check that the string isn't too long. */
    if(len > max_len)
        return -E2BIG;

    /* Allocate memory for the string. */
    out_str = castle_malloc(len, GFP_KERNEL);
    if(!out_str)
        return -ENOMEM;

    /* Copy it from userspace. */
    if(copy_from_user(out_str, from, len))
    {
        ret = -EFAULT;
        goto err_out;
    }

    /* Check that the string finishes with '\0'. */
    if (out_str[len-1] != '\0')
    {
        ret = -EINVAL;
        goto err_out;
    }

    /* Succeeded, returning. */
    *to = out_str;
    return 0;

err_out:
    /* Non-zero return code should have been set. */
    BUG_ON(ret == 0);
    castle_kfree(out_str);
    return ret;
}

/**********************************************************************************************
 * Utilities for vmapping/vunmapping pages. Assumes that virtual address to map/unmap is known.
 * Copied from RHEL mm/vmalloc.c.
 */

void pgd_clear_bad(pgd_t *pgd)
{
    pgd_ERROR(*pgd);
    pgd_clear(pgd);
}

void castle_unmap_vm_area(void *addr_p, int nr_pages)
{
    pgd_t *pgd;
    unsigned long next;
    unsigned long addr = (unsigned long) addr_p;
    unsigned long end = addr + nr_pages * PAGE_SIZE;

    BUG_ON(addr >= end);
    pgd = pgd_offset_k(addr);
    flush_cache_vunmap(addr, end);
    do {
        next = pgd_addr_end(addr, end);
        if (pgd_none_or_clear_bad(pgd))
            continue;
        vunmap_pud_range(pgd, addr, next);
    } while (pgd++, addr = next, addr != end);
    flush_tlb_kernel_range((unsigned long) addr_p, end);
}

int castle_map_vm_area(void *addr_p, struct page **pages, int nr_pages, pgprot_t prot)
{
    pgd_t *pgd;
    unsigned long next;
    unsigned long addr = (unsigned long) addr_p;
    unsigned long end = addr + nr_pages * PAGE_SIZE;
    int err;

    BUG_ON(addr >= end);
    pgd = pgd_offset_k(addr);
    do {
        next = pgd_addr_end(addr, end);
        err = vmap_pud_range(pgd, addr, next, prot, &pages);
        if (err)
            break;
    } while (pgd++, addr = next, addr != end);
    flush_cache_vmap((unsigned long) addr_p, end);
    return err;
}

/* Murmur hash */

#define _rotl64(X,n) ( ( ( X ) << n ) | ( ( X ) >> ( 64 - n ) ) )

//----------
// Block mix - combine the key bits with the hash bits and scramble everything

#define bmix64(h1, h2, k1, k2, c1, c2)  \
({                                      \
    k1 *= c1;                           \
    k1  = _rotl64(k1,23);               \
    k1 *= c2;                           \
    h1 ^= k1;                           \
    h1 += h2;                           \
                                        \
    h2 = _rotl64(h2,41);                \
                                        \
    k2 *= c2;                           \
    k2  = _rotl64(k2,23);               \
    k2 *= c1;                           \
    h2 ^= k2;                           \
    h2 += h1;                           \
                                        \
    h1 = h1*3+0x52dce729;               \
    h2 = h2*3+0x38495ab5;               \
                                        \
    c1 = c1*5+0x7b7d159c;               \
    c2 = c2*5+0x6bce6396;               \
})

//----------
// Finalization mix - avalanches all bits to within 0.05% bias

static inline uint64_t fmix64(uint64_t k)
{
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccd;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53;
    k ^= k >> 33;

    return k;
}

static void MurmurHash3_x64_128(const void *key, const int len, const uint32_t seed, void *out)
{
    const uint8_t * data = (const uint8_t*)key;
    const int nblocks = len / 16;
    const uint8_t *tail;
    uint64_t k1, k2;
    int i;

    uint64_t h1 = 0x9368e53c2f6af274 ^ seed;
    uint64_t h2 = 0x586dcd208f7cd3fd ^ seed;

    uint64_t c1 = 0x87c37b91114253d5;
    uint64_t c2 = 0x4cf5ad432745937f;

    //----------
    // body

    const uint64_t * blocks = (const uint64_t *)(data);

    for(i = 0; i < nblocks; i++)
    {
        uint64_t k1 = blocks[i*2 + 0];
        uint64_t k2 = blocks[i*2 + 1];

        bmix64(h1, h2, k1, k2, c1, c2);
    }

    //----------
    // tail

    tail = (const uint8_t*)(data + nblocks*16);

    k1 = 0;
    k2 = 0;

    switch(len & 15)
    {
    case 15: k2 ^= ((uint64_t) tail[14]) << 48;
    case 14: k2 ^= ((uint64_t) tail[13]) << 40;
    case 13: k2 ^= ((uint64_t) tail[12]) << 32;
    case 12: k2 ^= ((uint64_t) tail[11]) << 24;
    case 11: k2 ^= ((uint64_t) tail[10]) << 16;
    case 10: k2 ^= ((uint64_t) tail[ 9]) << 8;
    case  9: k2 ^= ((uint64_t) tail[ 8]) << 0;

    case  8: k1 ^= ((uint64_t) tail[ 7]) << 56;
    case  7: k1 ^= ((uint64_t) tail[ 6]) << 48;
    case  6: k1 ^= ((uint64_t) tail[ 5]) << 40;
    case  5: k1 ^= ((uint64_t) tail[ 4]) << 32;
    case  4: k1 ^= ((uint64_t) tail[ 3]) << 24;
    case  3: k1 ^= ((uint64_t) tail[ 2]) << 16;
    case  2: k1 ^= ((uint64_t) tail[ 1]) << 8;
    case  1: k1 ^= ((uint64_t) tail[ 0]) << 0;
             bmix64(h1,h2,k1,k2,c1,c2);
    };

    //----------
    // finalization

    h2 ^= len;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    ((uint64_t*)out)[0] = h1;
    ((uint64_t*)out)[1] = h2;
}

//-----------------------------------------------------------------------------
// If we need a smaller hash value, it's faster to just use a portion of the
// 128-bit hash

uint32_t murmur_hash_32(const void *key, int len, uint32_t seed)
{
    uint32_t temp[4];

    MurmurHash3_x64_128(key, len, seed, temp);

    return temp[0];
}

uint64_t murmur_hash_64(const void *key, int len, uint32_t seed)
{
    uint64_t temp[2];

    MurmurHash3_x64_128(key, len, seed, temp);

    return temp[0];
}

/* Append list2 (with head2) to list1 (with head1). Doesnt add head2. */
void list_append(struct list_head *head1, struct list_head *head2)
{
    head2->prev->next = head1;
    head2->next->prev = head1->prev;
    head1->prev->next = head2->next;
    head1->prev       = head2->prev;
}
