/*
 * castle_compat.c
 *
 * Compatibility workarounds for older versions of the Linux kernel (namely, the 2.6.18
 * kernel used by CentOS 5).
 */

#include <asm/bug.h>
#include <asm/system.h>
#include <linux/bitops.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/percpu.h>
#include <linux/sched.h>
#include <linux/spinlock.h>
#include <linux/wait.h>
#include "castle_compat.h"

/*
 * queue_work_on()
 */

#if LINUX_VERSION_CODE <= KERNEL_VERSION(2,6,18)
/*
 * The per-CPU workqueue (if single thread, we always use the first
 * possible cpu).
 *
 * The sequence counters are for flush_scheduled_work().  It wants to wait
 * until until all currently-scheduled works are completed, but it doesn't
 * want to be livelocked by new, incoming ones.  So it waits until
 * remove_sequence is >= the insert_sequence which pertained when
 * flush_scheduled_work() was called.
 */
struct cpu_workqueue_struct {

    spinlock_t lock;

    long remove_sequence;       /* Least-recently added (next to run) */
    long insert_sequence;       /* Next to add */
#ifndef __GENKSYMS__
    struct work_struct *current_work;
#endif

    struct list_head worklist;
    wait_queue_head_t more_work;
    wait_queue_head_t work_done;

    struct workqueue_struct *wq;
    struct task_struct *thread;

    int run_depth;              /* Detect run_workqueue() recursion depth */
} ____cacheline_aligned;

/*
 * The externally visible workqueue abstraction is an array of
 * per-CPU workqueues:
 */
struct workqueue_struct {
    struct cpu_workqueue_struct *cpu_wq;
    const char *name;
    struct list_head list;      /* Empty if single thread */
};

/*
 * INIT_WORK() doesn't initialize work->wq_data, and we can't change
 * this helper due to kabi problems. That is why we use bit 1 to mark
 * this work as having the valid ->wq_data == cwq.
 */
static void set_wq_data(struct work_struct *work,
                        struct cpu_workqueue_struct *cwq)
{
    work->wq_data = cwq;
    if (!test_bit(1, &work->pending)) {
        smp_wmb();              /* get_wq_data()->rmb() */
        set_bit(1, &work->pending);
    }
}

/* Preempt must be disabled. */
static void __queue_work(struct cpu_workqueue_struct *cwq,
                         struct work_struct *work)
{
    unsigned long flags;

    spin_lock_irqsave(&cwq->lock, flags);
    set_wq_data(work, cwq);
    /*
     * Ensure that we get the right work->wq_data if we see the
     * result of list_add() below, see try_to_grab_pending().
     */
    smp_wmb();
    list_add_tail(&work->entry, &cwq->worklist);
    cwq->insert_sequence++;
    wake_up(&cwq->more_work);
    spin_unlock_irqrestore(&cwq->lock, flags);
}

/**
 * Queue work on specific cpu.
 * @param cpu       CPU number to execute work on
 * @param wq        workqueue to use
 * @param work      work to queue
 *
 * Returns 0 if work was already on a queue, non-zero otherwise.
 *
 * We queue the work to a specific CPU, the caller must ensure it
 * can't go away.
 */
int queue_work_on(int cpu, struct workqueue_struct *wq, struct work_struct *work)
{
    int ret = 0;

    if (!test_and_set_bit(0, &work->pending)) {
        BUG_ON(!list_empty(&work->entry));
        __queue_work(per_cpu_ptr(wq->cpu_wq, cpu), work);
        ret = 1;
    }

    return ret;
}
#endif  /* LINUX_VERSION_CODE <= KERNEL_VERSION(2,6,18) */

/*
 * add_uevent_var_env()
 */

#if LINUX_VERSION_CODE <= KERNEL_VERSION(2,6,18)
/**
 * Add key value string to the environment buffer.
 * @param env       environment buffer structure
 * @param format    printf format for the key=value pair
 *
 * Returns 0 if environment variable was added successfully or -ENOMEM
 * if no space was available.
 */
int add_uevent_var_env(struct kobj_uevent_env *env, const char *format, ...)
{
    va_list args;
    int len;

    if (env->envp_idx >= ARRAY_SIZE(env->envp)) {
        printk(KERN_ERR "add_uevent_var: too many keys\n");
        WARN_ON(1);
        return -ENOMEM;
    }

    va_start(args, format);
    len = vsnprintf(&env->buf[env->buflen],
                    sizeof(env->buf) - env->buflen,
                    format, args);
    va_end(args);

    if (len >= (sizeof(env->buf) - env->buflen)) {
        printk(KERN_ERR "add_uevent_var: buffer size too small\n");
        WARN_ON(1);
        return -ENOMEM;
    }

    env->envp[env->envp_idx++] = &env->buf[env->buflen];
    env->buflen += len + 1;
    return 0;
}
#endif  /* LINUX_VERSION_CODE <= KERNEL_VERSION(2,6,18) */
