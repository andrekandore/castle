#include <linux/kthread.h>

#include "castle_public.h"
#include "castle.h"
#include "castle_time.h"
#include "castle_debug.h"

/* Aggregates stats from multiple timelines, one for each checkpoint */
typedef struct castle_checkpoint_stats {
    struct timespec max;
    struct timespec min;
    struct timespec agg;
    int cnt;
    char *file;
    int line;
} c_check_stats_t;


static       DEFINE_SPINLOCK(castle_timelines_list_lock);
static             LIST_HEAD(castle_timelines_list);
static             LIST_HEAD(castle_dead_timelines_list);
static int                   castle_checkpoint_collisions_print;
static uint32_t              castle_checkpoint_seq;
static struct task_struct   *time_thread;
static c_check_stats_t       castle_checkpoint_stats[MAX_CHECK_POINTS + 1]; 
                                                    /* +1 for the stats on timeline duration */
static atomic_t              castle_checkpoint_create_seq;
#define                      REQUEST_PERIOD         1000   /* Trace every 1000th request */
#define                      PRINT_PERIOD           100    /* Print every 100th trace */

#if LINUX_VERSION_CODE <= KERNEL_VERSION(2,6,18)
/* Copied from the kernel code, because not EXPORTed until ~2.6.24 */
/**
 * set_normalized_timespec - set timespec sec and nsec parts and normalize
 *
 * @ts:     pointer to timespec variable to be set
 * @sec:    seconds to set
 * @nsec:   nanoseconds to set
 *
 * Set seconds and nanoseconds field of a timespec variable and
 * normalize to the timespec storage format
 *
 * Note: The tv_nsec part is always in the range of
 *  0 <= tv_nsec < NSEC_PER_SEC
 * For negative values only the tv_sec field is negative !
 */
void set_normalized_timespec(struct timespec *ts, time_t sec, long nsec)
{
    while (nsec >= NSEC_PER_SEC) {
        nsec -= NSEC_PER_SEC;
        ++sec;
    }
    while (nsec < 0) {
        nsec += NSEC_PER_SEC;
        --sec;
    }
    ts->tv_sec = sec;
    ts->tv_nsec = nsec;
}

/**
 * ns_to_timespec - Convert nanoseconds to timespec
 * @nsec:       the nanoseconds value to be converted
 *
 * Returns the timespec representation of the nsec parameter.
 */
struct timespec ns_to_timespec(const s64 nsec)
{
    struct timespec ts;
                       
    if (!nsec)
        return (struct timespec) {0, 0};

    ts.tv_sec = div_long_long_rem_signed(nsec, NSEC_PER_SEC, &ts.tv_nsec);
    if (unlikely(nsec < 0))
        set_normalized_timespec(&ts, ts.tv_sec, ts.tv_nsec);

    return ts;
}
#endif

static void castle_request_timeline_add(c_req_time_t *timeline)
{
    spin_lock(&castle_timelines_list_lock);
    timeline->seq = castle_checkpoint_seq++;
    list_add(&timeline->list, &castle_timelines_list);
    spin_unlock(&castle_timelines_list_lock);
}

static void castle_request_timeline_del(c_req_time_t *timeline)
{
    spin_lock(&castle_timelines_list_lock);
    list_del(&timeline->list);
    spin_unlock(&castle_timelines_list_lock);
}

static int castle_request_checkpoint_get(c_req_time_t *timeline, char *file, int line)
{
    struct castle_checkpoint *checkpoint;
    int checkpoint_idx;

    /* This may be collision prone, fine for now */
    checkpoint_idx = line % MAX_CHECK_POINTS; 
    checkpoint = &timeline->checkpoints[checkpoint_idx]; 
    /* Check if that's a new checkpoint */
    if(checkpoint->file == NULL)
    {
        checkpoint->file = file;
        checkpoint->line = line;

        return checkpoint_idx;
    }
    /* Check if the checkpoints are the same */
    if((checkpoint->line == line) &&
       (strcmp(checkpoint->file, file) == 0))
    {
        return checkpoint_idx;
    }
    /* Collision! */
    if(castle_checkpoint_collisions_print)
        printk("Checkpoints collided: %s:%d & %s:%d\n", 
                checkpoint->file,
                checkpoint->line,
                file,
                line);
    return -1;
}

c_req_time_t* _castle_request_timeline_create(void)
{
    c_req_time_t* timeline;

    /* Create the timeline when sequence # is divisible by period. */ 
    if(atomic_inc_return(&castle_checkpoint_create_seq) % REQUEST_PERIOD != 0)
        return NULL;
    timeline = castle_zalloc(sizeof(c_req_time_t), GFP_KERNEL);
    if(!timeline)
        return NULL;
    timeline->active_checkpoint = -1;
    INIT_LIST_HEAD(&timeline->list);
    castle_request_timeline_add(timeline);
    getnstimeofday(&timeline->create_tm);

    return timeline;
}

static inline void timespec_next_max(struct timespec *curr, 
                                     struct timespec *store, 
                                     int cnt)
{
    s64 curr_ns, store_ns;

    curr_ns = timespec_to_ns(curr); 
    store_ns = timespec_to_ns(store); 
    if(curr_ns > store_ns)
        *store = ns_to_timespec(curr_ns);
}

static inline void timespec_next_min(struct timespec *curr, 
                                     struct timespec *store, 
                                     int cnt)
{
    s64 curr_ns, store_ns;

    curr_ns = timespec_to_ns(curr); 
    store_ns = timespec_to_ns(store); 
    if(curr_ns < store_ns)
        *store = ns_to_timespec(curr_ns);
}

static inline void timespec_next_avg(struct timespec *curr, 
                                     struct timespec *store, 
                                     int cnt)
{
    s64 curr_ns, store_ns;

    curr_ns = timespec_to_ns(curr); 
    if(cnt > 0)
        store_ns = timespec_to_ns(store); 
    else
        store_ns = 0;

    *store = ns_to_timespec(curr_ns + store_ns);
}

/* Records the start of operation, called from file:line */
void _castle_request_timeline_checkpoint_start(c_req_time_t *timeline,
                                               char *file,
                                               int line)
{
    struct castle_checkpoint *checkpoint;
    int checkpoint_idx;

    if(!timeline)
        return;
    /* Stop should have been called first */
    BUG_ON(timeline->active_checkpoint >= 0);

    checkpoint_idx = castle_request_checkpoint_get(timeline, file, line);
    if(checkpoint_idx < 0)
        return;
    checkpoint = &timeline->checkpoints[checkpoint_idx];    
    /* We checked that we are not in checkpoint ATM, so this should not be active */
    BUG_ON(checkpoint->active);
    checkpoint->active = 1;
    getnstimeofday(&checkpoint->start_tm);
    timeline->active_checkpoint = checkpoint_idx;
}

/* Records the end of sleep called from a particular place */
void castle_request_timeline_checkpoint_stop(c_req_time_t *timeline)
{
    struct castle_checkpoint *checkpoint;
    s64 start_ns, end_ns;
    struct timespec end_tm;

    if(!timeline)
        return;
    if(timeline->active_checkpoint < 0)
    {
        printk("Stopping an inactive checkpoint. Due to a collision?.\n");
        return;
    } 
    checkpoint = &timeline->checkpoints[timeline->active_checkpoint];
    BUG_ON(!checkpoint->active);
    getnstimeofday(&end_tm);

    /* Update the stats */
    start_ns = timespec_to_ns(&checkpoint->start_tm);
    end_ns = timespec_to_ns(&end_tm);
    end_tm = ns_to_timespec(end_ns - start_ns);
    timespec_next_max(&end_tm, &checkpoint->max_tm, checkpoint->cnts);
    timespec_next_min(&end_tm, &checkpoint->min_tm, checkpoint->cnts);
    timespec_next_avg(&end_tm, &checkpoint->aggregate_tm, checkpoint->cnts);

    checkpoint->cnts++;
    checkpoint->active = 0;
    timeline->active_checkpoint = -1;
}

void castle_request_timeline_destroy(c_req_time_t *timeline)
{
    if(!timeline)
        return;
    /* Record the time, and move to the dead list */
    getnstimeofday(&timeline->destroy_tm);
    castle_request_timeline_del(timeline);
    spin_lock(&castle_timelines_list_lock);
    list_add(&timeline->list, &castle_dead_timelines_list);
    spin_unlock(&castle_timelines_list_lock);
    wake_up_process(time_thread);    
}
        
static void castle_request_timeline_process(c_req_time_t *timeline)
{
    struct castle_checkpoint *checkpoint;
    c_check_stats_t *check_stats;
    s64 start_ns, end_ns;
    struct timespec dur_tm;
    int i;

    /* Process the duration of the entire timeline first */
    check_stats = &castle_checkpoint_stats[MAX_CHECK_POINTS];
    BUG_ON(check_stats->line != 0 || check_stats->file != NULL);
    end_ns = timespec_to_ns(&timeline->destroy_tm);
    start_ns = timespec_to_ns(&timeline->create_tm);
    dur_tm = ns_to_timespec(end_ns - start_ns);
    timespec_next_max(&dur_tm, &check_stats->max, check_stats->cnt);
    timespec_next_min(&dur_tm, &check_stats->min, check_stats->cnt);
    timespec_next_avg(&dur_tm, &check_stats->agg, check_stats->cnt);
    check_stats->cnt++;

    /* Now process each of the non-empty checkpoints */
    for(i=0; i<MAX_CHECK_POINTS; i++)
    {
        check_stats = &castle_checkpoint_stats[i];
        checkpoint = &timeline->checkpoints[i];
        
        /* Skip unused checkpoints */
        if(!checkpoint->file)
        {
            BUG_ON(checkpoint->line != 0);
            continue;
        }

        /* Check if checkpoint agrees with the stats */
        if((check_stats->file) &&
           ((checkpoint->line != check_stats->line) ||
            (strcmp(checkpoint->file, check_stats->file) != 0))) 
        {
            printk("Checkpoint %s:%d, doesn't agree with stats %s:%d\n", 
                checkpoint->file, checkpoint->line,
                check_stats->file, check_stats->line);
        }
        
        /* Update the stats */
        BUG_ON(checkpoint->file == NULL || checkpoint->line == 0);
        start_ns = timespec_to_ns(&checkpoint->aggregate_tm);
        dur_tm = ns_to_timespec(start_ns / checkpoint->cnts);
        timespec_next_max(&checkpoint->max_tm, &check_stats->max, check_stats->cnt);
        timespec_next_min(&checkpoint->min_tm, &check_stats->min, check_stats->cnt);
        timespec_next_avg(&dur_tm,             &check_stats->agg, check_stats->cnt);
        check_stats->file = checkpoint->file;
        check_stats->line = checkpoint->line;
        check_stats->cnt++;
    } 
}

static void castle_checkpoint_stats_print(void)
{
    c_check_stats_t *check_stats;
    struct timespec avg;
    s64 agg;
    int i;

    printk("Printing timing statistics.\n");
    for(i=0; i<=MAX_CHECK_POINTS; i++)
    {
        check_stats = &castle_checkpoint_stats[i];
        /* Skip empty stats */
        if(!check_stats->file && (i != MAX_CHECK_POINTS))
        {
            BUG_ON(check_stats->line != 0);
            continue;
        }

        /* Print */
        if(i < MAX_CHECK_POINTS)
            printk("For checkpoint started at %s:%d, samples=%d\n", 
                check_stats->file, check_stats->line, check_stats->cnt);
        else
            printk("For entire timeline, samples=%d:\n", check_stats->cnt);

        if(check_stats->cnt == 0)
        {
            printk("No data yet.\n");
            continue;
        }
        agg = timespec_to_ns(&check_stats->agg);
        avg = ns_to_timespec(agg / check_stats->cnt);
        printk("Average: %.2ld.%.6ld\n", avg.tv_sec, avg.tv_nsec / 1000); 
        printk("Min:     %.2ld.%.6ld\n", check_stats->min.tv_sec, check_stats->min.tv_nsec / 1000); 
        printk("Max:     %.2ld.%.6ld\n", check_stats->max.tv_sec, check_stats->max.tv_nsec / 1000); 
    }
    printk("\n");
}

static void castle_request_timeline_print(c_req_time_t *timeline)
{
    struct castle_checkpoint *checkpoint;
    struct timespec dur_tm;
    s64 dur;
    int i;

    printk("Stats for timeline %d\n", timeline->seq);
    dur = timespec_to_ns(&timeline->destroy_tm) - timespec_to_ns(&timeline->create_tm);
    dur_tm = ns_to_timespec(dur);
    printk("Durat. : %.2ld.%.6ld\n", dur_tm.tv_sec, dur_tm.tv_nsec / 1000); 

    for(i=0; i<MAX_CHECK_POINTS; i++)
    {
        checkpoint = &timeline->checkpoints[i]; 
        if(checkpoint->file == NULL)
            continue;
           
        printk("For checkpoint started at %s:%d\n", checkpoint->file, checkpoint->line);
        dur = timespec_to_ns(&checkpoint->aggregate_tm);
        dur_tm = ns_to_timespec(dur / checkpoint->cnts);
        printk("Average: %.2ld.%.6ld\n", dur_tm.tv_sec, dur_tm.tv_nsec / 1000); 
        printk("Min:     %.2ld.%.6ld\n", 
            checkpoint->min_tm.tv_sec, 
            checkpoint->min_tm.tv_nsec / 1000);
        printk("Max:     %.2ld.%.6ld\n", 
            checkpoint->max_tm.tv_sec, 
            checkpoint->max_tm.tv_nsec / 1000);
    }
}

static int castle_time_run(void *unused)
{
    c_req_time_t *timeline;
    int cnt = 0;

    while(1)
    {
        timeline = NULL;
        spin_lock(&castle_timelines_list_lock);
        if(!list_empty(&castle_dead_timelines_list))
        {
            timeline = list_entry(castle_dead_timelines_list.next, c_req_time_t, list);
            list_del(&timeline->list);
        }
        spin_unlock(&castle_timelines_list_lock);
        
        if(!timeline)
            goto no_timelines;

        /* Process the timeline */
        castle_request_timeline_process(timeline);
        if(cnt % PRINT_PERIOD == 0)
        {
            castle_request_timeline_print(timeline); 
            castle_checkpoint_stats_print();
        }
        castle_free(timeline);
        cnt++;

        /* Go to the next timeline */
        continue;

no_timelines:
        /* Go to sleep or exit the thread */
        if(!kthread_should_stop())
        {
            set_task_state(current, TASK_INTERRUPTIBLE);
            schedule();
        } else
            return 0;
    }
}

int castle_time_init(void)
{
    time_thread = kthread_run(castle_time_run, NULL, "castle-perf-debug");
    if(!time_thread)
        return -ENOMEM;

    atomic_set(&castle_checkpoint_create_seq, 0);
    castle_checkpoint_collisions_print = 1;
    castle_checkpoint_seq = 0;
    memset(castle_checkpoint_stats, 0, sizeof(castle_checkpoint_stats));

    return 0;
}

void castle_time_fini(void)
{
    kthread_stop(time_thread);
    spin_lock(&castle_timelines_list_lock);
    if(!list_empty(&castle_timelines_list))
        printk("WARNING: Haven't destroyed all the timelines before exiting.\n");
    spin_unlock(&castle_timelines_list_lock);
}


