#ifndef __CASTLE_COMPAT_H__
#define __CASTLE_COMPAT_H__

#include <linux/version.h>
#include <linux/workqueue.h>

#if LINUX_VERSION_CODE <= KERNEL_VERSION(2,6,18)
int queue_work_on(int cpu, struct workqueue_struct *wq, struct work_struct *work);
#endif

#endif  /* !defined(__CASTLE_COMPAT_H__) */
