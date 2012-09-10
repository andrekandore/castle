#ifndef __CASTLE_EVENTS_H__
#define __CASTLE_EVENTS_H__

#include <linux/kobject.h>

void castle_uevent6(uint16_t cmd, uint64_t arg1, uint64_t arg2, uint64_t arg3, uint64_t arg4,
                    uint64_t arg5, uint64_t arg6);
void castle_uevent5(uint16_t cmd, uint64_t arg1, uint64_t arg2, uint64_t arg3, uint64_t arg4,
                    uint64_t arg5);
void castle_uevent4(uint16_t cmd, uint64_t arg1, uint64_t arg2, uint64_t arg3, uint64_t arg4);
void castle_uevent3(uint16_t cmd, uint64_t arg1, uint64_t arg2, uint64_t arg3);
void castle_uevent2(uint16_t cmd, uint64_t arg1, uint64_t arg2);
void castle_uevent1(uint16_t cmd, uint64_t arg1);
void castle_events_slave_rebuild_notify(void);

int  castle_netlink_init(void);
void castle_netlink_fini(void);

/* Events which do not correspond to any particular command. Defined in 0x80+ range not
   to overlap with IOCTL command ids. */
#define CASTLE_EVENT_SPINUP                 (128)
#define CASTLE_EVENT_SPINDOWN               (129)
#define CASTLE_EVENT_TRANSFER_FINISHED      (130)

#define CASTLE_EVENT_NEW_TREE_ADDED         (131)
#define CASTLE_EVENT_MERGE_WORK_FINISHED    (132)
#define CASTLE_EVENT_VERSION_TREE_CREATED   (133)
#define CASTLE_EVENT_VERSION_TREE_DESTROYED (134)
#define CASTLE_EVENT_TREE_DELETED           (135)

/* Events delivered to the ctrl prog go through a different interface (netlink socket).
   This range controls which ones exactly. */
#define CASTLE_CTRL_PROG_EVENT_RANGE_START   CASTLE_EVENT_NEW_TREE_ADDED
#define CASTLE_CTRL_PROG_EVENT_RANGE_END     CASTLE_EVENT_TREE_DELETED

#define CASTLE_EVENTS_SUCCESS (0)

#define CASTLE_EVENTS_NOOP do { } while (0)

#define castle_events_slave_claim(_slave) \
    CASTLE_EVENTS_NOOP

#define castle_events_slave_changed(_slave) \
    kobject_uevent(&(_slave)->kobj, KOBJ_CHANGE)

#define castle_events_slave_rebuild(_slave) \
    kobject_uevent(&(_slave)->kobj, KOBJ_OFFLINE)

#define castle_events_collection_attach(_collection) \
    CASTLE_EVENTS_NOOP

#define castle_events_collection_reattach(_collection) \
    kobject_uevent(&(_collection)->kobj, KOBJ_CHANGE)

#define castle_events_collection_detach(_collection) \
    CASTLE_EVENTS_NOOP

#define castle_events_version_create(_version) \
    CASTLE_EVENTS_NOOP

#define castle_events_version_delete_version(_version) \
    CASTLE_EVENTS_NOOP

#define castle_events_init() \
    CASTLE_EVENTS_NOOP

#define castle_events_new_tree_added(_array_id, _da_id) \
    castle_uevent3(CASTLE_EVENT_NEW_TREE_ADDED, CASTLE_EVENTS_SUCCESS, _array_id, _da_id)

#define castle_events_tree_deleted(_array_id, _da_id) \
    castle_uevent3(CASTLE_EVENT_TREE_DELETED, CASTLE_EVENTS_SUCCESS, _array_id, _da_id)

#define castle_events_merge_work_finished(_da_id, _merge_id, _work_id, _work_done, _is_merge_finished) \
    castle_uevent6(CASTLE_EVENT_MERGE_WORK_FINISHED, CASTLE_EVENTS_SUCCESS, _da_id, _merge_id, _work_id, _work_done, _is_merge_finished)

#define castle_events_version_tree_created(_da_id) \
    castle_uevent2(CASTLE_EVENT_VERSION_TREE_CREATED, CASTLE_EVENTS_SUCCESS, _da_id)

#define castle_events_version_tree_destroyed(_da_id) \
    castle_uevent2(CASTLE_EVENT_VERSION_TREE_DESTROYED, CASTLE_EVENTS_SUCCESS, _da_id)

#endif /* __CASTLE_EVENTS_H__ */
