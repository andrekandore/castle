#ifndef __CASTLE_INSTREAM_H__
#define __CASTLE_INSTREAM_H__

#include "castle.h"
#include "castle_defines.h"
#include "castle_utils.h"
#include "castle_btree.h"
#include "castle_public.h"

typedef struct castle_instream_batch_processor
{
    char   *batch_buf;
    size_t  batch_buf_len_bytes;
    char   *cursor;
    size_t  bytes_consumed;
    struct  castle_immut_tree_construct *da_stream;
} c_instream_batch_proc;

void castle_instream_batch_proc_construct(c_instream_batch_proc *batch_proc,
                                          char* buf,
                                          size_t buf_len_bytes,
                                          struct  castle_immut_tree_construct *da_stream);
int castle_instream_batch_proc_next(c_instream_batch_proc *batch_proc, void ** raw_key, c_val_tup_t *cvt);
void castle_instream_batch_proc_destroy(c_instream_batch_proc *batch_proc);

#endif
