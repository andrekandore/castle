#include <linux/kthread.h>
#include <linux/bio.h>
#include <linux/net.h>
#include <linux/socket.h>
#include <linux/time.h>
#include <net/sock.h>
#include <linux/rxrpc.h>
#include <net/af_rxrpc.h>
#include <linux/errqueue.h>
#include <rxrpc/packet.h>
#include <linux/sched.h>

#include "castle_public.h"
#include "castle_utils.h"
#include "castle.h"
#include "castle_debug.h"
#include "castle_btree.h"
#include "castle_cache.h"
#include "castle_rxrpc.h"
#include "castle_ctrl.h"
#include "castle_objects.h"

//#define DEBUG
#ifndef DEBUG
#define debug(_f, ...)  ((void)0)
#else
#define debug(_f, _a...)  (printk("%s:%.4d: " _f, __FILE__, __LINE__ , ##_a))
#endif

/* Forward definitions */
struct castle_rxrpc_call;
static const struct castle_rxrpc_call_type castle_rxrpc_op_call;
static const struct castle_rxrpc_call_type castle_rxrpc_get_call;
static const struct castle_rxrpc_call_type castle_rxrpc_replace_call;
static const struct castle_rxrpc_call_type castle_rxrpc_slice_call;
static const struct castle_rxrpc_call_type castle_rxrpc_ctrl_call;
static void castle_rxrpc_reply_send       (struct castle_rxrpc_call *call, 
                                           const void *buf, size_t len, int last);
static void castle_rxrpc_double_reply_send(struct castle_rxrpc_call *call, 
                                           const void *buf1, size_t len1,
                                           const void *buf2, size_t len2,
                                           int last);

#define NR_WQS    4
static struct socket            *socket;
static struct workqueue_struct  *rxrpc_wqs[NR_WQS]; /* Need singlethreaded WQs,
                                                       because individual calls handling
                                                       is not multithread safe. Collection
                                                       of queues will alow concurrency
                                                       between calls through. */
static struct sk_buff_head       rxrpc_incoming_calls;
static void castle_rxrpc_incoming_call_collect(struct work_struct *work);
static DECLARE_WORK(castle_rxrpc_incoming_call_work, castle_rxrpc_incoming_call_collect);

/* Wait Queue to delay module exit (rmmod) till all outstanding requests are over */
static DECLARE_WAIT_QUEUE_HEAD(castle_rxrpc_rmmod_wq); 
static atomic_t castle_outst_call_cnt = ATOMIC_INIT(0);

struct castle_rxrpc_call_type {
    /* Deliver packet to this call type. Deliver should consume ENTIRE packet, 
       free it using rxrpc_kernel_data_delivered() */
    int (*deliver)     (struct castle_rxrpc_call *call, struct sk_buff *skb,  bool last);
    void (*destructor) (struct castle_rxrpc_call *call);
};

struct castle_rxrpc_call {
    struct work_struct             work;
    struct workqueue_struct       *wq;          /* One of the rxrpc_wqs. Used to process the call. */
    unsigned long                  call_id;
    struct rxrpc_call             *rxcall;
    struct sk_buff_head            rx_queue;    /* Queue of packets for this call */
    struct sk_buff                *current_skb; /* Set when packet is processed asynchronously */

    int                            op_id;
    const struct castle_rxrpc_call_type *type;
    volatile enum {
        /* Copied from AFS */
        RXRPC_CALL_AWAIT_OP_ID,   /* awaiting op ID on incoming call */
        RXRPC_CALL_AWAIT_REQUEST, /* awaiting request data on incoming call */
        RXRPC_CALL_AWAIT_DATA,    /* for large requests, awaiting more data */
        RXRPC_CALL_REPLYING,      /* replying to incoming call */
        RXRPC_CALL_AWAIT_ACK,     /* awaiting final ACK of incoming call */
        RXRPC_CALL_COMPLETE,      /* successfully completed */
        RXRPC_CALL_BUSY,          /* server was busy */
        RXRPC_CALL_ABORTED,       /* call was aborted */
        RXRPC_CALL_ERROR,         /* call failed due to error */
    }                              state;
    int                            error;
    int                            packet_cnt;
    /* Extra information needed by various types of calls */
    union
    {
        /* For CASTLE_OBJ_REQ_GET */
        struct {
            c2_block_t *data_c2b;
            uint32_t    data_c2b_length;
            uint32_t    data_length;
            int         first;
        } get;
        /* For CASTLE_OBJ_REQ_REPLACE */
        struct {
            c2_block_t *data_c2b;
            uint32_t    data_c2b_offset;
            uint32_t    data_length;
        } replace;
    };
};

void castle_rxrpc_get_call_get(struct castle_rxrpc_call *call, 
                               c2_block_t **data_c2b, 
                               uint32_t *data_c2b_length,
                               uint32_t *data_length,
                               int *first)
{
    BUG_ON(call->type != &castle_rxrpc_get_call);
    BUG_ON(!data_length || !data_c2b || !data_c2b_length || !first);
    *data_c2b        = call->get.data_c2b;
    *data_length     = call->get.data_length;
    *data_c2b_length = call->get.data_c2b_length;
    *first           = call->get.first;
}

void castle_rxrpc_get_call_set(struct castle_rxrpc_call *call, 
                               c2_block_t *data_c2b, 
                               uint32_t data_c2b_length,
                               uint32_t data_length,
                               int first)
{
    BUG_ON(call->type != &castle_rxrpc_get_call);
    call->get.data_c2b        = data_c2b;
    call->get.data_c2b_length = data_c2b_length;
    call->get.data_length     = data_length;
    call->get.first           = first;
}

void castle_rxrpc_replace_call_get(struct castle_rxrpc_call *call, 
                                   c2_block_t **data_c2b, 
                                   uint32_t *data_c2b_offset,
                                   uint32_t *data_length)
{
    BUG_ON(call->type != &castle_rxrpc_replace_call);
    BUG_ON(!data_c2b || !data_c2b_offset || !data_length);
    *data_c2b        = call->replace.data_c2b;
    *data_c2b_offset = call->replace.data_c2b_offset;
    *data_length     = call->replace.data_length;
}

void castle_rxrpc_replace_call_set(struct castle_rxrpc_call *call, 
                                   c2_block_t *data_c2b, 
                                   uint32_t data_c2b_offset,
                                   uint32_t data_length)
{
    BUG_ON(call->type != &castle_rxrpc_replace_call);
    call->replace.data_c2b        = data_c2b;
    call->replace.data_c2b_offset = data_c2b_offset;
    call->replace.data_length     = data_length;
}

static void castle_rxrpc_state_update(struct castle_rxrpc_call *call, int state)
{
    /* AWAIT_DATA -> AWAIT_REQUEST is allowed */
    if((state < call->state) && 
      !((state == RXRPC_CALL_AWAIT_REQUEST) && (call->state == RXRPC_CALL_AWAIT_DATA)))
    {
        printk("Asked to update state backwards: %d->%d\n", call->state, state);
        BUG();
    }
    call->state = state;
    /* Make sure that the change is visible from other threads.
       This is (especially) important for RXRPC_CALL_AWAIT_ACK which 
       is set from some da/btree wq, but read by rxrpc processors */
    mb();
}

/* Definition of different call types */
static int castle_rxrpc_op_decode(struct castle_rxrpc_call *call, struct sk_buff *skb,  bool last)
{
    if(skb->len < 4)
        return -EBADMSG;

#ifdef DEBUG    
    printk("\n Got following RxRPC packet:\n");
    skb_print(skb);
#endif

    castle_rxrpc_state_update(call, RXRPC_CALL_AWAIT_REQUEST);
    call->op_id = SKB_L_GET(skb);
    debug("op id: %d\n", call->op_id);

    switch(call->op_id)
    {
        case CASTLE_OBJ_REQ_GET:
            call->type = &castle_rxrpc_get_call;
            break;
        case CASTLE_OBJ_REQ_REPLACE:
            call->type = &castle_rxrpc_replace_call;
            break;
        case CASTLE_OBJ_REQ_SLICE:
            call->type = &castle_rxrpc_slice_call;
            break;
        case CASTLE_CTRL_REQ:
            call->type = &castle_rxrpc_ctrl_call;
            break;
        default:
            return -ENOTSUPP;
    }

    return call->type->deliver(call, skb, last);
}

#define BUFFER_PUT(_value, _value_len)                                          \
({                                                                              \
    uint32_t _pad_len;                                                          \
                                                                                \
    /* Work out how much pading do we need */                                   \
    _pad_len = (_value_len % 4 == 0 ? 0 : 4 - _value_len % 4);                  \
    if(buffer_len < _value_len + _pad_len)                                      \
        return 1;                                                               \
    /* Copy the value in */                                                     \
    memcpy(buffer, _value, _value_len);                                         \
    /* Copy the pad in */                                                       \
    memcpy(buffer + _value_len, &pad, _pad_len);                                \
    *buffer_used += _value_len + _pad_len;                                      \
     buffer_len  -= _value_len + _pad_len;                                      \
     buffer      += _value_len + _pad_len;                                      \
})
    

int castle_rxrpc_get_slice_reply_marshall(struct castle_rxrpc_call *call,
                                          c_vl_okey_t *k,
                                          char *value,
                                          uint32_t value_len,
                                          char *buffer,
                                          uint32_t buffer_len,
                                          uint32_t *buffer_used)
{
    uint32_t pad = 0;
    uint32_t rsp_val;
    int i;

    /* There has to be some space in the buffer, otherwise this function shouldn't be called */
    BUG_ON(buffer_len < 4);
    *buffer_used = 0;
    /* Key has to be marshalled first. Number of dimensions should be written out first. */
    rsp_val = htonl(k->nr_dims);
    BUFFER_PUT(&rsp_val, 4);
    /* Next, write out each dimension */
    for(i=0; i<k->nr_dims; i++)
    {
        /* Write the length of the key */
        rsp_val = htonl(k->dims[i]->length);
        BUFFER_PUT(&rsp_val, 4);
        /* Write the key itself */
        BUFFER_PUT(k->dims[i]->key, k->dims[i]->length); 
    }
    /* All keys written out, write out the value itself */
    rsp_val = htonl(CASTLE_OBJ_VALUE);
    BUFFER_PUT(&rsp_val, 4);
    rsp_val = htonl(value_len);
    BUFFER_PUT(&rsp_val, 4);
    BUFFER_PUT(value, value_len); 

    return 0;
}

void castle_rxrpc_get_slice_reply(struct castle_rxrpc_call *call,
                                  int err,
                                  int nr_vals,
                                  char *buffer,
                                  uint32_t buffer_len)
{
    uint32_t reply[2];
    
    /* Deal with errors first */
    if(err)
    {   
        reply[0] = htonl(CASTLE_OBJ_REPLY_ERROR);
        castle_rxrpc_reply_send(call, reply, 4, 1 /* last */);
        return;
    }

    /* Otherwise return the buffered reply message */
    reply[0] = htonl(CASTLE_OBJ_REPLY_GET_SLICE);
    reply[1] = htonl(nr_vals);

    castle_rxrpc_double_reply_send(call, 
                                   reply, 8,
                                   buffer, buffer_len,
                                   1);
    return;
}


void castle_rxrpc_get_reply_start(struct castle_rxrpc_call *call, 
                                  int err, 
                                  uint32_t data_length,
                                  void *buffer, 
                                  uint32_t buffer_length)
{
    uint32_t reply[2];
  
    /* Deal with errors first */
    if(err)
    {
        reply[0] = htonl(CASTLE_OBJ_REPLY_ERROR);
        castle_rxrpc_reply_send(call, reply, 4, 1 /* last */);
        return;
    }

    reply[0] = htonl(CASTLE_OBJ_REPLY_GET);
    /* Deal with tombstones next */
    if(!buffer)
    {
        BUG_ON((data_length != 0) || (buffer_length != 0));
        reply[1] = htonl(CASTLE_OBJ_TOMBSTONE);
        castle_rxrpc_reply_send(call, reply, 8, 1 /* last */);
        return;
    }
    
    /* Finally, deal with full values */
    reply[1] = htonl(CASTLE_OBJ_VALUE);
    /* Write out the entire data length, but send buffer_length worth of stuff now */
    reply[2] = htonl(data_length);

    castle_rxrpc_double_reply_send(call, 
                                   reply, 12,
                                   buffer, buffer_length,
                                  (data_length == buffer_length));
}


void castle_rxrpc_get_reply_continue(struct castle_rxrpc_call *call,
                                     int err,
                                     void *buffer,
                                     uint32_t buffer_length,
                                     int last)
{
    /* Deal with errors first (this will basically advance the state to AWAIT_ACK) */
    if(err)
    {
        castle_rxrpc_reply_send(call, NULL, 0, 1 /* last */);
        return;
    }

    /* Otherwise send the buffer through */ 
    castle_rxrpc_reply_send(call, buffer, buffer_length, last);
}


void castle_rxrpc_replace_complete(struct castle_rxrpc_call *call, int err)
{
    uint32_t reply[1];

    debug("Replace complete for call=%p\n", call); 
    rxrpc_kernel_data_delivered(call->current_skb);
    castle_rxrpc_state_update(call, RXRPC_CALL_REPLYING);

    if(err)
        reply[0] = htonl(CASTLE_OBJ_REPLY_ERROR);
    else
        reply[0] = htonl(CASTLE_OBJ_REPLY_REPLACE);

    castle_rxrpc_reply_send(call, reply, 4, 1 /* last */);
}

void castle_rxrpc_replace_continue(struct castle_rxrpc_call *call)
{
    debug("Continuing replace call (checking for more packets on the queue).\n");
    /* Go back in the state, because otherwise packet_process() will ignore us. */
    castle_rxrpc_state_update(call, RXRPC_CALL_AWAIT_REQUEST);
    rxrpc_kernel_data_delivered(call->current_skb);
    if(!skb_queue_empty(&call->rx_queue));
    {
        debug("Queueing packet process..\n");
        queue_work(call->wq, &call->work);
    }
}

uint32_t castle_rxrpc_packet_length(struct castle_rxrpc_call *call)
{
    return call->current_skb->len;
}

uint32_t castle_rxrpc_uint32_get(struct castle_rxrpc_call *call)
{
    return SKB_L_GET(call->current_skb);
}

void castle_rxrpc_str_copy(struct castle_rxrpc_call *call, void *buffer, int str_length, int partial)
{
    SKB_STR_CPY(call->current_skb, buffer, str_length, !partial);
}

static int castle_rxrpc_collection_get(struct sk_buff *skb,
                                       struct castle_attachment **attachment_p)
{
    struct castle_attachment *ca;
    
    ca = castle_collection_find(SKB_L_GET(skb));
    *attachment_p = ca;

    return (ca == NULL) ? -EINVAL : 0;
}
    
static int castle_rxrpc_key_get(struct sk_buff *skb,
                                c_vl_okey_t **key_p)
{
    uint32_t nr_dims, i;
    c_vl_okey_t *key;

    nr_dims = SKB_L_GET(skb);
    key = kzalloc(sizeof(c_vl_okey_t) + sizeof(c_vl_key_t *) * nr_dims, GFP_KERNEL);
    if(!key)
        return -ENOMEM;

    /* Init the key */
    key->nr_dims = nr_dims;
    for(i=0; i<nr_dims; i++)
    {
        key->dims[i] = SKB_VL_KEY_GET(skb, 128);
        if(!key->dims[i])
        {
            for(i--; i>=0; i--)
                kfree(key->dims[i]);
            kfree(key);

            return -ENOMEM;
        }
    }

    *key_p = key;

    return 0;
}

static int castle_rxrpc_collection_key_get(struct sk_buff *skb, 
                                           struct castle_attachment **attachment_p,
                                           c_vl_okey_t **key_p)
{
    int ret;

    ret = castle_rxrpc_collection_get(skb, attachment_p);
    if(ret)
        return ret;

    ret = castle_rxrpc_key_get(skb, key_p);
    if(ret)
        return ret;

    return 0;
}

static int castle_rxrpc_get_decode(struct castle_rxrpc_call *call, struct sk_buff *skb,  bool last)
{
    struct castle_attachment *attachment;
    c_vl_okey_t *key;
    int ret;

    ret = castle_rxrpc_collection_key_get(skb, &attachment, &key);
    if(ret)
        return ret;

    ret = castle_object_get(call, attachment, key);
    if(ret)
        return ret;

    rxrpc_kernel_data_delivered(skb);
    castle_rxrpc_state_update(call, RXRPC_CALL_REPLYING);

    return 0;
}
 
static int castle_rxrpc_replace_decode(struct castle_rxrpc_call *call, struct sk_buff *skb, bool last)
{
    struct castle_attachment *attachment;
    c_vl_okey_t *key;
    int ret;
static int cnt = 0;
    
    if((cnt++) % 100 == 0)
       printk("Got %d replaces\n", cnt); 

    debug("Packed %d in replace, is it last=%d\n", call->packet_cnt, last);
    call->current_skb = skb;
    /* First packet processing */
    if(call->packet_cnt == 1)
    {
        ret = castle_rxrpc_collection_key_get(skb, &attachment, &key);
        if(ret)
            return ret;
        castle_rxrpc_state_update(call, RXRPC_CALL_AWAIT_DATA);
        ret = castle_object_replace(call, attachment, key, (SKB_L_GET(skb) == CASTLE_OBJ_TOMBSTONE));
    } else
    /* Subsequent packet processing */
    {
        if(call->replace.data_length == 0)
        {
            printk("ERROR? Got packet for finished replace in call %p"
                   "(data_length=0, packet_length=%d, last=%d). Ignoring.\n",
                    call, skb->len, last);
        }
        ret = castle_object_replace_continue(call, last);
    }

    if(ret)
        return ret;

    return 0;
}

static int castle_rxrpc_slice_decode(struct castle_rxrpc_call *call, struct sk_buff *skb,  bool last)
{
    struct castle_attachment *attachment;
    c_vl_okey_t *start_key, *end_key;
    int ret;

    skb_print(skb);
    ret = castle_rxrpc_collection_get(skb, &attachment);
    if(ret)
        return ret;
    ret = castle_rxrpc_key_get(skb, &start_key);
    if(ret)
        return ret;
    ret = castle_rxrpc_key_get(skb, &end_key);
    if(ret)
    {
        castle_object_key_free(start_key);
        return ret;
    }
    rxrpc_kernel_data_delivered(skb);
    castle_rxrpc_state_update(call, RXRPC_CALL_REPLYING);
    printk("Executing a range query.\n");

    return castle_object_slice_get(call, attachment, start_key, end_key);
}

static int castle_rxrpc_ctrl_decode(struct castle_rxrpc_call *call, struct sk_buff *skb,  bool last)
{
    int ret;
    size_t len;
    void *reply = NULL;

    debug("Delivering ctrl packet.\n");
    ret = castle_control_packet_process(skb, &reply, &len);
    debug("Ctrl ret=%d\n", ret);

    rxrpc_kernel_data_delivered(skb);
    /* Advance the state, if we succeeded at decoding the packet */
    if(ret) 
    {
        if(reply) kfree(reply);
        return ret;
    }
    
    castle_rxrpc_state_update(call, RXRPC_CALL_REPLYING);
    debug("Sending reply of length=%d\n", len);
    castle_rxrpc_reply_send(call, reply, len, 1 /* last */);

    kfree(reply);
    return 0;
}


static const struct castle_rxrpc_call_type castle_rxrpc_op_call =
{
    .deliver = castle_rxrpc_op_decode,
};

static const struct castle_rxrpc_call_type castle_rxrpc_get_call =
{
    .deliver = castle_rxrpc_get_decode,
};

static const struct castle_rxrpc_call_type castle_rxrpc_replace_call =
{
    .deliver = castle_rxrpc_replace_decode,
};

static const struct castle_rxrpc_call_type castle_rxrpc_slice_call =
{
    .deliver = castle_rxrpc_slice_decode,
};

static const struct castle_rxrpc_call_type castle_rxrpc_ctrl_call =
{
    .deliver = castle_rxrpc_ctrl_decode,
};


static void castle_rxrpc_call_free(struct castle_rxrpc_call *call)
{
    debug("Freeing call: %p\n", call);    
    BUG_ON(call->rxcall != NULL);
    BUG_ON(!skb_queue_empty(&call->rx_queue));
    kfree(call);

    /* Decrement outstanding call count */
    atomic_dec(&castle_outst_call_cnt);
    wake_up(&castle_rxrpc_rmmod_wq);
}

static void castle_rxrpc_msg_send(struct castle_rxrpc_call *call, struct msghdr *msg, size_t len)
{
    int n;

    /* Check if we are sending the last message for this call, if so advance the state */
    if(!(msg->msg_flags & MSG_MORE))
        castle_rxrpc_state_update(call, RXRPC_CALL_AWAIT_ACK);
    n = rxrpc_kernel_send_data(call->rxcall, msg, len);
    debug("Sent %d bytes.\n", n);
    if (n < 0)
    {
        rxrpc_kernel_abort_call(call->rxcall, RX_USER_ABORT);
        return;
    }
    if(n != len)
    {
        printk("Failed to send the reply, wanted to send %ld bytes, sent %d bytes.\n",
                len, n);
        BUG();
    }
    if (n >= 0) 
        return;
}

static void castle_rxrpc_reply_send(struct castle_rxrpc_call *call, 
                                    const void *buf, 
                                    size_t len, 
                                    int last)
{
    struct msghdr msg;
    struct iovec iov[1];

    iov[0].iov_base     = (void *) buf;
    iov[0].iov_len      = len;
    msg.msg_name        = NULL;
    msg.msg_namelen     = 0;
    msg.msg_iov         = buf ? iov : NULL;
    msg.msg_iovlen      = buf ? 1 : 0;
    msg.msg_control     = NULL;
    msg.msg_controllen  = 0;
    msg.msg_flags       = last ? 0 : MSG_MORE;

    castle_rxrpc_msg_send(call, &msg, len);
}

static void castle_rxrpc_double_reply_send(struct castle_rxrpc_call *call, 
                                           const void *buf1, 
                                           size_t len1,
                                           const void *buf2, 
                                           size_t len2,
                                           int last)
{
    struct msghdr msg;
    struct iovec iov[3];
    uint8_t pad_buff[3] = {0, 0, 0};
    int pad = (4 - (len2 % 4)) % 4;

    iov[0].iov_base     = (void *) buf1;
    iov[0].iov_len      = len1;
    iov[1].iov_base     = (void *) buf2;
    iov[1].iov_len      = len2;
    if(pad)
    { 
        iov[2].iov_base = (void *)pad_buff;
        iov[2].iov_len  = pad;
    }

    msg.msg_name        = NULL;
    msg.msg_namelen     = 0;
    msg.msg_iov         = iov;
    msg.msg_iovlen      = pad ? 3 : 2;
    msg.msg_control     = NULL;
    msg.msg_controllen  = 0;
    msg.msg_flags       = last ? 0 : MSG_MORE;
    
    castle_rxrpc_msg_send(call, &msg, len1 + len2 + pad);
}

static void castle_rxrpc_call_delete(struct work_struct *work)
{
    struct castle_rxrpc_call *call = container_of(work, struct castle_rxrpc_call, work);

    castle_rxrpc_call_free(call);
}

static void castle_rxrpc_packet_process(struct work_struct *work)
{
    struct castle_rxrpc_call *call = container_of(work, struct castle_rxrpc_call, work);
    uint32_t abort_code;
    struct sk_buff *skb;
    int last, ret;

    /* Exit early if there are no packet on the queue (e.g. queue is flushed for 
       completed calls) */
    if (skb_queue_empty(&call->rx_queue))
        return;

    debug("Processing packets for call: %p, call->state=%d.\n", call, call->state);
    while ((call->state == RXRPC_CALL_AWAIT_OP_ID   ||
            call->state == RXRPC_CALL_AWAIT_REQUEST ||
            call->state == RXRPC_CALL_AWAIT_ACK) &&
           (skb = skb_dequeue(&call->rx_queue)))
    {
        debug("Processing packet: %d.\n", skb->mark);
        call->packet_cnt++;
        switch(skb->mark)
        {
            case RXRPC_SKB_MARK_DATA:
                last = rxrpc_kernel_is_data_last(skb);
                /* Deliver the packet to the call */
                ret = call->type->deliver(call, skb, last);
                debug("Processed data packet, got ret=%d\n", ret);
                switch (ret)
                {
                    case 0:
                        break;
                    case -ENOTCONN:
                        abort_code = RX_CALL_DEAD;
                        goto do_abort;
                    case -ENOTSUPP:
                        abort_code = RX_INVALID_OPERATION;
                        goto do_abort;
                    default:
                        abort_code = RXGEN_SS_UNMARSHAL;
                    do_abort:
                        rxrpc_kernel_abort_call(call->rxcall, abort_code);
                        call->error = ret;
                        castle_rxrpc_state_update(call, RXRPC_CALL_ERROR);
                        break;
                }
                skb = NULL;
                continue;
            case RXRPC_SKB_MARK_FINAL_ACK:
                castle_rxrpc_state_update(call, RXRPC_CALL_COMPLETE);
                break;
            case RXRPC_SKB_MARK_BUSY:
                call->error = -EBUSY;
                castle_rxrpc_state_update(call, RXRPC_CALL_BUSY);
                break;
            case RXRPC_SKB_MARK_REMOTE_ABORT:
                call->error = -rxrpc_kernel_get_abort_code(skb);
                castle_rxrpc_state_update(call, RXRPC_CALL_ABORTED);
                break;
            case RXRPC_SKB_MARK_NET_ERROR:
            case RXRPC_SKB_MARK_LOCAL_ERROR:
                call->error = -rxrpc_kernel_get_error_number(skb);
                castle_rxrpc_state_update(call, RXRPC_CALL_ERROR);
                break;
            default:
                BUG();
                break;
        }
        debug("Freeing non-DATA skb.\n");
        /* SKB processed, free it */
        rxrpc_kernel_free_skb(skb);
    }
    
    debug("Call state is %d.\n", call->state);

    /* make sure the queue is empty if the call is done with (we might have
     * aborted the call early because of an unmarshalling error) */
    if (call->state >= RXRPC_CALL_COMPLETE) {
        rxrpc_kernel_end_call(call->rxcall);
        while ((skb = skb_dequeue(&call->rx_queue)))
        {
            printk("WARNING: Untested, freeing rxrpc SKB after ending the call.\n");
            rxrpc_kernel_free_skb(skb);
        }

        call->rxcall = NULL;
        if(call->type->destructor)
            call->type->destructor(call);

        debug("Queueing call delete.\n");
        PREPARE_WORK(&call->work, castle_rxrpc_call_delete);
        queue_work(call->wq, &call->work);
    }
}

static void castle_rxrpc_incoming_call_collect(struct work_struct *work)
{
    struct castle_rxrpc_call *c_rxcall;
    struct sk_buff *skb;
    static atomic_t call_id = ATOMIC(0);
    static int wq_nr = 0;

    while((skb = skb_dequeue(&rxrpc_incoming_calls)))
    {
        /* Nothing interesting in the packet, free it */
        rxrpc_kernel_free_skb(skb);

        /* Try to allocate a call struct, reject call if failed */
        c_rxcall = kzalloc(sizeof(struct castle_rxrpc_call), GFP_KERNEL);
        if(!c_rxcall)
        {
            rxrpc_kernel_reject_call(socket);
            continue;
        }

        debug("Collecting call %p.\n", c_rxcall);
        /* Init the call struct */
        INIT_WORK(&c_rxcall->work, castle_rxrpc_packet_process);
        skb_queue_head_init(&c_rxcall->rx_queue); 	
        c_rxcall->wq         = rxrpc_wqs[(wq_nr++) % NR_WQS];
        c_rxcall->call_id    = atomic_inc_return(&call_id);
        c_rxcall->type       = &castle_rxrpc_op_call;
        c_rxcall->packet_cnt = 0;
        castle_rxrpc_state_update(c_rxcall, RXRPC_CALL_AWAIT_OP_ID);

        c_rxcall->rxcall = rxrpc_kernel_accept_call(socket,
                                                    (unsigned long)c_rxcall);
        if(IS_ERR(c_rxcall->rxcall))
        {
            castle_rxrpc_call_free(c_rxcall);
        }
        else 
        {
            /* Increment outstanding call count */
            atomic_inc(&castle_outst_call_cnt);
        }
    }
}

static void castle_rxrpc_interceptor(struct sock *sk,
                                     unsigned long user_call_ID,
			                         struct sk_buff *skb)
{
    struct castle_rxrpc_call *call = (struct castle_rxrpc_call *) user_call_ID;

    if(!call)
    {
        debug("Intercepting new call request.\n");
        skb_queue_tail(&rxrpc_incoming_calls, skb);
        schedule_work(&castle_rxrpc_incoming_call_work);
    } else
    {
        debug("Intercepting call 0x%lx\n", user_call_ID);
        skb_queue_tail(&call->rx_queue, skb);
        queue_work(call->wq, &call->work);
    }
}

int castle_rxrpc_init(void)
{
	struct sockaddr_rxrpc srx;
    int i, ret;
    char *wq_name;

    printk("Castle RXRPC init.\n");
    skb_queue_head_init(&rxrpc_incoming_calls);
    for(i=0; i<NR_WQS; i++)
    {
        char *name_prefix = "castle_rxrpc_";
        wq_name = kzalloc(strlen(name_prefix)+3, GFP_KERNEL);
        if(!wq_name)
            goto wq_error;
        sprintf(wq_name, "%s%d", name_prefix, i);
        rxrpc_wqs[i] = create_singlethread_workqueue(wq_name);
        if(!rxrpc_wqs[i])
        {
wq_error:
            kfree(wq_name);
            for(; i>=0; i--)
                destroy_workqueue(rxrpc_wqs[i]);

            return -ENOMEM;
        }
    }

	ret = sock_create_kern(AF_RXRPC, SOCK_DGRAM, PF_INET, &socket);
    if(ret < 0)
    {
        for(i=0; i<NR_WQS; i++)
            destroy_workqueue(rxrpc_wqs[i]);
        return ret;
    }

	socket->sk->sk_allocation = GFP_NOIO;

	srx.srx_family						= AF_RXRPC;
	srx.srx_service						= 1;
	srx.transport_type					= SOCK_DGRAM;
	srx.transport_len					= sizeof(srx.transport.sin);
	srx.transport.sin.sin_addr.s_addr   = htonl(INADDR_LOOPBACK);
	srx.transport.sin.sin_family	    = AF_INET;
	srx.transport.sin.sin_port	        = htons(34876);
	memset(&srx.transport.sin.sin_addr, 0, sizeof(srx.transport.sin.sin_addr));

    ret = kernel_bind(socket, (struct sockaddr *) &srx, sizeof(srx));
	if (ret < 0) {
        for(i=0; i<NR_WQS; i++)
            destroy_workqueue(rxrpc_wqs[i]);
		sock_release(socket);
        return ret;
	}

	rxrpc_kernel_intercept_rx_messages(socket, castle_rxrpc_interceptor);

    return 0;
}

void castle_rxrpc_fini(void)
{
    int i;

    printk("Castle RXRPC fini.\n");
    /* Wait until all outstanding calls are finished */
    wait_event(castle_rxrpc_rmmod_wq, (atomic_read(&castle_outst_call_cnt) == 0));
	sock_release(socket);
	for(i=0; i<NR_WQS; i++)
        destroy_workqueue(rxrpc_wqs[i]);
}

