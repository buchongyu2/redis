/* blocked.c 通用的阻塞操作库- generic support for blocking operations like BLPOP & WAIT.
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * ---------------------------------------------------------------------------
 *
 * API:
 *
 * blockClient() set the CLIENT_BLOCKED flag in the client, and set the
 * specified block type 'btype' filed to one of BLOCKED_* macros.
 *
 * unblockClient() unblocks the client doing the following:
 * 1) It calls the btype-specific function to cleanup the state.
 * 2) It unblocks the client by unsetting the CLIENT_BLOCKED flag.
 * 3) It puts the client into a list of just unblocked clients that are
 *    processed ASAP in the beforeSleep() event loop callback, so that
 *    if there is some query buffer to process, we do it. This is also
 *    required because otherwise there is no 'readable' event fired, we
 *    already read the pending commands. We also set the CLIENT_UNBLOCKED
 *    flag to remember the client is in the unblocked_clients list.
 *
 * processUnblockedClients() is called inside the beforeSleep() function
 * to process the query buffer from unblocked clients and remove the clients
 * from the blocked_clients queue.
 *
 * replyToBlockedClientTimedOut() is called by the cron function when
 * a client blocked reaches the specified timeout (if the timeout is set
 * to 0, no timeout is processed).
 * It usually just needs to send a reply to the client.
 *
 * When implementing a new type of blocking operation, the implementation
 * should modify unblockClient() and replyToBlockedClientTimedOut() in order
 * to handle the btype-specific behavior of this two functions.
 * If the blocking operation waits for certain keys to change state, the
 * clusterRedirectBlockedClientIfNeeded() function should also be updated.
 */

/*
 * API:
 *
 * blockClient() 会设置客户端的 CLIENT_BLOCKED 标志，并将指定的阻塞类型 btype 字段设置为 BLOCKED_* 宏之一。
 *
 * unblockClient() 解锁客户端，具体步骤如下：
 * 1) 调用与 btype 相关的函数清理状态。
 * 2) 通过取消 CLIENT_BLOCKED 标志来解锁客户端。
 * 3) 将客户端加入刚刚解锁的客户端列表，该列表会在 beforeSleep() 事件循环回调中尽快处理，
 *    如果有待处理的查询缓冲区，会立即处理。这样做也是必须的，否则不会触发 'readable' 事件，
 *    因为我们已经读取了待处理命令。同时会设置 CLIENT_UNBLOCKED 标志，以便记住客户端在 unblocked_clients 列表中。
 *
 * processUnblockedClients() 会在 beforeSleep() 函数中调用，
 * 用于处理解锁客户端的查询缓冲区，并将客户端从 blocked_clients 队列中移除。
 *
 * replyToBlockedClientTimedOut() 由 cron 函数在客户端阻塞达到指定超时时调用
 * （如果超时时间为 0，则不会处理超时）。
 * 通常只需要向客户端发送一个回复。
 *
 * 当实现新的阻塞操作类型时，应该修改 unblockClient() 和 replyToBlockedClientTimedOut()，
 * 以处理这两个函数的 btype 特定行为。
 * 如果阻塞操作等待某些键的状态改变，还应该更新 clusterRedirectBlockedClientIfNeeded() 函数。
 */
#include "server.h"
#include "slowlog.h"
#include "latency.h"
#include "monotonic.h"

int serveClientBlockedOnList(client *receiver, robj *key, robj *dstkey, redisDb *db, robj *value, int wherefrom, int whereto);
int getListPositionFromObjectOrReply(client *c, robj *arg, int *position);

/* This structure represents the blocked key information that we store
 * in the client structure. Each client blocked on keys, has a
 * client->bpop.keys hash table. The keys of the hash table are Redis
 * keys pointers to 'robj' structures. The value is this structure.
 * The structure has two goals: firstly we store the list node that this
 * client uses to be listed in the database "blocked clients for this key"
 * list, so we can later unblock in O(1) without a list scan.
 * Secondly for certain blocking types, we have additional info. Right now
 * the only use for additional info we have is when clients are blocked
 * on streams, as we have to remember the ID it blocked for. */
/* 该结构体表示我们存储在客户端结构中的阻塞键信息。
 * 每个被键阻塞的客户端都有一个 client->bpop.keys 哈希表。
 * 哈希表的键是指向 'robj' 结构的 Redis 键指针，值是这个结构体。
 * 这个结构体有两个目的：首先我们存储了该客户端用于在数据库
 * “该键的阻塞客户端”列表中的链表节点，这样可以在 O(1) 时间内解除阻塞，
 * 无需扫描整个列表。其次，对于某些阻塞类型，我们还需要额外信息。
 * 目前额外信息只用于客户端被流阻塞时，需要记住阻塞的流 ID。
 */
typedef struct bkinfo {
    listNode *listnode;     /* List node for db->blocking_keys[key] list. */
    streamID stream_id;     /* Stream ID if we blocked in a stream. */
} bkinfo;

/* Block a client for the specific operation type. Once the CLIENT_BLOCKED
 * flag is set client query buffer is not longer processed, but accumulated,
 * and will be processed when the client is unblocked. */
/* 阻塞客户端用于指定的操作类型。一旦设置了 CLIENT_BLOCKED 标志，
 * 客户端的查询缓冲区将不再被处理，而是累积起来，
 * 等客户端解除阻塞后再处理。 */
void blockClient(client *c, int btype) {
    /* Master client should never be blocked unless pause or module */
    serverAssert(!(c->flags & CLIENT_MASTER &&
                   btype != BLOCKED_MODULE &&
                   btype != BLOCKED_PAUSE));

    c->flags |= CLIENT_BLOCKED;
    c->btype = btype;
    server.blocked_clients++;
    server.blocked_clients_by_type[btype]++;
    addClientToTimeoutTable(c);
    if (btype == BLOCKED_PAUSE) {
        listAddNodeTail(server.paused_clients, c);
        c->paused_list_node = listLast(server.paused_clients);
        /* Mark this client to execute its command */
        c->flags |= CLIENT_PENDING_COMMAND;
    }
}

/* This function is called after a client has finished a blocking operation
 * in order to update the total command duration, log the command into
 * the Slow log if needed, and log the reply duration event if needed. */
/* 该函数在客户端完成阻塞操作后调用，
 * 用于更新总命令耗时、必要时记录到慢日志，
 * 并在需要时记录回复耗时事件。 */
void updateStatsOnUnblock(client *c, long blocked_us, long reply_us){
    const ustime_t total_cmd_duration = c->duration + blocked_us + reply_us;
    c->lastcmd->microseconds += total_cmd_duration;

    /* Log the command into the Slow log if needed. */
    slowlogPushCurrentCommand(c, c->lastcmd, total_cmd_duration);
    /* Log the reply duration event. */
    latencyAddSampleIfNeeded("command-unblocking",reply_us/1000);
}

/* This function is called in the beforeSleep() function of the event loop
 * in order to process the pending input buffer of clients that were
 * unblocked after a blocking operation. */
/* 该函数在事件循环的 beforeSleep() 中调用，
 * 用于处理那些在阻塞操作后被解除阻塞的客户端的待处理输入缓冲区。 */
void processUnblockedClients(void) {
    listNode *ln;
    client *c;

    while (listLength(server.unblocked_clients)) {
        ln = listFirst(server.unblocked_clients);
        serverAssert(ln != NULL);
        c = ln->value;
        listDelNode(server.unblocked_clients,ln);
        c->flags &= ~CLIENT_UNBLOCKED;

        /* Process remaining data in the input buffer, unless the client
         * is blocked again. Actually processInputBuffer() checks that the
         * client is not blocked before to proceed, but things may change and
         * the code is conceptually more correct this way. */
        /* 处理输入缓冲区剩余的数据，除非客户端再次被阻塞。
         * 实际上 processInputBuffer() 会检查客户端是否被阻塞，
         * 但这样写逻辑上更严谨。 */
        if (!(c->flags & CLIENT_BLOCKED)) {
            /* If we have a queued command, execute it now. */
            if (processPendingCommandsAndResetClient(c) == C_ERR) {
                continue;
            }
            /* 如果输入缓冲区还有数据，继续处理。 */ /* Then process client if it has more data in it's buffer. */
            if (c->querybuf && sdslen(c->querybuf) > 0) {
                processInputBuffer(c);
            }
        }
    }
}

/* This function will schedule the client for reprocessing at a safe time.
 *
 * This is useful when a client was blocked for some reason (blocking operation,
 * CLIENT PAUSE, or whatever), because it may end with some accumulated query
 * buffer that needs to be processed ASAP:
 *
 * 1. When a client is blocked, its readable handler is still active.
 * 2. However in this case it only gets data into the query buffer, but the
 *    query is not parsed or executed once there is enough to proceed as
 *    usually (because the client is blocked... so we can't execute commands).
 * 3. When the client is unblocked, without this function, the client would
 *    have to write some query in order for the readable handler to finally
 *    call processQueryBuffer*() on it.
 * 4. With this function instead we can put the client in a queue that will
 *    process it for queries ready to be executed at a safe time.
 */
/* 该函数会在安全的时机将客户端安排重新处理。
 *
 * 当客户端因为某些原因（阻塞操作、CLIENT PAUSE 等）被阻塞时非常有用，
 * 因为此时可能会积累一些查询缓冲区，需要尽快处理：
 *
 * 1. 当客户端被阻塞时，它的可读事件处理器仍然是激活的。
 * 2. 但此时只会把数据读入查询缓冲区，不会像正常情况那样在数据足够时解析和执行查询（因为客户端被阻塞，不能执行命令）。
 * 3. 如果没有这个函数，客户端解除阻塞后，必须再写入一些查询，才会由可读事件处理器调用 processQueryBuffer*() 进行处理。
 * 4. 有了这个函数，可以把客户端放入一个队列，在安全的时机处理那些已经准备好执行的查询。
 */
void queueClientForReprocessing(client *c) {
    /* The client may already be into the unblocked list because of a previous
     * blocking operation, don't add back it into the list multiple times. */
    /* 由于之前的阻塞操作，客户端可能已经在 unblocked 列表中了，不要重复添加到该列表。 */
    if (!(c->flags & CLIENT_UNBLOCKED)) {
        c->flags |= CLIENT_UNBLOCKED;
        listAddNodeTail(server.unblocked_clients,c);
    }
}

/* 根据客户端阻塞的操作类型，调用对应的函数解除阻塞。 */ /* Unblock a client calling the right function depending on the kind
 * of operation the client is blocking for. */
void unblockClient(client *c) {
    if (c->btype == BLOCKED_LIST ||
        c->btype == BLOCKED_ZSET ||
        c->btype == BLOCKED_STREAM) {
        unblockClientWaitingData(c);
    } else if (c->btype == BLOCKED_WAIT) {
        unblockClientWaitingReplicas(c);
    } else if (c->btype == BLOCKED_MODULE) {
        if (moduleClientIsBlockedOnKeys(c)) unblockClientWaitingData(c);
        unblockClientFromModule(c);
    } else if (c->btype == BLOCKED_PAUSE) {
        listDelNode(server.paused_clients,c->paused_list_node);
        c->paused_list_node = NULL;
    } else {
        serverPanic("Unknown btype in unblockClient().");
    }

    /* Reset the client for a new query since, for blocking commands
     * we do not do it immediately after the command returns (when the
     * client got blocked) in order to be still able to access the argument
     * vector from module callbacks and updateStatsOnUnblock. */
    // 重置客户端以处理新的查询，因为对于阻塞命令，
    // 我们不会在命令返回后（客户端被阻塞时）立即重置，
    // 这样可以让模块回调和 updateStatsOnUnblock 仍然能够访问参数向量。
    if (c->btype != BLOCKED_PAUSE) {
        freeClientOriginalArgv(c);
        resetClient(c);
    }

    /* Clear the flags, and put the client in the unblocked list so that
     * we'll process new commands in its query buffer ASAP. */
    // 清除标志，并将客户端放入未阻塞列表，以便尽快处理其查询缓冲区中的新命令。
    server.blocked_clients--;
    server.blocked_clients_by_type[c->btype]--;
    c->flags &= ~CLIENT_BLOCKED;
    c->btype = BLOCKED_NONE;
    removeClientFromTimeoutTable(c);
    queueClientForReprocessing(c);
}

/* This function gets called when a blocked client timed out in order to
 * send it a reply of some kind. After this function is called,
 * unblockClient() will be called with the same client as argument. */
// 当一个被阻塞的客户端超时时会调用此函数，以便发送某种回复。
// 在此函数调用后，会用同一个客户端作为参数调用 unblockClient()。
void replyToBlockedClientTimedOut(client *c) {
    if (c->btype == BLOCKED_LIST ||
        c->btype == BLOCKED_ZSET ||
        c->btype == BLOCKED_STREAM) {
        addReplyNullArray(c);
    } else if (c->btype == BLOCKED_WAIT) {
        addReplyLongLong(c,replicationCountAcksByOffset(c->bpop.reploffset));
    } else if (c->btype == BLOCKED_MODULE) {
        moduleBlockedClientTimedOut(c);
    } else {
        serverPanic("Unknown btype in replyToBlockedClientTimedOut().");
    }
}

/* Mass-unblock clients because something changed in the instance that makes
 * blocking no longer safe. For example clients blocked in list operations
 * in an instance which turns from master to slave is unsafe, so this function
 * is called when a master turns into a slave.
 *
 * The semantics is to send an -UNBLOCKED error to the client, disconnecting
 * it at the same time. */
// 批量解除客户端阻塞，因为实例发生了变化导致阻塞不再安全。
// 例如，在主节点变为从节点时，阻塞在列表操作上的客户端是不安全的，
// 所以当主节点变为从节点时会调用此函数。
// 其语义是向客户端发送 -UNBLOCKED 错误，并同时断开连接。

void disconnectAllBlockedClients(void) {
    listNode *ln;
    listIter li;

    listRewind(server.clients,&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);

        if (c->flags & CLIENT_BLOCKED) {
            /* PAUSED clients are an exception, when they'll be unblocked, the
             * command processing will start from scratch, and the command will
             * be either executed or rejected. (unlike LIST blocked clients for
             * which the command is already in progress in a way. */
            // PAUSED 状态的客户端是个例外，当它们被解除阻塞时，
            // 命令处理将从头开始，命令要么被执行，要么被拒绝。
            // （不同于 LIST 阻塞的客户端，其命令已经在某种程度上进行中。）
            if (c->btype == BLOCKED_PAUSE)
                continue;

            addReplyError(c,
                "-UNBLOCKED force unblock from blocking operation, "
                "instance state changed (master -> replica?)");
            unblockClient(c);
            c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        }
    }
}

/* Helper function for handleClientsBlockedOnKeys(). This function is called
 * when there may be clients blocked on a list key, and there may be new
 * data to fetch (the key is ready). */
// handleClientsBlockedOnKeys() 的辅助函数。
// 当可能有客户端阻塞在某个列表键上，并且可能有新数据可获取（该键已就绪）时调用此函数。
void serveClientsBlockedOnListKey(robj *o, readyList *rl) {
    /* We serve clients in the same order they blocked for
     * this key, from the first blocked to the last. */
    // 按照客户端阻塞该键的顺序依次处理，从第一个阻塞到最后一个。
    dictEntry *de = dictFind(rl->db->blocking_keys,rl->key);
    if (de) {
        list *clients = dictGetVal(de);
        int numclients = listLength(clients);

        while(numclients--) {
            listNode *clientnode = listFirst(clients);
            client *receiver = clientnode->value;

            if (receiver->btype != BLOCKED_LIST) {
                /* Put at the tail, so that at the next call
                 * we'll not run into it again. */
                // 放到队尾，这样下次调用时就不会再次遇到它。
                listRotateHeadToTail(clients);
                continue;
            }

            robj *dstkey = receiver->bpop.target;
            int wherefrom = receiver->bpop.listpos.wherefrom;
            int whereto = receiver->bpop.listpos.whereto;
            robj *value = listTypePop(o, wherefrom);

            if (value) {
                /* Protect receiver->bpop.target, that will be
                 * freed by the next unblockClient()
                 * call. */
                // 保护 receiver->bpop.target，下次调用 unblockClient() 时会释放它。
                if (dstkey) incrRefCount(dstkey);

                client *old_client = server.current_client;
                server.current_client = receiver;
                monotime replyTimer;
                elapsedStart(&replyTimer);
                if (serveClientBlockedOnList(receiver,
                    rl->key,dstkey,rl->db,value,
                    wherefrom, whereto) == C_ERR)
                {
                    /* If we failed serving the client we need
                     * to also undo the POP operation. */
                    // 如果处理客户端失败，还需要撤销 POP 操作。
                    listTypePush(o,value,wherefrom);
                }
                updateStatsOnUnblock(receiver, 0, elapsedUs(replyTimer));
                unblockClient(receiver);
                afterCommand(receiver);
                server.current_client = old_client;

                if (dstkey) decrRefCount(dstkey);
                decrRefCount(value);
            } else {
                break;
            }
        }
    }

    if (listTypeLength(o) == 0) {
        dbDelete(rl->db,rl->key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",rl->key,rl->db->id);
    }
    /* We don't call signalModifiedKey() as it was already called
     * when an element was pushed on the list. */
    // 不再调用 signalModifiedKey()，因为在有元素被推入列表时已经调用过了。
}

/* Helper function for handleClientsBlockedOnKeys(). This function is called
 * when there may be clients blocked on a sorted set key, and there may be new
 * data to fetch (the key is ready). */
// handleClientsBlockedOnKeys() 的辅助函数。
// 当可能有客户端阻塞在某个有序集合键上，并且可能有新数据可获取（该键已就绪）时调用此函数。
void serveClientsBlockedOnSortedSetKey(robj *o, readyList *rl) {
    /* We serve clients in the same order they blocked for
     * this key, from the first blocked to the last. */
    // 按照客户端阻塞该键的顺序依次处理，从第一个阻塞到最后一个。
    dictEntry *de = dictFind(rl->db->blocking_keys,rl->key);
    if (de) {
        list *clients = dictGetVal(de);
        int numclients = listLength(clients);
        unsigned long zcard = zsetLength(o);

        while(numclients-- && zcard) {
            listNode *clientnode = listFirst(clients);
            client *receiver = clientnode->value;

            if (receiver->btype != BLOCKED_ZSET) {
                /* Put at the tail, so that at the next call
                 * we'll not run into it again. */
                // 放到队尾，这样下次调用时就不会再次遇到它。
                listRotateHeadToTail(clients);
                continue;
            }

            int where = (receiver->lastcmd &&
                         receiver->lastcmd->proc == bzpopminCommand)
                         ? ZSET_MIN : ZSET_MAX;
            client *old_client = server.current_client;
            server.current_client = receiver;
            monotime replyTimer;
            elapsedStart(&replyTimer);
            genericZpopCommand(receiver,&rl->key,1,where,1,NULL);
            updateStatsOnUnblock(receiver, 0, elapsedUs(replyTimer));
            unblockClient(receiver);
            afterCommand(receiver);
            server.current_client = old_client;
            zcard--;

            // 复制该命令。 /* Replicate the command. */
            robj *argv[2];
            struct redisCommand *cmd = where == ZSET_MIN ?
                                       server.zpopminCommand :
                                       server.zpopmaxCommand;
            argv[0] = createStringObject(cmd->name,strlen(cmd->name));
            argv[1] = rl->key;
            incrRefCount(rl->key);
            propagate(cmd,receiver->db->id,
                      argv,2,PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(argv[0]);
            decrRefCount(argv[1]);
        }
    }
}

/* Helper function for handleClientsBlockedOnKeys(). This function is called
 * when there may be clients blocked on a stream key, and there may be new
 * data to fetch (the key is ready). */
/* serveClientsBlockedOnStreamKey() 的辅助函数。当有可能有客户端阻塞在某个 stream 键上，
 * 并且该键可能有新数据可获取（键已就绪）时调用此函数。*/
void serveClientsBlockedOnStreamKey(robj *o, readyList *rl) {
    dictEntry *de = dictFind(rl->db->blocking_keys,rl->key);
    stream *s = o->ptr;

    /* We need to provide the new data arrived on the stream
     * to all the clients that are waiting for an offset smaller
     * than the current top item. */
    /* 我们需要将新到达的 stream 数据提供给所有等待偏移量小于当前顶部项的客户端。*/
    if (de) {
        list *clients = dictGetVal(de);
        listNode *ln;
        listIter li;
        listRewind(clients,&li);

        while((ln = listNext(&li))) {
            client *receiver = listNodeValue(ln);
            if (receiver->btype != BLOCKED_STREAM) continue;
            bkinfo *bki = dictFetchValue(receiver->bpop.keys,rl->key);
            streamID *gt = &bki->stream_id;

            /* If we blocked in the context of a consumer
             * group, we need to resolve the group and update the
             * last ID the client is blocked for: this is needed
             * because serving other clients in the same consumer
             * group will alter the "last ID" of the consumer
             * group, and clients blocked in a consumer group are
             * always blocked for the ">" ID: we need to deliver
             * only new messages and avoid unblocking the client
             * otherwise. */
            /* 如果我们是在消费者组的上下文中阻塞的，需要解析该组并更新客户端阻塞的最后 ID：
             * 这是因为为同一消费者组的其他客户端服务时会改变该组的“last ID”，
             * 并且阻塞在消费者组中的客户端总是阻塞在 “>” ID 上：
             * 我们只需要投递新消息，否则不要解除客户端阻塞。*/
            streamCG *group = NULL;
            if (receiver->bpop.xread_group) {
                group = streamLookupCG(s,
                        receiver->bpop.xread_group->ptr);
                /* 如果没有找到该组，则向消费者发送错误。*/ /* If the group was not found, send an error
                 * to the consumer. */
                if (!group) {
                    addReplyError(receiver,
                        "-NOGROUP the consumer group this client "
                        "was blocked on no longer exists");
                    unblockClient(receiver);
                    continue;
                } else {
                    *gt = group->last_id;
                }
            }

            if (streamCompareID(&s->last_id, gt) > 0) {
                streamID start = *gt;
                streamIncrID(&start);

                /* 查找该组中的消费者（如果有的话）。*/ /* Lookup the consumer for the group, if any. */
                streamConsumer *consumer = NULL;
                int noack = 0;

                if (group) {
                    int created = 0;
                    consumer =
                        streamLookupConsumer(group,
                                             receiver->bpop.xread_consumer->ptr,
                                             SLC_NONE,
                                             &created);
                    noack = receiver->bpop.xread_group_noack;
                    if (created && noack) {
                        streamPropagateConsumerCreation(receiver,rl->key,
                                                        receiver->bpop.xread_group,
                                                        consumer->name);
                    }
                }

                client *old_client = server.current_client;
                server.current_client = receiver;
                monotime replyTimer;
                elapsedStart(&replyTimer);
                /* Emit the two elements sub-array consisting of
                 * the name of the stream and the data we
                 * extracted from it. Wrapped in a single-item
                 * array, since we have just one key. */
                /* 发出包含两个元素的子数组：stream 的名称和我们从中提取的数据。
                 * 由于这里只有一个键，所以用单项数组包裹。*/
                if (receiver->resp == 2) {
                    addReplyArrayLen(receiver,1);
                    addReplyArrayLen(receiver,2);
                } else {
                    addReplyMapLen(receiver,1);
                }
                addReplyBulk(receiver,rl->key);

                streamPropInfo pi = {
                    rl->key,
                    receiver->bpop.xread_group
                };
                streamReplyWithRange(receiver,s,&start,NULL,
                                     receiver->bpop.xread_count,
                                     0, group, consumer, noack, &pi);
                updateStatsOnUnblock(receiver, 0, elapsedUs(replyTimer));

                /* Note that after we unblock the client, 'gt'
                 * and other receiver->bpop stuff are no longer
                 * valid, so we must do the setup above before
                 * this call. */
                /* 注意，在解除客户端阻塞后，'gt' 和其他 receiver->bpop 相关内容将不再有效，
                 * 所以必须在此调用之前完成上述设置。*/
                unblockClient(receiver);
                afterCommand(receiver);
                server.current_client = old_client;
            }
        }
    }
}

/* Helper function for handleClientsBlockedOnKeys(). This function is called
 * in order to check if we can serve clients blocked by modules using
 * RM_BlockClientOnKeys(), when the corresponding key was signaled as ready:
 * our goal here is to call the RedisModuleBlockedClient reply() callback to
 * see if the key is really able to serve the client, and in that case,
 * unblock it. */
/* serveClientsBlockedOnKeyByModule() 的辅助函数。该函数用于检查是否可以服务被模块通过
 * RM_BlockClientOnKeys() 阻塞的客户端，当对应的键被标记为就绪时：
 * 这里的目标是调用 RedisModuleBlockedClient 的 reply() 回调，
 * 以判断该键是否真的可以服务客户端，如果可以，则解除阻塞。*/
void serveClientsBlockedOnKeyByModule(readyList *rl) {
    dictEntry *de;

    /* Optimization: If no clients are in type BLOCKED_MODULE,
     * we can skip this loop. */
    /* 优化：如果没有 BLOCKED_MODULE 类型的客户端，可以跳过这个循环。*/
    if (!server.blocked_clients_by_type[BLOCKED_MODULE]) return;

    /* We serve clients in the same order they blocked for
     * this key, from the first blocked to the last. */
    /* 按照客户端阻塞该键的顺序，从第一个阻塞到最后一个依次服务。*/
    de = dictFind(rl->db->blocking_keys,rl->key);
    if (de) {
        list *clients = dictGetVal(de);
        int numclients = listLength(clients);

        while(numclients--) {
            listNode *clientnode = listFirst(clients);
            client *receiver = clientnode->value;

            /* Put at the tail, so that at the next call
             * we'll not run into it again: clients here may not be
             * ready to be served, so they'll remain in the list
             * sometimes. We want also be able to skip clients that are
             * not blocked for the MODULE type safely. */
            /* 放到链表尾部，这样下次调用时就不会再次遇到它：这里的客户端可能还没准备好被服务，
             * 所以有时会留在列表中。我们还希望能够安全地跳过那些不是 MODULE 类型阻塞的客户端。*/
            listRotateHeadToTail(clients);

            if (receiver->btype != BLOCKED_MODULE) continue;

            /* Note that if *this* client cannot be served by this key,
             * it does not mean that another client that is next into the
             * list cannot be served as well: they may be blocked by
             * different modules with different triggers to consider if a key
             * is ready or not. This means we can't exit the loop but need
             * to continue after the first failure. */
            /* 注意，如果这个客户端不能被当前键服务，并不意味着下一个客户端也不能被服务：
             * 他们可能被不同的模块阻塞，并且判断键是否就绪的触发条件也不同。
             * 这意味着我们不能在第一次失败后退出循环，而是要继续处理后面的客户端。*/
            client *old_client = server.current_client;
            server.current_client = receiver;
            monotime replyTimer;
            elapsedStart(&replyTimer);
            if (!moduleTryServeClientBlockedOnKey(receiver, rl->key)) continue;
            updateStatsOnUnblock(receiver, 0, elapsedUs(replyTimer));

            moduleUnblockClient(receiver);
            afterCommand(receiver);
            server.current_client = old_client;
        }
    }
}

/* This function should be called by Redis every time a single command,
 * a MULTI/EXEC block, or a Lua script, terminated its execution after
 * being called by a client. It handles serving clients blocked in
 * lists, streams, and sorted sets, via a blocking commands.
 *
 * All the keys with at least one client blocked that received at least
 * one new element via some write operation are accumulated into
 * the server.ready_keys list. This function will run the list and will
 * serve clients accordingly. Note that the function will iterate again and
 * again as a result of serving BLMOVE we can have new blocking clients
 * to serve because of the PUSH side of BLMOVE.
 *
 * This function is normally "fair", that is, it will server clients
 * using a FIFO behavior. However this fairness is violated in certain
 * edge cases, that is, when we have clients blocked at the same time
 * in a sorted set and in a list, for the same key (a very odd thing to
 * do client side, indeed!). Because mismatching clients (blocking for
 * a different type compared to the current key type) are moved in the
 * other side of the linked list. However as long as the key starts to
 * be used only for a single type, like virtually any Redis application will
 * do, the function is already fair. */
/* 该函数应在 Redis 每次执行完单条命令、MULTI/EXEC 块或 Lua 脚本后被调用，
 * 用于处理因阻塞命令而被阻塞在列表、流和有序集合上的客户端。
 *
 * 所有收到至少一个新元素写入操作且有至少一个客户端阻塞的键，
 * 都会被累积到 server.ready_keys 列表中。该函数会遍历该列表并相应地服务客户端。
 * 注意：由于服务 BLMOVE 时 PUSH 端可能会有新的阻塞客户端需要处理，
 * 所以该函数会不断迭代。
 *
 * 该函数通常是“公平”的，即会以 FIFO 行为服务客户端。
 * 但在某些边缘情况下会破坏这种公平性，比如当有客户端同时阻塞在同一个键的有序集合和列表上
 * （客户端这样做其实很罕见）。因为类型不匹配的客户端（阻塞类型与当前键类型不同）
 * 会被移动到链表的另一端。不过只要该键只被用于单一类型（几乎所有 Redis 应用都会这样），
 * 该函数就是公平的。
 */
void handleClientsBlockedOnKeys(void) {
    while(listLength(server.ready_keys) != 0) {
        list *l;

        /* Point server.ready_keys to a fresh list and save the current one
         * locally. This way as we run the old list we are free to call
         * signalKeyAsReady() that may push new elements in server.ready_keys
         * when handling clients blocked into BLMOVE. */
        /* 将 server.ready_keys 指向一个新的列表，并将当前列表保存到本地变量。
         * 这样在遍历旧列表时，可以自由调用 signalKeyAsReady()，
         * 以便在处理 BLMOVE 阻塞客户端时向 server.ready_keys 推入新元素。*/
        l = server.ready_keys;
        server.ready_keys = listCreate();

        while(listLength(l) != 0) {
            listNode *ln = listFirst(l);
            readyList *rl = ln->value;

            /* First of all remove this key from db->ready_keys so that
             * we can safely call signalKeyAsReady() against this key. */
            /* 首先从 db->ready_keys 中移除该键，这样就可以安全地对该键调用 signalKeyAsReady()。*/
            dictDelete(rl->db->ready_keys,rl->key);

            /* Even if we are not inside call(), increment the call depth
             * in order to make sure that keys are expired against a fixed
             * reference time, and not against the wallclock time. This
             * way we can lookup an object multiple times (BLMOVE does
             * that) without the risk of it being freed in the second
             * lookup, invalidating the first one.
             * See https://github.com/redis/redis/pull/6554. */
            /* 即使当前不在 call() 内部，也要增加调用深度，
             * 以确保键的过期时间是基于固定参考时间而不是墙上时钟时间。
             * 这样可以多次查找同一个对象（BLMOVE 会这样做），
             * 而不会因为第二次查找被释放而导致第一次查找失效。
             * 详见：https://github.com/redis/redis/pull/6554 */
            server.fixed_time_expire++;
            updateCachedTime(0);

            /* 服务阻塞在该键上的客户端。*/ /* Serve clients blocked on the key. */
            robj *o = lookupKeyWrite(rl->db,rl->key);

            if (o != NULL) {
                if (o->type == OBJ_LIST)
                    serveClientsBlockedOnListKey(o,rl);
                else if (o->type == OBJ_ZSET)
                    serveClientsBlockedOnSortedSetKey(o,rl);
                else if (o->type == OBJ_STREAM)
                    serveClientsBlockedOnStreamKey(o,rl);
                /* We want to serve clients blocked on module keys
                 * regardless of the object type: we don't know what the
                 * module is trying to accomplish right now. */
                /* 无论对象类型如何，都要服务阻塞在模块键上的客户端：
                 * 因为我们不知道模块当前要完成什么操作。*/
                serveClientsBlockedOnKeyByModule(rl);
            }
            server.fixed_time_expire--;

            /* 释放该项。*/ /* Free this item. */
            decrRefCount(rl->key);
            zfree(rl);
            listDelNode(l,ln);
        }
        listRelease(l); /* 此时我们已经有了新的列表，释放旧列表。*/ /* We have the new list on place at this point. */
    }
}

/* This is how the current blocking lists/sorted sets/streams work, we use
 * BLPOP as example, but the concept is the same for other list ops, sorted
 * sets and XREAD.
 * - If the user calls BLPOP and the key exists and contains a non empty list
 *   then LPOP is called instead. So BLPOP is semantically the same as LPOP
 *   if blocking is not required.
 * - If instead BLPOP is called and the key does not exists or the list is
 *   empty we need to block. In order to do so we remove the notification for
 *   new data to read in the client socket (so that we'll not serve new
 *   requests if the blocking request is not served). Also we put the client
 *   in a dictionary (db->blocking_keys) mapping keys to a list of clients
 *   blocking for this keys.
 * - If a PUSH operation against a key with blocked clients waiting is
 *   performed, we mark this key as "ready", and after the current command,
 *   MULTI/EXEC block, or script, is executed, we serve all the clients waiting
 *   for this list, from the one that blocked first, to the last, accordingly
 *   to the number of elements we have in the ready list.
 */
/* 当前阻塞的列表/有序集合/流的工作方式如下，以 BLPOP 为例，
 * 但其他列表操作、有序集合和 XREAD 的原理相同：
 * - 如果用户调用 BLPOP 且键存在且列表非空，则直接调用 LPOP。
 *   所以如果不需要阻塞，BLPOP 语义上等同于 LPOP。
 * - 如果 BLPOP 被调用时键不存在或列表为空，则需要阻塞。
 *   为此，我们会移除客户端 socket 的新数据可读通知（这样如果阻塞请求未被服务就不会处理新请求），
 *   并将客户端放入一个字典（db->blocking_keys），该字典将键映射到阻塞该键的客户端列表。
 * - 如果对有阻塞客户端等待的键执行 PUSH 操作，则将该键标记为“就绪”，
 *   并在当前命令、MULTI/EXEC 块或脚本执行完后，按阻塞顺序服务所有等待该列表的客户端，
 *   从第一个阻塞的到最后一个，按 ready 列表中的元素数量依次处理。
 */

/* Set a client in blocking mode for the specified key (list, zset or stream),
 * with the specified timeout. The 'type' argument is BLOCKED_LIST,
 * BLOCKED_ZSET or BLOCKED_STREAM depending on the kind of operation we are
 * waiting for an empty key in order to awake the client. The client is blocked
 * for all the 'numkeys' keys as in the 'keys' argument. When we block for
 * stream keys, we also provide an array of streamID structures: clients will
 * be unblocked only when items with an ID greater or equal to the specified
 * one is appended to the stream. */
/* 让客户端以阻塞模式等待指定的键（列表、有序集合或流），并设置超时时间。
 * type 参数为 BLOCKED_LIST、BLOCKED_ZSET 或 BLOCKED_STREAM，
 * 取决于我们等待的操作类型。客户端会阻塞在 keys 参数中的所有 numkeys 个键上。
 * 如果是流键，还会提供 streamID 数组：只有当追加的项的 ID 大于等于指定值时，客户端才会被唤醒。*/
void blockForKeys(client *c, int btype, robj **keys, int numkeys, mstime_t timeout, robj *target, struct listPos *listpos, streamID *ids) {
    dictEntry *de;
    list *l;
    int j;

    c->bpop.timeout = timeout;
    c->bpop.target = target;

    if (listpos != NULL) c->bpop.listpos = *listpos;

    if (target != NULL) incrRefCount(target);

    for (j = 0; j < numkeys; j++) {
        /* 为每个客户端阻塞的键分配 bkinfo 结构体。*/ /* Allocate our bkinfo structure, associated to each key the client
         * is blocked for. */
        bkinfo *bki = zmalloc(sizeof(*bki));
        if (btype == BLOCKED_STREAM)
            bki->stream_id = ids[j];

        /* 如果该键已存在于字典中则忽略。*/ /* If the key already exists in the dictionary ignore it. */
        if (dictAdd(c->bpop.keys,keys[j],bki) != DICT_OK) {
            zfree(bki);
            continue;
        }
        incrRefCount(keys[j]);

        /* 另一方面，将键映射到客户端。*/ /* And in the other "side", to map keys -> clients */
        de = dictFind(c->db->blocking_keys,keys[j]);
        if (de == NULL) {
            int retval;

            /* 对于每个键，我们都需要一个阻塞该键的客户端列表。*/ /* For every key we take a list of clients blocked for it */
            l = listCreate();
            retval = dictAdd(c->db->blocking_keys,keys[j],l);
            incrRefCount(keys[j]);
            serverAssertWithInfo(c,keys[j],retval == DICT_OK);
        } else {
            l = dictGetVal(de);
        }
        listAddNodeTail(l,c);
        bki->listnode = listLast(l);
    }
    blockClient(c,btype);
}

/* Unblock a client that's waiting in a blocking operation such as BLPOP.
 * You should never call this function directly, but unblockClient() instead. */
/* 解除因阻塞操作（如 BLPOP）而等待的客户端的阻塞。
 * 不要直接调用此函数，应使用 unblockClient()。*/
void unblockClientWaitingData(client *c) {
    dictEntry *de;
    dictIterator *di;
    list *l;

    serverAssertWithInfo(c,NULL,dictSize(c->bpop.keys) != 0);
    di = dictGetIterator(c->bpop.keys);
    /* The client may wait for multiple keys, so unblock it for every key. */
    /* 客户端可能会等待多个键，因此要为每个键解除阻塞。*/
    while((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);
        bkinfo *bki = dictGetVal(de);

        /* Remove this client from the list of clients waiting for this key. */
        /* 从等待该键的客户端列表中移除该客户端。*/
        l = dictFetchValue(c->db->blocking_keys,key);
        serverAssertWithInfo(c,key,l != NULL);
        listDelNode(l,bki->listnode);
        /* If the list is empty we need to remove it to avoid wasting memory */
        /* 如果列表为空，则需要删除它以避免浪费内存。*/
        if (listLength(l) == 0)
            dictDelete(c->db->blocking_keys,key);
    }
    dictReleaseIterator(di);

    /* Cleanup the client structure */
    /* 清理客户端结构体。*/
    dictEmpty(c->bpop.keys,NULL);
    if (c->bpop.target) {
        decrRefCount(c->bpop.target);
        c->bpop.target = NULL;
    }
    if (c->bpop.xread_group) {
        decrRefCount(c->bpop.xread_group);
        decrRefCount(c->bpop.xread_consumer);
        c->bpop.xread_group = NULL;
        c->bpop.xread_consumer = NULL;
    }
}

static int getBlockedTypeByType(int type) {
    switch (type) {
        case OBJ_LIST: return BLOCKED_LIST;
        case OBJ_ZSET: return BLOCKED_ZSET;
        case OBJ_MODULE: return BLOCKED_MODULE;
        case OBJ_STREAM: return BLOCKED_STREAM;
        default: return BLOCKED_NONE;
    }
}

/* If the specified key has clients blocked waiting for list pushes, this
 * function will put the key reference into the server.ready_keys list.
 * Note that db->ready_keys is a hash table that allows us to avoid putting
 * the same key again and again in the list in case of multiple pushes
 * made by a script or in the context of MULTI/EXEC.
 *
 * The list will be finally processed by handleClientsBlockedOnKeys() */

/* 如果指定的键有客户端因等待列表推入而阻塞，
 * 则将该键引用放入 server.ready_keys 列表。
 * 注意 db->ready_keys 是一个哈希表，可以避免在脚本或 MULTI/EXEC 场景下
 * 多次推入同一个键。
 *
 * 该列表最终会由 handleClientsBlockedOnKeys() 处理。
*/
void signalKeyAsReady(redisDb *db, robj *key, int type) {
    readyList *rl;

    /* 快速返回。*/ /* Quick returns. */
    int btype = getBlockedTypeByType(type);
    if (btype == BLOCKED_NONE) {
        /* 该类型永远不会阻塞。*/ /* The type can never block. */
        return;
    }
    if (!server.blocked_clients_by_type[btype] &&
        !server.blocked_clients_by_type[BLOCKED_MODULE]) {
        /* No clients block on this type. Note: Blocked modules are represented
         * by BLOCKED_MODULE, even if the intention is to wake up by normal
         * types (list, zset, stream), so we need to check that there are no
         * blocked modules before we do a quick return here. */
        /* 没有客户端阻塞在该类型上。注意：被模块阻塞的客户端用 BLOCKED_MODULE 表示，
         * 即使唤醒意图是通过普通类型（list、zset、stream），
         * 所以这里需要确保没有模块阻塞客户端才能快速返回。*/
        return;
    }

    /* 没有客户端阻塞该键？无需入队。*/ /* No clients blocking for this key? No need to queue it. */
    if (dictFind(db->blocking_keys,key) == NULL) return;

    /* 该键已经被标记为就绪？无需再次入队。*/ /* Key was already signaled? No need to queue it again. */
    if (dictFind(db->ready_keys,key) != NULL) return;

    /* 好的，需要将该键入队到 server.ready_keys。*/ /* Ok, we need to queue this key into server.ready_keys. */
    rl = zmalloc(sizeof(*rl));
    rl->key = key;
    rl->db = db;
    incrRefCount(key);
    listAddNodeTail(server.ready_keys,rl);

    /* We also add the key in the db->ready_keys dictionary in order
     * to avoid adding it multiple times into a list with a simple O(1)
     * check. */
    /* 还要将该键加入 db->ready_keys 字典，
     * 以便通过 O(1) 检查避免多次入队。*/
    incrRefCount(key);
    serverAssert(dictAdd(db->ready_keys,key,NULL) == DICT_OK);
}

