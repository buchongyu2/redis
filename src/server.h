/*
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
 */

#ifndef __REDIS_H
#define __REDIS_H

#include "atomicvar.h"
#include "config.h"
#include "fmacros.h"
#include "rio.h"
#include "solarisfixes.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <lua.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

#ifdef HAVE_LIBSYSTEMD
#include <systemd/sd-daemon.h>
#endif

typedef long long mstime_t; /* millisecond time type. */
typedef long long ustime_t; /* microsecond time type. */

#include "adlist.h"     /* Linked lists */
#include "ae.h"         /* Event driven programming library */
#include "anet.h"       /* Networking the easy way */
#include "dict.h"       /* Hash tables */
#include "intset.h"     /* Compact integer set structure */
#include "latency.h"    /* Latency monitor API */
#include "quicklist.h"  /* Lists are encoded as linked lists of
                           N-elements flat arrays */
#include "sds.h"        /* Dynamic safe strings */
#include "sparkline.h"  /* ASCII graphs API */
#include "util.h"       /* Misc functions useful in many places */
#include "version.h"    /* Version macro */
#include "ziplist.h"    /* Compact list data structure */
#include "zmalloc.h"    /* total memory usage aware version of malloc/free */
#include "connection.h" /* Connection abstraction */
#include "rax.h"        /* Radix tree */

#define REDISMODULE_CORE 1
#include "redismodule.h" /* Redis modules API defines. */

/* Following includes allow test functions to be called from Redis main() */
#include "crc64.h"
#include "endianconv.h"
#include "sha1.h"
#include "zipmap.h"

/* min/max 宏定义 */ /* min/max */
#undef min
#undef max
#define min(a, b) ((a) < (b) ? (a) : (b))
#define max(a, b) ((a) > (b) ? (a) : (b))

/* 错误码 */ /* Error codes */
#define C_OK 0
#define C_ERR -1

/* Static server configuration */
#define CONFIG_DEFAULT_HZ 10 /* 默认每秒调用定时器（serverCron）10次。 */ /* Time interrupt calls/sec. */
#define CONFIG_MIN_HZ 1                                                   /* hz 最小值，不能低于 1。 */
#define CONFIG_MAX_HZ 500                                                 /* hz 最大值，不能高于 500。 */
#define MAX_CLIENTS_PER_CLOCK_TICK 200                                    /* HZ is adapted based on that. */
#define CONFIG_MAX_LINE 1024
#define CRON_DBS_PER_CALL 16
#define NET_MAX_WRITES_PER_EVENT (1024 * 64)
#define PROTO_SHARED_SELECT_CMDS 10
#define OBJ_SHARED_INTEGERS 10000
#define OBJ_SHARED_BULKHDR_LEN 32
#define OBJ_SHARED_HDR_STRLEN(_len_) (((_len_) < 10) ? 4 : 5) /* see shared.mbulkhdr etc. */
#define LOG_MAX_LEN 1024                                      /* Default maximum length of syslog messages.*/
#define AOF_REWRITE_ITEMS_PER_CMD 64
#define AOF_READ_DIFF_INTERVAL_BYTES (1024 * 10)
#define CONFIG_AUTHPASS_MAX_LEN 512
#define CONFIG_RUN_ID_SIZE 40
#define RDB_EOF_MARK_SIZE 40
#define CONFIG_REPL_BACKLOG_MIN_SIZE (1024 * 16) /* 16k */
#define CONFIG_BGSAVE_RETRY_DELAY 5              /* Wait a few secs before trying again. */
#define CONFIG_DEFAULT_PID_FILE "/var/run/redis.pid"
#define CONFIG_DEFAULT_CLUSTER_CONFIG_FILE "nodes.conf"
#define CONFIG_DEFAULT_UNIX_SOCKET_PERM 0
#define CONFIG_DEFAULT_LOGFILE ""
#define NET_HOST_STR_LEN 256                          /* Longest valid hostname */
#define NET_IP_STR_LEN 46                             /* INET6_ADDRSTRLEN is 46, but we need to be sure */
#define NET_ADDR_STR_LEN (NET_IP_STR_LEN + 32)        /* Must be enough for ip:port */
#define NET_HOST_PORT_STR_LEN (NET_HOST_STR_LEN + 32) /* Must be enough for hostname:port */
#define CONFIG_BINDADDR_MAX 16
#define CONFIG_MIN_RESERVED_FDS 32
#define CONFIG_DEFAULT_PROC_TITLE_TEMPLATE "{title} {listen-addr} {server-mode}"

#define ACTIVE_EXPIRE_CYCLE_SLOW 0
#define ACTIVE_EXPIRE_CYCLE_FAST 1

/* Children process will exit with this status code to signal that the
 * process terminated without an error: this is useful in order to kill
 * a saving child (RDB or AOF one), without triggering in the parent the
 * write protection that is normally turned on on write errors.
 * Usually children that are terminated with SIGUSR1 will exit with this
 * special code. */
#define SERVER_CHILD_NOERROR_RETVAL 255

/* Reading copy-on-write info is sometimes expensive and may slow down child
 * processes that report it continuously. We measure the cost of obtaining it
 * and hold back additional reading based on this factor. */
#define CHILD_COW_DUTY_CYCLE 100

/* Instantaneous metrics tracking. */
#define STATS_METRIC_SAMPLES 16   /* Number of samples per metric. */
#define STATS_METRIC_COMMAND 0    /* Number of commands executed. */
#define STATS_METRIC_NET_INPUT 1  /* Bytes read to network .*/
#define STATS_METRIC_NET_OUTPUT 2 /* Bytes written to network. */
#define STATS_METRIC_COUNT 3

/* Protocol and I/O related defines */
#define PROTO_IOBUF_LEN (1024 * 16)         /* Generic I/O buffer size */
#define PROTO_REPLY_CHUNK_BYTES (16 * 1024) /* 16k output buffer */
#define PROTO_INLINE_MAX_SIZE (1024 * 64)   /* Max size of inline reads */
#define PROTO_MBULK_BIG_ARG (1024 * 32)
#define LONG_STR_SIZE 21                        /* Bytes needed for long -> str + '\0' */
#define REDIS_AUTOSYNC_BYTES (1024 * 1024 * 32) /* fdatasync every 32MB */

#define LIMIT_PENDING_QUERYBUF (4 * 1024 * 1024) /* 4mb */

/* When configuring the server eventloop, we setup it so that the total number
 * of file descriptors we can handle are server.maxclients + RESERVED_FDS +
 * a few more to stay safe. Since RESERVED_FDS defaults to 32, we add 96
 * in order to make sure of not over provisioning more than 128 fds. */
#define CONFIG_FDSET_INCR (CONFIG_MIN_RESERVED_FDS + 96)

/* OOM Score Adjustment classes. */
#define CONFIG_OOM_MASTER 0
#define CONFIG_OOM_REPLICA 1
#define CONFIG_OOM_BGCHILD 2
#define CONFIG_OOM_COUNT 3

extern int configOOMScoreAdjValuesDefaults[CONFIG_OOM_COUNT];

/* 哈希表参数 */ /* Hash table parameters */
#define HASHTABLE_MIN_FILL 10            /* 哈希表最小填充率 10% */ /* Minimal hash table fill 10% */
#define HASHTABLE_MAX_LOAD_FACTOR 1.618  /* 哈希表最大负载因子 */ /* Maximum hash table load factor. */

/* 命令标志。具体含义请参考 server.c 中的命令表。 */ /* Command flags. Please check the command table defined in the server.c file
 * for more information about the meaning of every flag. */
#define CMD_WRITE (1ULL << 0)           /* "写" 标志 */ /* "write" flag */
#define CMD_READONLY (1ULL << 1)        /* "只读" 标志 */ /* "read-only" flag */
#define CMD_DENYOOM (1ULL << 2)         /* "内存不足时拒绝" 标志 */ /* "use-memory" flag */
#define CMD_MODULE (1ULL << 3)          /* 由模块导出的命令 */ /* Command exported by module. */
#define CMD_ADMIN (1ULL << 4)           /* "管理" 标志 */ /* "admin" flag */
#define CMD_PUBSUB (1ULL << 5)          /* "发布/订阅" 标志 */ /* "pub-sub" flag */
#define CMD_NOSCRIPT (1ULL << 6)        /* "禁止在脚本中执行" 标志 *//* "no-script" flag */
#define CMD_RANDOM (1ULL << 7)          /* "随机" 标志 */ /* "random" flag */
#define CMD_SORT_FOR_SCRIPT (1ULL << 8) /* "脚本排序" 标志 */ /* "to-sort" flag */
#define CMD_LOADING (1ULL << 9)         /* "加载期间可用" 标志 *//* "ok-loading" flag */
#define CMD_STALE (1ULL << 10)          /* "数据过期期间可用" 标志 */ /* "ok-stale" flag */
#define CMD_SKIP_MONITOR (1ULL << 11)   /* "跳过 MONITOR" 标志 */ /* "no-monitor" flag */
#define CMD_SKIP_SLOWLOG (1ULL << 12)   /* "跳过 SLOWLOG" 标志 */ /* "no-slowlog" flag */
#define CMD_ASKING (1ULL << 13)         /* "集群 ASKING" 标志 */ /* "cluster-asking" flag */
#define CMD_FAST (1ULL << 14)           /* "快速" 标志 */ /* "fast" flag */
#define CMD_NO_AUTH (1ULL << 15)        /* "无需认证" 标志 */ /* "no-auth" flag */
#define CMD_MAY_REPLICATE (1ULL << 16)  /* "可能被复制" 标志 */ /* "may-replicate" flag */

/* 模块系统使用的命令标志位 */ /* Command flags used by the module system. */
#define CMD_MODULE_GETKEYS (1ULL << 17)    /* 使用模块的 getkeys 接口 */ /* Use the modules getkeys interface. */
#define CMD_MODULE_NO_CLUSTER (1ULL << 18) /* 在 Redis 集群模式下禁止使用 */ /* Deny on Redis Cluster. */

/* 描述 ACL 类别的命令标志位 */ /* Command flags that describe ACLs categories. */
#define CMD_CATEGORY_KEYSPACE (1ULL << 19)
#define CMD_CATEGORY_READ (1ULL << 20)
#define CMD_CATEGORY_WRITE (1ULL << 21)
#define CMD_CATEGORY_SET (1ULL << 22)
#define CMD_CATEGORY_SORTEDSET (1ULL << 23)
#define CMD_CATEGORY_LIST (1ULL << 24)
#define CMD_CATEGORY_HASH (1ULL << 25)
#define CMD_CATEGORY_STRING (1ULL << 26)
#define CMD_CATEGORY_BITMAP (1ULL << 27)
#define CMD_CATEGORY_HYPERLOGLOG (1ULL << 28)
#define CMD_CATEGORY_GEO (1ULL << 29)
#define CMD_CATEGORY_STREAM (1ULL << 30)
#define CMD_CATEGORY_PUBSUB (1ULL << 31)
#define CMD_CATEGORY_ADMIN (1ULL << 32)
#define CMD_CATEGORY_FAST (1ULL << 33)
#define CMD_CATEGORY_SLOW (1ULL << 34)
#define CMD_CATEGORY_BLOCKING (1ULL << 35)
#define CMD_CATEGORY_DANGEROUS (1ULL << 36)
#define CMD_CATEGORY_CONNECTION (1ULL << 37)
#define CMD_CATEGORY_TRANSACTION (1ULL << 38)
#define CMD_CATEGORY_SCRIPTING (1ULL << 39)

/* AOF 状态 */ /* AOF states */
#define AOF_OFF 0          /* AOF 关闭 */ /* AOF is off */
#define AOF_ON 1           /* AOF 开启 */ /* AOF is on */
#define AOF_WAIT_REWRITE 2 /* AOF 等待重写以开始追加 */ /* AOF waits rewrite to start appending */

/* 客户端标志 */ /* Client flags */
#define CLIENT_SLAVE (1 << 0)             /* 此客户端是Slave副本 */ /* This client is a replica */
#define CLIENT_MASTER (1 << 1)            /* 此客户端是主节点 */ /* This client is a master */
#define CLIENT_MONITOR (1 << 2)           /* 此客户端是副本监控器，见 MONITOR */ /* This client is a slave monitor, see MONITOR */
#define CLIENT_MULTI (1 << 3)             /* 此客户端处于 MULTI 上下文，即处于事务中 */ /* This client is in a MULTI context */
#define CLIENT_BLOCKED (1 << 4)           /* 客户端正在等待阻塞操作 */ /* The client is waiting in a blocking operation */
#define CLIENT_DIRTY_CAS (1 << 5)         /* 被 WATCH 的键已被修改，EXEC 会失败 */ /* Watched keys modified. EXEC will fail. */
#define CLIENT_CLOSE_AFTER_REPLY (1 << 6) /* 回复全部写完后关闭连接 */ /* Close after writing entire reply. */
#define CLIENT_UNBLOCKED                                                                                               \
    (1 << 7)                                /* 此客户端已解除阻塞，存储在 server.unblocked_clients 中 */ /* This client was unblocked and is stored in                              \
                                              server.unblocked_clients */
#define CLIENT_LUA (1 << 8)                 /* 这是 Lua 使用的非连接客户端 */ /* This is a non connected client used by Lua */
#define CLIENT_ASKING (1 << 9)              /* 客户端执行了 ASKING 命令 */ /* Client issued the ASKING command */
#define CLIENT_CLOSE_ASAP (1 << 10)         /* 尽快关闭此客户端 */ /* Close this client ASAP */
#define CLIENT_UNIX_SOCKET (1 << 11)        /* 客户端通过 Unix 域套接字连接 */ /* Client connected via Unix domain socket */
#define CLIENT_DIRTY_EXEC (1 << 12)         /* 队列命令出错，EXEC 会失败 */ /* EXEC will fail for errors while queueing */
#define CLIENT_MASTER_FORCE_REPLY (1 << 13) /* 即使是主节点也强制队列回复 */ /* Queue replies even if is master */
#define CLIENT_FORCE_AOF (1 << 14)          /* 强制当前命令 AOF 持久化 */ /* Force AOF propagation of current cmd. */
#define CLIENT_FORCE_REPL (1 << 15)         /* 强制当前命令复制到副本 */ /* Force replication of current cmd. */
#define CLIENT_PRE_PSYNC (1 << 16)          /* 实例不支持 PSYNC */ /* Instance don't understand PSYNC. */
#define CLIENT_READONLY (1 << 17)           /* 集群客户端处于只读状态 */ /* Cluster client is in read-only state. */
#define CLIENT_PUBSUB (1 << 18)             /* 客户端处于发布/订阅模式 */ /* Client is in Pub/Sub mode. */
#define CLIENT_PREVENT_AOF_PROP (1 << 19)   /* 不传播到 AOF */ /* Don't propagate to AOF. */
#define CLIENT_PREVENT_REPL_PROP (1 << 20)  /* 不传播到salve *//* Don't propagate to slaves. */
#define CLIENT_PREVENT_PROP (CLIENT_PREVENT_AOF_PROP | CLIENT_PREVENT_REPL_PROP)
#define CLIENT_PENDING_WRITE                                                                                           \
    (1 << 21)                            /*客户端有待发送的数据，但尚未安装写处理器。*/ /* Client has output to send but a write                                      \
                                            handler is yet not installed. */
#define CLIENT_REPLY_OFF (1 << 22)       /*不向客户端发送回复。*/ /* Don't send replies to client. */
#define CLIENT_REPLY_SKIP_NEXT (1 << 23) /*下一个命令设置跳过回复标志。*/ /* Set CLIENT_REPLY_SKIP for next cmd */
#define CLIENT_REPLY_SKIP (1 << 24)      /*仅跳过本次回复，不发送。*/ /* Don't send just this reply. */
#define CLIENT_LUA_DEBUG (1 << 25)       /*以调试模式运行 EVAL（Lua 脚本）。*/ /* Run EVAL in debug mode. */
#define CLIENT_LUA_DEBUG_SYNC (1 << 26)  /*EVAL 调试时不使用 fork()。*/ /* EVAL debugging without fork() */
#define CLIENT_MODULE (1 << 27)          /*由某些模块使用的非连接客户端。*/ /* Non connected client used by some module. */
#define CLIENT_PROTECTED (1 << 28)       /*当前不应释放该客户端。*/ /* Client should not be freed for now. */
#define CLIENT_PENDING_READ                                                                                            \
    (1 << 29) /* 该客户端有待处理的读操作，并已被加入可读取的客户端列表。 */ /* The client has pending reads and was   \
                 put in the list of clients we can read from. */
#define CLIENT_PENDING_COMMAND                                                                                         \
    (1 << 30) /* Indicates the client has a fully                                                                      \
               * parsed command ready for execution. */
#define CLIENT_TRACKING                                                                                                \
    (1ULL << 31)                                  /* Client enabled keys tracking in order to                          \
                                                  perform client side caching. */
#define CLIENT_TRACKING_BROKEN_REDIR (1ULL << 32) /* Target client is invalid. */
#define CLIENT_TRACKING_BCAST (1ULL << 33)        /* Tracking in BCAST mode. */
#define CLIENT_TRACKING_OPTIN (1ULL << 34)        /* Tracking in opt-in mode. */
#define CLIENT_TRACKING_OPTOUT (1ULL << 35)       /* Tracking in opt-out mode. */
#define CLIENT_TRACKING_CACHING                                                                                        \
    (1ULL << 36) /* 客户端启用缓存跟踪（CACHING yes/no），具体取决于选择加入或退出模式。 /* CACHING yes/no was given,                                                                          \
                    depending on optin/optout mode. */
#define CLIENT_TRACKING_NOLOOP                                                                                         \
    (1ULL << 37)                           /*客户端不会收到自己写入数据的失效消息（避免循环通知）。*//* Don't send invalidation messages                                         \
                                              about writes performed by myself.*/
#define CLIENT_IN_TO_TABLE (1ULL << 38)    /*客户端已加入超时表（用于管理超时）。*/ /* This client is in the timeout table. */
#define CLIENT_PROTOCOL_ERROR (1ULL << 39) /*客户端与服务器通信时发生协议错误。*//* Protocol error chatting with it. */
#define CLIENT_CLOSE_AFTER_COMMAND                                                                                     \
    (1ULL << 40) /*客户端在执行完命令并写完全部回复后关闭连接。*//* Close after executing commands                                                                     \
                  * and writing entire reply. */
#define CLIENT_DENY_BLOCKING                                                                                           \
    (1ULL << 41) /*客户端不允许被阻塞（如在 MULTI、Lua、RM_Call、AOF 客户端中启用）。*//* Indicate that the client should not be blocked.                                                    \
                    currently, turned on inside MULTI, Lua, RM_Call,                                                   \
                    and AOF client */
#define CLIENT_REPL_RDBONLY                                                                                            \
    (1ULL << 42)                    /*客户端是只需要 RDB 文件的副本，不需要复制缓冲区。*//* This client is a replica that only wants                                        \
                                       RDB without replication buffer. */
#define CLIENT_PUSHING (1ULL << 43) /*客户端正在推送通知。*//* This client is pushing notifications. */

/**
 * 客户端阻塞类型（btype 字段）
 */
/* Client block type (btype field in client structure)
 * if CLIENT_BLOCKED flag is set. */
#define BLOCKED_NONE 0   /*未阻塞。*/ */ /* Not blocked, no CLIENT_BLOCKED flag set. */
#define BLOCKED_LIST 1   /*被 BLPOP 等列表阻塞命令阻塞。*/ /* BLPOP & co. */
#define BLOCKED_WAIT 2   /*被 WAIT 命令（用于同步复制）阻塞。*/ /* WAIT for synchronous replication. */
#define BLOCKED_MODULE 3 /*被可加载模块阻塞。*/ /* Blocked by a loadable module. */
#define BLOCKED_STREAM 4 /*被 XREAD 流命令阻塞。*/ /* XREAD. */
#define BLOCKED_ZSET 5   /*被 BZPOP 等有序集合阻塞命令阻塞。*/ /* BZPOP et al. */
#define BLOCKED_PAUSE 6  /*被 CLIENT PAUSE 命令阻塞。*/ /* Blocked by CLIENT PAUSE */
#define BLOCKED_NUM 7    /*阻塞状态的数量。*/ /* Number of blocked states. */

/*行内请求协议。 */ /* Client request types */
#define PROTO_REQ_INLINE 1     // 行内请求协议
#define PROTO_REQ_MULTIBULK 2  // 多批量请求协议。

/* 客户端类别，用于客户端限制，目前仅用于最大客户端输出缓冲区限制实现。 */ /* Client classes for client limits, currently used only for
 * the max-client-output-buffer limit implementation. */
#define CLIENT_TYPE_NORMAL 0 /* 普通请求-回复客户端和 MONITOR 客户端 */ /* Normal req-reply clients + MONITORs */
#define CLIENT_TYPE_SLAVE 1  /* slave 副本客户端 */ /* Slaves. */
#define CLIENT_TYPE_PUBSUB 2 /* 订阅了 PubSub 频道的客户端 */ /* Clients subscribed to PubSub channels. */
#define CLIENT_TYPE_MASTER 3 /* 主节点客户端 */ /* Master. */
#define CLIENT_TYPE_COUNT 4  /* 客户端类型总数 */ /* Total number of client types. */
#define CLIENT_TYPE_OBUF_COUNT                                                                                         \
    3 /* 需要暴露输出缓冲区配置的客户端数量，仅前三种：普通、slave、副本、pubsub */ /* Number of clients to expose to output                                                                         \
         buffer configuration. Just the first                                                                          \
         three: normal, slave, pubsub. */

/* Slave replication state. Used in server.repl_state for slaves to remember
 * what to do next. */
typedef enum
{
    REPL_STATE_NONE = 0,   /* 没有激活的复制 */ /* No active replication */
    REPL_STATE_CONNECT,    /* 需要连接主节点 */ /* Must connect to master */
    REPL_STATE_CONNECTING,  /* 正在连接主节点 */ /* Connecting to master */
    /* --- 握手阶段，必须按顺序 --- */ /* --- Handshake states, must be ordered --- */
    REPL_STATE_RECEIVE_PING_REPLY,  /* 等待 PING 回复 */ /* Wait for PING reply */
    REPL_STATE_SEND_HANDSHAKE,      /* 向主节点发送握手序列 */ /* Send handshake sequance to master */
    REPL_STATE_RECEIVE_AUTH_REPLY,  /* 等待 AUTH 回复 */ /* Wait for AUTH reply */
    REPL_STATE_RECEIVE_PORT_REPLY,  /* 等待 REPLCONF 回复 */ /* Wait for REPLCONF reply */
    REPL_STATE_RECEIVE_IP_REPLY,    /* 等待 REPLCONF 回复 */ /* Wait for REPLCONF reply */
    REPL_STATE_RECEIVE_CAPA_REPLY,  /* 等待 REPLCONF 回复 */ /* Wait for REPLCONF reply */
    REPL_STATE_SEND_PSYNC,          /* 发送 PSYNC */ /* Send PSYNC */
    REPL_STATE_RECEIVE_PSYNC_REPLY, /* 等待 PSYNC 回复 */ /* Wait for PSYNC reply */
     /* --- 握手阶段结束 --- */ /* --- End of handshake states --- */
    REPL_STATE_TRANSFER,   /* 正在从主节点接收 .rdb 文件 */ /* Receiving .rdb from master */
    REPL_STATE_CONNECTED,  /* 已连接主节点 */ /* Connected to master */
} repl_state;

/* 协调故障转移的状态 */ /* The state of an in progress coordinated failover */
typedef enum
{
    NO_FAILOVER = 0,        /* 没有故障转移 */ /* No failover in progress */
    FAILOVER_WAIT_FOR_SYNC, /* 等待目标副本追上主节点 */ /* Waiting for target replica to catch up */
    FAILOVER_IN_PROGRESS    /* 等待目标副本接受 PSYNC FAILOVER 请求 */ /* Waiting for target replica to accept
                             * PSYNC FAILOVER request. */
} failover_state;

/* State of slaves from the POV of the master. Used in client->replstate.
 * In SEND_BULK and ONLINE state the slave receives new updates
 * in its output queue. In the WAIT_BGSAVE states instead the server is waiting
 * to start the next background saving in order to send updates to it. */
/* master 视角下的 slave 状态。用于 client->replstate。
 * 在 SEND_BULK 和 ONLINE 状态下，slave 会在输出队列接收新数据。
 * 在 WAIT_BGSAVE 状态下，服务器等待开始下一次后台保存以便向其发送数据。 */
#define SLAVE_STATE_WAIT_BGSAVE_START 6 /* 需要生成新的 RDB 文件 */ /* We need to produce a new RDB file. */
#define SLAVE_STATE_WAIT_BGSAVE_END 7   /* 等待 RDB 文件创建完成 */ /* Waiting RDB file creation to finish. */
#define SLAVE_STATE_SEND_BULK 8         /* 正在向 slave 发送 RDB 文件 */ /* Sending RDB file to slave. */
#define SLAVE_STATE_ONLINE 9            /* RDB 文件已传输完毕，仅发送更新数据 */ /* RDB file transmitted, sending just updates. */

/* Slave 能力标志 */ /* Slave capabilities. */
#define SLAVE_CAPA_NONE 0
#define SLAVE_CAPA_EOF (1 << 0)    /* 能解析 RDB EOF 流式格式 */ /* Can parse the RDB EOF streaming format. */
#define SLAVE_CAPA_PSYNC2 (1 << 1) /* 可以解析psync2协议 */* Supports PSYNC2 protocol. */

/* Synchronous read timeout - slave side */
#define CONFIG_REPL_SYNCIO_TIMEOUT 5

/* List related stuff */
#define LIST_HEAD 0
#define LIST_TAIL 1
#define ZSET_MIN 0
#define ZSET_MAX 1

/* Sort operations */
#define SORT_OP_GET 0

/* Log levels */
#define LL_DEBUG 0
#define LL_VERBOSE 1
#define LL_NOTICE 2
#define LL_WARNING 3
#define LL_RAW (1 << 10) /* Modifier to log without timestamp */

/* Supervision options */
#define SUPERVISED_NONE 0
#define SUPERVISED_AUTODETECT 1
#define SUPERVISED_SYSTEMD 2
#define SUPERVISED_UPSTART 3

/* Anti-warning macro... */
#define UNUSED(V) ((void)V)

#define ZSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^64 elements */
#define ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */

/* Append only defines */
#define AOF_FSYNC_NO 0
#define AOF_FSYNC_ALWAYS 1
#define AOF_FSYNC_EVERYSEC 2

/* Replication diskless load defines */
#define REPL_DISKLESS_LOAD_DISABLED 0
#define REPL_DISKLESS_LOAD_WHEN_DB_EMPTY 1
#define REPL_DISKLESS_LOAD_SWAPDB 2

/* TLS Client Authentication */
#define TLS_CLIENT_AUTH_NO 0
#define TLS_CLIENT_AUTH_YES 1
#define TLS_CLIENT_AUTH_OPTIONAL 2

/* Sanitize dump payload */
#define SANITIZE_DUMP_NO 0
#define SANITIZE_DUMP_YES 1
#define SANITIZE_DUMP_CLIENTS 2

/* Sets operations codes */
#define SET_OP_UNION 0
#define SET_OP_DIFF 1
#define SET_OP_INTER 2

/* oom-score-adj defines */
#define OOM_SCORE_ADJ_NO 0
#define OOM_SCORE_RELATIVE 1
#define OOM_SCORE_ADJ_ABSOLUTE 2

/* Redis maxmemory strategies. Instead of using just incremental number
 * for this defines, we use a set of flags so that testing for certain
 * properties common to multiple policies is faster. */
#define MAXMEMORY_FLAG_LRU (1 << 0)
#define MAXMEMORY_FLAG_LFU (1 << 1)
#define MAXMEMORY_FLAG_ALLKEYS (1 << 2)
#define MAXMEMORY_FLAG_NO_SHARED_INTEGERS (MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_LFU)

#define MAXMEMORY_VOLATILE_LRU ((0 << 8) | MAXMEMORY_FLAG_LRU)
#define MAXMEMORY_VOLATILE_LFU ((1 << 8) | MAXMEMORY_FLAG_LFU)
#define MAXMEMORY_VOLATILE_TTL (2 << 8)
#define MAXMEMORY_VOLATILE_RANDOM (3 << 8)
#define MAXMEMORY_ALLKEYS_LRU ((4 << 8) | MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_ALLKEYS_LFU ((5 << 8) | MAXMEMORY_FLAG_LFU | MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_ALLKEYS_RANDOM ((6 << 8) | MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_NO_EVICTION (7 << 8)

/* Units */
#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1

/* SHUTDOWN flags */
#define SHUTDOWN_NOFLAGS 0 /* No flags. */
#define SHUTDOWN_SAVE                                                                                                  \
    1                     /* Force SAVE on SHUTDOWN even if no save                                                    \
                             points are configured. */
#define SHUTDOWN_NOSAVE 2 /* Don't SAVE on SHUTDOWN. */

/* Command call flags, see call() function */
#define CMD_CALL_NONE 0
#define CMD_CALL_SLOWLOG (1 << 0)
#define CMD_CALL_STATS (1 << 1)
#define CMD_CALL_PROPAGATE_AOF (1 << 2)
#define CMD_CALL_PROPAGATE_REPL (1 << 3)
#define CMD_CALL_PROPAGATE (CMD_CALL_PROPAGATE_AOF | CMD_CALL_PROPAGATE_REPL)
#define CMD_CALL_FULL (CMD_CALL_SLOWLOG | CMD_CALL_STATS | CMD_CALL_PROPAGATE)
#define CMD_CALL_NOWRAP                                                                                                \
    (1 << 4) /* Don't wrap also propagate array into                                                                   \
                MULTI/EXEC: the caller will handle it.  */

/* 命令传播标志，见 propagate() 函数 */ /* Command propagation flags, see propagate() function */
#define PROPAGATE_NONE 0
#define PROPAGATE_AOF 1
#define PROPAGATE_REPL 2

/* 客户端暂停类型，数值越大限制越严格 */ /* Client pause types, larger types are more restrictive
 * pause types than smaller pause types. */
typedef enum
{
    CLIENT_PAUSE_OFF = 0, /* 不暂停任何命令 */ /* Pause no commands */
    CLIENT_PAUSE_WRITE,   /* 暂停写命令 */ /* Pause write commands */
    CLIENT_PAUSE_ALL      /* 暂停所有命令 */  /* Pause all commands */
} pause_type;

/* RDB 活动子进程保存类型 */ /* RDB active child save type. */
#define RDB_CHILD_TYPE_NONE 0
#define RDB_CHILD_TYPE_DISK 1   /* RDB 写入磁盘 */ /* RDB is written to disk. */
#define RDB_CHILD_TYPE_SOCKET 2 /* 暂停所有命令 */ /* RDB is written to slave socket. */

/* Keyspace changes notification classes. Every class is associated with a
 * character for configuration purposes. */
/* 键空间变更通知类别。每个类别都关联一个字符用于配置。 */
#define NOTIFY_KEYSPACE (1 << 0)  /* K */
#define NOTIFY_KEYEVENT (1 << 1)  /* E */
#define NOTIFY_GENERIC (1 << 2)   /* g */
#define NOTIFY_STRING (1 << 3)    /* $ */
#define NOTIFY_LIST (1 << 4)      /* l */
#define NOTIFY_SET (1 << 5)       /* s */
#define NOTIFY_HASH (1 << 6)      /* h */
#define NOTIFY_ZSET (1 << 7)      /* z */
#define NOTIFY_EXPIRED (1 << 8)   /* x */
#define NOTIFY_EVICTED (1 << 9)   /* e */
#define NOTIFY_STREAM (1 << 10)   /* t */
#define NOTIFY_KEY_MISS (1 << 11) /* m（注意：故意不包含在 NOTIFY_ALL 中） */ /* m (Note: This one is excluded from NOTIFY_ALL on purpose) */
#define NOTIFY_LOADED (1 << 12)   /* 仅模块用，表示从 rdb 加载的 key */ /* module only key space notification, indicate a key loaded from rdb */
#define NOTIFY_MODULE (1 << 13)   /* d，模块键空间通知 */ /* d, module key space notification */
#define NOTIFY_ALL                                                                                                     \
    (NOTIFY_GENERIC | NOTIFY_STRING | NOTIFY_LIST | NOTIFY_SET | NOTIFY_HASH | NOTIFY_ZSET | NOTIFY_EXPIRED |          \
     NOTIFY_EVICTED | NOTIFY_STREAM | NOTIFY_MODULE) /* 一个标志 */ /* A flag */

/* 获取第一个绑定地址，没有则返回 NULL */ /* Get the first bind addr or NULL */
#define NET_FIRST_BIND_ADDR (server.bindaddr_count ? server.bindaddr[0] : NULL)

/* Using the following macro you can run code inside serverCron() with the
 * specified period, specified in milliseconds.
 * The actual resolution depends on server.hz. */
/* 用下面的宏可以在 serverCron() 中按指定周期（毫秒）运行代码。
 * 实际分辨率取决于 server.hz。 */
#define run_with_period(_ms_) if ((_ms_ <= 1000 / server.hz) || !(server.cronloops % ((_ms_) / (1000 / server.hz))))

/* We can print the stacktrace, so our assert is defined this way: */
/* 可以打印堆栈信息，所以断言这样定义： */
#define serverAssertWithInfo(_c, _o, _e)                                                                               \
    ((_e) ? (void)0 : (_serverAssertWithInfo(_c, _o, #_e, __FILE__, __LINE__), redis_unreachable()))
#define serverAssert(_e) ((_e) ? (void)0 : (_serverAssert(#_e, __FILE__, __LINE__), redis_unreachable()))
#define serverPanic(...) _serverPanic(__FILE__, __LINE__, __VA_ARGS__), redis_unreachable()

/*-----------------------------------------------------------------------------
 * 数据类型 Data types
 *----------------------------------------------------------------------------*/

/* 一个 Redis 对象，可以保存字符串/列表/集合等类型 */ /* A redis object, that is a type able to hold a string / list / set */

/* 实际的 Redis 对象类型定义 */ /* The actual Redis Object */
#define OBJ_STRING 0 /* 字符串对象 */ /* String object. */
#define OBJ_LIST 1   /* 列表对象 */ /* List object. */
#define OBJ_SET 2    /* 集合对象 */  /* Set object. */
#define OBJ_ZSET 3   /* 有序集合对象 */ /* Sorted set object. */
#define OBJ_HASH 4  /* 哈希对象 */  /* Hash object. */

/* The "module" object type is a special one that signals that the object
 * is one directly managed by a Redis module. In this case the value points
 * to a moduleValue struct, which contains the object value (which is only
 * handled by the module itself) and the RedisModuleType struct which lists
 * function pointers in order to serialize, deserialize, AOF-rewrite and
 * free the object.
 *
 * Inside the RDB file, module types are encoded as OBJ_MODULE followed
 * by a 64 bit module type ID, which has a 54 bits module-specific signature
 * in order to dispatch the loading to the right module, plus a 10 bits
 * encoding version. */
/* "module" 对象类型是特殊类型，表示该对象由 Redis 模块直接管理。
 * 此时 value 指向一个 moduleValue 结构体，包含对象值（仅模块自己处理）
 * 和 RedisModuleType 结构体，后者包含序列化、反序列化、AOF 重写、释放等函数指针。
 *
 * 在 RDB 文件中，模块类型编码为 OBJ_MODULE，后面跟一个 64 位模块类型 ID，
 * 其中有 54 位是模块特定签名，用于分发加载到正确的模块，10 位是编码版本。 */
#define OBJ_MODULE 5 /* 模块对象 */ /* Module object. */
#define OBJ_STREAM 6 /* Stream 对象 */ /* Stream object. */

/* 从模块类型 ID 中提取编码版本/签名 */ /* Extract encver / signature from a module type ID. */
#define REDISMODULE_TYPE_ENCVER_BITS 10
#define REDISMODULE_TYPE_ENCVER_MASK ((1 << REDISMODULE_TYPE_ENCVER_BITS) - 1)
#define REDISMODULE_TYPE_ENCVER(id) (id & REDISMODULE_TYPE_ENCVER_MASK)
#define REDISMODULE_TYPE_SIGN(id) ((id & ~((uint64_t)REDISMODULE_TYPE_ENCVER_MASK)) >> REDISMODULE_TYPE_ENCVER_BITS)

/* moduleTypeAuxSaveFunc 的位标志 */ /* Bit flags for moduleTypeAuxSaveFunc */
#define REDISMODULE_AUX_BEFORE_RDB (1 << 0)
#define REDISMODULE_AUX_AFTER_RDB (1 << 1)

/* 各种模块相关结构体声明 */
struct RedisModule;
struct RedisModuleIO;
struct RedisModuleDigest;
struct RedisModuleCtx;
struct redisObject;
struct RedisModuleDefragCtx;

/* Each module type implementation should export a set of methods in order
 * to serialize and deserialize the value in the RDB file, rewrite the AOF
 * log, create the digest for "DEBUG DIGEST", and free the value when a key
 * is deleted. */
/* 每个模块类型实现都需要导出一组方法，用于序列化/反序列化 RDB 文件中的值，
 * 重写 AOF 日志，创建 DEBUG DIGEST 的摘要，以及在 key 被删除时释放值。 */
typedef void *(*moduleTypeLoadFunc)(struct RedisModuleIO *io, int encver);
typedef void (*moduleTypeSaveFunc)(struct RedisModuleIO *io, void *value);
typedef int (*moduleTypeAuxLoadFunc)(struct RedisModuleIO *rdb, int encver, int when);
typedef void (*moduleTypeAuxSaveFunc)(struct RedisModuleIO *rdb, int when);
typedef void (*moduleTypeRewriteFunc)(struct RedisModuleIO *io, struct redisObject *key, void *value);
typedef void (*moduleTypeDigestFunc)(struct RedisModuleDigest *digest, void *value);
typedef size_t (*moduleTypeMemUsageFunc)(const void *value);
typedef void (*moduleTypeFreeFunc)(void *value);
typedef size_t (*moduleTypeFreeEffortFunc)(struct redisObject *key, const void *value);
typedef void (*moduleTypeUnlinkFunc)(struct redisObject *key, void *value);
typedef void *(*moduleTypeCopyFunc)(struct redisObject *fromkey, struct redisObject *tokey, const void *value);
typedef int (*moduleTypeDefragFunc)(struct RedisModuleDefragCtx *ctx, struct redisObject *key, void **value);

/* This callback type is called by moduleNotifyUserChanged() every time
 * a user authenticated via the module API is associated with a different
 * user or gets disconnected. This needs to be exposed since you can't cast
 * a function pointer to (void *). */
/* 该回调类型用于 moduleNotifyUserChanged()，每次通过模块 API 认证的用户
 * 被关联到不同用户或断开连接时都会调用。需要暴露出来，因为不能直接将函数指针强制转换为 void*。 */
typedef void (*RedisModuleUserChangedFunc)(uint64_t client_id, void *privdata);

/* The module type, which is referenced in each value of a given type, defines
 * the methods and links to the module exporting the type. */
/* 模块类型结构体，每个值都引用该结构体，定义方法并链接到导出该类型的模块。 */
typedef struct RedisModuleType
{
    uint64_t id; /* 类型 ID 的高 54 位 + 编码版本的低 10 位 */ /* Higher 54 bits of type ID + 10 lower bits of encoding ver. */
    struct RedisModule *module;
    moduleTypeLoadFunc rdb_load;
    moduleTypeSaveFunc rdb_save;
    moduleTypeRewriteFunc aof_rewrite;
    moduleTypeMemUsageFunc mem_usage;
    moduleTypeDigestFunc digest;
    moduleTypeFreeFunc free;
    moduleTypeFreeEffortFunc free_effort;
    moduleTypeUnlinkFunc unlink;
    moduleTypeCopyFunc copy;
    moduleTypeDefragFunc defrag;
    moduleTypeAuxLoadFunc aux_load;
    moduleTypeAuxSaveFunc aux_save;
    int aux_save_triggers;
    char name[10]; /* 9 字节名称 + 结尾空字符。字符集：A-Z a-z 0-9 _- */ /* 9 bytes name + null term. Charset: A-Z a-z 0-9 _- */
} moduleType;

/* In Redis objects 'robj' structures of type OBJ_MODULE, the value pointer
 * is set to the following structure, referencing the moduleType structure
 * in order to work with the value, and at the same time providing a raw
 * pointer to the value, as created by the module commands operating with
 * the module type.
 *
 * So for example in order to free such a value, it is possible to use
 * the following code:
 *
 *  if (robj->type == OBJ_MODULE) {
 *      moduleValue *mt = robj->ptr;
 *      mt->type->free(mt->value);
 *      zfree(mt); // We need to release this in-the-middle struct as well.
 *  }
 */
/* 在 Redis 对象 robj 的 OBJ_MODULE 类型中，value 指针指向如下结构体，
 * 引用 moduleType 结构体以便操作 value，同时提供原始 value 指针，
 * 该指针由模块命令创建和操作。
 *
 * 例如释放该值时，可以这样写：
 *  if (robj->type == OBJ_MODULE) {
 *      moduleValue *mt = robj->ptr;
 *      mt->type->free(mt->value);
 *      zfree(mt); // 还需要释放这个中间结构体
 *  }
 */
typedef struct moduleValue
{
    moduleType *type;
    void *value;
} moduleValue;

/* This is a wrapper for the 'rio' streams used inside rdb.c in Redis, so that
 * the user does not have to take the total count of the written bytes nor
 * to care about error conditions. */
/* 这是 rdb.c 内部 rio 流的包装结构体，用户无需关心已写字节数或错误状态。 */
typedef struct RedisModuleIO
{
    size_t bytes;               /* 已读/已写字节数 */ /* Bytes read / written so far. */
    rio *rio;                   /* Rio 流 *//* Rio stream. */
    moduleType *type;           /* 当前操作的模块类型 */ /* Module type doing the operation. */
    int error;                  /* 是否发生错误 *//* True if error condition happened. */
    int ver;                    /* 模块序列化版本：1（旧版），2（当前带操作码注释） */ /* Module serialization version: 1 (old),
                                 * 2 (current version with opcodes annotation). */
    struct RedisModuleCtx *ctx; /* 可选上下文，见 RM_GetContextFromIO() */ /* Optional context, see RM_GetContextFromIO()*/
    struct redisObject *key;    /* 可选，正在处理的 key 名称 */ /* Optional name of key processed */
} RedisModuleIO;

/* Macro to initialize an IO context. Note that the 'ver' field is populated
 * inside rdb.c according to the version of the value to load. */
/* 初始化 IO 上下文的宏。ver 字段会在 rdb.c 中根据加载值的版本填充。 */
#define moduleInitIOContext(iovar, mtype, rioptr, keyptr)                                                              \
    do                                                                                                                 \
    {                                                                                                                  \
        iovar.rio = rioptr;                                                                                            \
        iovar.type = mtype;                                                                                            \
        iovar.bytes = 0;                                                                                               \
        iovar.error = 0;                                                                                               \
        iovar.ver = 0;                                                                                                 \
        iovar.key = keyptr;                                                                                            \
        iovar.ctx = NULL;                                                                                              \
    } while (0)

/* This is a structure used to export DEBUG DIGEST capabilities to Redis
 * modules. We want to capture both the ordered and unordered elements of
 * a data structure, so that a digest can be created in a way that correctly
 * reflects the values. See the DEBUG DIGEST command implementation for more
 * background. */
/* 该结构体用于向 Redis 模块导出 DEBUG DIGEST 能力。
 * 需要同时捕获数据结构的有序和无序元素，以便正确反映值。
 * 详见 DEBUG DIGEST 命令实现。 */
typedef struct RedisModuleDigest
{
    unsigned char o[20]; /* 有序元素 */ /* Ordered elements. */
    unsigned char x[20]; /* 异或元素 *//* Xored elements. */
} RedisModuleDigest;

/* 用全零字节初始化 digest。 */ /* Just start with a digest composed of all zero bytes. */
#define moduleInitDigestContext(mdvar)                                                                                 \
    do                                                                                                                 \
    {                                                                                                                  \
        memset(mdvar.o, 0, sizeof(mdvar.o));                                                                           \
        memset(mdvar.x, 0, sizeof(mdvar.x));                                                                           \
    } while (0)

/* Objects encoding. Some kind of objects like Strings and Hashes can be
 * internally represented in multiple ways. The 'encoding' field of the object
 * is set to one of this fields for this object. */
/* 对象编码。某些对象如字符串和哈希可以有多种内部表示方式。
 * 对象的 encoding 字段会设置为以下之一。 */
#define OBJ_ENCODING_RAW 0        /* 原始表示 */ /* Raw representation */
#define OBJ_ENCODING_INT 1        /* 整数编码 */ /* Encoded as integer */
#define OBJ_ENCODING_HT 2         /* 哈希表编码 */ /* Encoded as hash table */
#define OBJ_ENCODING_ZIPMAP 3     /* zipmap 编码 */ /* Encoded as zipmap */
#define OBJ_ENCODING_LINKEDLIST 4 /* 已废弃：旧列表编码 */ /* No longer used: old list encoding. */
#define OBJ_ENCODING_ZIPLIST 5    /* ziplist 编码 */ /* Encoded as ziplist */
#define OBJ_ENCODING_INTSET 6     /* intset 编码 */ /* Encoded as intset */
#define OBJ_ENCODING_SKIPLIST 7   /* 跳表编码 */ /* Encoded as skiplist */
#define OBJ_ENCODING_EMBSTR 8     /* 嵌入式 sds 字符串编码 */ /* Embedded sds string encoding */
#define OBJ_ENCODING_QUICKLIST 9  /* ziplist 链表编码 */ /* Encoded as linked list of ziplists */
#define OBJ_ENCODING_STREAM 10    /* listpack 的 radix tree 编码 */ /* Encoded as a radix tree of listpacks */

#define LRU_BITS 24
#define LRU_CLOCK_MAX ((1 << LRU_BITS) - 1) /* obj->lru 的最大值 */ /* Max value of obj->lru */
#define LRU_CLOCK_RESOLUTION 1000           /* LRU 时钟分辨率（毫秒） */ /* LRU clock resolution in ms */

#define OBJ_SHARED_REFCOUNT INT_MAX       /* 全局共享对象永不销毁 */ /* Global object never destroyed. */
#define OBJ_STATIC_REFCOUNT (INT_MAX - 1) /* 栈上分配的对象 */ /* Object allocated in the stack. */
#define OBJ_FIRST_SPECIAL_REFCOUNT OBJ_STATIC_REFCOUNT
typedef struct redisObject
{
    unsigned type : 4; // 对象类型，占 4 位。 OBJ_STRING、OBJ_LIST、OBJ_SET、OBJ_ZSET、OBJ_HASH、OBJ_MODULE、OBJ_STREAM
    unsigned
        encoding : 4; // 对象编码，占 4 位。
                      // OBJ_ENCODING_RAW、OBJ_ENCODING_INT、OBJ_ENCODING_HT、OBJ_ENCODING_ZIPMAP、OBJ_ENCODING_ZIPLIST、OBJ_ENCODING_INTSET等
    unsigned lru : LRU_BITS; /* LRU time (relative to global lru_clock) or
                              * LFU data (least significant 8 bits frequency
                              * and most significant 16 bits access time). */
    // LRU 时间（相对于全局 lru_clock）或 LFU 数据（低 8 位表示访问频率，高 16 位表示访问时间）。
    int refcount; // 引用计数
    void *ptr;    // 实际存储的数据指针
} robj;

/* The a string name for an object's type as listed above
 * Native types are checked against the OBJ_STRING, OBJ_LIST, OBJ_* defines,
 * and Module types have their registered name returned. */
/* 获取对象类型的字符串名称（如上所列）。
 * 原生类型会根据 OBJ_STRING、OBJ_LIST、OBJ_* 等宏进行判断，
 * 模块类型则返回其注册的名称。 */
char *getObjectTypeName(robj *);

/* Macro used to initialize a Redis object allocated on the stack.
 * Note that this macro is taken near the structure definition to make sure
 * we'll update it when the structure is changed, to avoid bugs like
 * bug #85 introduced exactly in this way. */
/* 用于在栈上初始化 Redis 对象的宏。
 * 注意，这个宏定义放在结构体定义附近，以确保结构体变更时能及时更新，
 * 避免像 bug #85 那样的错误。 */
#define initStaticStringObject(_var, _ptr)                                                                             \
    do                                                                                                                 \
    {                                                                                                                  \
        _var.refcount = OBJ_STATIC_REFCOUNT;                                                                           \
        _var.type = OBJ_STRING;                                                                                        \
        _var.encoding = OBJ_ENCODING_RAW;                                                                              \
        _var.ptr = _ptr;                                                                                               \
    } while (0)

struct evictionPoolEntry; /* 在 evict.c 中定义 */ /* Defined in evict.c */

/* 此结构体用于表示客户端的输出缓冲区，
 * 实际上是一个链表，每个节点如下：client->reply。 */
/* This structure is used in order to represent the output buffer of a client,
 * which is actually a linked list of blocks like that, that is: client->reply. */
typedef struct clientReplyBlock
{
    size_t size, used;
    char buf[];
} clientReplyBlock;

/* Redis 数据库结构体。Redis 支持多个数据库，通过整数编号区分，
 * 从 0（默认数据库）到配置的最大数据库号。数据库编号即结构体中的 'id' 字段。 */ /* Redis database representation. There are multiple databases identified
 * by integers from 0 (the default database) up to the max configured
 * database. The database number is the 'id' field in the structure. */
typedef struct redisDb
{
    dict *dict;                   /* 当前数据库的键空间（所有键值对）【键空间】 */ /* The keyspace for this DB */
    dict *expires;                /* 设置了过期时间的键的超时信息【过期键空间】 *//* Timeout of keys with a timeout set */
    dict *blocking_keys;          /* 有客户端等待数据的键（如 BLPOP） */ /* Keys with clients waiting for data (BLPOP)*/
    dict *ready_keys;             /* 已收到 PUSH 的被阻塞键 */ /* Blocked keys that received a PUSH */
    dict *watched_keys;           /* MULTI/EXEC CAS 机制下被 WATCH 的键 */ /* WATCHED keys for MULTI/EXEC CAS */
    int id;                       /* 数据库编号 */ /* Database ID */
    long long avg_ttl;            /* 平均 TTL，仅用于统计 */ /* Average TTL, just for stats */
    unsigned long expires_cursor; /* 活动过期周期的游标 */ /* Cursor of the active expire cycle. */
    list *defrag_later;           /* 需要后续逐步碎片整理的键名列表 */ /* List of key names to attempt to defrag one by one, gradually. */
} redisDb;

/* 声明数据库备份结构体，包括主数据库和槽到键的映射。
 * 定义在 db.c 中。不能在此定义，因为 CLUSTER_SLOTS 在 cluster.h 中定义。 */ /* Declare database backup that include redis main DBs and slots to keys map.
 * Definition is in db.c. We can't define it here since we define CLUSTER_SLOTS
 * in cluster.h. */
typedef struct dbBackup dbBackup;

/* 客户端 MULTI/EXEC 状态 */ /* Client MULTI/EXEC state */
typedef struct multiCmd
{
    robj **argv;               // 命令参数
    int argc;                  // 参数数量
    struct redisCommand *cmd;  // 命令指针 
} multiCmd;

typedef struct multiState  /* 客户端 MULTI/EXEC 状态 */
{
    multiCmd *commands; /* MULTI 命令数组 */ /* Array of MULTI commands */
    int count;          /* MULTI 命令总数 */ /* Total number of MULTI commands */
    int cmd_flags;      /* 所有命令的标志按位或后的结果，只要有一个命令有某标志就会设置 */ /* The accumulated command flags OR-ed together.
                           So if at least a command has a given flag, it
                           will be set in this field. */
    int cmd_inv_flags;  /* 与 cmd_flags 类似，按位或 ~flags，用于判断是否所有命令都有某标志 */ /* Same as cmd_flags, OR-ing the ~flags. so that it
                           is possible to know if all the commands have a
                           certain flag. */
} multiState;

/* 此结构体保存客户端的阻塞操作状态。
 * 使用的字段取决于 client->btype。 */ /* This structure holds the blocking operation state for a client.
 * The fields used depend on client->btype. */
typedef struct blockingState
{
    /* 通用字段 */ /* Generic fields. */
    mstime_t timeout; /* 阻塞操作超时时间。如果当前 UNIX 时间 > timeout，则操作超时 */ /* Blocking operation timeout. If UNIX current time
                       * is > timeout then the operation timed out. */

    /* BLOCKED_LIST, BLOCKED_ZSET and BLOCKED_STREAM */
    dict *keys;   /* 等待结束阻塞操作的键，如 BLPOP 或 XREAD，或为 NULL */ /* The keys we are waiting to terminate a blocking
                   * operation such as BLPOP or XREAD. Or NULL. */
    robj *target; /* BLMOVE 时接收元素的目标键 */ /* The key that should receive the element,
                   * for BLMOVE. */
    struct listPos
    {
        int wherefrom; /* 从哪里弹出 */ /* Where to pop from */
        int whereto;   /* 推送到哪里 */ /* Where to push to */
    } listpos;         /* BLPOP、BRPOP 和 BLMOVE 时源/目标列表的位置 */ /* The positions in the src/dst lists
                        * where we want to pop/push an element
                        * for BLPOP, BRPOP and BLMOVE. */

    /* BLOCK_STREAM */
    size_t xread_count;    /* XREAD COUNT 选项 */ /* XREAD COUNT option. */
    robj *xread_group;     /* XREADGROUP 的组名 */ /* XREADGROUP group name. */
    robj *xread_consumer;  /* XREADGROUP 的消费者名 */ /* XREADGROUP consumer name. */
    int xread_group_noack; /* XREADGROUP 是否不需要 ACK */

    /* BLOCKED_WAIT */
    int numreplicas;      /* 等待 ACK 的副本数量 */ /* Number of replicas we are waiting for ACK. */
    long long reploffset; /* 需要达到的复制偏移量 */ /* Replication offset to reach. */

    /* BLOCKED_MODULE */
    void *module_blocked_handle; /* RedisModuleBlockedClient 结构体。
                                  * 对于 Redis 核心来说是不可见的，只在 module.c 中处理。 */ /* RedisModuleBlockedClient structure.
                                    which is opaque for the Redis core, only
                                    handled in module.c. */
} blockingState;

/* The following structure represents a node in the server.ready_keys list,
 * where we accumulate all the keys that had clients blocked with a blocking
 * operation such as B[LR]POP, but received new data in the context of the
 * last executed command.
 *
 * After the execution of every command or script, we run this list to check
 * if as a result we should serve data to clients blocked, unblocking them.
 * Note that server.ready_keys will not have duplicates as there dictionary
 * also called ready_keys in every structure representing a Redis database,
 * where we make sure to remember if a given key was already added in the
 * server.ready_keys list. */
/* 下述结构体表示 server.ready_keys 列表中的一个节点，
 * 该列表用于收集所有因阻塞操作（如 B[LR]POP）而被阻塞的客户端对应的 key，
 * 这些 key 在最近一次命令执行过程中收到了新数据。
 *
 * 每次命令或脚本执行后，都会遍历这个列表，检查是否有客户端可以被唤醒并提供数据。
 * 注意 server.ready_keys 列表中不会有重复项，因为每个 Redis 数据库结构体中
 * 也有一个名为 ready_keys 的字典，用于确保某个 key 只会被加入一次到 server.ready_keys 列表。 */
typedef struct readyList
{
    redisDb *db;
    robj *key;
} readyList;

/* This structure represents a Redis user. This is useful for ACLs, the
 * user is associated to the connection after the connection is authenticated.
 * If there is no associated user, the connection uses the default user. */
/* 该结构体表示一个 Redis 用户。用于 ACL（访问控制列表），
 * 用户会在连接认证后与连接关联。
 * 如果没有关联用户，则连接使用默认用户。 */
#define USER_COMMAND_BITS_COUNT                                                                                        \
    1024                                /* 用户结构体中命令位的总数。
                                         * 最后一个可用的命令 ID 是 USER_COMMAND_BITS_COUNT-1。 */ /* The total number of command bits                                             \
                                          in the user structure. The last valid                                        \
                                          command ID we can set in the user                                            \
                                          is USER_COMMAND_BITS_COUNT-1. */
#define USER_FLAG_ENABLED (1 << 0)     /* The user is active. */
#define USER_FLAG_DISABLED (1 << 1)    /* The user is disabled. */
#define USER_FLAG_ALLKEYS (1 << 2)     /* The user can mention any key. */
#define USER_FLAG_ALLCOMMANDS (1 << 3) /* The user can run all commands. */
#define USER_FLAG_NOPASS                                                                                               \
    (1 << 4) /* The user requires no password, any                                                                     \
                provided password will work. For the                                                                   \
                default user, this also means that                                                                     \
                no AUTH is needed, and every                                                                           \
                connection is immediately                                                                              \
                authenticated. */
#define USER_FLAG_ALLCHANNELS                                                                                          \
    (1 << 5) /* The user can mention any Pub/Sub                                                                       \
                channel. */
#define USER_FLAG_SANITIZE_PAYLOAD                                                                                     \
    (1 << 6) /* The user require a deep RESTORE                                                                        \
              * payload sanitization. */
#define USER_FLAG_SANITIZE_PAYLOAD_SKIP                                                                                \
    (1 << 7) /* The user should skip the                                                                               \
              * deep sanitization of RESTORE                                                                           \
              * payload. */

typedef struct
{
    sds name;       /* The username as an SDS string. */
    uint64_t flags; /* See USER_FLAG_* */

    /* The bit in allowed_commands is set if this user has the right to
     * execute this command. In commands having subcommands, if this bit is
     * set, then all the subcommands are also available.
     *
     * If the bit for a given command is NOT set and the command has
     * subcommands, Redis will also check allowed_subcommands in order to
     * understand if the command can be executed. */
    uint64_t allowed_commands[USER_COMMAND_BITS_COUNT / 64];

    /* This array points, for each command ID (corresponding to the command
     * bit set in allowed_commands), to an array of SDS strings, terminated by
     * a NULL pointer, with all the sub commands that can be executed for
     * this command. When no subcommands matching is used, the field is just
     * set to NULL to avoid allocating USER_COMMAND_BITS_COUNT pointers. */
    sds **allowed_subcommands;
    list *passwords; /* A list of SDS valid passwords for this user. */
    list *patterns;  /* A list of allowed key patterns. If this field is NULL
                        the user cannot mention any key in a command, unless
                        the flag ALLKEYS is set in the user. */
    list *channels;  /* A list of allowed Pub/Sub channel patterns. If this
                        field is NULL the user cannot mention any channel in a
                        `PUBLISH` or [P][UNSUBSCRIBE] command, unless the flag
                        ALLCHANNELS is set in the user. */
} user;

/* With multiplexing we need to take per-client state.
 * Clients are taken in a linked list. */

#define CLIENT_ID_AOF                                                                                                  \
    (UINT64_MAX) /* Reserved ID for the AOF client. If you                                                             \
                    need more reserved IDs use UINT64_MAX-1,                                                           \
                    -2, ... and so forth. */

typedef struct client
{
    uint64_t id;                        /* 客户端自增唯一 ID */  /* Client incremental unique ID. */
    connection *conn;                   /* 客户端连接对象 */
    int resp;                           /* RESP 协议版本，2 或 3 */ /* RESP protocol version. Can be 2 or 3. */
    redisDb *db;                        /* 当前 SELECT 的数据库指针 */ /* Pointer to currently SELECTed DB. */
    robj *name;                         /* 客户端名称（CLIENT SETNAME 设置） */ /* As set by CLIENT SETNAME. */
    sds querybuf;                       /* 用于累积客户端查询的缓冲区 */ /* Buffer we use to accumulate client queries. */
    size_t qb_pos;                      /* 已读取的 querybuf 位置 */ /* The position we have read in querybuf. */
    sds pending_querybuf;               /* 如果是主节点，该缓冲区表示尚未应用的复制流部分 */  /* If this client is flagged as master, this buffer
                                           represents the yet not applied portion of the
                                           replication stream that we are receiving from
                                           the master. */
    size_t querybuf_peak;               /* 最近（100ms 或更长）querybuf 的峰值大小 */ /* Recent (100ms or more) peak of querybuf size. */
    int argc;                           /* 当前命令参数数量 */ /* Num of arguments of current command. */
    robj **argv;                        /* 当前命令参数列表 */ /* Arguments of current command. */
    int original_argc;                  /* 如果参数被重写，原始命令参数数量 */ /* Num of arguments of original command if arguments were rewritten. */
    robj **original_argv;               /* 如果参数被重写，原始命令参数列表 */ /* Arguments of original command if arguments were rewritten. */
    size_t argv_len_sum;                /* argv 列表中所有对象的长度总和 */ /* Sum of lengths of objects in argv list. */
    struct redisCommand *cmd, *lastcmd; /* 当前和上一次执行的命令 */ /* Last command executed. */
    user *user;                         /* 关联的用户对象，为 NULL 时为管理员权限 */ /* User associated with this connection. If the
                                           user is set to NULL the connection can do
                                           anything (admin). */
    int reqtype;                        /* 请求协议类型：PROTO_REQ_* */ /* Request protocol type: PROTO_REQ_* */
    int multibulklen;                   /* 剩余 multi bulk 参数数量 */ /* Number of multi bulk arguments left to read. */
    long bulklen;                       /* multi bulk 请求中 bulk 参数的长度 */ /* Length of bulk argument in multi bulk request. */
    list *reply;                        /* 待发送给客户端的回复对象列表 */ /* List of reply objects to send to the client. */
    unsigned long long reply_bytes;     /* reply 列表中对象的总字节数 */  /* Tot bytes of objects in reply list. */
    list *deferred_reply_errors;        /* 用于模块线程安全上下文的错误列表 */ /* Used for module thread safe contexts. */
    size_t sentlen;                     /* 当前缓冲区或对象已发送的字节数 */ /* Amount of bytes already sent in the current
                                           buffer or object being sent. */
    time_t ctime;                       /* 客户端创建时间 */ /* Client creation time. */
    long duration;          /* 当前命令耗时，用于阻塞/非阻塞命令的延迟统计 */ /* Current command duration. Used for measuring latency of blocking/non-blocking cmds */
    time_t lastinteraction;  /* 最后一次交互时间，用于超时处理 */ /* Time of the last interaction, used for timeout */
    time_t obuf_soft_limit_reached_time; /* 输出缓冲区软限制达到的时间 */
    uint64_t flags;                      /* 客户端标志位：CLIENT_* 宏 */ /* Client flags: CLIENT_* macros. */
    int authenticated;                   /* 默认用户需要认证时使用 */ /* Needed when the default user requires auth. */
    int replstate;                       /* 如果是从节点，标识从节点的复制状态 */ /* Replication state if this is a slave. */
    int repl_put_online_on_ack;          /* 第一次 ACK 时安装从节点写处理器 */ /* Install slave write handler on first ACK. */
    int repldbfd;                        /* 复制 DB 文件描述符 */ /* Replication DB file descriptor. */
    off_t repldboff;                     /* 复制 DB 文件偏移 */ /* Replication DB file offset. */
    off_t repldbsize;                    /* 复制 DB 文件大小 */ /* Replication DB file size. */
    sds replpreamble;                    /* 复制 DB 前导数据 */ /* Replication DB preamble. */
    long long read_reploff;              /* 如果是主节点，读取复制偏移量 */ /* Read replication offset if this is a master. */
    long long reploff;                   /* 如果是主节点，已应用的复制偏移量 */ /* Applied replication offset if this is a master. */
    long long repl_ack_off;              /* 如果是从节点，复制 ACK 偏移量 */ /* Replication ack offset, if this is a slave. */
    long long repl_ack_time;             /* 如果是从节点，复制 ACK 时间 */ /* Replication ack time, if this is a slave. */
    long long repl_last_partial_write;   /* 最近一次部分写入 RDB 子进程管道到该副本的时间 */ /* The last time the server did a partial write from the RDB child pipe to this
                                            replica  */
    long long psync_initial_offset;      /* FULLRESYNC 回复偏移量，其他从节点复制该输出缓冲区时使用 */ /* FULLRESYNC reply offset other slaves
                                            copying this slave output buffer
                                            should use. */
    char replid[CONFIG_RUN_ID_SIZE + 1]; /* 主节点复制 ID（如果是主节点） */ /* Master replication ID (if master). */
    int slave_listening_port;            /* REPLCONF listening-port 配置的端口 */ /* As configured with: REPLCONF listening-port */
    char *slave_addr;                    /* REPLCONF ip-address 配置的地址 */ /* Optionally given by REPLCONF ip-address */
    int slave_capa;                      /* 从节点能力：SLAVE_CAPA_* 按位或 */ /* Slave capabilities: SLAVE_CAPA_* bitwise OR. */
    multiState mstate;                   /* MULTI/EXEC 状态 */ /* MULTI/EXEC state */
    int btype;                           /* 如果 CLIENT_BLOCKED，阻塞操作类型 */ /* Type of blocking op if CLIENT_BLOCKED. */
    blockingState bpop;                  /* 阻塞状态 */ /* blocking state */
    long long woff;                      /* 最近一次写入的全局复制偏移量 */ /* Last write global replication offset. */
    list *watched_keys;                  /* MULTI/EXEC CAS 监控的键列表 */ /* Keys WATCHED for MULTI/EXEC CAS */
    dict *pubsub_channels;               /* 客户端订阅的频道（SUBSCRIBE） */ /* channels a client is interested in (SUBSCRIBE) */
    list *pubsub_patterns;               /* 客户端订阅的模式（SUBSCRIBE） */ /* patterns a client is interested in (SUBSCRIBE) */
    sds peerid;                          /* 缓存的对端 ID */ /* Cached peer ID. */
    sds sockname;                        /* 缓存的连接目标地址 */ /* Cached connection target address. */
    listNode *client_list_node;          /* 客户端链表中的节点 */ /* list node in client list */
    listNode *paused_list_node;          /* 暂停列表中的节点 */ /* list node within the pause list */
    RedisModuleUserChangedFunc auth_callback; /* 用户认证变更时的模块回调 */ /* Module callback to execute
                                               * when the authenticated user
                                               * changes. */
    void *auth_callback_privdata;             /* 认证变更回调的私有数据，Redis Core 不关心 */ /* Private data that is passed when the auth
                                               * changed callback is executed. Opaque for
                                               * Redis Core. */
    void *auth_module;                        /* 拥有该回调的模块指针，用于模块卸载时断开客户端，Redis Core 不关心 *//* The module that owns the callback, which is used
                                               * to disconnect the client if the module is
                                               * unloaded for cleanup. Opaque for Redis Core.*/

     /* 如果客户端处于 tracking 模式且该字段非零，
     * 则为该客户端获取的键发送失效消息到指定的客户端 ID */ /* If this client is in tracking mode and this field is non zero,
     * invalidation messages for keys fetched by this client will be send to
     * the specified client ID. */
    uint64_t client_tracking_redirection;
    rax *client_tracking_prefixes; /* BCAST 模式下已订阅的前缀字典，用于客户端缓存 */ /* A dictionary of prefixes we are already
                                      subscribed to in BCAST mode, in the
                                      context of client side caching. */
    /* 在 clientsCronTrackClientsMemUsage() 中统计每个客户端的内存使用，
     * 并加到对应类型的客户端总和中，
     * 但需要记住每个客户端之前的贡献值和类型，以便更新时先移除旧值。 */ /* In clientsCronTrackClientsMemUsage() we track the memory usage of
     * each client and add it to the sum of all the clients of a given type,
     * however we need to remember what was the old contribution of each
     * client, and in which categoty the client was, in order to remove it
     * before adding it the new value. */
    uint64_t client_cron_last_memory_usage;
    int client_cron_last_memory_type;
    /* 响应缓冲区 */ /* Response buffer */
    int bufpos;
    char buf[PROTO_REPLY_CHUNK_BYTES];
} client;

struct saveparam
{
    time_t seconds;
    int changes;
};

struct moduleLoadQueueEntry
{
    sds path;
    int argc;
    robj **argv;
};

struct sentinelLoadQueueEntry
{
    int argc;
    sds *argv;
    int linenum;
    sds line;
};

struct sentinelConfig
{
    list *pre_monitor_cfg;
    list *monitor_cfg;
    list *post_monitor_cfg;
};

struct sharedObjectsStruct
{
    robj *crlf, *ok, *err, *emptybulk, *czero, *cone, *pong, *space, *colon, *queued, *null[4], *nullarray[4],
        *emptymap[4], *emptyset[4], *emptyarray, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr, *outofrangeerr,
        *noscripterr, *loadingerr, *slowscripterr, *bgsaveerr, *masterdownerr, *roslaveerr, *execaborterr, *noautherr,
        *noreplicaserr, *busykeyerr, *oomerr, *plus, *messagebulk, *pmessagebulk, *subscribebulk, *unsubscribebulk,
        *psubscribebulk, *punsubscribebulk, *del, *unlink, *rpop, *lpop, *lpush, *rpoplpush, *lmove, *blmove, *zpopmin,
        *zpopmax, *emptyscan, *multi, *exec, *left, *right, *hset, *srem, *xgroup, *xclaim, *script, *replconf, *eval,
        *persist, *set, *pexpireat, *pexpire, *time, *pxat, *px, *retrycount, *force, *justid, *lastid, *ping, *setid,
        *keepttl, *load, *createconsumer, *getack, *special_asterick, *special_equals, *default_username, *redacted,
        *select[PROTO_SHARED_SELECT_CMDS], *integers[OBJ_SHARED_INTEGERS],
        *mbulkhdr[OBJ_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
        *bulkhdr[OBJ_SHARED_BULKHDR_LEN],  /* "$<value>\r\n" */
        *maphdr[OBJ_SHARED_BULKHDR_LEN],   /* "%<value>\r\n" */
        *sethdr[OBJ_SHARED_BULKHDR_LEN];   /* "~<value>\r\n" */
    sds minstring, maxstring;
};

/* ZSETs use a specialized version of Skiplists */
typedef struct zskiplistNode
{
    sds ele;
    double score;
    struct zskiplistNode *backward;
    struct zskiplistLevel
    {
        struct zskiplistNode *forward;
        unsigned long span;
    } level[];
} zskiplistNode;

typedef struct zskiplist
{
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} zskiplist;

typedef struct zset
{
    dict *dict;
    zskiplist *zsl;
} zset;

typedef struct clientBufferLimitsConfig
{
    unsigned long long hard_limit_bytes;
    unsigned long long soft_limit_bytes;
    time_t soft_limit_seconds;
} clientBufferLimitsConfig;

extern clientBufferLimitsConfig clientBufferLimitsDefaults[CLIENT_TYPE_OBUF_COUNT];

/* The redisOp structure defines a Redis Operation, that is an instance of
 * a command with an argument vector, database ID, propagation target
 * (PROPAGATE_*), and command pointer.
 *
 * Currently only used to additionally propagate more commands to AOF/Replication
 * after the propagation of the executed command. */
/* 命令传播的结构体
 * redisOp 结构体定义了一个 Redis 操作，即一个带有参数向量、数据库 ID、传播目标（PROPAGATE_*）和命令指针的命令实例。
 *
 * 目前仅用于在执行命令后，向 AOF/复制额外传播更多命令。 */
typedef struct redisOp
{
    robj **argv;  /* 命令参数列表 */ 
    int argc, dbid, target; /* 参数数量 */  /* 数据库编号 */ /* 传播目标（PROPAGATE_*） */
    struct redisCommand *cmd;  /* 命令指针 */
} redisOp;

/* Defines an array of Redis operations. There is an API to add to this
 * structure in an easy way.
 *
 * redisOpArrayInit();
 * redisOpArrayAppend();
 * redisOpArrayFree();
 */
/* 定义了一个 Redis 操作数组。提供了相关 API 用于方便地添加操作。
 *
 * redisOpArrayInit();      // 初始化
 * redisOpArrayAppend();    // 添加操作
 * redisOpArrayFree();      // 释放操作数组
 */
typedef struct redisOpArray
{
    redisOp *ops; /* 操作数组 */
    int numops;   /* 操作数量 */
} redisOpArray;

/* This structure is returned by the getMemoryOverheadData() function in
 * order to return memory overhead information. */
/* 该结构体由 getMemoryOverheadData() 函数返回，用于提供内存开销信息。 */
struct redisMemOverhead
{
    size_t peak_allocated;
    size_t total_allocated;
    size_t startup_allocated;
    size_t repl_backlog;
    size_t clients_slaves;
    size_t clients_normal;
    size_t aof_buffer;
    size_t lua_caches;
    size_t overhead_total;
    size_t dataset;
    size_t total_keys;
    size_t bytes_per_key;
    float dataset_perc;
    float peak_perc;
    float total_frag;
    ssize_t total_frag_bytes;
    float allocator_frag;
    ssize_t allocator_frag_bytes;
    float allocator_rss;
    ssize_t allocator_rss_bytes;
    float rss_extra;
    size_t rss_extra_bytes;
    size_t num_dbs;
    struct
    {
        size_t dbid;
        size_t overhead_ht_main;
        size_t overhead_ht_expires;
    } *db;
};

/* This structure can be optionally passed to RDB save/load functions in
 * order to implement additional functionalities, by storing and loading
 * metadata to the RDB file.
 *
 * Currently the only use is to select a DB at load time, useful in
 * replication in order to make sure that chained slaves (slaves of slaves)
 * select the correct DB and are able to accept the stream coming from the
 * top-level master. */
/* 该结构体可选地传递给 RDB 的保存/加载函数，
 * 用于实现额外功能，比如在 RDB 文件中存储和加载元数据。
 *
 * 当前唯一用途是在加载时选择数据库，这对于复制场景很有用，
 * 能确保级联从节点（即从节点的从节点）选择正确的数据库，
 * 并能接受来自顶层主节点的复制流。 */
typedef struct rdbSaveInfo
{
    /* 保存和加载时使用 */ /* Used saving and loading. */
    int repl_stream_db;  /* 在 server.master 客户端中要选择的数据库编号 */ /* DB to select in server.master client. */

    /* 仅加载时使用 */ /* Used only loading. */
    int repl_id_is_set;                   /* 如果 repl_id 字段已设置则为真 */ /* True if repl_id field is set. */
    char repl_id[CONFIG_RUN_ID_SIZE + 1]; /* 复制 ID */ /* Replication ID. */
    long long repl_offset;                /* 复制偏移量 */ /* Replication offset. */
} rdbSaveInfo;

#define RDB_SAVE_INFO_INIT {-1, 0, "0000000000000000000000000000000000000000", -1}

struct malloc_stats
{
    size_t zmalloc_used;
    size_t process_rss;
    size_t allocator_allocated;
    size_t allocator_active;
    size_t allocator_resident;
};

typedef struct socketFds
{
    int fd[CONFIG_BINDADDR_MAX];
    int count;
} socketFds;

/*-----------------------------------------------------------------------------
 * TLS 上下文配置 TLS Context Configuration
 *----------------------------------------------------------------------------*/

typedef struct redisTLSContextConfig
{
    char *cert_file;            /* 服务器端证书文件名（可选客户端证书） */ /* Server side and optionally client side cert file name */
    char *key_file;             /* cert_file 对应的私钥文件名 */ /* Private key filename for cert_file */
    char *key_file_pass;        /* 私钥文件的可选密码 */ /* Optional password for key_file */
    char *client_cert_file;     /* 用作客户端的证书文件名；如果没有则使用 cert_file */ /* Certificate to use as a client; if none, use cert_file */
    char *client_key_file;      /* client_cert_file 对应的私钥文件名 */ /* Private key filename for client_cert_file */
    char *client_key_file_pass; /* 客户端私钥文件的可选密码 */ /* Optional password for client_key_file */
    char *dh_params_file;       /* DH 参数文件 */
    char *ca_cert_file;         /* CA 证书文件 */
    char *ca_cert_dir;          /* CA 证书目录 */
    char *protocols;            /* 支持的协议列表 */
    char *ciphers;              /* 加密套件列表 */
    char *ciphersuites;         /* TLS 1.3 加密套件列表 */
    int prefer_server_ciphers;  /* 是否优先使用服务器端加密套件 */
    int session_caching;        /* 是否启用会话缓存 */
    int session_cache_size;     /* 会话缓存大小 */
    int session_cache_timeout;  /* 会话缓存超时时间 */
} redisTLSContextConfig;

/*-----------------------------------------------------------------------------
 * 全局服务器状态 Global server state
 *----------------------------------------------------------------------------*/

struct clusterState;

/* AIX defines hz to __hz, we don't use this define and in order to allow
 * Redis build on AIX we need to undef it. */
/* AIX 系统将 hz 定义为 __hz，我们并不使用这个定义，
 * 为了让 Redis 能在 AIX 上编译，需要取消这个定义。 */
#ifdef _AIX
#undef hz
#endif

#define CHILD_TYPE_NONE 0      // 没有子进程在运行。
#define CHILD_TYPE_RDB 1       // RDB 持久化子进程。用于生成 RDB 快照文件。
#define CHILD_TYPE_AOF 2       // AOF 重写子进程。用于重写 AOF 文件。
#define CHILD_TYPE_LDB 3       // 用于 Redis 内部调试工具（如 Lua 调试器）的子进程。
#define CHILD_TYPE_MODULE 4    // 由模块（如 Redis Module）发起的后台子进程。

typedef enum childInfoType
{
    CHILD_INFO_TYPE_CURRENT_INFO,
    CHILD_INFO_TYPE_AOF_COW_SIZE,
    CHILD_INFO_TYPE_RDB_COW_SIZE,
    CHILD_INFO_TYPE_MODULE_COW_SIZE
} childInfoType;

struct redisServer
{
    /* 通用配置 */ /* General */
    pid_t pid;                                           /* 主进程的 pid。 */ /* Main process pid. */
    pthread_t main_thread_id;                            /* 主线程的 id。 */ /* Main thread id */
    char *configfile;                                    /* 配置文件的绝对路径，如果没有则为 NULL。 */ /* Absolute config file path, or NULL */
    char *executable;                                    /* 可执行文件的绝对路径。 */ /* Absolute executable file path. */
    char **exec_argv;                                    /* 可执行文件的参数向量（副本）。 */ /* Executable argv vector (copy). */
    int dynamic_hz;      /* 根据客户端数量动态调整 hz 值。 */ /* Change hz value depending on # of clients. */
    int config_hz;       /* 配置的 HZ 值。如果启用 dynamic-hz，可能与实际的 hz 字段值不同。 */ /* Configured HZ value. May be
                      different than the actual 'hz' field value if dynamic-hz is enabled. */
    mode_t umask;        /* 进程启动时的 umask 值 */ /* The umask value of the process on startup */
    int hz;              /* serverCron() 调用频率（赫兹），用来控制定时任务的执行频率 */ /* serverCron() calls frequency in hertz */
    int in_fork_child;   /* 标记当前是否为 fork 出来的子进程 */ /* indication that this is a fork child */
    redisDb *db;         /* 数据库数组指针 */
    dict *commands;      /* 命令表 */ /* Command table */
    dict *orig_commands; /* 命令重命名前的原始命令表 */ /* Command table before command renaming. */
    aeEventLoop *el;                     /* 事件循环对象 */
    rax *errors;                         /* 错误表 */ /* Errors table */
    redisAtomic unsigned int lruclock;   /* LRU 淘汰用的时钟 */ /* Clock for LRU eviction */
    volatile sig_atomic_t shutdown_asap; /* 是否需要尽快关闭服务器 */ /* SHUTDOWN needed ASAP */
    int activerehashing;                 /* serverCron() 中是否启用增量 rehash */ /* Incremental rehash in serverCron() */
    int active_defrag_running;           /* 是否正在进行主动碎片整理（记录当前扫描强度） */ /* Active defragmentation running (holds current scan aggressiveness) */
    char *pidfile;                       /* PID 文件路径 */ /* PID file path */
    int arch_bits;                       /* 架构位数（32 或 64，取决于 sizeof(long)） */ /* 32 or 64 depending on sizeof(long) */
    int cronloops;                       /* cron 函数已运行的次数 */ /* Number of times the cron function run */
    char runid[CONFIG_RUN_ID_SIZE + 1];  /* 每次服务启动时 RUN ID 都会不同。都会重新设置。 */ /* ID always different at every exec. */
    int sentinel_mode;                                                     /* 是否为 Sentinel 模式 */ /* True if this instance is a Sentinel. */
    size_t initial_memory_usage;                                           /* 初始化后使用的字节数 */ /* Bytes used after initialization. */
    int always_show_logo;                                                  /* 即使不是 stdout 日志也显示 logo */ /* Show logo even for non-stdout logging. */
    int in_eval;                                                           /* 当前是否处于 EVAL 命令中 */ /* Are we inside EVAL? */
    int in_exec;                                                           /* 当前是否处于 EXEC 命令中 */ /* Are we inside EXEC? */
    int propagate_in_transaction;    /* 保证不会嵌套传播 MULTI/EXEC */ /* Make sure we don't propagate nested MULTI/EXEC */
    char *ignore_warnings;           /* 配置：需要忽略的警告 */ /* Config: warnings that should be ignored. */
    int client_pause_in_transaction; /* 在本次事务期间是否执行了客户端暂停 */ /* Was a client pause executed during this Exec? */
    /* Modules */
    dict *moduleapi;            /* Exported core APIs dictionary for modules. */
    dict *sharedapi;            /* Like moduleapi but containing the APIs that
                                   modules share with each other. */
    list *loadmodule_queue;     /* List of modules to load at startup. */
    int module_blocked_pipe[2]; /* Pipe used to awake the event loop if a
                                   client blocked on a module command needs
                                   to be processed. */
    pid_t child_pid;            /* 当前子进程的 PID */ /* PID of current child */
    int child_type;             /* 当前子进程的类型 */ /* Type of current child */
    client *module_client;      /* 用于模块调用 Redis 的“伪造”客户端 */ /* "Fake" client to call Redis from modules */
    /* 网络相关 */ /* Networking */
    int port;                            /* TCP 监听端口 */ /* TCP listening port */
    int tls_port;                        /* TLS 监听端口 */ /* TLS listening port */
    int tcp_backlog;                     /* TCP listen() 的 backlog 参数 */ /* TCP listen() backlog */
    char *bindaddr[CONFIG_BINDADDR_MAX]; /* Addresses we should bind to */
    int bindaddr_count;                  /* server.bindaddr[] 中地址的数量 */ /* Number of addresses in server.bindaddr[] */
    char *unixsocket;                    /* UNIX socket 路径 */ /* UNIX socket path */
    mode_t unixsocketperm;               /* UNIX socket 权限 */ /* UNIX socket permission */
    socketFds ipfd;                      /* TCP socket 文件描述符 */ /* TCP socket file descriptors */
    socketFds tlsfd;                     /* TLS socket 文件描述符 */ /* TLS socket file descriptors */
    int sofd;                            /* Unix socket 文件描述符 */ /* Unix socket file descriptor */
    socketFds cfd;                       /* 集群总线监听 socket 文件描述符 */ /* Cluster bus listening socket */
    list *clients;                       /* 活跃客户端列表 */ /* List of active clients */
    list *clients_to_close;              /* 异步关闭的客户端列表 */ /* Clients to close asynchronously */
    list *clients_pending_write;         /* 有待写或需安装写处理器的客户端列表 */ /* There is to write or install handler. */
    list *clients_pending_read;          /* 有待读 socket 缓冲区的客户端列表 */ /* Client has pending read socket buffers. */
    /**
     * slaves[0]->value 指向客户端指针
     */
    list *slaves, *monitors;             /* 从节点和 MONITOR 客户端列表 */ /* List of slaves and MONITORs */
    client *current_client;              /* 当前正在执行命令的客户端 */ /* Current client executing the command. */
    rax *clients_timeout_table;          /* 用于阻塞客户端超时的基数树 */ /* Radix tree for blocked clients timeouts. */
    long fixed_time_expire;              /* 如果 > 0，则按 server.mstime 过期键 */ /* If > 0, expire keys against server.mstime. */
    int in_nested_call;                  /* 如果 > 0，表示处于嵌套调用中 */ /* If > 0, in a nested call of a call */
    rax *clients_index;                  /* 按客户端 ID 索引的活跃客户端字典 */ /* Active clients dictionary by client ID. */
    pause_type client_pause_type;        /* 当前客户端是否处于暂停状态 */ /* True if clients are currently paused */
    list *paused_clients;                /* 暂停的客户端列表 */ /* List of pause clients */
    mstime_t client_pause_end_time;      /* 解除客户端暂停的时间点 */ /* Time when we undo clients_paused */
    char neterr[ANET_ERR_LEN];           /* anet.c 的错误缓冲区 */ /* Error buffer for anet.c */
    dict *migrate_cached_sockets;        /* MIGRATE 命令缓存的 socket */ /* MIGRATE cached sockets */
    redisAtomic uint64_t next_client_id; /* 下一个客户端唯一 ID，自增 */ /* Next client unique ID. Incremental. */
    int protected_mode;                  /* 是否启用保护模式，不接受外部连接 */ /* Don't accept external connections. */
    int gopher_enabled;                  /* 是否支持 gopher 查询，同时仍支持 RESP2 查询 */ /* If true the server will reply to gopher
                                            queries. Will still serve RESP2 queries. */
    int io_threads_num;      /* 要使用的 IO 线程数量；根据该变量判断启用线程 IO？ */ /* Number of IO threads to use. */
    int io_threads_do_reads; /* 是否使用 IO 线程进行读和解析？ */               /* Read and parse from IO threads? */
    int io_threads_active;   /* IO 线程当前是否处于激活状态？ */                  /* Is IO threads currently active? */
    long long events_processed_while_blocked;                               /* 被阻塞期间处理的事件数量 */ /* processEventsWhileBlocked() */

    /* RDB / AOF loading information */
    /* RDB / AOF 正在加载信息 */
    volatile sig_atomic_t loading; /* 如果为 true，表示正在从磁盘加载数据 */  /* 如果为 true，表示正在从磁盘加载数据 */ /* We are loading data from disk if true */
    off_t loading_total_bytes;     /* 加载总字节数 */ 
    off_t loading_rdb_used_mem;    /* 加载 RDB 时已使用的内存 */
    off_t loading_loaded_bytes;    /* 已加载的字节数 */
    time_t loading_start_time;     /* 加载开始时间 */
    off_t loading_process_events_interval_bytes;    /* 加载期间处理事件的间隔字节数 */
    /* 常用命令的快速指针 */ /* Fast pointers to often looked up command */
    struct redisCommand *delCommand, *multiCommand, *lpushCommand, *lpopCommand, *rpopCommand, *zpopminCommand,
        *zpopmaxCommand, *sremCommand, *execCommand, *expireCommand, *pexpireCommand, *xclaimCommand, *xgroupCommand,
        *rpoplpushCommand, *lmoveCommand;
    /* 仅用于统计的字段 */ /* Fields used only for stats */
    time_t stat_starttime;                                /* 服务器启动时间 */ /* Server start time */
    long long stat_numcommands;                           /* 已处理命令数量 */ /* Number of processed commands */
    long long stat_numconnections;                        /* 已接收连接数量 */  /* Number of connections received */
    long long stat_expiredkeys;                           /* 已过期键数量 */ /* Number of expired keys */
    double stat_expired_stale_perc;                       /* 可能已过期键的百分比 */ /* Percentage of keys probably expired */
    long long stat_expired_time_cap_reached_count;        /* 过期周期提前终止次数 */ /* Early expire cylce stops.*/
    long long stat_expire_cycle_time_used;                /* 过期周期累计耗时（微秒） */ /* Cumulative microseconds used. */
    long long stat_evictedkeys;                           /* 已淘汰键数量（maxmemory） */ /* Number of evicted keys (maxmemory) */
    long long stat_keyspace_hits;                         /* 键空间命中次数 */ /* Number of successful lookups of keys */
    long long stat_keyspace_misses;                       /* 键空间未命中次数 */ /* Number of failed lookups of keys */
    long long stat_active_defrag_hits;                    /* 主动碎片整理移动的分配次数 */ /* number of allocations moved */
    long long stat_active_defrag_misses;                  /* 主动碎片整理扫描但未移动的分配次数 */ /* number of allocations scanned but not moved */
    long long stat_active_defrag_key_hits;                /* 主动碎片整理移动分配的键数量 */ /* number of keys with moved allocations */
    long long stat_active_defrag_key_misses;              /* 主动碎片整理扫描但未移动的键数量 */ /* number of keys scanned and not moved */
    long long stat_active_defrag_scanned;                 /* 主动碎片整理扫描的 dictEntry 数量 */ /* number of dictEntries scanned */
    size_t stat_peak_memory;                              /* 最大内存使用记录 */ /* Max used memory record */
    long long stat_fork_time;                             /* 最近一次 fork() 耗时 */ /* Time needed to perform latest fork() */
    double stat_fork_rate;                                /* fork 速率（GB/秒） */ /* Fork rate in GB/sec. */
    long long stat_total_forks;                           /* fork 总次数 */ /* Total count of fork. */
    long long stat_rejected_conn;                         /* 因 maxclients 被拒绝的客户端数量 *//* Clients rejected because of maxclients */
    long long stat_sync_full;                             /* 与从节点全量同步次数 */ /* Number of full resyncs with slaves. */
    long long stat_sync_partial_ok;                       /* 接受的 PSYNC 请求次数 */ /* Number of accepted PSYNC requests. */
    long long stat_sync_partial_err;                      /* 未接受的 PSYNC 请求次数 */ /* Number of unaccepted PSYNC requests. */
    list *slowlog;                                        /* SLOWLOG 命令列表 */ /* SLOWLOG list of commands */
    long long slowlog_entry_id;                           /* SLOWLOG 当前条目 ID */ /* SLOWLOG current entry ID */
    long long slowlog_log_slower_than;                    /* SLOWLOG 记录的时间阈值 */ /* SLOWLOG time limit (to get logged) */
    unsigned long slowlog_max_len;                        /* SLOWLOG 最大记录条数 */ /* SLOWLOG max number of items logged */
    struct malloc_stats cron_malloc_stats;                /* serverCron() 中采样的内存统计 */ /* sampled in serverCron(). */
    redisAtomic long long stat_net_input_bytes;           /* 网络读取字节数 */ /* Bytes read from network. */
    redisAtomic long long stat_net_output_bytes;          /* 网络写出字节数 */ /* Bytes written to network. */
    size_t stat_current_cow_bytes;                        /* 子进程活动期间的写时复制字节数 */ /* Copy on write bytes while child is active. */
    monotime stat_current_cow_updated;                    /* stat_current_cow_bytes 的最后更新时间 */ /* Last update time of stat_current_cow_bytes */
    size_t stat_current_save_keys_processed;              /* 子进程活动期间已处理的键数量 */ /* Processed keys while child is active. */
    size_t stat_current_save_keys_total;                  /* 子进程启动时的键总数 */ /* Number of keys when child started. */
    size_t stat_rdb_cow_bytes;                            /* RDB 保存期间的写时复制字节数 */ /* Copy on write bytes during RDB saving. */
    size_t stat_aof_cow_bytes;                            /* AOF 重写期间的写时复制字节数 */ /* Copy on write bytes during AOF rewrite. */
    size_t stat_module_cow_bytes;                         /* 模块 fork 期间的写时复制字节数 */ /* Copy on write bytes during module fork. */
    double stat_module_progress;                          /* 模块保存进度 */ /* Module save progress. */
    uint64_t stat_clients_type_memory[CLIENT_TYPE_COUNT]; /* 按客户端类型统计的内存使用 */ /* Mem usage by type */
    long long
        stat_unexpected_error_replies;  /* 意外错误回复数量（如 aof-loading、replica to master 等） */ /* Number of unexpected (aof-loading, replica to master, etc.) error replies */
    long long stat_total_error_replies; /* 总错误回复数量（命令 + 拒绝错误） */ /* Total number of issued error replies ( command + rejected errors ) */
    long long stat_dump_payload_sanitizations;         /* 深度 dump payload 完整性校验次数 */ /* Number deep dump payloads integrity validations. */
    long long stat_io_reads_processed;                 /* IO/主线程处理的读事件数量 */ /* Number of read events processed by IO / Main threads */
    long long stat_io_writes_processed;                /* IO/主线程处理的写事件数量 */ /* Number of write events processed by IO / Main threads */
    redisAtomic long long stat_total_reads_processed;  /* 处理的读事件总数 */ /* Total number of read events processed */
    redisAtomic long long stat_total_writes_processed; /* 处理的写事件总数 */ /* Total number of write events processed */
    /* 以下两个用于统计瞬时指标，比如每秒操作数、网络流量等。 */ /* The following two are used to track instantaneous metrics, like
     * number of operations per second, network traffic. */
    struct
    {
        long long last_sample_time;   /* 最近一次采样的时间戳（毫秒） */ /* Timestamp of last sample in ms */
        long long last_sample_count;  /* 最近一次采样的计数 */ /* Count in last sample */
        long long samples[STATS_METRIC_SAMPLES];    /* 采样值数组 */
        int idx;                                    /* 当前采样索引 */
    } inst_metric[STATS_METRIC_COUNT];
    /* 配置信息 Configuration */
    int verbosity;                                                      /* 日志级别（redis.conf 中的 loglevel） */ /* Loglevel in redis.conf */
    int maxidletime;                                                    /* 客户端超时时间（秒） */ /* Client timeout in seconds */
    int tcpkeepalive;                                                   /* 如果非零，则设置 SO_KEEPALIVE */ /* Set SO_KEEPALIVE if non-zero. */
    int active_expire_enabled; /* 可用于测试目的，允许禁用主动过期。 */ /* Can be disabled for testing purposes. */
    int active_expire_effort;  /* 主动过期的努力程度，范围从 1（默认）到 10。 */ /* From 1 (default) to 10, active effort. */
    int active_defrag_enabled;         /* 是否启用主动碎片整理 */
    int sanitize_dump_payload;         /* 启用 RDB 和 RESTORE 时对 ziplist 和 listpack 进行深度校验 */ /* Enables deep sanitization for ziplist and listpack in RDB and RESTORE. */
    int skip_checksum_validation;      /* 禁用 RDB 和 RESTORE 的校验和验证 */ /* Disables checksum validateion for RDB and RESTORE payload. */
    int jemalloc_bg_thread;            /* 是否启用 jemalloc 后台线程 */ /* Enable jemalloc background thread */
    size_t active_defrag_ignore_bytes; /* 触发主动碎片整理的最小碎片字节数 */ /* minimum amount of fragmentation waste to start active defrag */
    int active_defrag_threshold_lower; /* 触发主动碎片整理的最小碎片百分比 */ /* minimum percentage of fragmentation to start active defrag */
    int active_defrag_threshold_upper; /* 使用最大整理力度的最大碎片百分比 */ /* maximum percentage of fragmentation at which we use maximum effort */
    int active_defrag_cycle_min;       /* 最小碎片整理力度（CPU 百分比） */ /* minimal effort for defrag in CPU percentage */
    int active_defrag_cycle_max;       /* 最大碎片整理力度（CPU 百分比） */ /* maximal effort for defrag in CPU percentage */
    unsigned long active_defrag_max_scan_fields; /* 主 dict 扫描时，set/hash/zset/list 最大处理字段数 */ /* maximum number of fields of set/hash/zset/list to process from
                                                    within the main dict scan */
    size_t client_max_querybuf_len;              /* 客户端查询缓冲区长度限制 */ /* Limit for client query buffer length */
    int dbnum;                                   /* 配置的数据库总数 */ /* Total number of configured DBs */
    int supervised;                              /* 是否由进程管理器托管（1 表示托管，0 表示未托管） */ /* 1 if supervised, 0 otherwise. */
    int supervised_mode;                         /* 具体托管模式，见 SUPERVISED_* 宏 */ /* See SUPERVISED_* */
    int daemonize;                               /* 是否以守护进程方式运行 */ /* True if running as a daemon */
    int set_proc_title;                          /* 是否修改进程标题 */ /* True if change proc title */
    char *proc_title_template;                   /* 进程标题模板格式 */ /* Process title template format */
    clientBufferLimitsConfig client_obuf_limits[CLIENT_TYPE_OBUF_COUNT];  /* 客户端输出缓冲区限制配置 */
    int pause_cron; /* 仅仅在测试的时候生效。不运行 cron 任务（用于调试） */ /* Don't run cron tasks (debug) */
    /* AOF 持久化相关 */ /* AOF persistence */
    int aof_enabled;                                          /* AOF 配置是否启用 */ /* AOF configuration */
    int aof_state;                                            /* AOF 状态（AOF_ON/AOF_OFF/AOF_WAIT_REWRITE） */ /* AOF_(ON|OFF|WAIT_REWRITE) */
    int aof_fsync;                                            /* fsync() 策略类型 */ /* Kind of fsync() policy */
    char *aof_filename;                                       /* AOF 文件名 */ /* Name of the AOF file */
    int aof_no_fsync_on_rewrite;                              /* 如果正在重写，则不执行 fsync */ /* Don't fsync if a rewrite is in prog. */
    int aof_rewrite_perc;                                     /* 当 AOF 文件增长百分比超过 M 时触发重写 */ /* Rewrite AOF if % growth is > M and... */
    off_t aof_rewrite_min_size;                               /* 只有当 AOF 文件至少达到 N 字节时才重写 */ /* the AOF file is at least N bytes. */
    off_t aof_rewrite_base_size;                              /* 最近一次启动或重写时的 AOF 文件大小 */ /* AOF size on latest startup or rewrite. */
    off_t aof_current_size;                                   /* 当前 AOF 文件大小 */ /* AOF current size. */
    off_t aof_fsync_offset;                                   /* 已经同步到磁盘的 AOF 偏移量 */ /* AOF offset which is already synced to disk. */
    int aof_flush_sleep;                                      /* 刷新前休眠的微秒数。（测试用） */ /* Micros to sleep before flush. (used by tests) */
    int aof_rewrite_scheduled;                                /* 在 BGSAVE 结束后进行重写。 */ /* Rewrite once BGSAVE terminates. */
    list *aof_rewrite_buf_blocks;                             /* AOF 重写期间保存变更的缓冲区块 */ /* Hold changes during an AOF rewrite. */
    sds aof_buf;                                              /* 事件循环前写入的 AOF 缓冲区 */ /* AOF buffer, written before entering the event loop */
    int aof_fd;                                               /* 当前选中的 AOF 文件描述符 *//* File descriptor of currently selected AOF file */
    int aof_selected_db;                                      /* 当前 AOF 选中的数据库编号 */ /* Currently selected DB in AOF */
    time_t aof_flush_postponed_start;                         /* 延迟 AOF 刷新的开始时间 */ /* UNIX time of postponed AOF flush */
    time_t aof_last_fsync;                                    /* 最近一次 fsync 的时间 */ /* UNIX time of last fsync() */
    time_t aof_rewrite_time_last;                             /* 最近一次 AOF 重写耗时 */ /* Time used by last AOF rewrite run. */
    time_t aof_rewrite_time_start;                            /* 当前 AOF 重写开始时间 */ /* Current AOF rewrite start time. */
    int aof_lastbgrewrite_status;                             /* 最近一次后台重写状态（C_OK 或 C_ERR） */ /* C_OK or C_ERR */
    unsigned long aof_delayed_fsync;                          /* 延迟 AOF fsync 的计数 */ /* delayed AOF fsync() counter */
    int aof_rewrite_incremental_fsync;                        /* 重写期间是否增量 fsync */ /* fsync incrementally while aof rewriting? */
    int rdb_save_incremental_fsync;                           /* RDB 保存期间是否增量 fsync */ /* fsync incrementally while rdb saving? */
    int aof_last_write_status;                                /* 最近一次写入/同步状态（C_OK 或 C_ERR） */ /* C_OK or C_ERR */
    int aof_last_write_errno;                                 /* 最近一次写入/同步错误码（仅在出错时有效） */ /* Valid if aof write/fsync status is ERR */
    int aof_load_truncated;                                   /* 遇到异常 AOF EOF 时是否继续加载 */ /* Don't stop on unexpected AOF EOF. */
    int aof_use_rdb_preamble;                                 /* AOF 重写时是否使用 RDB 头部 */ /* Use RDB preamble on AOF rewrites. */
    redisAtomic int aof_bio_fsync_status;                     /* 后台 fsync 任务状态 */ /* Status of AOF fsync in bio job. */
    redisAtomic int aof_bio_fsync_errno;                      /* 后台 fsync 任务错误码 */ /* Errno of AOF fsync in bio job. */
    /* AOF 重写期间用于父进程和子进程通信的管道 */ /* AOF pipes used to communicate between parent and child during rewrite. */
    int aof_pipe_write_data_to_child;                         /* 父进程写数据到子进程的管道 */
    int aof_pipe_read_data_from_parent;                       /* 子进程从父进程读取数据的管道 */
    int aof_pipe_write_ack_to_parent;                         /* 子进程写 ACK 到父进程的管道 */
    int aof_pipe_read_ack_from_child;                         /* 父进程从子进程读取 ACK 的管道 */
    int aof_pipe_write_ack_to_child;                          /* 父进程写 ACK 到子进程的管道 */
    int aof_pipe_read_ack_from_parent;                        /* 子进程从父进程读取 ACK 的管道 */
    int aof_stop_sending_diff;  /* 如果为 true，则停止向子进程发送累计的差异数据 */ /* If true stop sending accumulated diffs
                                  to child process. */
    sds aof_child_diff;         /* 子进程端用于累计 AOF 差异的缓冲区 */ /* AOF diff accumulator child side. */
    /* RDB 持久化相关 */ /* RDB persistence */
    long long dirty;               /* 距离上次保存后数据库的变更次数 */ /* Changes to DB from the last save */
    long long dirty_before_bgsave; /* BGSAVE 失败时用于恢复 dirty 的值 */ /* Used to restore dirty on failed BGSAVE */
    struct saveparam *saveparams;  /* RDB 保存点数组 *//* Save points array for RDB */
    int saveparamslen;             /* 保存点数量 */ /* Number of saving points */
    char *rdb_filename;            /* RDB 文件名 */ /* Name of RDB file */
    int rdb_compression;           /* 是否启用 RDB 压缩 */ /* Use compression in RDB? */
    int rdb_checksum;              /* 是否启用 RDB 校验和 */ /* Use RDB checksum? */
    int rdb_del_sync_files;        /* 如果实例不使用持久化，则删除仅用于 SYNC 的 RDB 文件 *//* Remove RDB files used only for SYNC if
                                      the instance does not use persistence. */
    time_t lastsave;               /* 最近一次成功保存的时间 */ /* Unix time of last successful save */
    time_t lastbgsave_try;         /* 最近一次尝试 BGSAVE 的时间 */ /* Unix time of last attempted bgsave */
    time_t rdb_save_time_last;     /* 最近一次 RDB 保存耗时 *//* Time used by last RDB save run. */
    time_t rdb_save_time_start;    /* 当前 RDB 保存开始时间 */ /* Current RDB save start time. */
    int rdb_bgsave_scheduled;      /* 如果为 true，则在合适时机执行 BGSAVE */ /* BGSAVE when possible if true. */
    int rdb_child_type;            /* 活动子进程的保存类型 */ /* Type of save by active child. */
    int lastbgsave_status;         /* 最近一次 BGSAVE 状态（C_OK 或 C_ERR） */ /* C_OK or C_ERR */
    int stop_writes_on_bgsave_err; /* 如果无法 BGSAVE，则禁止写操作 */ /* Don't allow writes if can't BGSAVE */
    int rdb_pipe_read;             /* RDB 管道，用于无盘复制时向父进程传输 RDB 数据 */ /* RDB pipe used to transfer the rdb data */
                                   /* to the parent process in diskless repl. */
    int rdb_child_exit_pipe;       /* 用于无盘复制父进程允许子进程退出的管道 */ /* Used by the diskless parent allow child exit. */
    connection **rdb_pipe_conns;   /* 当前无盘 RDB fork 子进程的连接数组 *//* Connections which are currently the */
    int rdb_pipe_numconns;         /* 无盘 RDB fork 子进程的连接数量 */ /* target of diskless rdb fork child. */
    int rdb_pipe_numconns_writing; /* 有待写数据的 RDB 连接数量 */ /* Number of rdb conns with pending writes. */
    char *rdb_pipe_buff;           /* 无盘复制时用于保存从 RDB 管道读取的数据的缓冲区 */ /* In diskless replication, this buffer holds data */
    int rdb_pipe_bufflen;          /* RDB 管道缓冲区长度 */ /* that was read from the the rdb pipe. */
    int rdb_key_save_delay;        /* RDB 写入时每个 key 之间的延迟（微秒，测试用）负值表示平均分数微秒 */ /* Delay in microseconds between keys while
                                    * writing the RDB. (for testings). negative
                                    * value means fractions of microsecons (on average). */
    int key_load_delay;            /* 加载 AOF 或 RDB 时每个 key 之间的延迟（微秒，测试用）负值表示平均分数微秒 */
                                   /* Delay in microseconds between keys while
                                    * loading aof or rdb. (for testings). negative
                                    * value means fractions of microsecons (on average). */
    /* 子进程与父进程信息共享的管道和数据结构 */ /* Pipe and data structures for child -> parent info sharing. */
    int child_info_pipe[2];  /* 用于写入 child_info_data 的管道 */ /* Pipe used to write the child_info_data. */
    int child_info_nread;    /* 最近一次从管道读取的字节数 */ /* Num of bytes of the last read from pipe */
    /* AOF / 复制中的命令传播 */ /* Propagation of commands in AOF / replication */
    redisOpArray also_propagate; /* 需要额外传播的命令数组 */ /* Additional command to propagate. */
    int replication_allowed;     /* 是否允许进行复制 */ /* Are we allowed to replicate? */
    /* 日志相关 */ /* Logging */
    char *logfile;         /* 日志文件路径 *//* Path of log file */
    int syslog_enabled;    /* 是否启用 syslog */ /* Is syslog enabled? */
    char *syslog_ident;    /* syslog 标识符 */ /* Syslog ident */
    int syslog_facility;   /* syslog 设施类型 */ /* Syslog facility */
    int crashlog_enabled;  /* 是否启用崩溃日志信号处理器，关闭可获得更干净的 core dump */ /* Enable signal handler for crashlog.
                            * disable for clean core dumps. */
    int memcheck_enabled;  /* 崩溃时是否启用内存检查 */ /* Enable memory check on crash. */
    int use_exit_on_panic; /* panic 或 assert 时使用 exit()，而不是 abort()，便于 Valgrind 调试 */ /* Use exit() on panic and assert rather than
                            * abort(). useful for Valgrind. */
    /* 复制（主节点相关） */ /* Replication (master) */
    char replid[CONFIG_RUN_ID_SIZE + 1]; /* 指的是当前实例的复制 ID */ /* My current replication ID. */
    /**
     * 用于支持 psync2协议情况
     */
    char replid2[CONFIG_RUN_ID_SIZE+1];  /* 从主节点继承的复制ID。 当 Redis 实例发生主从切换（如主节点故障，新的主节点上线），原主节点的 replid 会被新的主节点继承，存储在 replid2 字段。
这样可以支持部分复制（PSYNC），让从节点能够识别和追踪之前的主节点复制流，提升数据同步的灵活性和容错性。 */ /* replid inherited from master*/
    long long master_repl_offset;   /* 当前实例的复制偏移量 */ /* My current replication offset */
    long long second_replid_offset; /* 对于 replid2，允许的最大偏移量 */ /* Accept offsets up to this for replid2. */
    int slaveseldb;                 /* 复制输出中最近一次 SELECT 的数据库编号 */ /* Last SELECTed DB in replication output */
    int repl_ping_slave_period;     /* 主节点每 N 秒 ping 一次从节点 *//* Master pings the slave every N seconds */
    char *repl_backlog;             /* 用于部分同步的复制积压缓冲区 */ /* Replication backlog for partial syncs */
    long long repl_backlog_size;    /* 积压缓冲区的环形缓冲区大小 */ /* Backlog circular buffer size */
    long long repl_backlog_histlen; /* 积压缓冲区实际数据长度 */ /* Backlog actual data length */
    long long repl_backlog_idx;     /* 积压缓冲区当前写入偏移量，下一个要写入的字节位置 */ /* Backlog circular buffer current offset,
                                       that is the next byte will'll write to.*/
    long long repl_backlog_off;     /* 积压缓冲区第一个字节的主节点复制偏移量 */ /* Replication "master offset" of first
                                       byte in the replication backlog buffer.*/
    time_t repl_backlog_time_limit; /* 没有从节点时，积压缓冲区释放的时间限制 *//* Time without slaves after the backlog
                                       gets released. */
    time_t repl_no_slaves_since;    /* 没有从节点的起始时间，仅在 server.slaves 长度为 0 时有效 */ /* We have no slaves since that time.
                                       Only valid if server.slaves len is 0. */
    int repl_min_slaves_to_write;   /* 允许写操作的最小从节点数量 */ /* Min number of slaves to write. */
    int repl_min_slaves_max_lag;    /* 允许写操作的最大从节点延迟 */ /* Max lag of <count> slaves to write. */
    int repl_good_slaves_count;     /* 延迟小于等于最大延迟的从节点数量 *//* Number of slaves with lag <= max_lag. */
    int repl_diskless_sync;         /* 主节点直接将 RDB 发送到从节点的套接字。 */ /* Master send RDB to slaves sockets directly. */
    int repl_diskless_load;         /* 从节点是否直接从套接字解析 RDB，见 REPL_DISKLESS_LOAD_* 枚举 *//* Slave parse RDB directly from the socket.
                                                       * see REPL_DISKLESS_LOAD_* enum */
    int repl_diskless_sync_delay;   /* 启动无盘复制 BGSAVE 的延迟时间 */ /* Delay to start a diskless repl BGSAVE. */
    /* 复制（从节点相关的配置） */ /* Replication (slave) */
    char *masteruser;                   /* 用于与主节点认证的用户名（AUTH） */ /* AUTH with this user and masterauth with master */
    sds masterauth;                     /* 用于与主节点认证的密码（AUTH） */ /* AUTH with this password with master */
    char *masterhost;                   /* 主节点主机名 */ /* Hostname of master */
    int masterport;                     /* 主节点端口 */ /* Port of master */
    int repl_timeout;                   /* 主节点空闲 N 秒后超时 */ /* Timeout after N seconds of master idle */
    client *master;                     /* 当前从节点对应的主节点客户端 */ /* Client that is master for this slave */
    client *cached_master;              /* 用于 PSYNC 的缓存主节点 */ /* Cached master to be reused for PSYNC. */
    int repl_syncio_timeout;            /* 同步 I/O 调用的超时时间 */ /* Timeout for synchronous I/O calls */
    int repl_state;                     /* 当前实例作为从节点时的复制状态 */ /* Replication status if the instance is a slave */
    off_t repl_transfer_size;           /* 同步期间需要从主节点读取的 RDB 文件大小 */ /* Size of RDB to read from master during sync. */
    off_t repl_transfer_read;           /* 已从主节点读取的 RDB 字节数 */ /* Amount of RDB read from master during sync. */
    off_t repl_transfer_last_fsync_off; /* 最近一次 fsync 的偏移量 */ /* Offset when we fsync-ed last time. */
    connection *repl_transfer_s;        /* 从节点到主节点的 SYNC 连接 */ /* Slave -> Master SYNC connection */
    int repl_transfer_fd;               /* 从节点到主节点的 SYNC 临时文件描述符 */ /* Slave -> Master SYNC temp file descriptor */
    char *repl_transfer_tmpfile;        /* 从节点到主节点的 SYNC 临时文件名 *//* Slave-> master SYNC temp file name */
    time_t repl_transfer_lastio;        /* 最近一次读取的时间戳，用于超时判断 */ /* Unix time of the latest read, for timeout */
    int repl_serve_stale_data;          /* 断开主节点时是否提供过期数据 */ /* Serve stale data when link is down? */
    int repl_slave_ro;                  /* 从节点是否只读 */ /* Slave is read only? */
    int repl_slave_ignore_maxmemory;    /* 如果为 true，从节点不会淘汰数据 */ /* If true slaves do not evict. */
    time_t repl_down_since;             /* 与主节点断开连接的时间戳 */ /* Unix time at which link with master went down */
    int repl_disable_tcp_nodelay;       /* SYNC 后是否禁用 TCP_NODELAY */ /* Disable TCP_NODELAY after SYNC? */
    int slave_priority;                 /* 在 INFO 中报告，Sentinel 用于选主 */ /* Reported in INFO and used by Sentinel. */
    int replica_announced;              /* 如果为 true，Sentinel 会宣布该副本 */ /* If true, replica is announced by Sentinel */
    int slave_announce_port;            /* 向主节点报告的监听端口 */ /* Give the master this listening port. */
    char *slave_announce_ip;            /* 向主节点报告的 IP 地址 */ /* Give the master this ip address. */
    /* 以下两个字段用于在 PSYNC 过程中存储主节点的 replid/offset，结束后会复制到 server->master 客户端结构体中 */
    /* The following two fields is where we store master PSYNC replid/offset
     * while the PSYNC is in progress. At the end we'll copy the fields into
     * the server->master client structure. */
    char master_replid[CONFIG_RUN_ID_SIZE + 1]; /* 主节点 PSYNC runid */ /* Master PSYNC runid. */
    long long master_initial_offset;            /* 主节点 PSYNC offset *//* Master PSYNC offset. */
    int repl_slave_lazy_flush;                  /* 加载 DB 前是否延迟 FLUSHALL */ /* Lazy FLUSHALL before loading DB? */
    /* 复制脚本缓存 */ /* Replication script cache. */
    dict *repl_scriptcache_dict;        /* 所有从节点已知的 SHA1 */ /* SHA1 all slaves are aware of. */
    list *repl_scriptcache_fifo;        /* FIFO LRU 淘汰队列 */ /* First in, first out LRU eviction. */
    unsigned int repl_scriptcache_size; /* 最大缓存元素数量 *//* Max number of elements. */
    /* 同步复制 */ /* Synchronous replication. */
    list *clients_waiting_acks;  /* 等待 WAIT 命令的客户端列表 */ /* Clients waiting in WAIT command. */
    int get_ack_from_slaves;     /* 如果为 true，则发送 REPLCONF GETACK。 */ /* If true we send REPLCONF GETACK. */
    /* 限制相关 */ /* Limits */
    unsigned int maxclients;                    /* 最大同时客户端数量 */ /* Max number of simultaneous clients */
    unsigned long long maxmemory;               /* 最大可用内存字节数 */ /* Max number of memory bytes to use */
    int maxmemory_policy;                       /* 键淘汰策略 */ /* Policy for key eviction */
    int maxmemory_samples;                      /* 随机采样精度 */ /* Precision of random sampling */
    int maxmemory_eviction_tenacity;            /* 淘汰处理的激进程度 */ /* Aggressiveness of eviction processing */
    int lfu_log_factor;                         /* LFU 日志计数因子 */ /* LFU logarithmic counter factor. */
    int lfu_decay_time;                         /* LFU 计数衰减因子 */ /* LFU counter decay factor. */
    long long proto_max_bulk_len;               /* 协议最大 bulk 长度 */ /* Protocol bulk length maximum size. */
    int oom_score_adj_base;                     /* 启动时观察到的 oom_score_adj 基值 *//* Base oom_score_adj value, as observed on startup */
    int oom_score_adj_values[CONFIG_OOM_COUNT]; /* Linux oom_score_adj 配置 */ /* Linux oom_score_adj configuration */
    int oom_score_adj;                          /* 如果为 true，管理 oom_score_adj */ /* If true, oom_score_adj is managed */
    int disable_thp;                             /* 如果为 true，通过 syscall 禁用 THP */ /* If true, disable THP by syscall */
    /* 被阻塞的客户端 */ /* Blocked clients */
    unsigned int blocked_clients; /* 正在执行阻塞命令的客户端数量 */ /* # of clients executing a blocking cmd.*/
    unsigned int blocked_clients_by_type[BLOCKED_NUM]; /* 按类型统计的被阻塞客户端数量 */
    list *unblocked_clients; /* 下一个循环前需要解除阻塞的客户端列表 */ /* list of clients to unblock before next loop */
    list *ready_keys;        /* BLPOP等命令的readyList结构列表 */ /* List of readyList structures for BLPOP & co */
    /* 客户端缓存相关 */ /* Client side caching. */
    unsigned int tracking_clients;  /* 启用跟踪功能的客户端数量 */ /* # of clients with tracking enabled.*/
    size_t tracking_table_max_keys; /* 跟踪表中允许的最大键数量 */ /* Max number of keys in tracking table. */
    list *tracking_pending_keys;    /* 待刷新失效的跟踪键列表 *//* tracking invalidation keys pending to flush */
    /* 排序参数 - qsort_r() 只在 BSD 下可用，所以我们
     * 需要将此状态设置为全局，以便传递给 sortCompare() *//* Sort parameters - qsort_r() is only available under BSD so we
     * have to take this state global, in order to pass it to sortCompare() */
    int sort_desc;
    int sort_alpha;
    int sort_bypattern;
    int sort_store;
    /* Zip 结构配置，详细信息见 redis.conf */ /* Zip structure config, see redis.conf for more information  */
    size_t hash_max_ziplist_entries;
    size_t hash_max_ziplist_value;
    size_t set_max_intset_entries;
    size_t zset_max_ziplist_entries;
    size_t zset_max_ziplist_value;
    size_t hll_sparse_max_bytes;
    size_t stream_node_max_bytes;       /* Stream 节点最大字节数 */
    long long stream_node_max_entries;  /* Stream 节点最大条目数 */
    /* 列表参数 */ /* List parameters */
    int list_max_ziplist_size;
    int list_compress_depth;
    /* 时间缓存 */               /* time cache */
    redisAtomic time_t unixtime; //* 每次 cron 循环采样的 Unix 时间。 */ * Unix time sampled every cron cycle. */
    time_t timezone;     /* 缓存的时区，由 tzset() 设置。 */ /* Cached timezone. As set by tzset(). */
    int daylight_active; /* 当前是否处于夏令时。 */      /* Currently in daylight saving time. */
    mstime_t mstime;     /* 毫秒级的 Unix 时间。 */     /* 'unixtime' in milliseconds. */
    ustime_t ustime;     /* 微秒级的 Unix 时间。 */     /* 'unixtime' in microseconds. */
    size_t blocking_op_nesting;                       /* 阻塞操作的嵌套层级，用于重置 blocked_last_cron。 */ /* Nesting level of blocking operation, used to reset
                                                                  blocked_last_cron. */
    long long blocked_last_cron;  /* 标记上一次因阻塞操作执行定时任务的毫秒时间 */ /* Indicate the mstime of the last time we did cron jobs from a blocking operation */
    /* 发布订阅 */ /* Pubsub */
    dict *pubsub_channels;      /* 频道到已订阅客户端列表的映射 */ /* Map channels to list of subscribed clients */
    dict *pubsub_patterns;      /* 发布订阅模式的字典 */ /* A dict of pubsub_patterns */
    int notify_keyspace_events; /* 通过发布/订阅传播的事件，是NOTIFY_...标志的异或值 */ /* Events to propagate via Pub/Sub. This is an
                                   xor of NOTIFY_... flags. */
    /* 集群 */ /* Cluster */
    int cluster_enabled;                 /* 是否启用集群 */ /* Is cluster enabled? */
    mstime_t cluster_node_timeout;       /* 集群节点超时时间 */ /* Cluster node timeout. */
    char *cluster_configfile;            /* 集群自动生成的配置文件名 */ /* Cluster auto-generated config file name. */
    struct clusterState *cluster;        /* 集群状态 */ /* State of the cluster */
    int cluster_migration_barrier;       /* 集群副本迁移屏障 */ /* Cluster replicas migration barrier. */
    int cluster_allow_replica_migration; /* 是否允许自动将副本迁移到孤立主节点或空主节点 *//* Automatic replica migrations to orphaned masters and from empty masters */
    int cluster_slave_validity_factor;   /* 副本最大数据年龄，用于故障转移 *//* Slave max data age for failover. */
    int cluster_require_full_coverage;   /* 如果为真，至少有一个未覆盖槽时集群将下线 */ /* If true, put the cluster down if
                                            there is at least an uncovered slot.*/
    int cluster_slave_no_failover;       /* 如果主节点故障，阻止副本发起故障转移 */ /* Prevent slave from starting a failover
                                            if the master is in failure state. */
    char *cluster_announce_ip;           /* 集群总线通告的IP地址 */ /* IP address to announce on cluster bus. */
    int cluster_announce_port;           /* 集群总线通告的基础端口 */ /* base port to announce on cluster bus. */
    int cluster_announce_tls_port;       /* 集群总线通告的TLS端口 */ /* TLS port to announce on cluster bus. */
    int cluster_announce_bus_port;       /* 集群总线通告的总线端口 */ /* bus port to announce on cluster bus. */
    int cluster_module_flags;            /* Redis模块可设置的标志，用于抑制某些原生集群特性，见REDISMODULE_CLUSTER_FLAG_* */ /* Set of flags that Redis modules are able
                                            to set in order to suppress certain
                                            native Redis Cluster features. Check the
                                            REDISMODULE_CLUSTER_FLAG_*. */
    int cluster_allow_reads_when_down;  /* 集群下线时是否允许读操作 *//* Are reads allowed when the cluster
                                          is down? */
    int cluster_config_file_lock_fd;    /* 集群配置文件的文件描述符，将被flock锁定 */ /* cluster config fd, will be flock */
    /* 脚本 */ /* Scripting */
    lua_State *lua;                     /* Lua解释器，所有客户端共用一个 */ /* The Lua interpreter. We use just one for all clients */
    client *lua_client;                 /* 用于Lua查询Redis的“伪客户端” */ /* The "fake client" to query Redis from Lua */
    client *lua_caller;                 /* 当前运行EVAL的客户端，或NULL */ /* The client running EVAL right now, or NULL */
    char *lua_cur_script;               /* 当前运行脚本的SHA1，或NULL */ /* SHA1 of the script currently running, or NULL */
    dict *lua_scripts;                  /* SHA1到Lua脚本的字典 */ /* A dictionary of SHA1 -> Lua scripts */
    unsigned long long lua_scripts_mem; /* 缓存脚本占用的内存及开销 */ /* Cached scripts' memory + oh */
    mstime_t lua_time_limit;            /* 脚本超时时间（毫秒） */ /* Script timeout in milliseconds */
    monotime lua_time_start;            /* 用于检测脚本超时的单调计时器 */ /* monotonic timer to detect timed-out script */
    mstime_t lua_time_snapshot;         /* 脚本启动时的毫秒快照 */ /* Snapshot of mstime when script is started */
    int lua_write_dirty;                /* 当前脚本执行期间是否调用了写命令 */ /* True if a write command was called during the
                                           execution of the current script. */
    int lua_random_dirty;               /* 当前脚本执行期间是否调用了随机命令 */ /* True if a random command was called during the
                                           execution of the current script. */
    int lua_replicate_commands;         /* 是否进行单命令复制 */ /* True if we are doing single commands repl. */
    int lua_multi_emitted;              /* 是否已传播MULTI命令 */ /* True if we already propagated MULTI. */
    int lua_repl;                       /* redis.set_repl()的脚本复制标志 */ /* Script replication flags for redis.set_repl(). */
    int lua_timedout;                   /* 是否达到脚本执行时间限制 */ /* True if we reached the time limit for script
                                           execution. */
    int lua_kill;                       /* 是否终止脚本 */ /* Kill the script if true. */
    int lua_always_replicate_commands;  /* 默认复制类型 *//* Default replication type. */
    int lua_oom;                         /* 脚本启动时是否检测到OOM */ /* OOM detected when script start? */
    /* 惰性释放 */ /* Lazy free */
    int lazyfree_lazy_eviction;
    int lazyfree_lazy_expire;
    int lazyfree_lazy_server_del;
    int lazyfree_lazy_user_del;
    int lazyfree_lazy_user_flush;
    /* 延迟监控 */ /* Latency monitor */
    long long latency_monitor_threshold;
    dict *latency_events;
    /* ACLs */
    char *acl_filename;           /* ACL Users file. NULL if not configured. */
    unsigned long acllog_max_len; /* Maximum length of the ACL LOG list. */
    sds requirepass;              /* Remember the cleartext password set with
                                     the old "requirepass" directive for
                                     backward compatibility with Redis <= 5. */
    int acl_pubsub_default;       /* Default ACL pub/sub channels flag */
    /* Assert & bug reporting */
    int watchdog_period; /* Software watchdog period in ms. 0 = off */
    /* System hardware info */
    size_t system_memory_size; /* Total memory in system as reported by OS */
    /* TLS Configuration */
    int tls_cluster;
    int tls_replication;
    int tls_auth_clients;
    redisTLSContextConfig tls_ctx_config;
    /* CPU 亲和性 */ /* cpu affinity */
    char *server_cpulist;      /* Redis服务器主线程/IO线程的CPU亲和性列表 */ /* cpu affinity list of redis server main/io thread. */
    char *bio_cpulist;         /* BIO线程的CPU亲和性列表 */ /* cpu affinity list of bio thread. */
    char *aof_rewrite_cpulist; /* AOF重写进程的CPU亲和性列表 */ /* cpu affinity list of aof rewrite process. */
    char *bgsave_cpulist;      /* 后台保存进程的CPU亲和性列表 */ /* cpu affinity list of bgsave process. */
    /* Sentinel 配置 */ /* Sentinel config */
    struct sentinelConfig *sentinel_config; /* 启动时加载的Sentinel配置 */ /* sentinel config to load at startup time. */
    /* 协调故障转移信息 */ /* Coordinate failover info */
    mstime_t failover_end_time; /* 故障转移命令的截止时间 */ /* Deadline for failover command. */
    int force_failover;         /* 如果为真，到截止时间强制故障转移，否则中止故障转移 */ /* If true then failover will be foreced at the
                                 * deadline, otherwise failover is aborted. */
    char *target_replica_host;  /* 故障转移目标主机。如果在故障转移期间为NULL，则可使用任意副本 */ /* Failover target host. If null during a
                                 * failover then any replica can be used. */
    int target_replica_port;    /* 故障转移目标端口 */ /* Failover target port */
    int failover_state;         /* 故障转移状态 *//* Failover state */
};

#define MAX_KEYS_BUFFER 256

/* 用于各种 getkeys 函数调用的结果结构体。它将 key 以索引的形式列出，索引指向提供的 argv 参数。 */
/* A result structure for the various getkeys function calls. It lists the
 * keys as indices to the provided argv.
 */
typedef struct
{
    int keysbuf[MAX_KEYS_BUFFER]; /* 预分配的缓冲区，用于减少堆分配 */ /* Pre-allocated buffer, to save heap allocations */
    int *keys;                    /* key 索引数组，指向 keysbuf 或堆分配的空间 */ /* Key indices array, points to keysbuf or heap */
    int numkeys;                   /* 返回的 key 索引数量 */ /* Number of key indices return */
    int size;                     /* 数组可用大小 */ /* Available array size */
} getKeysResult;
#define GETKEYS_RESULT_INIT {{0}, NULL, 0, MAX_KEYS_BUFFER}

typedef void redisCommandProc(client *c); // 命令处理函数类型
typedef int redisGetKeysProc(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
struct redisCommand
{
    char *name;             // 命令名称 
    redisCommandProc *proc; // 命令处理函数
    int arity;              // 参数数量（正数表示固定参数，负数表示可变参数）
    char *sflags;           // 字符串形式的命令标志，每个标志一个字符 /* Flags as string representation, one char per flag. */
    uint64_t flags;         // 实际的命令标志，从 sflags 解析得到 /* The actual flags, obtained from the 'sflags' field. */
    // 用于 Redis Cluster 重定向，确定命令行中哪些参数是 key 的函数
    /* Use a function to determine keys arguments in a command line.
     * Used for Redis Cluster redirect. */
    redisGetKeysProc *getkeys_proc;
    /* What keys should be loaded in background when calling this command? */
    int firstkey;    // 第一个 key 参数的位置（0 表示没有 key /* The first argument that's a key (0 = no keys) */
    int lastkey;     // 最后一个 key 参数的位置 /* The last argument that's a key */
    int keystep;     // key 参数之间的步长 /* The step between first and last key */
    long long microseconds, calls, rejected_calls, failed_calls;
    int id;   // 命令 ID。运行时分配的递增 ID，用于 ACL 权限校验。
              // 连接能否执行某命令取决于用户的命令位图是否包含该命令的 bit。
              /* Command ID. This is a progressive ID starting from 0 that
               is assigned at runtime, and is used in order to check
               ACLs. A connection is able to execute a given command if
               the user associated to the connection has this command
               bit set in the bitmap of allowed commands. */
};

struct redisError
{
    long long count;
};

struct redisFunctionSym
{
    char *name;
    unsigned long pointer;
};

typedef struct _redisSortObject
{
    robj *obj;
    union
    {
        double score;
        robj *cmpobj;
    } u;
} redisSortObject;

typedef struct _redisSortOperation
{
    int type;
    robj *pattern;
} redisSortOperation;

/* 用于列表迭代抽象的结构体。 */ /* Structure to hold list iteration abstraction. */
typedef struct
{
    robj *subject;
    unsigned char encoding;
    unsigned char direction; /* Iteration direction */
    quicklistIter *iter;
} listTypeIterator;

/* 用于列表迭代时的条目结构体。 */ /* Structure for an entry while iterating over a list. */
typedef struct
{
    listTypeIterator *li;
    quicklistEntry entry; /* Entry in quicklist */
} listTypeEntry;

/* 用于集合迭代抽象的结构体。 */ /* Structure to hold set iteration abstraction. */
typedef struct
{
    robj *subject;
    int encoding;
    int ii; /* intset iterator */
    dictIterator *di;
} setTypeIterator;

/* Structure to hold hash iteration abstraction. Note that iteration over
 * hashes involves both fields and values. Because it is possible that
 * not both are required, store pointers in the iterator to avoid
 * unnecessary memory allocation for fields/values. */
/* 用于哈希迭代抽象的结构体。注意哈希迭代涉及字段和值。
 * 因为有时可能只需要其中之一，所以迭代器中存储指针以避免不必要的内存分配。 */
typedef struct
{
    robj *subject;
    int encoding;

    unsigned char *fptr, *vptr;

    dictIterator *di;
    dictEntry *de;
} hashTypeIterator;

#include "stream.h" /* Stream data type header file. */

#define OBJ_HASH_KEY 1
#define OBJ_HASH_VALUE 2

/*-----------------------------------------------------------------------------
 * Extern declarations
 *----------------------------------------------------------------------------*/

extern struct redisServer server;
extern struct sharedObjectsStruct shared;
extern dictType objectKeyPointerValueDictType;
extern dictType objectKeyHeapPointerValueDictType;
extern dictType setDictType;
extern dictType zsetDictType;
extern dictType clusterNodesDictType;
extern dictType clusterNodesBlackListDictType;
extern dictType dbDictType;
extern dictType shaScriptObjectDictType;
extern double R_Zero, R_PosInf, R_NegInf, R_Nan;
extern dictType hashDictType;
extern dictType replScriptCacheDictType;
extern dictType dbExpiresDictType;
extern dictType modulesDictType;
extern dictType sdsReplyDictType;

/*-----------------------------------------------------------------------------
 * Functions prototypes
 *----------------------------------------------------------------------------*/

/* Modules */
void moduleInitModulesSystem(void);
void moduleInitModulesSystemLast(void);
int moduleLoad(const char *path, void **argv, int argc);
void moduleLoadFromQueue(void);
int moduleGetCommandKeysViaAPI(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
moduleType *moduleTypeLookupModuleByID(uint64_t id);
void moduleTypeNameByID(char *name, uint64_t moduleid);
const char *moduleTypeModuleName(moduleType *mt);
void moduleFreeContext(struct RedisModuleCtx *ctx);
void unblockClientFromModule(client *c);
void moduleHandleBlockedClients(void);
void moduleBlockedClientTimedOut(client *c);
void moduleBlockedClientPipeReadable(aeEventLoop *el, int fd, void *privdata, int mask);
size_t moduleCount(void);
void moduleAcquireGIL(void);
int moduleTryAcquireGIL(void);
void moduleReleaseGIL(void);
void moduleNotifyKeyspaceEvent(int type, const char *event, robj *key, int dbid);
void moduleCallCommandFilters(client *c);
void ModuleForkDoneHandler(int exitcode, int bysignal);
int TerminateModuleForkChild(int child_pid, int wait);
ssize_t rdbSaveModulesAux(rio *rdb, int when);
int moduleAllDatatypesHandleErrors();
sds modulesCollectInfo(sds info, const char *section, int for_crash_report, int sections);
void moduleFireServerEvent(uint64_t eid, int subid, void *data);
void processModuleLoadingProgressEvent(int is_aof);
int moduleTryServeClientBlockedOnKey(client *c, robj *key);
void moduleUnblockClient(client *c);
int moduleBlockedClientMayTimeout(client *c);
int moduleClientIsBlockedOnKeys(client *c);
void moduleNotifyUserChanged(client *c);
void moduleNotifyKeyUnlink(robj *key, robj *val);
robj *moduleTypeDupOrReply(client *c, robj *fromkey, robj *tokey, robj *value);
int moduleDefragValue(robj *key, robj *obj, long *defragged);
int moduleLateDefrag(robj *key, robj *value, unsigned long *cursor, long long endtime, long long *defragged);
long moduleDefragGlobals(void);

/* Utils */
long long ustime(void);
long long mstime(void);
void getRandomHexChars(char *p, size_t len);
void getRandomBytes(unsigned char *p, size_t len);
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);
void exitFromChild(int retcode);
long long redisPopcount(void *s, long count);
int redisSetProcTitle(char *title);
int validateProcTitleTemplate(const char *template);
int redisCommunicateSystemd(const char *sd_notify_msg);
void redisSetCpuAffinity(const char *cpulist);

/* networking.c -- Networking and Client related operations */
client *createClient(connection *conn);
void closeTimedoutClients(void);
void freeClient(client *c);
void freeClientAsync(client *c);
void resetClient(client *c);
void freeClientOriginalArgv(client *c);
void sendReplyToClient(connection *conn);
void *addReplyDeferredLen(client *c);
void setDeferredArrayLen(client *c, void *node, long length);
void setDeferredMapLen(client *c, void *node, long length);
void setDeferredSetLen(client *c, void *node, long length);
void setDeferredAttributeLen(client *c, void *node, long length);
void setDeferredPushLen(client *c, void *node, long length);
void processInputBuffer(client *c);
void processGopherRequest(client *c);
void acceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void acceptTLSHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void readQueryFromClient(connection *conn);
void addReplyNull(client *c);
void addReplyNullArray(client *c);
void addReplyBool(client *c, int b);
void addReplyVerbatim(client *c, const char *s, size_t len, const char *ext);
void addReplyProto(client *c, const char *s, size_t len);
void AddReplyFromClient(client *c, client *src);
void addReplyBulk(client *c, robj *obj);
void addReplyBulkCString(client *c, const char *s);
void addReplyBulkCBuffer(client *c, const void *p, size_t len);
void addReplyBulkLongLong(client *c, long long ll);
void addReply(client *c, robj *obj);
void addReplySds(client *c, sds s);
void addReplyBulkSds(client *c, sds s);
void setDeferredReplyBulkSds(client *c, void *node, sds s);
void addReplyErrorObject(client *c, robj *err);
void addReplyErrorSds(client *c, sds err);
void addReplyError(client *c, const char *err);
void addReplyStatus(client *c, const char *status);
void addReplyDouble(client *c, double d);
void addReplyBigNum(client *c, const char *num, size_t len);
void addReplyHumanLongDouble(client *c, long double d);
void addReplyLongLong(client *c, long long ll);
void addReplyArrayLen(client *c, long length);
void addReplyMapLen(client *c, long length);
void addReplySetLen(client *c, long length);
void addReplyAttributeLen(client *c, long length);
void addReplyPushLen(client *c, long length);
void addReplyHelp(client *c, const char **help);
void addReplySubcommandSyntaxError(client *c);
void addReplyLoadedModules(client *c);
void copyClientOutputBuffer(client *dst, client *src);
void deferredAfterErrorReply(client *c, list *errors);
size_t sdsZmallocSize(sds s);
size_t getStringObjectSdsUsedMemory(robj *o);
void freeClientReplyValue(void *o);
void *dupClientReplyValue(void *o);
void getClientsMaxBuffers(unsigned long *longest_output_list, unsigned long *biggest_input_buffer);
char *getClientPeerId(client *client);
char *getClientSockName(client *client);
sds catClientInfoString(sds s, client *client);
sds getAllClientsInfoString(int type);
void rewriteClientCommandVector(client *c, int argc, ...);
void rewriteClientCommandArgument(client *c, int i, robj *newval);
void replaceClientCommandVector(client *c, int argc, robj **argv);
void redactClientCommandArgument(client *c, int argc);
unsigned long getClientOutputBufferMemoryUsage(client *c);
int freeClientsInAsyncFreeQueue(void);
int closeClientOnOutputBufferLimitReached(client *c, int async);
int getClientType(client *c);
int getClientTypeByName(char *name);
char *getClientTypeName(int class);
void flushSlavesOutputBuffers(void);
void disconnectSlaves(void);
int listenToPort(int port, socketFds *fds);
void pauseClients(mstime_t duration, pause_type type);
void unpauseClients(void);
int areClientsPaused(void);
int checkClientPauseTimeoutAndReturnIfPaused(void);
void processEventsWhileBlocked(void);
void loadingCron(void);
void whileBlockedCron();
void blockingOperationStarts();
void blockingOperationEnds();
int handleClientsWithPendingWrites(void);
int handleClientsWithPendingWritesUsingThreads(void);
int handleClientsWithPendingReadsUsingThreads(void);
int stopThreadedIOIfNeeded(void);
int clientHasPendingReplies(client *c);
void unlinkClient(client *c);
int writeToClient(client *c, int handler_installed);
void linkClient(client *c);
void protectClient(client *c);
void unprotectClient(client *c);
void initThreadedIO(void);
client *lookupClientByID(uint64_t id);
int authRequired(client *c);

#ifdef __GNUC__
void addReplyErrorFormat(client *c, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
void addReplyStatusFormat(client *c, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
#else
void addReplyErrorFormat(client *c, const char *fmt, ...);
void addReplyStatusFormat(client *c, const char *fmt, ...);
#endif

/* Client side caching (tracking mode) */
void enableTracking(client *c, uint64_t redirect_to, uint64_t options, robj **prefix, size_t numprefix);
void disableTracking(client *c);
void trackingRememberKeys(client *c);
void trackingInvalidateKey(client *c, robj *keyobj, int bcast);
void trackingScheduleKeyInvalidation(uint64_t client_id, robj *keyobj);
void trackingHandlePendingKeyInvalidations(void);
void trackingInvalidateKeysOnFlush(int async);
void freeTrackingRadixTree(rax *rt);
void freeTrackingRadixTreeAsync(rax *rt);
void trackingLimitUsedSlots(void);
uint64_t trackingGetTotalItems(void);
uint64_t trackingGetTotalKeys(void);
uint64_t trackingGetTotalPrefixes(void);
void trackingBroadcastInvalidationMessages(void);
int checkPrefixCollisionsOrReply(client *c, robj **prefix, size_t numprefix);

/* List data type */
void listTypeTryConversion(robj *subject, robj *value);
void listTypePush(robj *subject, robj *value, int where);
robj *listTypePop(robj *subject, int where);
unsigned long listTypeLength(const robj *subject);
listTypeIterator *listTypeInitIterator(robj *subject, long index, unsigned char direction);
void listTypeReleaseIterator(listTypeIterator *li);
int listTypeNext(listTypeIterator *li, listTypeEntry *entry);
robj *listTypeGet(listTypeEntry *entry);
void listTypeInsert(listTypeEntry *entry, robj *value, int where);
int listTypeEqual(listTypeEntry *entry, robj *o);
void listTypeDelete(listTypeIterator *iter, listTypeEntry *entry);
void listTypeConvert(robj *subject, int enc);
robj *listTypeDup(robj *o);
void unblockClientWaitingData(client *c);
void popGenericCommand(client *c, int where);
void listElementsRemoved(client *c, robj *key, int where, robj *o, long count);

/* MULTI/EXEC/WATCH... */
void unwatchAllKeys(client *c);
void initClientMultiState(client *c);
void freeClientMultiState(client *c);
void queueMultiCommand(client *c);
void touchWatchedKey(redisDb *db, robj *key);
int isWatchedKeyExpired(client *c);
void touchAllWatchedKeysInDb(redisDb *emptied, redisDb *replaced_with);
void discardTransaction(client *c);
void flagTransaction(client *c);
void execCommandAbort(client *c, sds error);
void execCommandPropagateMulti(int dbid);
void execCommandPropagateExec(int dbid);
void beforePropagateMulti();
void afterPropagateExec();

/* Redis object implementation */
void decrRefCount(robj *o);
void decrRefCountVoid(void *o);
void incrRefCount(robj *o);
robj *makeObjectShared(robj *o);
robj *resetRefCount(robj *obj);
void freeStringObject(robj *o);
void freeListObject(robj *o);
void freeSetObject(robj *o);
void freeZsetObject(robj *o);
void freeHashObject(robj *o);
robj *createObject(int type, void *ptr);
robj *createStringObject(const char *ptr, size_t len);
robj *createRawStringObject(const char *ptr, size_t len);
robj *createEmbeddedStringObject(const char *ptr, size_t len);
robj *tryCreateRawStringObject(const char *ptr, size_t len);
robj *tryCreateStringObject(const char *ptr, size_t len);
robj *dupStringObject(const robj *o);
int isSdsRepresentableAsLongLong(sds s, long long *llval);
int isObjectRepresentableAsLongLong(robj *o, long long *llongval);
robj *tryObjectEncoding(robj *o);
robj *getDecodedObject(robj *o);
size_t stringObjectLen(robj *o);
robj *createStringObjectFromLongLong(long long value);
robj *createStringObjectFromLongLongForValue(long long value);
robj *createStringObjectFromLongDouble(long double value, int humanfriendly);
robj *createQuicklistObject(void);
robj *createZiplistObject(void);
robj *createSetObject(void);
robj *createIntsetObject(void);
robj *createHashObject(void);
robj *createZsetObject(void);
robj *createZsetZiplistObject(void);
robj *createStreamObject(void);
robj *createModuleObject(moduleType *mt, void *value);
int getLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg);
int getPositiveLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg);
int getRangeLongFromObjectOrReply(client *c, robj *o, long min, long max, long *target, const char *msg);
int checkType(client *c, robj *o, int type);
int getLongLongFromObjectOrReply(client *c, robj *o, long long *target, const char *msg);
int getDoubleFromObjectOrReply(client *c, robj *o, double *target, const char *msg);
int getDoubleFromObject(const robj *o, double *target);
int getLongLongFromObject(robj *o, long long *target);
int getLongDoubleFromObject(robj *o, long double *target);
int getLongDoubleFromObjectOrReply(client *c, robj *o, long double *target, const char *msg);
int getIntFromObjectOrReply(client *c, robj *o, int *target, const char *msg);
char *strEncoding(int encoding);
int compareStringObjects(robj *a, robj *b);
int collateStringObjects(robj *a, robj *b);
int equalStringObjects(robj *a, robj *b);
unsigned long long estimateObjectIdleTime(robj *o);
void trimStringObjectIfNeeded(robj *o);
#define sdsEncodedObject(objptr) (objptr->encoding == OBJ_ENCODING_RAW || objptr->encoding == OBJ_ENCODING_EMBSTR)

/* Synchronous I/O with timeout */
ssize_t syncWrite(int fd, char *ptr, ssize_t size, long long timeout);
ssize_t syncRead(int fd, char *ptr, ssize_t size, long long timeout);
ssize_t syncReadLine(int fd, char *ptr, ssize_t size, long long timeout);

/* Replication */
void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc);
void replicationFeedSlavesFromMasterStream(list *slaves, char *buf, size_t buflen);
void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc);
void updateSlavesWaitingBgsave(int bgsaveerr, int type);
void replicationCron(void);
void replicationStartPendingFork(void);
void replicationHandleMasterDisconnection(void);
void replicationCacheMaster(client *c);
void resizeReplicationBacklog(long long newsize);
void replicationSetMaster(char *ip, int port);
void replicationUnsetMaster(void);
void refreshGoodSlavesCount(void);
void replicationScriptCacheInit(void);
void replicationScriptCacheFlush(void);
void replicationScriptCacheAdd(sds sha1);
int replicationScriptCacheExists(sds sha1);
void processClientsWaitingReplicas(void);
void unblockClientWaitingReplicas(client *c);
int replicationCountAcksByOffset(long long offset);
void replicationSendNewlineToMaster(void);
long long replicationGetSlaveOffset(void);
char *replicationGetSlaveName(client *c);
long long getPsyncInitialOffset(void);
int replicationSetupSlaveForFullResync(client *slave, long long offset);
void changeReplicationId(void);
void clearReplicationId2(void);
void chopReplicationBacklog(void);
void replicationCacheMasterUsingMyself(void);
void feedReplicationBacklog(void *ptr, size_t len);
void showLatestBacklog(void);
void rdbPipeReadHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
void rdbPipeWriteHandlerConnRemoved(struct connection *conn);
void clearFailoverState(void);
void updateFailoverStatus(void);
void abortFailover(const char *err);
const char *getFailoverStateString();

/* Generic persistence functions */
void startLoadingFile(FILE *fp, char *filename, int rdbflags);
void startLoading(size_t size, int rdbflags);
void loadingProgress(off_t pos);
void stopLoading(int success);
void startSaving(int rdbflags);
void stopSaving(int success);
int allPersistenceDisabled(void);

#define DISK_ERROR_TYPE_AOF 1  /* Don't accept writes: AOF errors. */
#define DISK_ERROR_TYPE_RDB 2  /* Don't accept writes: RDB errors. */
#define DISK_ERROR_TYPE_NONE 0 /* No problems, we can accept writes. */
int writeCommandsDeniedByDiskError(void);

/* RDB persistence */
#include "rdb.h"
void killRDBChild(void);
int bg_unlink(const char *filename);

/* AOF persistence */
void flushAppendOnlyFile(int force);
void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc);
void aofRemoveTempFile(pid_t childpid);
int rewriteAppendOnlyFileBackground(void);
int loadAppendOnlyFile(char *filename);
void stopAppendOnly(void);
int startAppendOnly(void);
void backgroundRewriteDoneHandler(int exitcode, int bysignal);
void aofRewriteBufferReset(void);
unsigned long aofRewriteBufferSize(void);
ssize_t aofReadDiffFromParent(void);
void killAppendOnlyChild(void);
void restartAOFAfterSYNC();

/* 子进程信息相关 */ /* Child info */
void openChildInfoPipe(void);
void closeChildInfoPipe(void);
void sendChildInfoGeneric(childInfoType info_type, size_t keys, double progress, char *pname);
void sendChildCowInfo(childInfoType info_type, char *pname);
void sendChildInfo(childInfoType info_type, size_t keys, char *pname);
void receiveChildInfo(void);

/* fork 相关辅助函数 */ /* Fork helpers */
int redisFork(int type);
int hasActiveChildProcess();
void resetChildState();
int isMutuallyExclusiveChildType(int type);

/* acl.c -- 认证相关的函数声明 */ /* acl.c -- Authentication related prototypes. */
extern rax *Users;
extern user *DefaultUser;
void ACLInit(void);
/* ACLCheckAllPerm() 的返回值 */ /* Return values for ACLCheckAllPerm(). */
#define ACL_OK 0
#define ACL_DENIED_CMD 1
#define ACL_DENIED_KEY 2
#define ACL_DENIED_AUTH 3    /* 仅用于 ACL 日志条目 */ /* Only used for ACL LOG entries. */
#define ACL_DENIED_CHANNEL 4 /* 仅用于 pub/sub 命令 */ /* Only used for pub/sub commands */
int ACLCheckUserCredentials(robj *username, robj *password);
int ACLAuthenticateUser(client *c, robj *username, robj *password);
unsigned long ACLGetCommandID(const char *cmdname);
void ACLClearCommandID(void);
user *ACLGetUserByName(const char *name, size_t namelen);
int ACLCheckAllPerm(client *c, int *idxptr);
int ACLSetUser(user *u, const char *op, ssize_t oplen);
sds ACLDefaultUserFirstPassword(void);
uint64_t ACLGetCommandCategoryFlagByName(const char *name);
int ACLAppendUserForLoading(sds *argv, int argc, int *argc_err);
const char *ACLSetUserStringError(void);
int ACLLoadConfiguredUsers(void);
sds ACLDescribeUser(user *u);
void ACLLoadUsersAtStartup(void);
void addReplyCommandCategories(client *c, struct redisCommand *cmd);
user *ACLCreateUnlinkedUser();
void ACLFreeUserAndKillClients(user *u);
void addACLLogEntry(client *c, int reason, int keypos, sds username);
void ACLUpdateDefaultUserPassword(sds password);

/* 有序集合数据类型 */ /* Sorted sets data type */

/* 输入标志 */ /* Input flags. */
#define ZADD_IN_NONE 0
#define ZADD_IN_INCR (1 << 0) /* 增加分数而不是设置分数 */ /* Increment the score instead of setting it. */
#define ZADD_IN_NX (1 << 1)   /* 只处理已存在的元素 */ /* Don't touch elements not already existing. */
#define ZADD_IN_XX (1 << 2)   /* 只处理已存在的元素 */ /* Only touch elements already existing. */
#define ZADD_IN_GT (1 << 3)   /* 只在新分数更高时更新已存在元素 */ /* Only update existing when new scores are higher. */
#define ZADD_IN_LT (1 << 4)   /* 只在新分数更低时更新已存在元素 */ /* Only update existing when new scores are lower. */

/* 输出标志 */ /* Output flags. */
#define ZADD_OUT_NOP (1 << 0)     /* 因条件未满足未执行操作 */ /* Operation not performed because of conditionals.*/
#define ZADD_OUT_NAN (1 << 1)     /* 只处理已存在的元素 */ /* Only touch elements already existing. */
#define ZADD_OUT_ADDED (1 << 2)   /* 元素是新加的 */ /* The element was new and was added. */
#define ZADD_OUT_UPDATED (1 << 3) /* 元素已存在，分数被更新 */ /* The element already existed, score updated. */

/* Struct to hold an inclusive/exclusive range spec by score comparison. */
/* 用于分数比较的包含/排除范围结构体 */
typedef struct
{
    double min, max;
    int minex, maxex;  /* min 或 max 是否为排除（exclusive） */ /* are min or max exclusive? */
} zrangespec;

/* 用于字典序比较的包含/排除范围结构体 */ /* Struct to hold an inclusive/exclusive range spec by lexicographic comparison. */
typedef struct
{
    sds min, max;     /* 可以设置为 shared.(minstring|maxstring) */ /* May be set to shared.(minstring|maxstring) */
    int minex, maxex; /* min 或 max 是否为排除（exclusive） */  /* are min or max exclusive? */
} zlexrangespec;

zskiplist *zslCreate(void);
void zslFree(zskiplist *zsl);
zskiplistNode *zslInsert(zskiplist *zsl, double score, sds ele);
unsigned char *zzlInsert(unsigned char *zl, sds ele, double score);
int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node);
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range);
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range);
double zzlGetScore(unsigned char *sptr);
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range);
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range);
unsigned long zsetLength(const robj *zobj);
void zsetConvert(robj *zobj, int encoding);
void zsetConvertToZiplistIfNeeded(robj *zobj, size_t maxelelen, size_t totelelen);
int zsetScore(robj *zobj, sds member, double *score);
unsigned long zslGetRank(zskiplist *zsl, double score, sds o);
int zsetAdd(robj *zobj, double score, sds ele, int in_flags, int *out_flags, double *newscore);
long zsetRank(robj *zobj, sds ele, int reverse);
int zsetDel(robj *zobj, sds ele);
robj *zsetDup(robj *o);
int zsetZiplistValidateIntegrity(unsigned char *zl, size_t size, int deep);
void genericZpopCommand(client *c, robj **keyv, int keyc, int where, int emitkey, robj *countarg);
sds ziplistGetObject(unsigned char *sptr);
int zslValueGteMin(double value, zrangespec *spec);
int zslValueLteMax(double value, zrangespec *spec);
void zslFreeLexRange(zlexrangespec *spec);
int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec);
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range);
unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range);
zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range);
zskiplistNode *zslLastInLexRange(zskiplist *zsl, zlexrangespec *range);
int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec);
int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec);
int zslLexValueGteMin(sds value, zlexrangespec *spec);
int zslLexValueLteMax(sds value, zlexrangespec *spec);

/* Core functions */
int getMaxmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level);
size_t freeMemoryGetNotCountedMemory();
int overMaxmemoryAfterAlloc(size_t moremem);
int processCommand(client *c);
int processPendingCommandsAndResetClient(client *c);
void setupSignalHandlers(void);
void removeSignalHandlers(void);
int createSocketAcceptHandler(socketFds *sfd, aeFileProc *accept_handler);
int changeListenPort(int port, socketFds *sfd, aeFileProc *accept_handler);
int changeBindAddr(sds *addrlist, int addrlist_len);
struct redisCommand *lookupCommand(sds name);
struct redisCommand *lookupCommandByCString(const char *s);
struct redisCommand *lookupCommandOrOriginal(sds name);
void call(client *c, int flags);
void propagate(struct redisCommand *cmd, int dbid, robj **argv, int argc, int flags);
void alsoPropagate(struct redisCommand *cmd, int dbid, robj **argv, int argc, int target);
void redisOpArrayInit(redisOpArray *oa);
void redisOpArrayFree(redisOpArray *oa);
void forceCommandPropagation(client *c, int flags);
void preventCommandPropagation(client *c);
void preventCommandAOF(client *c);
void preventCommandReplication(client *c);
void slowlogPushCurrentCommand(client *c, struct redisCommand *cmd, ustime_t duration);
int prepareForShutdown(int flags);
void afterCommand(client *c);
int inNestedCall(void);
#ifdef __GNUC__
void _serverLog(int level, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
#else
void _serverLog(int level, const char *fmt, ...);
#endif
void serverLogRaw(int level, const char *msg);
void serverLogFromHandler(int level, const char *msg);
void usage(void);
void updateDictResizePolicy(void);
int htNeedsResize(dict *dict);
void populateCommandTable(void);
void resetCommandTableStats(void);
void resetErrorTableStats(void);
void adjustOpenFilesLimit(void);
void incrementErrorCount(const char *fullerr, size_t namelen);
void closeListeningSockets(int unlink_unix_socket);
void updateCachedTime(int update_daylight_info);
void resetServerStats(void);
void activeDefragCycle(void);
unsigned int getLRUClock(void);
unsigned int LRU_CLOCK(void);
const char *evictPolicyToString(void);
struct redisMemOverhead *getMemoryOverheadData(void);
void freeMemoryOverheadData(struct redisMemOverhead *mh);
void checkChildrenDone(void);
int setOOMScoreAdj(int process_class);
void rejectCommandFormat(client *c, const char *fmt, ...);
void *activeDefragAlloc(void *ptr);
robj *activeDefragStringOb(robj *ob, long *defragged);

#define RESTART_SERVER_NONE 0
#define RESTART_SERVER_GRACEFULLY (1 << 0)     /* Do proper shutdown. */
#define RESTART_SERVER_CONFIG_REWRITE (1 << 1) /* CONFIG REWRITE before restart.*/
int restartServer(int flags, mstime_t delay);

/* Set data type */
robj *setTypeCreate(sds value);
int setTypeAdd(robj *subject, sds value);
int setTypeRemove(robj *subject, sds value);
int setTypeIsMember(robj *subject, sds value);
setTypeIterator *setTypeInitIterator(robj *subject);
void setTypeReleaseIterator(setTypeIterator *si);
int setTypeNext(setTypeIterator *si, sds *sdsele, int64_t *llele);
sds setTypeNextObject(setTypeIterator *si);
int setTypeRandomElement(robj *setobj, sds *sdsele, int64_t *llele);
unsigned long setTypeRandomElements(robj *set, unsigned long count, robj *aux_set);
unsigned long setTypeSize(const robj *subject);
void setTypeConvert(robj *subject, int enc);
robj *setTypeDup(robj *o);

/* Hash data type */
#define HASH_SET_TAKE_FIELD (1 << 0)
#define HASH_SET_TAKE_VALUE (1 << 1)
#define HASH_SET_COPY 0

void hashTypeConvert(robj *o, int enc);
void hashTypeTryConversion(robj *subject, robj **argv, int start, int end);
int hashTypeExists(robj *o, sds key);
int hashTypeDelete(robj *o, sds key);
unsigned long hashTypeLength(const robj *o);
hashTypeIterator *hashTypeInitIterator(robj *subject);
void hashTypeReleaseIterator(hashTypeIterator *hi);
int hashTypeNext(hashTypeIterator *hi);
void hashTypeCurrentFromZiplist(
    hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll);
sds hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what);
void hashTypeCurrentObject(hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll);
sds hashTypeCurrentObjectNewSds(hashTypeIterator *hi, int what);
robj *hashTypeLookupWriteOrCreate(client *c, robj *key);
robj *hashTypeGetValueObject(robj *o, sds field);
int hashTypeSet(robj *o, sds field, sds value, int flags);
robj *hashTypeDup(robj *o);
int hashZiplistValidateIntegrity(unsigned char *zl, size_t size, int deep);

/* Pub / Sub */
int pubsubUnsubscribeAllChannels(client *c, int notify);
int pubsubUnsubscribeAllPatterns(client *c, int notify);
int pubsubPublishMessage(robj *channel, robj *message);
void addReplyPubsubMessage(client *c, robj *channel, robj *msg);

/* Keyspace events notification */
void notifyKeyspaceEvent(int type, char *event, robj *key, int dbid);
int keyspaceEventsStringToFlags(char *classes);
sds keyspaceEventsFlagsToString(int flags);

/* Configuration */
void loadServerConfig(char *filename, char config_from_stdin, char *options);
void appendServerSaveParams(time_t seconds, int changes);
void resetServerSaveParams(void);
struct rewriteConfigState; /* Forward declaration to export API. */
void rewriteConfigRewriteLine(struct rewriteConfigState *state, const char *option, sds line, int force);
void rewriteConfigMarkAsProcessed(struct rewriteConfigState *state, const char *option);
int rewriteConfig(char *path, int force_all);
void initConfigValues();

/* db.c -- Keyspace access API */
int removeExpire(redisDb *db, robj *key);
void deleteExpiredKeyAndPropagate(redisDb *db, robj *keyobj);
void propagateExpire(redisDb *db, robj *key, int lazy);
int keyIsExpired(redisDb *db, robj *key);
int expireIfNeeded(redisDb *db, robj *key);
long long getExpire(redisDb *db, robj *key);
void setExpire(client *c, redisDb *db, robj *key, long long when);
int checkAlreadyExpired(long long when);
robj *lookupKey(redisDb *db, robj *key, int flags);
robj *lookupKeyRead(redisDb *db, robj *key);
robj *lookupKeyWrite(redisDb *db, robj *key);
robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply);
robj *lookupKeyWriteOrReply(client *c, robj *key, robj *reply);
robj *lookupKeyReadWithFlags(redisDb *db, robj *key, int flags);
robj *lookupKeyWriteWithFlags(redisDb *db, robj *key, int flags);
robj *objectCommandLookup(client *c, robj *key);
robj *objectCommandLookupOrReply(client *c, robj *key, robj *reply);
void SentReplyOnKeyMiss(client *c, robj *reply);
int objectSetLRUOrLFU(robj *val, long long lfu_freq, long long lru_idle, long long lru_clock, int lru_multiplier);
#define LOOKUP_NONE 0
#define LOOKUP_NOTOUCH (1 << 0)
#define LOOKUP_NONOTIFY (1 << 1)
void dbAdd(redisDb *db, robj *key, robj *val);
int dbAddRDBLoad(redisDb *db, sds key, robj *val);
void dbOverwrite(redisDb *db, robj *key, robj *val);
void genericSetKey(client *c, redisDb *db, robj *key, robj *val, int keepttl, int signal);
void setKey(client *c, redisDb *db, robj *key, robj *val);
robj *dbRandomKey(redisDb *db);
int dbSyncDelete(redisDb *db, robj *key);
int dbDelete(redisDb *db, robj *key);
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o);

#define EMPTYDB_NO_FLAGS 0     /* No flags. */
#define EMPTYDB_ASYNC (1 << 0) /* Reclaim memory in another thread. */
long long emptyDb(int dbnum, int flags, void(callback)(void *));
long long emptyDbStructure(redisDb *dbarray, int dbnum, int async, void(callback)(void *));
void flushAllDataAndResetRDB(int flags);
long long dbTotalServerKeyCount();
dbBackup *backupDb(void);
void restoreDbBackup(dbBackup *buckup);
void discardDbBackup(dbBackup *buckup, int flags, void(callback)(void *));

int selectDb(client *c, int id);
void signalModifiedKey(client *c, redisDb *db, robj *key);
void signalFlushedDb(int dbid, int async);
unsigned int getKeysInSlot(unsigned int hashslot, robj **keys, unsigned int count);
unsigned int countKeysInSlot(unsigned int hashslot);
unsigned int delKeysInSlot(unsigned int hashslot);
int verifyClusterConfigWithData(void);
void scanGenericCommand(client *c, robj *o, unsigned long cursor);
int parseScanCursorOrReply(client *c, robj *o, unsigned long *cursor);
void slotToKeyAdd(sds key);
void slotToKeyDel(sds key);
int dbAsyncDelete(redisDb *db, robj *key);
void emptyDbAsync(redisDb *db);
void slotToKeyFlush(int async);
size_t lazyfreeGetPendingObjectsCount(void);
size_t lazyfreeGetFreedObjectsCount(void);
void freeObjAsync(robj *key, robj *obj);
void freeSlotsToKeysMapAsync(rax *rt);
void freeSlotsToKeysMap(rax *rt, int async);

/* API to get key arguments from commands */
int *getKeysPrepareResult(getKeysResult *result, int numkeys);
int getKeysFromCommand(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
void getKeysFreeResult(getKeysResult *result);
int zunionInterDiffGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int zunionInterDiffStoreGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int evalGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int sortGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int migrateGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int georadiusGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int xreadGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int memoryGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int lcsGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);

/* Cluster */
void clusterInit(void);
unsigned short crc16(const char *buf, int len);
unsigned int keyHashSlot(char *key, int keylen);
void clusterCron(void);
void clusterPropagatePublish(robj *channel, robj *message);
void migrateCloseTimedoutSockets(void);
void clusterBeforeSleep(void);
int clusterSendModuleMessageToTarget(
    const char *target, uint64_t module_id, uint8_t type, unsigned char *payload, uint32_t len);

/* Sentinel */
void initSentinelConfig(void);
void initSentinel(void);
void sentinelTimer(void);
const char *sentinelHandleConfiguration(char **argv, int argc);
void queueSentinelConfig(sds *argv, int argc, int linenum, sds line);
void loadSentinelConfigFromQueue(void);
void sentinelIsRunning(void);
void sentinelCheckConfigFile(void);

/* redis-check-rdb & aof */
int redis_check_rdb(char *rdbfilename, FILE *fp);
int redis_check_rdb_main(int argc, char **argv, FILE *fp);
int redis_check_aof_main(int argc, char **argv);

/* Scripting */
void scriptingInit(int setup);
int ldbRemoveChild(pid_t pid);
void ldbKillForkedSessions(void);
int ldbPendingChildren(void);
sds luaCreateFunction(client *c, lua_State *lua, robj *body);
void freeLuaScriptsAsync(dict *lua_scripts);

/* Blocked clients */
void processUnblockedClients(void);
void blockClient(client *c, int btype);
void unblockClient(client *c);
void queueClientForReprocessing(client *c);
void replyToBlockedClientTimedOut(client *c);
int getTimeoutFromObjectOrReply(client *c, robj *object, mstime_t *timeout, int unit);
void disconnectAllBlockedClients(void);
void handleClientsBlockedOnKeys(void);
void signalKeyAsReady(redisDb *db, robj *key, int type);
void blockForKeys(client *c,
                  int btype,
                  robj **keys,
                  int numkeys,
                  mstime_t timeout,
                  robj *target,
                  struct listPos *listpos,
                  streamID *ids);
void updateStatsOnUnblock(client *c, long blocked_us, long reply_us);

/* timeout.c -- Blocked clients timeout and connections timeout. */
void addClientToTimeoutTable(client *c);
void removeClientFromTimeoutTable(client *c);
void handleBlockedClientsTimeout(void);
int clientsCronHandleTimeout(client *c, mstime_t now_ms);

/* expire.c -- Handling of expired keys */
void activeExpireCycle(int type);
void expireSlaveKeys(void);
void rememberSlaveKeyWithExpire(redisDb *db, robj *key);
void flushSlaveKeysWithExpireList(void);
size_t getSlaveKeyWithExpireCount(void);

/* evict.c -- maxmemory handling and LRU eviction. */
void evictionPoolAlloc(void);
#define LFU_INIT_VAL 5
unsigned long LFUGetTimeInMinutes(void);
uint8_t LFULogIncr(uint8_t value);
unsigned long LFUDecrAndReturn(robj *o);
#define EVICT_OK 0
#define EVICT_RUNNING 1
#define EVICT_FAIL 2
int performEvictions(void);

/* Keys hashing / comparison functions for dict.c hash tables. */
uint64_t dictSdsHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);
void dictSdsDestructor(void *privdata, void *val);

/* Git SHA1 */
char *redisGitSHA1(void);
char *redisGitDirty(void);
uint64_t redisBuildId(void);
char *redisBuildIdString(void);

/* 命令原型 *//* Commands prototypes */
void authCommand(client *c);
void pingCommand(client *c);
void echoCommand(client *c);
void commandCommand(client *c);
void setCommand(client *c);
void setnxCommand(client *c);
void setexCommand(client *c);
void psetexCommand(client *c);
void getCommand(client *c);
void getexCommand(client *c);
void getdelCommand(client *c);
void delCommand(client *c);
void unlinkCommand(client *c);
void existsCommand(client *c);
void setbitCommand(client *c);
void getbitCommand(client *c);
void bitfieldCommand(client *c);
void bitfieldroCommand(client *c);
void setrangeCommand(client *c);
void getrangeCommand(client *c);
void incrCommand(client *c);
void decrCommand(client *c);
void incrbyCommand(client *c);
void decrbyCommand(client *c);
void incrbyfloatCommand(client *c);
void selectCommand(client *c);
void swapdbCommand(client *c);
void randomkeyCommand(client *c);
void keysCommand(client *c);
void scanCommand(client *c);
void dbsizeCommand(client *c);
void lastsaveCommand(client *c);
void saveCommand(client *c);
void bgsaveCommand(client *c);
void bgrewriteaofCommand(client *c);
void shutdownCommand(client *c);
void moveCommand(client *c);
void copyCommand(client *c);
void renameCommand(client *c);
void renamenxCommand(client *c);
void lpushCommand(client *c);
void rpushCommand(client *c);
void lpushxCommand(client *c);
void rpushxCommand(client *c);
void linsertCommand(client *c);
void lpopCommand(client *c);
void rpopCommand(client *c);
void llenCommand(client *c);
void lindexCommand(client *c);
void lrangeCommand(client *c);
void ltrimCommand(client *c);
void typeCommand(client *c);
void lsetCommand(client *c);
void saddCommand(client *c);
void sremCommand(client *c);
void smoveCommand(client *c);
void sismemberCommand(client *c);
void smismemberCommand(client *c);
void scardCommand(client *c);
void spopCommand(client *c);
void srandmemberCommand(client *c);
void sinterCommand(client *c);
void sinterstoreCommand(client *c);
void sunionCommand(client *c);
void sunionstoreCommand(client *c);
void sdiffCommand(client *c);
void sdiffstoreCommand(client *c);
void sscanCommand(client *c);
void syncCommand(client *c);
void flushdbCommand(client *c);
void flushallCommand(client *c);
void sortCommand(client *c);
void lremCommand(client *c);
void lposCommand(client *c);
void rpoplpushCommand(client *c);
void lmoveCommand(client *c);
void infoCommand(client *c);
void mgetCommand(client *c);
void monitorCommand(client *c);
void expireCommand(client *c);
void expireatCommand(client *c);
void pexpireCommand(client *c);
void pexpireatCommand(client *c);
void getsetCommand(client *c);
void ttlCommand(client *c);
void touchCommand(client *c);
void pttlCommand(client *c);
void persistCommand(client *c);
void replicaofCommand(client *c);
void roleCommand(client *c);
void debugCommand(client *c);
void msetCommand(client *c);
void msetnxCommand(client *c);
void zaddCommand(client *c);
void zincrbyCommand(client *c);
void zrangeCommand(client *c);
void zrangebyscoreCommand(client *c);
void zrevrangebyscoreCommand(client *c);
void zrangebylexCommand(client *c);
void zrevrangebylexCommand(client *c);
void zcountCommand(client *c);
void zlexcountCommand(client *c);
void zrevrangeCommand(client *c);
void zcardCommand(client *c);
void zremCommand(client *c);
void zscoreCommand(client *c);
void zmscoreCommand(client *c);
void zremrangebyscoreCommand(client *c);
void zremrangebylexCommand(client *c);
void zpopminCommand(client *c);
void zpopmaxCommand(client *c);
void bzpopminCommand(client *c);
void bzpopmaxCommand(client *c);
void zrandmemberCommand(client *c);
void multiCommand(client *c);
void execCommand(client *c);
void discardCommand(client *c);
void blpopCommand(client *c);
void brpopCommand(client *c);
void brpoplpushCommand(client *c);
void blmoveCommand(client *c);
void appendCommand(client *c);
void strlenCommand(client *c);
void zrankCommand(client *c);
void zrevrankCommand(client *c);
void hsetCommand(client *c);
void hsetnxCommand(client *c);
void hgetCommand(client *c);
void hmsetCommand(client *c);
void hmgetCommand(client *c);
void hdelCommand(client *c);
void hlenCommand(client *c);
void hstrlenCommand(client *c);
void zremrangebyrankCommand(client *c);
void zunionstoreCommand(client *c);
void zinterstoreCommand(client *c);
void zdiffstoreCommand(client *c);
void zunionCommand(client *c);
void zinterCommand(client *c);
void zrangestoreCommand(client *c);
void zdiffCommand(client *c);
void zscanCommand(client *c);
void hkeysCommand(client *c);
void hvalsCommand(client *c);
void hgetallCommand(client *c);
void hexistsCommand(client *c);
void hscanCommand(client *c);
void hrandfieldCommand(client *c);
void configCommand(client *c);
void hincrbyCommand(client *c);
void hincrbyfloatCommand(client *c);
void subscribeCommand(client *c);
void unsubscribeCommand(client *c);
void psubscribeCommand(client *c);
void punsubscribeCommand(client *c);
void publishCommand(client *c);
void pubsubCommand(client *c);
void watchCommand(client *c);
void unwatchCommand(client *c);
void clusterCommand(client *c);
void restoreCommand(client *c);
void migrateCommand(client *c);
void askingCommand(client *c);
void readonlyCommand(client *c);
void readwriteCommand(client *c);
void dumpCommand(client *c);
void objectCommand(client *c);
void memoryCommand(client *c);
void clientCommand(client *c);
void helloCommand(client *c);
void evalCommand(client *c);
void evalShaCommand(client *c);
void scriptCommand(client *c);
void timeCommand(client *c);
void bitopCommand(client *c);
void bitcountCommand(client *c);
void bitposCommand(client *c);
void replconfCommand(client *c);
void waitCommand(client *c);
void geoencodeCommand(client *c);
void geodecodeCommand(client *c);
void georadiusbymemberCommand(client *c);
void georadiusbymemberroCommand(client *c);
void georadiusCommand(client *c);
void georadiusroCommand(client *c);
void geoaddCommand(client *c);
void geohashCommand(client *c);
void geoposCommand(client *c);
void geodistCommand(client *c);
void geosearchCommand(client *c);
void geosearchstoreCommand(client *c);
void pfselftestCommand(client *c);
void pfaddCommand(client *c);
void pfcountCommand(client *c);
void pfmergeCommand(client *c);
void pfdebugCommand(client *c);
void latencyCommand(client *c);
void moduleCommand(client *c);
void securityWarningCommand(client *c);
void xaddCommand(client *c);
void xrangeCommand(client *c);
void xrevrangeCommand(client *c);
void xlenCommand(client *c);
void xreadCommand(client *c);
void xgroupCommand(client *c);
void xsetidCommand(client *c);
void xackCommand(client *c);
void xpendingCommand(client *c);
void xclaimCommand(client *c);
void xautoclaimCommand(client *c);
void xinfoCommand(client *c);
void xdelCommand(client *c);
void xtrimCommand(client *c);
void lolwutCommand(client *c);
void aclCommand(client *c);
void stralgoCommand(client *c);
void resetCommand(client *c);
void failoverCommand(client *c);

#if defined(__GNUC__)
void *calloc(size_t count, size_t size) __attribute__((deprecated));
void free(void *ptr) __attribute__((deprecated));
void *malloc(size_t size) __attribute__((deprecated));
void *realloc(void *ptr, size_t size) __attribute__((deprecated));
#endif

/* Debugging stuff */
void _serverAssertWithInfo(const client *c, const robj *o, const char *estr, const char *file, int line);
void _serverAssert(const char *estr, const char *file, int line);
#ifdef __GNUC__
void _serverPanic(const char *file, int line, const char *msg, ...) __attribute__((format(printf, 3, 4)));
#else
void _serverPanic(const char *file, int line, const char *msg, ...);
#endif
void serverLogObjectDebugInfo(const robj *o);
void sigsegvHandler(int sig, siginfo_t *info, void *secret);
const char *getSafeInfoString(const char *s, size_t len, char **tmp);
sds genRedisInfoString(const char *section);
sds genModulesInfoString(sds info);
void enableWatchdog(int period);
void disableWatchdog(void);
void watchdogScheduleSignal(int period);
void serverLogHexDump(int level, char *descr, void *value, size_t len);
int memtest_preserving_test(unsigned long *m, size_t bytes, int passes);
void mixDigest(unsigned char *digest, void *ptr, size_t len);
void xorDigest(unsigned char *digest, void *ptr, size_t len);
int populateCommandTableParseFlags(struct redisCommand *c, char *strflags);
void debugDelay(int usec);
void killIOThreads(void);
void killThreads(void);
void makeThreadKillable(void);

/* Use macro for checking log level to avoid evaluating arguments in cases log
 * should be ignored due to low level. */
#define serverLog(level, ...)                                                                                          \
    do                                                                                                                 \
    {                                                                                                                  \
        if (((level) & 0xff) < server.verbosity)                                                                       \
            break;                                                                                                     \
        _serverLog(level, __VA_ARGS__);                                                                                \
    } while (0)

/* TLS stuff */
void tlsInit(void);
void tlsCleanup(void);
int tlsConfigure(redisTLSContextConfig *ctx_config);

#define redisDebug(fmt, ...) printf("DEBUG %s:%d > " fmt "\n", __FILE__, __LINE__, __VA_ARGS__)
#define redisDebugMark() printf("-- MARK %s:%d --\n", __FILE__, __LINE__)

int iAmMaster(void);

#endif
