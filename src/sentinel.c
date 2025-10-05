/* Redis 哨兵的实现 Redis Sentinel implementation
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
 */

#include "server.h"
#include "hiredis.h"
#ifdef USE_OPENSSL
#include "openssl/ssl.h"
#include "hiredis_ssl.h"
#endif
#include "async.h"

#include <ctype.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <fcntl.h>

extern char **environ;

#ifdef USE_OPENSSL
extern SSL_CTX *redis_tls_ctx;
extern SSL_CTX *redis_tls_client_ctx;
#endif

#define REDIS_SENTINEL_PORT 26379

/* ======================== 哨兵的全局状态 Sentinel global state =========================== */

/* Address object, used to describe an ip:port pair. */
/* 地址对象，用于描述一个 ip:port 对。 */
typedef struct sentinelAddr {
    char *hostname;         /* 主机名或地址（根据指定） */ /* Hostname OR address, as specified */
    char *ip;               /* 始终是解析后的地址 */ /* Always a resolved address */
    int port;
} sentinelAddr;

/* 一个哨兵正在监控的 Redis 实例对象。 */ /* A Sentinel Redis Instance object is monitoring. */
#define SRI_MASTER  (1<<0)
#define SRI_SLAVE   (1<<1)
#define SRI_SENTINEL (1<<2)
#define SRI_S_DOWN (1<<3)   /* 主观下线（无法达成法定人数）。 */ /* Subjectively down (no quorum). */
#define SRI_O_DOWN (1<<4)   /* 客观下线（已被其他哨兵确认）。 */ /* Objectively down (confirmed by others). */
#define SRI_MASTER_DOWN (1<<5) /* 设置了此标志的哨兵认为其主节点已下线。 */ /* A Sentinel with this flag set thinks that
                                   its master is down. */
#define SRI_FAILOVER_IN_PROGRESS (1<<6) /* 此主节点正在进行故障转移。 */ /* Failover is in progress for
                                           this master. */
#define SRI_PROMOTED (1<<7)            /* 被选为提升的从节点。 */ /* Slave selected for promotion. */
#define SRI_RECONF_SENT (1<<8)       /* 已发送 SLAVEOF <newmaster> 命令。 */ /* SLAVEOF <newmaster> sent. */
#define SRI_RECONF_INPROG (1<<9)     /* 从节点同步正在进行中。 */ /* Slave synchronization in progress. */
#define SRI_RECONF_DONE (1<<10)      /* 从节点已与新主节点同步完成。 */ /* Slave synchronized with new master. */
#define SRI_FORCE_FAILOVER (1<<11)   /* 在主节点正常时强制故障转移。 */ /* Force failover with master up. */
#define SRI_SCRIPT_KILL_SENT (1<<12) /* 已发送 SCRIPT KILL 命令以处理 -BUSY 错误。 */ /* SCRIPT KILL already sent on -BUSY */

/* 注意：时间以毫秒为单位。 */ /* Note: times are in milliseconds. */
#define SENTINEL_INFO_PERIOD 10000
#define SENTINEL_PING_PERIOD 1000
#define SENTINEL_ASK_PERIOD 1000
#define SENTINEL_PUBLISH_PERIOD 2000
#define SENTINEL_DEFAULT_DOWN_AFTER 30000
#define SENTINEL_HELLO_CHANNEL "__sentinel__:hello"
#define SENTINEL_TILT_TRIGGER 2000
#define SENTINEL_TILT_PERIOD (SENTINEL_PING_PERIOD*30)
#define SENTINEL_DEFAULT_SLAVE_PRIORITY 100
#define SENTINEL_SLAVE_RECONF_TIMEOUT 10000
#define SENTINEL_DEFAULT_PARALLEL_SYNCS 1
#define SENTINEL_MIN_LINK_RECONNECT_PERIOD 15000
#define SENTINEL_DEFAULT_FAILOVER_TIMEOUT (60*3*1000)
#define SENTINEL_MAX_PENDING_COMMANDS 100
#define SENTINEL_ELECTION_TIMEOUT 10000
#define SENTINEL_MAX_DESYNC 1000
#define SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG 1
#define SENTINEL_DEFAULT_RESOLVE_HOSTNAMES 0
#define SENTINEL_DEFAULT_ANNOUNCE_HOSTNAMES 0

/* 故障转移状态机的不同状态。 */ /* Failover machine different states. */
#define SENTINEL_FAILOVER_STATE_NONE 0         /* 没有正在进行的故障转移。 */ /* No failover in progress. */
#define SENTINEL_FAILOVER_STATE_WAIT_START 1   /* 等待 failover_start_time。 *//* Wait for failover_start_time*/
#define SENTINEL_FAILOVER_STATE_SELECT_SLAVE 2 /* 选择要提升的从节点。 */ /* Select slave to promote */
#define SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE 3 /* 从节点 -> 主节点。 */ /* Slave -> Master */
#define SENTINEL_FAILOVER_STATE_WAIT_PROMOTION 4 /* 等待从节点更改角色。 */ /* Wait slave to change role */
#define SENTINEL_FAILOVER_STATE_RECONF_SLAVES 5  /* 发送 SLAVEOF newmaster。 */ /* SLAVEOF newmaster */
#define SENTINEL_FAILOVER_STATE_UPDATE_CONFIG 6  /* 监控被提升的从节点。 *//* Monitor promoted slave. */

#define SENTINEL_MASTER_LINK_STATUS_UP 0
#define SENTINEL_MASTER_LINK_STATUS_DOWN 1

/* Generic flags that can be used with different functions.
 * They use higher bits to avoid colliding with the function specific
 * flags. */
/* 可与不同函数一起使用的通用标志。
 * 它们使用较高的位以避免与函数特定的标志冲突。 */
#define SENTINEL_NO_FLAGS 0
#define SENTINEL_GENERATE_EVENT (1<<16)
#define SENTINEL_LEADER (1<<17)
#define SENTINEL_OBSERVER (1<<18)

/* 脚本执行标志和限制。 */ /* Script execution flags and limits. */
#define SENTINEL_SCRIPT_NONE 0
#define SENTINEL_SCRIPT_RUNNING 1
#define SENTINEL_SCRIPT_MAX_QUEUE 256
#define SENTINEL_SCRIPT_MAX_RUNNING 16
#define SENTINEL_SCRIPT_MAX_RUNTIME 60000  /* 最大执行时间为 60 秒。 */ /* 60 seconds max exec time. */
#define SENTINEL_SCRIPT_MAX_RETRY 10
#define SENTINEL_SCRIPT_RETRY_DELAY 30000  /* 每次重试间隔 30 秒。 *//* 30 seconds between retries. */

/* SENTINEL SIMULATE-FAILURE 命令标志。 */ /* SENTINEL SIMULATE-FAILURE command flags. */
#define SENTINEL_SIMFAILURE_NONE 0
#define SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION (1<<0)
#define SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION (1<<1)

/* The link to a sentinelRedisInstance. When we have the same set of Sentinels
 * monitoring many masters, we have different instances representing the
 * same Sentinels, one per master, and we need to share the hiredis connections
 * among them. Otherwise if 5 Sentinels are monitoring 100 masters we create
 * 500 outgoing connections instead of 5.
 *
 * So this structure represents a reference counted link in terms of the two
 * hiredis connections for commands and Pub/Sub, and the fields needed for
 * failure detection, since the ping/pong time are now local to the link: if
 * the link is available, the instance is available. This way we don't just
 * have 5 connections instead of 500, we also send 5 pings instead of 500.
 *
 * Links are shared only for Sentinels: master and slave instances have
 * a link with refcount = 1, always. */
/* 指向 sentinelRedisInstance 的链接。当我们有一组相同的 Sentinels
 * 监控多个主节点时，我们为每个主节点创建不同的实例来表示相同的 Sentinels，
 * 并且我们需要在它们之间共享 hiredis 连接。否则，如果 5 个 Sentinels
 * 监控 100 个主节点，我们将创建 500 个外部连接，而不是 5 个。
 *
 * 因此，此结构表示一个引用计数的链接，包含用于命令和 Pub/Sub 的两个
 * hiredis 连接，以及故障检测所需的字段，因为 ping/pong 时间现在是链接本地的：
 * 如果链接可用，则实例可用。通过这种方式，我们不仅将连接数从 500 减少到 5，
 * 还将 ping 数量从 500 减少到 5。
 *
 * 链接仅为 Sentinels 共享：主节点和从节点实例的链接始终具有 refcount = 1。 */
typedef struct instanceLink {
    int refcount;          /* sentinelRedisInstance 的拥有者数量。 */ /* Number of sentinelRedisInstance owners. */
    int disconnected;      /* 如果需要重新连接 cc 或 pc，则为非零值。 */ /* Non-zero if we need to reconnect cc or pc. */
    int pending_commands;  /* 已发送但等待回复的命令数量。 */ /* Number of commands sent waiting for a reply. */
    redisAsyncContext *cc; /* 用于命令的 Hiredis 上下文。 */ /* Hiredis context for commands. */
    redisAsyncContext *pc; /* 用于 Pub/Sub 的 Hiredis 上下文。 */ /* Hiredis context for Pub / Sub. */
    mstime_t cc_conn_time; /* cc 连接的时间。 */ /* cc connection time. */
    mstime_t pc_conn_time; /* pc 连接的时间。 */ /* pc connection time. */
    mstime_t pc_last_activity;  /* 上次接收到任何消息的时间。 */ /* Last time we received any message. */
    mstime_t last_avail_time;   /* 实例上次回复 ping 且回复有效的时间。 *//* Last time the instance replied to ping with
                                 a reply we consider valid. */
    mstime_t act_ping_time;     /* 上次发送的待处理 ping（未收到 pong）的时间。
                                 * 当收到 pong 时，该字段设置为 0；
                                 * 如果值为 0 且发送了新的 ping，则再次设置为当前时间。 */ /* Time at which the last pending ping (no pong
                                 received after it) was sent. This field is
                                 set to 0 when a pong is received, and set again
                                 to the current time if the value is 0 and a new
                                 ping is sent. */
    mstime_t last_ping_time;     /* 上次发送 ping 的时间。
                                  * 仅用于在故障期间避免发送过多的 ping。
                                  * 空闲时间使用 act_ping_time 字段计算。 */ /* Time at which we sent the last ping. This is
                                 only used to avoid sending too many pings
                                 during failure. Idle time is computed using
                                 the act_ping_time field. */
    mstime_t last_pong_time;     /* 实例上次回复 ping 的时间，无论回复是什么。
                                  * 用于检查链接是否空闲以及是否需要重新连接。 */ /* Last time the instance replied to ping,
                                 whatever the reply was. That's used to check
                                 if the link is idle and must be reconnected. */
    mstime_t last_reconn_time;  /* 链接断开时上次尝试重新连接的时间。 */ /* Last reconnection attempt performed when
                                   the link was down. */
} instanceLink;

typedef struct sentinelRedisInstance {
    int flags;      /* See SRI_... defines */
    char *name;     /* 从这个哨兵的角度来看，主节点的名称。 */ /* Master name from the point of view of this sentinel. */
    char *runid;    /* 此实例的运行 ID，或者如果是 Sentinel，则为唯一 ID。 */ /* Run ID of this instance, or unique ID if is a Sentinel.*/
    uint64_t config_epoch;  /* 配置纪元。 */ /* Configuration epoch. */
    sentinelAddr *addr;     /* 主机地址。 */ /* Master host. */
    instanceLink *link;     /* 指向实例的链接，可能会被多个 Sentinels 共享。 */ /* Link to the instance, may be shared for Sentinels. */
    mstime_t last_pub_time;   /* 上次通过 Pub/Sub 发送 hello 的时间。 */ /* Last time we sent hello via Pub/Sub. */
    mstime_t last_hello_time; /* 仅在设置了 SRI_SENTINEL 时使用。
                               * 上次通过 Pub/Sub 从此 Sentinel 接收到 hello 的时间。 */ /* Only used if SRI_SENTINEL is set. Last time
                                 we received a hello from this Sentinel
                                 via Pub/Sub. */
    mstime_t last_master_down_reply_time; /* 上次回复 SENTINEL is-master-down 命令的时间。 */ /* Time of last reply to
                                             SENTINEL is-master-down command. */
    mstime_t s_down_since_time; /* 主观下线的起始时间。 */ /* Subjectively down since time. */
    mstime_t o_down_since_time; /* 客观下线的起始时间。 */ /* Objectively down since time. */
    mstime_t down_after_period; /* 在此时间段后认为实例下线。 */ /* Consider it down after that period. */
    mstime_t info_refresh;      /* 我们从实例接收到 INFO 输出的时间。 */ /* Time at which we received INFO output from it. */
    dict *renamed_commands;     /* 此实例中重命名的命令：
                                 * Sentinel 将使用映射到此表中的替代命令发送诸如 SLAVEOF、CONFIG、INFO 等命令。 */ /* Commands renamed in this instance:
                                   Sentinel will use the alternative commands
                                   mapped on this table to send things like
                                   SLAVEOF, CONFING, INFO, ... */

    /* Role and the first time we observed it.
     * This is useful in order to delay replacing what the instance reports
     * with our own configuration. We need to always wait some time in order
     * to give a chance to the leader to report the new configuration before
     * we do silly things. */
    /* 角色以及我们首次观察到它的时间。
    * 这很有用，可以延迟用我们自己的配置替换实例报告的内容。
    * 我们需要始终等待一段时间，以便给领导者报告新配置的机会，
    * 然后再执行不必要的操作。 */
    int role_reported;
    mstime_t role_reported_time;
    mstime_t slave_conf_change_time;  /* 上次从节点主地址更改的时间。 */ /* Last time slave master addr changed. */

    /* 主节点相关。 */ /* Master specific. */
    dict *sentinels;    /* 监控同一主节点的其他 Sentinels。 */ /* Other sentinels monitoring the same master. */
    dict *slaves;       /* 此主节点实例的从节点。 */ /* Slaves for this master instance. */
    unsigned int quorum;/* 需要同意故障的 Sentinels 数量。 */ /* Number of sentinels that need to agree on failure. */
    int parallel_syncs; /* 同时重新配置的从节点数量。 */ /* How many slaves to reconfigure at same time. */
    char *auth_pass;    /* 用于对主节点和副本进行 AUTH 的密码。 */ /* Password to use for AUTH against master & replica. */
    char *auth_user;    /* 用于对主节点和副本进行 ACLs AUTH 的用户名。 */ /* Username for ACLs AUTH against master & replica. */

    /* 从节点相关。 */ /* Slave specific. */
    mstime_t master_link_down_time;  /* 从节点复制链接断开的时间。 */ /* Slave replication link down time. */
    int slave_priority;     /* 根据 INFO 输出的从节点优先级。 */ /* Slave priority according to its INFO output. */
    int replica_announced;  /* 根据 INFO 输出的副本通告状态。 */ /* Replica announcing according to its INFO output. */
    mstime_t slave_reconf_sent_time; /* 我们发送 SLAVE OF <new> 的时间。 */ /* Time at which we sent SLAVE OF <new> */
    struct sentinelRedisInstance *master; /* 如果是从节点，则指向主节点实例。 */ /* Master instance if it's slave. */
    char *slave_master_host;    /* INFO 报告的主节点主机。 */ /* Master host as reported by INFO */
    int slave_master_port;      /* INFO 报告的主节点端口。 */ /* Master port as reported by INFO */
    int slave_master_link_status; /* INFO 报告的主节点链接状态。 */ /* Master link status as reported by INFO */
    unsigned long long slave_repl_offset; /* 从节点复制偏移量。 */ /* Slave replication offset. */
    /* 故障转移相关。 */ /* Failover */
    char *leader;       /* 如果这是一个主节点实例，这是应该执行故障转移的 Sentinel 的运行 ID。
                         * 如果这是一个 Sentinel，这是此 Sentinel 投票为领导者的 Sentinel 的运行 ID。 */ /* If this is a master instance, this is the runid of
                           the Sentinel that should perform the failover. If
                           this is a Sentinel, this is the runid of the Sentinel
                           that this Sentinel voted as leader. */
    uint64_t leader_epoch;   /* 'leader' 字段的纪元。 */ /* Epoch of the 'leader' field. */
    uint64_t failover_epoch; /* 当前启动的故障转移的纪元。 */ /* Epoch of the currently started failover. */
    int failover_state;      /* 请参阅 SENTINEL_FAILOVER_STATE_* 定义。 */ /* See SENTINEL_FAILOVER_STATE_* defines. */
    mstime_t failover_state_change_time;  /* 故障转移状态更改的时间。 */
    mstime_t failover_start_time;         /* 上次尝试故障转移的开始时间。 */ /* Last failover attempt start time. */
    mstime_t failover_timeout;            /* 刷新故障转移状态的最大时间。 */ /* Max time to refresh failover state. */
    mstime_t failover_delay_logged;       /* 我们记录故障转移延迟的 failover_start_time 值。 */ /* For what failover_start_time value we
                                           logged the failover delay. */
    struct sentinelRedisInstance *promoted_slave; /* 提升的从节点实例。 */  /* Promoted slave instance. */
    /* Scripts executed to notify admin or reconfigure clients: when they
     * are set to NULL no script is executed. */
    /* 用于通知管理员或重新配置客户端的脚本：当它们设置为 NULL 时，不会执行任何脚本。 */
    char *notification_script;
    char *client_reconfig_script;
    sds info;   /* 缓存的 INFO 输出 */ /* cached INFO output */
} sentinelRedisInstance;

/* 主状态。 */ /* Main state. */
struct sentinelState {
    char myid[CONFIG_RUN_ID_SIZE+1]; /* 此 Sentinel 的 ID。 */ /* This sentinel ID. */
    uint64_t current_epoch;          /* 当前纪元。 */ /* Current epoch. */
    dict *masters;      /* Dictionary of master sentinelRedisInstances.
                           Key is the instance name, value is the
                           sentinelRedisInstance structure pointer. */
                        /* 主节点 sentinelRedisInstance 的字典。
                         * 键是实例名称，值是 sentinelRedisInstance 结构指针。 */
    int tilt;           /* 是否处于 TILT 模式？ */ /* Are we in TILT mode? */
    int running_scripts;            /* 当前正在执行的脚本数量。 *//* Number of scripts in execution right now. */
    mstime_t tilt_start_time;       /* TILT 模式开始的时间。 */ /* When TITL started. */
    mstime_t previous_time;         /* 上次运行时间处理程序的时间。 */ /* Last time we ran the time handler. */
    list *scripts_queue;            /* 用户脚本的执行队列。 */ /* Queue of user scripts to execute. */
    char *announce_ip;   /* 如果不为 NULL，则向其他 Sentinels 通告的 IP 地址。 */ /* IP addr that is gossiped to other sentinels if
                           not NULL. */
    int announce_port;    /* 如果非零，则向其他 Sentinels 通告的端口。 */ /* Port that is gossiped to other sentinels if
                           non zero. */
    unsigned long simfailure_flags; /* 故障模拟标志。 */ /* Failures simulation. */
    int deny_scripts_reconfig;      /* 是否允许 SENTINEL SET ... 在运行时更改脚本路径？ */ /* Allow SENTINEL SET ... to change script
                                  paths at runtime? */
    char *sentinel_auth_pass;       /* 用于对其他 Sentinel 进行 AUTH 的密码。 */ /* Password to use for AUTH against other sentinel */
    char *sentinel_auth_user;       /* 用于对其他 Sentinel 进行 ACLs AUTH 的用户名。 *//* Username for ACLs AUTH against other sentinel. */
    int resolve_hostnames;          /* 是否支持使用主机名（假设 DNS 配置良好）。 */ /* Support use of hostnames, assuming DNS is well configured. */
    int announce_hostnames;         /* 如果有主机名，是否通告主机名而不是 IP。 */ /* Announce hostnames instead of IPs when we have them. */
} sentinel;

/* 一个脚本执行任务。 */ /* A script execution job. */
typedef struct sentinelScriptJob {
    int flags;              /* 脚本任务标志：SENTINEL_SCRIPT_* */ /* Script job flags: SENTINEL_SCRIPT_* */
    int retry_num;          /* 尝试执行脚本的次数。 */ /* Number of times we tried to execute it. */
    char **argv;            /* 调用脚本的参数。 */ /* Arguments to call the script. */
    mstime_t start_time;    /* 如果脚本正在运行，则为脚本执行时间；
                               如果我们可以随时重试执行，则为 0。
                               如果脚本未运行且不为 0，则表示：不要在指定时间之前运行。 */ /* Script execution time if the script is running,
                               otherwise 0 if we are allowed to retry the
                               execution at any time. If the script is not
                               running and it's not 0, it means: do not run
                               before the specified time. */
    pid_t pid;              /* 脚本执行的进程 ID。 */ /* Script execution pid. */
} sentinelScriptJob;

/* ======================= hiredis ae.c adapters =============================
 * Note: this implementation is taken from hiredis/adapters/ae.h, however
 * we have our modified copy for Sentinel in order to use our allocator
 * and to have full control over how the adapter works. */
/* ======================= hiredis ae.c 适配器 =============================
 * 注意：此实现取自 hiredis/adapters/ae.h，但我们为 Sentinel 制作了修改后的副本，
 * 以便使用我们自己的分配器并完全控制适配器的工作方式。 */

typedef struct redisAeEvents {
    redisAsyncContext *context;
    aeEventLoop *loop;
    int fd;
    int reading, writing;
} redisAeEvents;

static void redisAeReadEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAsyncHandleRead(e->context);
}

static void redisAeWriteEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAsyncHandleWrite(e->context);
}

static void redisAeAddRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->reading) {
        e->reading = 1;
        aeCreateFileEvent(loop,e->fd,AE_READABLE,redisAeReadEvent,e);
    }
}

static void redisAeDelRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (e->reading) {
        e->reading = 0;
        aeDeleteFileEvent(loop,e->fd,AE_READABLE);
    }
}

static void redisAeAddWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->writing) {
        e->writing = 1;
        aeCreateFileEvent(loop,e->fd,AE_WRITABLE,redisAeWriteEvent,e);
    }
}

static void redisAeDelWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (e->writing) {
        e->writing = 0;
        aeDeleteFileEvent(loop,e->fd,AE_WRITABLE);
    }
}

static void redisAeCleanup(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAeDelRead(privdata);
    redisAeDelWrite(privdata);
    zfree(e);
}

static int redisAeAttach(aeEventLoop *loop, redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    redisAeEvents *e;

    /* 当已经有事件附加时，不应再附加任何内容 */ /* Nothing should be attached when something is already attached */
    if (ac->ev.data != NULL)
        return C_ERR;

    /* 为上下文和读/写事件创建容器 */ /* Create container for context and r/w events */
    e = (redisAeEvents*)zmalloc(sizeof(*e));
    e->context = ac;
    e->loop = loop;
    e->fd = c->fd;
    e->reading = e->writing = 0;

    /* 注册函数以开始/停止监听事件 */ /* Register functions to start/stop listening for events */
    ac->ev.addRead = redisAeAddRead;
    ac->ev.delRead = redisAeDelRead;
    ac->ev.addWrite = redisAeAddWrite;
    ac->ev.delWrite = redisAeDelWrite;
    ac->ev.cleanup = redisAeCleanup;
    ac->ev.data = e;

    return C_OK;
}

/* ============================= Prototypes ================================= */

void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status);
void sentinelDisconnectCallback(const redisAsyncContext *c, int status);
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata);
sentinelRedisInstance *sentinelGetMasterByName(char *name);
char *sentinelGetSubjectiveLeader(sentinelRedisInstance *master);
char *sentinelGetObjectiveLeader(sentinelRedisInstance *master);
int yesnotoi(char *s);
void instanceLinkConnectionError(const redisAsyncContext *c);
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri);
void sentinelAbortFailover(sentinelRedisInstance *ri);
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri, const char *fmt, ...);
sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master);
void sentinelScheduleScriptExecution(char *path, ...);
void sentinelStartFailover(sentinelRedisInstance *master);
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata);
int sentinelSendSlaveOf(sentinelRedisInstance *ri, const sentinelAddr *addr);
char *sentinelVoteLeader(sentinelRedisInstance *master, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch);
void sentinelFlushConfig(void);
void sentinelGenerateInitialMonitorEvents(void);
int sentinelSendPing(sentinelRedisInstance *ri);
int sentinelForceHelloUpdateForMaster(sentinelRedisInstance *master);
sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *ip, int port, char *runid);
void sentinelSimFailureCrash(void);

/* ========================= 字典类型 Dictionary types =============================== */

uint64_t dictSdsHash(const void *key);
uint64_t dictSdsCaseHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);
int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2);
void releaseSentinelRedisInstance(sentinelRedisInstance *ri);

void dictInstancesValDestructor (void *privdata, void *obj) {
    UNUSED(privdata);
    releaseSentinelRedisInstance(obj);
}

/* Instance name (sds) -> instance (sentinelRedisInstance pointer)
 *
 * also used for: sentinelRedisInstance->sentinels dictionary that maps
 * sentinels ip:port to last seen time in Pub/Sub hello message. */
/* 实例名称 (sds) -> 实例 (sentinelRedisInstance 指针)
 *
 * 同样用于：sentinelRedisInstance->sentinels 字典，
 * 该字典将 sentinels 的 ip:port 映射到 Pub/Sub hello 消息中的最后一次看到时间。 */
dictType instancesDictType = {
    dictSdsHash,               /* 哈希函数 */ /* hash function */
    NULL,                      /* 键复制函数 */ /* key dup */
    NULL,                      /* 值复制函数 */ /* val dup */
    dictSdsKeyCompare,         /* 键比较函数 */ /* key compare */
    NULL,                      /* 键析构函数 */ /* key destructor */
    dictInstancesValDestructor,/* 值析构函数 */ /* val destructor */
    NULL                       /* 允许扩展 */ /* allow to expand */
};

/* Instance runid (sds) -> votes (long casted to void*)
 *
 * This is useful into sentinelGetObjectiveLeader() function in order to
 * count the votes and understand who is the leader. */
/* 实例运行 ID (sds) -> 投票数 (long 转换为 void*)
 *
 * 这在 sentinelGetObjectiveLeader() 函数中很有用，
 * 用于统计投票数并确定谁是领导者。 */
dictType leaderVotesDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL,                      /* val destructor */
    NULL                       /* allow to expand */
};

/* 实例重命名命令表。 */ /* Instance renamed commands table. */
dictType renamedCommandsDictType = {
    dictSdsCaseHash,           /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCaseCompare,     /* key compare */
    dictSdsDestructor,         /* key destructor */
    dictSdsDestructor,         /* val destructor */
    NULL                       /* allow to expand */
};

/* =========================== Initialization =============================== */

void sentinelCommand(client *c);
void sentinelInfoCommand(client *c);
void sentinelSetCommand(client *c);
void sentinelPublishCommand(client *c);
void sentinelRoleCommand(client *c);
void sentinelConfigGetCommand(client *c);
void sentinelConfigSetCommand(client *c);

struct redisCommand sentinelcmds[] = {
    {"ping",pingCommand,1,"fast @connection",0,NULL,0,0,0,0,0},
    {"sentinel",sentinelCommand,-2,"admin",0,NULL,0,0,0,0,0},
    {"subscribe",subscribeCommand,-2,"pub-sub",0,NULL,0,0,0,0,0},
    {"unsubscribe",unsubscribeCommand,-1,"pub-sub",0,NULL,0,0,0,0,0},
    {"psubscribe",psubscribeCommand,-2,"pub-sub",0,NULL,0,0,0,0,0},
    {"punsubscribe",punsubscribeCommand,-1,"pub-sub",0,NULL,0,0,0,0,0},
    {"publish",sentinelPublishCommand,3,"pub-sub fast",0,NULL,0,0,0,0,0},
    {"info",sentinelInfoCommand,-1,"random @dangerous",0,NULL,0,0,0,0,0},
    {"role",sentinelRoleCommand,1,"fast read-only @dangerous",0,NULL,0,0,0,0,0},
    {"client",clientCommand,-2,"admin random @connection",0,NULL,0,0,0,0,0},
    {"shutdown",shutdownCommand,-1,"admin",0,NULL,0,0,0,0,0},
    {"auth",authCommand,-2,"no-auth fast @connection",0,NULL,0,0,0,0,0},
    {"hello",helloCommand,-1,"no-auth fast @connection",0,NULL,0,0,0,0,0},
    {"acl",aclCommand,-2,"admin",0,NULL,0,0,0,0,0,0},
    {"command",commandCommand,-1, "random @connection", 0,NULL,0,0,0,0,0,0}
};

/* this array is used for sentinel config lookup, which need to be loaded
 * before monitoring masters config to avoid dependency issues */
/* 此数组用于 Sentinel 配置查找，需要在监控主节点配置之前加载，
 * 以避免依赖问题。 */
const char *preMonitorCfgName[] = { 
    "announce-ip",
    "announce-port",
    "deny-scripts-reconfig",
    "sentinel-user",
    "sentinel-pass",
    "current-epoch",
    "myid",
    "resolve-hostnames",
    "announce-hostnames"
};

/* This function overwrites a few normal Redis config default with Sentinel
 * specific defaults. */
/* 该函数会用 Sentinel 专用的默认值覆盖部分普通 Redis 配置的默认值。 */
void initSentinelConfig(void) {
    server.port = REDIS_SENTINEL_PORT;
    server.protected_mode = 0; /* Sentinel 必须对外暴露。 */ /* Sentinel must be exposed. */
}

void freeSentinelLoadQueueEntry(void *item);

/**
 * 哨兵模式，会把命令表给清理掉，只保留 SENTINEL 命令。
 */
/* 执行 Sentinel 模式的初始化。 */ /* Perform the Sentinel mode initialization. */
void initSentinel(void) {
    unsigned int j;

    /* 从命令表中移除常规 Redis 命令，只保留 SENTINEL 命令。 */
    /* Remove usual Redis commands from the command table, then just add
     * the SENTINEL command. */
    dictEmpty(server.commands,NULL);
    dictEmpty(server.orig_commands,NULL);
    ACLClearCommandID();
    for (j = 0; j < sizeof(sentinelcmds)/sizeof(sentinelcmds[0]); j++) {
        int retval;
        struct redisCommand *cmd = sentinelcmds+j;
        cmd->id = ACLGetCommandID(cmd->name);  /* 分配 ACL 用的命令 ID。 */ /* Assign the ID used for ACL. */
        retval = dictAdd(server.commands, sdsnew(cmd->name), cmd);
        serverAssert(retval == DICT_OK);
        retval = dictAdd(server.orig_commands, sdsnew(cmd->name), cmd);
        serverAssert(retval == DICT_OK);

        /* 将命令字符串标志翻译为实际的标志位。 */ /* Translate the command string flags description into an actual
         * set of flags. */
        if (populateCommandTableParseFlags(cmd,cmd->sflags) == C_ERR)
            serverPanic("Unsupported command flag");
    }

    /* 初始化各种数据结构。 */ /* Initialize various data structures. */
    sentinel.current_epoch = 0;
    sentinel.masters = dictCreate(&instancesDictType,NULL);
    sentinel.tilt = 0;
    sentinel.tilt_start_time = 0;
    sentinel.previous_time = mstime();
    sentinel.running_scripts = 0;
    sentinel.scripts_queue = listCreate();
    sentinel.announce_ip = NULL;
    sentinel.announce_port = 0;
    sentinel.simfailure_flags = SENTINEL_SIMFAILURE_NONE;
    sentinel.deny_scripts_reconfig = SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG;
    sentinel.sentinel_auth_pass = NULL;
    sentinel.sentinel_auth_user = NULL;
    sentinel.resolve_hostnames = SENTINEL_DEFAULT_RESOLVE_HOSTNAMES;
    sentinel.announce_hostnames = SENTINEL_DEFAULT_ANNOUNCE_HOSTNAMES;
    memset(sentinel.myid,0,sizeof(sentinel.myid));
    server.sentinel_config = NULL;
}

/* This function is for checking whether sentinel config file has been set,
 * also checking whether we have write permissions. */
/* 此函数用于检查是否已设置 Sentinel 配置文件，
 * 以及是否具有写权限。 */
void sentinelCheckConfigFile(void) {
    if (server.configfile == NULL) {
        serverLog(LL_WARNING,
            "Sentinel needs config file on disk to save state. Exiting...");
        exit(1);
    } else if (access(server.configfile,W_OK) == -1) {
        serverLog(LL_WARNING,
            "Sentinel config file %s is not writable: %s. Exiting...",
            server.configfile,strerror(errno));
        exit(1);
    }
}

/* This function gets called when the server is in Sentinel mode, started,
 * loaded the configuration, and is ready for normal operations. */
/* 此函数在服务器处于 Sentinel 模式、启动、加载配置并准备好正常运行时调用。 */
void sentinelIsRunning(void) {
    int j;

    /* If this Sentinel has yet no ID set in the configuration file, we
     * pick a random one and persist the config on disk. From now on this
     * will be this Sentinel ID across restarts. */
     /* 如果此 Sentinel 在配置文件中尚未设置 ID，
     * 则生成一个随机 ID 并将配置持久化到磁盘。从现在开始，
     * 该 ID 将在重启期间保持不变。 */
    for (j = 0; j < CONFIG_RUN_ID_SIZE; j++)
        if (sentinel.myid[j] != 0) break;

    if (j == CONFIG_RUN_ID_SIZE) {
        /* Pick ID and persist the config. */
        getRandomHexChars(sentinel.myid,CONFIG_RUN_ID_SIZE);
        sentinelFlushConfig();
    }

    /* Log its ID to make debugging of issues simpler. */
    /* 记录其 ID，以便更轻松地调试问题。 */
    serverLog(LL_WARNING,"Sentinel ID is %s", sentinel.myid);

    /* We want to generate a +monitor event for every configured master
     * at startup. */
    /* 我们希望在启动时为每个已配置的主节点生成一个 +monitor 事件。 */
    sentinelGenerateInitialMonitorEvents();
}

/* ============================== sentinelAddr ============================== */

/* Create a sentinelAddr object and return it on success.
 * On error NULL is returned and errno is set to:
 *  ENOENT: Can't resolve the hostname, unless accept_unresolved is non-zero.
 *  EINVAL: Invalid port number.
 */
/* 创建一个 sentinelAddr 对象并在成功时返回它。
 * 如果出错，则返回 NULL，并将 errno 设置为以下值：
 *  ENOENT: 无法解析主机名，除非 accept_unresolved 为非零。
 *  EINVAL: 无效的端口号。
 */
sentinelAddr *createSentinelAddr(char *hostname, int port, int is_accept_unresolved) {
    char ip[NET_IP_STR_LEN];
    sentinelAddr *sa;

    if (port < 0 || port > 65535) {
        errno = EINVAL;
        return NULL;
    }
    if (anetResolve(NULL,hostname,ip,sizeof(ip),
                    sentinel.resolve_hostnames ? ANET_NONE : ANET_IP_ONLY) == ANET_ERR) {
        serverLog(LL_WARNING, "Failed to resolve hostname '%s'", hostname);
        if (sentinel.resolve_hostnames && is_accept_unresolved) {
            ip[0] = '\0';
        }
        else {
            errno = ENOENT;
            return NULL;
        }
    }
    sa = zmalloc(sizeof(*sa));
    sa->hostname = sdsnew(hostname);
    sa->ip = sdsnew(ip);
    sa->port = port;
    return sa;
}

/* 返回源地址的副本。 */ /* Return a duplicate of the source address. */
sentinelAddr *dupSentinelAddr(sentinelAddr *src) {
    sentinelAddr *sa;

    sa = zmalloc(sizeof(*sa));
    sa->hostname = sdsnew(src->hostname);
    sa->ip = sdsnew(src->ip);
    sa->port = src->port;
    return sa;
}

/* 释放一个 Sentinel 地址对象。此操作不会失败。 */ /* Free a Sentinel address. Can't fail. */
void releaseSentinelAddr(sentinelAddr *sa) {
    sdsfree(sa->hostname);
    sdsfree(sa->ip);
    zfree(sa);
}

/* Return non-zero if the two addresses are equal, either by address
 * or by hostname if they could not have been resolved.
 */
/* 如果两个地址相等，则返回非零值。比较时会根据地址，
 * 或者在无法解析的情况下根据主机名进行判断。
 */
int sentinelAddrOrHostnameEqual(sentinelAddr *a, sentinelAddr *b) {
    return a->port == b->port &&
            (!strcmp(a->ip, b->ip)  ||
            !strcasecmp(a->hostname, b->hostname));
}

/* 如果主机名与地址匹配，则返回非零值。 */ /* Return non-zero if a hostname matches an address. */
int sentinelAddrEqualsHostname(sentinelAddr *a, char *hostname) {
    char ip[NET_IP_STR_LEN];

    /* 尝试解析主机名并将其与地址进行比较 */ /* Try resolve the hostname and compare it to the address */
    if (anetResolve(NULL, hostname, ip, sizeof(ip),
                    sentinel.resolve_hostnames ? ANET_NONE : ANET_IP_ONLY) == ANET_ERR) {

        /* If failed resolve then compare based on hostnames. That is our best effort as
         * long as the server is unavailable for some reason. It is fine since Redis 
         * instance cannot have multiple hostnames for a given setup */
        /* 如果解析失败，则基于主机名进行比较。这是我们在服务器由于某些原因不可用时的最佳尝试。
        * 这样做是可以的，因为在给定的设置中，Redis 实例不可能有多个主机名。 */
        return !strcasecmp(sentinel.resolve_hostnames ? a->hostname : a->ip, hostname);
    }
    /* 基于地址进行比较 */ /* Compare based on address */
    return !strcasecmp(a->ip, ip);
}

const char *announceSentinelAddr(const sentinelAddr *a) {
    return sentinel.announce_hostnames ? a->hostname : a->ip;
}

/* Return an allocated sds with hostname/address:port. IPv6
 * addresses are bracketed the same way anetFormatAddr() does.
 */
/* 返回一个指向主机名或地址的字符串指针，格式为 hostname/address:port。
 * IPv6 地址会像 anetFormatAddr() 那样用方括号括起来。
 */
sds announceSentinelAddrAndPort(const sentinelAddr *a) {
    const char *addr = announceSentinelAddr(a);
    if (strchr(addr, ':') != NULL)
        return sdscatprintf(sdsempty(), "[%s]:%d", addr, a->port);
    else
        return sdscatprintf(sdsempty(), "%s:%d", addr, a->port);
}

/* =========================== Events notification ========================== */
/* =========================== 事件通知 ========================== */

/* Send an event to log, pub/sub, user notification script.
 *
 * 'level' is the log level for logging. Only LL_WARNING events will trigger
 * the execution of the user notification script.
 *
 * 'type' is the message type, also used as a pub/sub channel name.
 *
 * 'ri', is the redis instance target of this event if applicable, and is
 * used to obtain the path of the notification script to execute.
 *
 * The remaining arguments are printf-alike.
 * If the format specifier starts with the two characters "%@" then ri is
 * not NULL, and the message is prefixed with an instance identifier in the
 * following format:
 *
 *  <instance type> <instance name> <ip> <port>
 *
 *  If the instance type is not master, than the additional string is
 *  added to specify the originating master:
 *
 *  @ <master name> <master ip> <master port>
 *
 *  Any other specifier after "%@" is processed by printf itself.
 */

/* 发送事件到日志、Pub/Sub、用户通知脚本。
 *
 * 'level' 是日志记录的日志级别。只有 LL_WARNING 级别的事件会触发
 * 用户通知脚本的执行。
 *
 * 'type' 是消息类型，同时也用作 Pub/Sub 的频道名称。
 *
 * 'ri' 是此事件的目标 Redis 实例（如果适用），
 * 并用于获取要执行的通知脚本路径。
 *
 * 剩余的参数类似于 printf。
 * 如果格式说明符以两个字符 "%@" 开头，则 ri 不为 NULL，
 * 并且消息会以以下格式的实例标识符作为前缀：
 *
 *  <实例类型> <实例名称> <IP> <端口>
 *
 *  如果实例类型不是主节点，则会添加额外的字符串以指定来源主节点：
 *
 *  @ <主节点名称> <主节点 IP> <主节点端口>
 *
 *  "%@" 之后的任何其他说明符由 printf 自行处理。
 */
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri,
                   const char *fmt, ...) {
    va_list ap;
    char msg[LOG_MAX_LEN];
    robj *channel, *payload;

    /* Handle %@ */
    if (fmt[0] == '%' && fmt[1] == '@') {
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ?
                                         NULL : ri->master;

        if (master) {
            snprintf(msg, sizeof(msg), "%s %s %s %d @ %s %s %d",
                sentinelRedisInstanceTypeStr(ri),
                ri->name, announceSentinelAddr(ri->addr), ri->addr->port,
                master->name, announceSentinelAddr(master->addr), master->addr->port);
        } else {
            snprintf(msg, sizeof(msg), "%s %s %s %d",
                sentinelRedisInstanceTypeStr(ri),
                ri->name, announceSentinelAddr(ri->addr), ri->addr->port);
        }
        fmt += 2;
    } else {
        msg[0] = '\0';
    }

    /* 如果有剩余的格式化内容，使用 vsprintf 处理。 */ /* Use vsprintf for the rest of the formatting if any. */
    if (fmt[0] != '\0') {
        va_start(ap, fmt);
        vsnprintf(msg+strlen(msg), sizeof(msg)-strlen(msg), fmt, ap);
        va_end(ap);
    }

    /* 如果日志级别允许，则记录消息。 */ /* Log the message if the log level allows it to be logged. */
    if (level >= server.verbosity)
        serverLog(level,"%s %s",type,msg);

    /* 如果消息不是调试级别，则通过 Pub/Sub 发布消息。 */ /* Publish the message via Pub/Sub if it's not a debugging one. */
    if (level != LL_DEBUG) {
        channel = createStringObject(type,strlen(type));
        payload = createStringObject(msg,strlen(msg));
        pubsubPublishMessage(channel,payload);
        decrRefCount(channel);
        decrRefCount(payload);
    }

    /* 如果适用，调用通知脚本。 */ /* Call the notification script if applicable. */
    if (level == LL_WARNING && ri != NULL) {
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ?
                                         ri : ri->master;
        if (master && master->notification_script) {
            sentinelScheduleScriptExecution(master->notification_script,
                type,msg,NULL);
        }
    }
}

/* This function is called only at startup and is used to generate a
 * +monitor event for every configured master. The same events are also
 * generated when a master to monitor is added at runtime via the
 * SENTINEL MONITOR command. */
/* 此函数仅在启动时调用，用于为每个已配置的主节点生成 +monitor 事件。
 * 当通过 SENTINEL MONITOR 命令在运行时添加要监控的主节点时，
 * 也会生成相同的事件。 */
void sentinelGenerateInitialMonitorEvents(void) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        sentinelEvent(LL_WARNING,"+monitor",ri,"%@ quorum %d",ri->quorum);
    }
    dictReleaseIterator(di);
}

/* ============================ script execution ============================ */
/* ============================ 脚本执行 ============================ */

/* Release a script job structure and all the associated data. */
/* 释放脚本任务结构及其所有关联数据。 */
void sentinelReleaseScriptJob(sentinelScriptJob *sj) {
    int j = 0;

    while(sj->argv[j]) sdsfree(sj->argv[j++]);
    zfree(sj->argv);
    zfree(sj);
}

#define SENTINEL_SCRIPT_MAX_ARGS 16
void sentinelScheduleScriptExecution(char *path, ...) {
    va_list ap;
    char *argv[SENTINEL_SCRIPT_MAX_ARGS+1];
    int argc = 1;
    sentinelScriptJob *sj;

    va_start(ap, path);
    while(argc < SENTINEL_SCRIPT_MAX_ARGS) {
        argv[argc] = va_arg(ap,char*);
        if (!argv[argc]) break;
        argv[argc] = sdsnew(argv[argc]); /* Copy the string. */
        argc++;
    }
    va_end(ap);
    argv[0] = sdsnew(path);

    sj = zmalloc(sizeof(*sj));
    sj->flags = SENTINEL_SCRIPT_NONE;
    sj->retry_num = 0;
    sj->argv = zmalloc(sizeof(char*)*(argc+1));
    sj->start_time = 0;
    sj->pid = 0;
    memcpy(sj->argv,argv,sizeof(char*)*(argc+1));

    listAddNodeTail(sentinel.scripts_queue,sj);

    /* 如果我们已经达到脚本队列的限制，则移除最旧的未运行脚本。 */ /* Remove the oldest non running script if we already hit the limit. */
    if (listLength(sentinel.scripts_queue) > SENTINEL_SCRIPT_MAX_QUEUE) {
        listNode *ln;
        listIter li;

        listRewind(sentinel.scripts_queue,&li);
        while ((ln = listNext(&li)) != NULL) {
            sj = ln->value;

            if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;
            /* The first node is the oldest as we add on tail. */
            listDelNode(sentinel.scripts_queue,ln);
            sentinelReleaseScriptJob(sj);
            break;
        }
        serverAssert(listLength(sentinel.scripts_queue) <=
                    SENTINEL_SCRIPT_MAX_QUEUE);
    }
}

/* Lookup a script in the scripts queue via pid, and returns the list node
 * (so that we can easily remove it from the queue if needed). */
/* 通过 pid 在脚本队列中查找脚本，并返回列表节点
 * （这样我们可以在需要时轻松地将其从队列中移除）。 */
listNode *sentinelGetScriptListNodeByPid(pid_t pid) {
    listNode *ln;
    listIter li;

    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        if ((sj->flags & SENTINEL_SCRIPT_RUNNING) && sj->pid == pid)
            return ln;
    }
    return NULL;
}

/* Run pending scripts if we are not already at max number of running
 * scripts. */
/* 如果当前运行的脚本数量未达到最大限制，则运行待处理脚本。 */
void sentinelRunPendingScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    /* Find jobs that are not running and run them, from the top to the
     * tail of the queue, so we run older jobs first. */
    /* 找到未运行的任务并运行它们，从队列的头部到尾部，
     * 这样我们会优先运行较旧的任务。 */
    listRewind(sentinel.scripts_queue,&li);
    while (sentinel.running_scripts < SENTINEL_SCRIPT_MAX_RUNNING &&
           (ln = listNext(&li)) != NULL)
    {
        sentinelScriptJob *sj = ln->value;
        pid_t pid;

        /* Skip if already running. */
        if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;

        /* 如果是重试任务，但未经过足够的时间，则跳过。 */ /* Skip if it's a retry, but not enough time has elapsed. */
        if (sj->start_time && sj->start_time > now) continue;

        sj->flags |= SENTINEL_SCRIPT_RUNNING;
        sj->start_time = mstime();
        sj->retry_num++;
        pid = fork();

        if (pid == -1) {
            /* Parent (fork error).
             * We report fork errors as signal 99, in order to unify the
             * reporting with other kind of errors. */
            /* 父进程（fork 出错）。
             * 我们将 fork 错误报告为信号 99，以便与其他类型的错误统一报告。 */
            sentinelEvent(LL_WARNING,"-script-error",NULL,
                          "%s %d %d", sj->argv[0], 99, 0);
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
        } else if (pid == 0) {
            /* Child */
            tlsCleanup();
            execve(sj->argv[0],sj->argv,environ);
            /* If we are here an error occurred. */
            /* 如果我们到达这里，说明发生了错误。 */
            _exit(2); /* Don't retry execution. */
        } else {
            sentinel.running_scripts++;
            sj->pid = pid;
            sentinelEvent(LL_DEBUG,"+script-child",NULL,"%ld",(long)pid);
        }
    }
}

/* How much to delay the execution of a script that we need to retry after
 * an error?
 *
 * We double the retry delay for every further retry we do. So for instance
 * if RETRY_DELAY is set to 30 seconds and the max number of retries is 10
 * starting from the second attempt to execute the script the delays are:
 * 30 sec, 60 sec, 2 min, 4 min, 8 min, 16 min, 32 min, 64 min, 128 min. */
/* 在发生错误后，我们需要重试脚本执行时，延迟执行的时间是多少？
 *
 * 每次进一步重试时，我们将重试延迟时间加倍。例如，
 * 如果 RETRY_DELAY 设置为 30 秒，最大重试次数为 10，
 * 从第二次尝试执行脚本开始，延迟时间依次为：
 * 30 秒，60 秒，2 分钟，4 分钟，8 分钟，16 分钟，32 分钟，64 分钟，128 分钟。 */
mstime_t sentinelScriptRetryDelay(int retry_num) {
    mstime_t delay = SENTINEL_SCRIPT_RETRY_DELAY;

    while (retry_num-- > 1) delay *= 2;
    return delay;
}

/* Check for scripts that terminated, and remove them from the queue if the
 * script terminated successfully. If instead the script was terminated by
 * a signal, or returned exit code "1", it is scheduled to run again if
 * the max number of retries did not already elapsed. */
/* 检查已终止的脚本，如果脚本成功终止，则将其从队列中移除。
 * 如果脚本因信号终止或返回退出代码 "1"，并且尚未达到最大重试次数，
 * 则重新调度脚本运行。 */
void sentinelCollectTerminatedScripts(void) {
    int statloc;
    pid_t pid;

    while ((pid = waitpid(-1, &statloc, WNOHANG)) > 0) {
        int exitcode = WEXITSTATUS(statloc);
        int bysignal = 0;
        listNode *ln;
        sentinelScriptJob *sj;

        if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);
        sentinelEvent(LL_DEBUG,"-script-child",NULL,"%ld %d %d",
            (long)pid, exitcode, bysignal);

        ln = sentinelGetScriptListNodeByPid(pid);
        if (ln == NULL) {
            serverLog(LL_WARNING,"waitpid() returned a pid (%ld) we can't find in our scripts execution queue!", (long)pid);
            continue;
        }
        sj = ln->value;

        /* If the script was terminated by a signal or returns an
         * exit code of "1" (that means: please retry), we reschedule it
         * if the max number of retries is not already reached. */
        /* 如果脚本因信号终止或返回退出代码 "1"（表示需要重试），
         * 并且尚未达到最大重试次数，则重新调度脚本运行。 */
        if ((bysignal || exitcode == 1) &&
            sj->retry_num != SENTINEL_SCRIPT_MAX_RETRY)
        {
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
            sj->start_time = mstime() +
                             sentinelScriptRetryDelay(sj->retry_num);
        } else {
            /* Otherwise let's remove the script, but log the event if the
             * execution did not terminated in the best of the ways. */
            /* 否则移除脚本，但如果脚本未以最佳方式终止，则记录事件日志。 */
            if (bysignal || exitcode != 0) {
                sentinelEvent(LL_WARNING,"-script-error",NULL,
                              "%s %d %d", sj->argv[0], bysignal, exitcode);
            }
            listDelNode(sentinel.scripts_queue,ln);
            sentinelReleaseScriptJob(sj);
        }
        sentinel.running_scripts--;
    }
}

/* Kill scripts in timeout, they'll be collected by the
 * sentinelCollectTerminatedScripts() function. */
/* 如果脚本因超时被杀死，它们将由 sentinelCollectTerminatedScripts() 函数回收。 */
void sentinelKillTimedoutScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        if (sj->flags & SENTINEL_SCRIPT_RUNNING &&
            (now - sj->start_time) > SENTINEL_SCRIPT_MAX_RUNTIME)
        {
            sentinelEvent(LL_WARNING,"-script-timeout",NULL,"%s %ld",
                sj->argv[0], (long)sj->pid);
            kill(sj->pid,SIGKILL);
        }
    }
}

/* Implements SENTINEL PENDING-SCRIPTS command. */
/* 实现 SENTINEL PENDING-SCRIPTS 命令。 */
void sentinelPendingScriptsCommand(client *c) {
    listNode *ln;
    listIter li;

    addReplyArrayLen(c,listLength(sentinel.scripts_queue));
    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;
        int j = 0;

        addReplyMapLen(c,5);

        addReplyBulkCString(c,"argv");
        while (sj->argv[j]) j++;
        addReplyArrayLen(c,j);
        j = 0;
        while (sj->argv[j]) addReplyBulkCString(c,sj->argv[j++]);

        addReplyBulkCString(c,"flags");
        addReplyBulkCString(c,
            (sj->flags & SENTINEL_SCRIPT_RUNNING) ? "running" : "scheduled");

        addReplyBulkCString(c,"pid");
        addReplyBulkLongLong(c,sj->pid);

        if (sj->flags & SENTINEL_SCRIPT_RUNNING) {
            addReplyBulkCString(c,"run-time");
            addReplyBulkLongLong(c,mstime() - sj->start_time);
        } else {
            mstime_t delay = sj->start_time ? (sj->start_time-mstime()) : 0;
            if (delay < 0) delay = 0;
            addReplyBulkCString(c,"run-delay");
            addReplyBulkLongLong(c,delay);
        }

        addReplyBulkCString(c,"retry-num");
        addReplyBulkLongLong(c,sj->retry_num);
    }
}

/* This function calls, if any, the client reconfiguration script with the
 * following parameters:
 *
 * <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
 *
 * It is called every time a failover is performed.
 *
 * <state> is currently always "failover".
 * <role> is either "leader" or "observer".
 *
 * from/to fields are respectively master -> promoted slave addresses for
 * "start" and "end". */
/* 此函数调用（如果有）客户端重新配置脚本，并传递以下参数：
 *
 * <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
 *
 * 每次执行故障转移时都会调用此函数。
 *
 * <state> 当前始终为 "failover"。
 * <role> 可以是 "leader" 或 "observer"。
 *
 * from/to 字段分别表示 "start" 和 "end" 的主节点 -> 提升的从节点地址。
 */
void sentinelCallClientReconfScript(sentinelRedisInstance *master, int role, char *state, sentinelAddr *from, sentinelAddr *to) {
    char fromport[32], toport[32];

    if (master->client_reconfig_script == NULL) return;
    ll2string(fromport,sizeof(fromport),from->port);
    ll2string(toport,sizeof(toport),to->port);
    sentinelScheduleScriptExecution(master->client_reconfig_script,
        master->name,
        (role == SENTINEL_LEADER) ? "leader" : "observer",
        state, announceSentinelAddr(from), fromport,
        announceSentinelAddr(to), toport, NULL);
}

/* =============================== instanceLink ============================= */
/* =============================== instanceLink ============================= */

/* 创建一个尚未连接的链接对象。 */ /* Create a not yet connected link object. */
instanceLink *createInstanceLink(void) {
    instanceLink *link = zmalloc(sizeof(*link));

    link->refcount = 1;
    link->disconnected = 1;
    link->pending_commands = 0;
    link->cc = NULL;
    link->pc = NULL;
    link->cc_conn_time = 0;
    link->pc_conn_time = 0;
    link->last_reconn_time = 0;
    link->pc_last_activity = 0;
    /* We set the act_ping_time to "now" even if we actually don't have yet
     * a connection with the node, nor we sent a ping.
     * This is useful to detect a timeout in case we'll not be able to connect
     * with the node at all. */
    /* 我们将 act_ping_time 设置为“当前时间”，即使实际上我们还没有
     * 与节点建立连接，也没有发送 ping。
     * 这样做是为了在我们完全无法连接到节点的情况下检测超时。 */
    link->act_ping_time = mstime();
    link->last_ping_time = 0;
    link->last_avail_time = mstime();
    link->last_pong_time = mstime();
    return link;
}

/* 在实例链接的上下文中断开一个 hiredis 连接。 */ /* Disconnect a hiredis connection in the context of an instance link. */
void instanceLinkCloseConnection(instanceLink *link, redisAsyncContext *c) {
    if (c == NULL) return;

    if (link->cc == c) {
        link->cc = NULL;
        link->pending_commands = 0;
    }
    if (link->pc == c) link->pc = NULL;
    c->data = NULL;
    link->disconnected = 1;
    redisAsyncFree(c);
}

/* Decrement the refcount of a link object, if it drops to zero, actually
 * free it and return NULL. Otherwise don't do anything and return the pointer
 * to the object.
 *
 * If we are not going to free the link and ri is not NULL, we rebind all the
 * pending requests in link->cc (hiredis connection for commands) to a
 * callback that will just ignore them. This is useful to avoid processing
 * replies for an instance that no longer exists. */
/* 减少链接对象的引用计数，如果引用计数降为零，则释放它并返回 NULL。
 * 否则，不执行任何操作并返回指向该对象的指针。
 *
 * 如果我们不会释放该链接且 ri 不为 NULL，我们会将 link->cc（用于命令的 hiredis 连接）
 * 中所有挂起的请求重新绑定到一个仅忽略它们的回调函数。
 * 这样可以避免处理已不存在的实例的回复。 */
instanceLink *releaseInstanceLink(instanceLink *link, sentinelRedisInstance *ri)
{
    serverAssert(link->refcount > 0);
    link->refcount--;
    if (link->refcount != 0) {
        if (ri && ri->link->cc) {
            /* This instance may have pending callbacks in the hiredis async
             * context, having as 'privdata' the instance that we are going to
             * free. Let's rewrite the callback list, directly exploiting
             * hiredis internal data structures, in order to bind them with
             * a callback that will ignore the reply at all. */
            /* 这个实例可能在 hiredis 异步上下文中有挂起的回调，
            * 它们的 'privdata' 是我们即将释放的实例。
            * 让我们直接利用 hiredis 的内部数据结构重写回调列表，
            * 以便将它们绑定到一个完全忽略回复的回调函数上。 */
            redisCallback *cb;
            redisCallbackList *callbacks = &link->cc->replies;

            cb = callbacks->head;
            while(cb) {
                if (cb->privdata == ri) {
                    cb->fn = sentinelDiscardReplyCallback;
                    cb->privdata = NULL; /* 不严格需要。 */ /* Not strictly needed. */
                }
                cb = cb->next;
            }
        }
        return link; /* 其他活跃用户。 */ /* Other active users. */
    }

    instanceLinkCloseConnection(link,link->cc);
    instanceLinkCloseConnection(link,link->pc);
    zfree(link);
    return NULL;
}

/* This function will attempt to share the instance link we already have
 * for the same Sentinel in the context of a different master, with the
 * instance we are passing as argument.
 *
 * This way multiple Sentinel objects that refer all to the same physical
 * Sentinel instance but in the context of different masters will use
 * a single connection, will send a single PING per second for failure
 * detection and so forth.
 *
 * Return C_OK if a matching Sentinel was found in the context of a
 * different master and sharing was performed. Otherwise C_ERR
 * is returned. */
/* 这个函数会尝试在不同主节点的上下文中，与我们传入的实例共享
 * 我们已经拥有的、针对同一个 Sentinel 的实例链接。
 *
 * 这样，多个 Sentinel 对象（它们都引用同一个物理 Sentinel 实例，
 * 但在不同主节点的上下文中）将使用单一连接，每秒发送一个 PING
 * 用于故障检测，等等。
 *
 * 如果在不同主节点的上下文中找到匹配的 Sentinel 并成功共享，则返回 C_OK。
 * 否则返回 C_ERR。 */
int sentinelTryConnectionSharing(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_SENTINEL);
    dictIterator *di;
    dictEntry *de;

    if (ri->runid == NULL) return C_ERR;      /* 无法识别它。 */ /* No way to identify it. */
    if (ri->link->refcount > 1) return C_ERR; /* 已经共享。 */ /* Already shared. */

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *master = dictGetVal(de), *match;
        /* We want to share with the same physical Sentinel referenced
         * in other masters, so skip our master. */
        /* 我们希望与其他主节点中引用的相同物理 Sentinel 共享，
         * 因此跳过我们的主节点。 */
        if (master == ri->master) continue;
        match = getSentinelRedisInstanceByAddrAndRunID(master->sentinels,
                                                       NULL,0,ri->runid);
        if (match == NULL) continue; /* 没有匹配项。 */ /* No match. */
        if (match == ri) continue;   /* 不应该发生，但……更安全。 */ /* Should never happen but... safer. */

        /* We identified a matching Sentinel, great! Let's free our link
         * and use the one of the matching Sentinel. */
        /* 我们找到了一个匹配的 Sentinel，很好！让我们释放当前的链接，
         * 并使用匹配 Sentinel 的链接。 */
        releaseInstanceLink(ri->link,NULL);
        ri->link = match->link;
        match->link->refcount++;
        dictReleaseIterator(di);
        return C_OK;
    }
    dictReleaseIterator(di);
    return C_ERR;
}

/* 断开与其他 Sentinel 的所有连接。返回断开连接的数量。 */ /* Drop all connections to other sentinels. Returns the number of connections
 * dropped.*/
int sentinelDropConnections(void) {
    dictIterator *di;
    dictEntry *de;
    int dropped = 0;

    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL) {
        dictIterator *sdi;
        dictEntry *sde;

        sentinelRedisInstance *ri = dictGetVal(de);
        sdi = dictGetIterator(ri->sentinels);
        while ((sde = dictNext(sdi)) != NULL) {
            sentinelRedisInstance *si = dictGetVal(sde);
            if (!si->link->disconnected) {
                instanceLinkCloseConnection(si->link, si->link->pc);
                instanceLinkCloseConnection(si->link, si->link->cc);
                dropped++;
            }
        }
        dictReleaseIterator(sdi);
    }
    dictReleaseIterator(di);

    return dropped;
}

/* When we detect a Sentinel to switch address (reporting a different IP/port
 * pair in Hello messages), let's update all the matching Sentinels in the
 * context of other masters as well and disconnect the links, so that everybody
 * will be updated.
 *
 * Return the number of updated Sentinel addresses. */
/* 当我们检测到一个 Sentinel 切换地址（在 Hello 消息中报告了不同的 IP/端口对）时，
 * 我们会更新其他主节点上下文中所有匹配的 Sentinel，并断开链接，
 * 以便所有实例都能更新。
 *
 * 返回更新的 Sentinel 地址数量。 */
int sentinelUpdateSentinelAddressInAllMasters(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_SENTINEL);
    dictIterator *di;
    dictEntry *de;
    int reconfigured = 0;

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *master = dictGetVal(de), *match;
        match = getSentinelRedisInstanceByAddrAndRunID(master->sentinels,
                                                       NULL,0,ri->runid);
        /* If there is no match, this master does not know about this
         * Sentinel, try with the next one. */
        /* 如果没有匹配项，则该主节点不知道这个 Sentinel，尝试下一个。 */
        if (match == NULL) continue;

        /* Disconnect the old links if connected. */
        /* 如果已连接，则断开旧链接。 */
        if (match->link->cc != NULL)
            instanceLinkCloseConnection(match->link,match->link->cc);
        if (match->link->pc != NULL)
            instanceLinkCloseConnection(match->link,match->link->pc);

        if (match == ri) continue; /* 地址已经为其更新。 */ /* Address already updated for it. */

        /* Update the address of the matching Sentinel by copying the address
         * of the Sentinel object that received the address update. */
        /* 通过复制接收到地址更新的 Sentinel 对象的地址，
        * 更新匹配 Sentinel 的地址。 */
        releaseSentinelAddr(match->addr);
        match->addr = dupSentinelAddr(ri->addr);
        reconfigured++;
    }
    dictReleaseIterator(di);
    if (reconfigured)
        sentinelEvent(LL_NOTICE,"+sentinel-address-update", ri,
                    "%@ %d additional matching instances", reconfigured);
    return reconfigured;
}

/* This function is called when a hiredis connection reported an error.
 * We set it to NULL and mark the link as disconnected so that it will be
 * reconnected again.
 *
 * Note: we don't free the hiredis context as hiredis will do it for us
 * for async connections. */
/* 当 hiredis 连接报告错误时调用此函数。
 * 我们将其设置为 NULL，并将链接标记为已断开连接，
 * 以便它可以重新连接。
 *
 * 注意：我们不会释放 hiredis 上下文，因为对于异步连接，
 * hiredis 会为我们处理释放。 */
void instanceLinkConnectionError(const redisAsyncContext *c) {
    instanceLink *link = c->data;
    int pubsub;

    if (!link) return;

    pubsub = (link->pc == c);
    if (pubsub)
        link->pc = NULL;
    else
        link->cc = NULL;
    link->disconnected = 1;
}

/* Hiredis connection established / disconnected callbacks. We need them
 * just to cleanup our link state. */
/* Hiredis 连接建立/断开回调。我们需要它们只是为了清理链接状态。 */
void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status) {
    if (status != C_OK) instanceLinkConnectionError(c);
}

void sentinelDisconnectCallback(const redisAsyncContext *c, int status) {
    UNUSED(status);
    instanceLinkConnectionError(c);
}

/* ========================== sentinelRedisInstance ========================= */
/* ========================== sentinelRedisInstance ========================= */

/* Create a redis instance, the following fields must be populated by the
 * caller if needed:
 * runid: set to NULL but will be populated once INFO output is received.
 * info_refresh: is set to 0 to mean that we never received INFO so far.
 *
 * If SRI_MASTER is set into initial flags the instance is added to
 * sentinel.masters table.
 *
 * if SRI_SLAVE or SRI_SENTINEL is set then 'master' must be not NULL and the
 * instance is added into master->slaves or master->sentinels table.
 *
 * If the instance is a slave or sentinel, the name parameter is ignored and
 * is created automatically as hostname:port.
 *
 * The function fails if hostname can't be resolved or port is out of range.
 * When this happens NULL is returned and errno is set accordingly to the
 * createSentinelAddr() function.
 *
 * The function may also fail and return NULL with errno set to EBUSY if
 * a master with the same name, a slave with the same address, or a sentinel
 * with the same ID already exists. */
/* 创建一个 Redis 实例，以下字段如果需要必须由调用者填充：
 * runid：设置为 NULL，但在接收到 INFO 输出后会被填充。
 * info_refresh：设置为 0，表示我们尚未接收到 INFO。
 *
 * 如果在初始标志中设置了 SRI_MASTER，则实例会被添加到
 * sentinel.masters 表中。
 *
 * 如果设置了 SRI_SLAVE 或 SRI_SENTINEL，则 'master' 必须不为 NULL，
 * 并且实例会被添加到 master->slaves 或 master->sentinels 表中。
 *
 * 如果实例是从节点或 Sentinel，则会忽略 name 参数，
 * 并自动创建为 hostname:port 的形式。
 *
 * 如果无法解析 hostname 或端口超出范围，函数会失败。
 * 此时返回 NULL，并根据 createSentinelAddr() 函数设置 errno。
 *
 * 如果具有相同名称的主节点、相同地址的从节点或相同 ID 的 Sentinel 已存在，
 * 函数也可能失败并返回 NULL，同时 errno 设置为 EBUSY。 */
sentinelRedisInstance *createSentinelRedisInstance(char *name, int flags, char *hostname, int port, int quorum, sentinelRedisInstance *master) {
    sentinelRedisInstance *ri;
    sentinelAddr *addr;
    dict *table = NULL;
    sds sdsname;

    serverAssert(flags & (SRI_MASTER|SRI_SLAVE|SRI_SENTINEL));
    serverAssert((flags & SRI_MASTER) || master != NULL);

    /* 检查地址的有效性。 */ /* Check address validity. */
    addr = createSentinelAddr(hostname,port,1);
    if (addr == NULL) return NULL;

    /* 对于从节点，使用 ip/host:port 作为名称。 */ /* For slaves use ip/host:port as name. */
    if (flags & SRI_SLAVE)
        sdsname = announceSentinelAddrAndPort(addr);
    else
        sdsname = sdsnew(name);

    /* Make sure the entry is not duplicated. This may happen when the same
     * name for a master is used multiple times inside the configuration or
     * if we try to add multiple times a slave or sentinel with same ip/port
     * to a master. */
    /* 确保条目不重复。这可能发生在配置中多次使用相同的主节点名称，
     * 或者我们尝试多次将具有相同 ip/port 的从节点或 Sentinel 添加到主节点时。 */
    if (flags & SRI_MASTER) table = sentinel.masters;
    else if (flags & SRI_SLAVE) table = master->slaves;
    else if (flags & SRI_SENTINEL) table = master->sentinels;
    if (dictFind(table,sdsname)) {
        releaseSentinelAddr(addr);
        sdsfree(sdsname);
        errno = EBUSY;
        return NULL;
    }

    /* 创建实例对象。 */ /* Create the instance object. */
    ri = zmalloc(sizeof(*ri));
    /* Note that all the instances are started in the disconnected state,
     * the event loop will take care of connecting them. */
    /* 注意，所有实例都以断开连接状态启动，
     * 事件循环将负责连接它们。 */
    ri->flags = flags;
    ri->name = sdsname;
    ri->runid = NULL;
    ri->config_epoch = 0;
    ri->addr = addr;
    ri->link = createInstanceLink();
    ri->last_pub_time = mstime();
    ri->last_hello_time = mstime();
    ri->last_master_down_reply_time = mstime();
    ri->s_down_since_time = 0;
    ri->o_down_since_time = 0;
    ri->down_after_period = master ? master->down_after_period :
                            SENTINEL_DEFAULT_DOWN_AFTER;
    ri->master_link_down_time = 0;
    ri->auth_pass = NULL;
    ri->auth_user = NULL;
    ri->slave_priority = SENTINEL_DEFAULT_SLAVE_PRIORITY;
    ri->replica_announced = 1;
    ri->slave_reconf_sent_time = 0;
    ri->slave_master_host = NULL;
    ri->slave_master_port = 0;
    ri->slave_master_link_status = SENTINEL_MASTER_LINK_STATUS_DOWN;
    ri->slave_repl_offset = 0;
    ri->sentinels = dictCreate(&instancesDictType,NULL);
    ri->quorum = quorum;
    ri->parallel_syncs = SENTINEL_DEFAULT_PARALLEL_SYNCS;
    ri->master = master;
    ri->slaves = dictCreate(&instancesDictType,NULL);
    ri->info_refresh = 0;
    ri->renamed_commands = dictCreate(&renamedCommandsDictType,NULL);

    /* 故障转移状态。 */ /* Failover state. */
    ri->leader = NULL;
    ri->leader_epoch = 0;
    ri->failover_epoch = 0;
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0;
    ri->failover_timeout = SENTINEL_DEFAULT_FAILOVER_TIMEOUT;
    ri->failover_delay_logged = 0;
    ri->promoted_slave = NULL;
    ri->notification_script = NULL;
    ri->client_reconfig_script = NULL;
    ri->info = NULL;

    /* Role */
    ri->role_reported = ri->flags & (SRI_MASTER|SRI_SLAVE);
    ri->role_reported_time = mstime();
    ri->slave_conf_change_time = mstime();

    /* 添加到正确的表中。 */ /* Add into the right table. */
    dictAdd(table, ri->name, ri);
    return ri;
}

/* Release this instance and all its slaves, sentinels, hiredis connections.
 * This function does not take care of unlinking the instance from the main
 * masters table (if it is a master) or from its master sentinels/slaves table
 * if it is a slave or sentinel. */
/* 释放此实例及其所有从节点、Sentinel 和 hiredis 连接。
 * 此函数不会负责将实例从主节点表中取消链接（如果它是主节点），
 * 或从其主节点的 sentinels/slaves 表中取消链接（如果它是从节点或 Sentinel）。 */
void releaseSentinelRedisInstance(sentinelRedisInstance *ri) {
    /* 释放其所有从节点或 Sentinel（如果有）。 */ /* Release all its slaves or sentinels if any. */
    dictRelease(ri->sentinels);
    dictRelease(ri->slaves);

    /* 断开实例的连接。 */ /* Disconnect the instance. */
    releaseInstanceLink(ri->link,ri);

    /* 释放其他资源。 */ /* Free other resources. */
    sdsfree(ri->name);
    sdsfree(ri->runid);
    sdsfree(ri->notification_script);
    sdsfree(ri->client_reconfig_script);
    sdsfree(ri->slave_master_host);
    sdsfree(ri->leader);
    sdsfree(ri->auth_pass);
    sdsfree(ri->auth_user);
    sdsfree(ri->info);
    releaseSentinelAddr(ri->addr);
    dictRelease(ri->renamed_commands);

    /* 如果需要，清除主节点中的状态。 */ /* Clear state into the master if needed. */
    if ((ri->flags & SRI_SLAVE) && (ri->flags & SRI_PROMOTED) && ri->master)
        ri->master->promoted_slave = NULL;

    zfree(ri);
}

/* 在主 Redis 实例中通过 ip 和端口查找从节点。 */ /* Lookup a slave in a master Redis instance, by ip and port. */
sentinelRedisInstance *sentinelRedisInstanceLookupSlave(
                sentinelRedisInstance *ri, char *slave_addr, int port)
{
    sds key;
    sentinelRedisInstance *slave;
    sentinelAddr *addr;

    serverAssert(ri->flags & SRI_MASTER);

    /* We need to handle a slave_addr that is potentially a hostname.
     * If that is the case, depending on configuration we either resolve
     * it and use the IP addres or fail.
     */
    /* 我们需要处理一个可能是主机名的 slave_addr。
     * 如果是这种情况，根据配置，我们要么解析它并使用 IP 地址，要么失败。 */
    addr = createSentinelAddr(slave_addr, port, 0);
    if (!addr) return NULL;
    key = announceSentinelAddrAndPort(addr);
    releaseSentinelAddr(addr);

    slave = dictFetchValue(ri->slaves,key);
    sdsfree(key);
    return slave;
}

/* 返回实例类型的名称作为字符串。 */ /* Return the name of the type of the instance as a string. */
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri) {
    if (ri->flags & SRI_MASTER) return "master";
    else if (ri->flags & SRI_SLAVE) return "slave";
    else if (ri->flags & SRI_SENTINEL) return "sentinel";
    else return "unknown";
}

/* This function remove the Sentinel with the specified ID from the
 * specified master.
 *
 * If "runid" is NULL the function returns ASAP.
 *
 * This function is useful because on Sentinels address switch, we want to
 * remove our old entry and add a new one for the same ID but with the new
 * address.
 *
 * The function returns 1 if the matching Sentinel was removed, otherwise
 * 0 if there was no Sentinel with this ID. */
/* 从指定的主节点中移除具有指定 ID 的 Sentinel。
 *
 * 如果 "runid" 为 NULL，则函数会尽快返回。
 *
 * 此函数很有用，因为在 Sentinel 地址切换时，
 * 我们需要移除旧条目，并为相同的 ID 添加一个具有新地址的新条目。
 *
 * 如果匹配的 Sentinel 被移除，函数返回 1，否则如果没有具有该 ID 的 Sentinel，则返回 0。 */
int removeMatchingSentinelFromMaster(sentinelRedisInstance *master, char *runid) {
    dictIterator *di;
    dictEntry *de;
    int removed = 0;

    if (runid == NULL) return 0;

    di = dictGetSafeIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->runid && strcmp(ri->runid,runid) == 0) {
            dictDelete(master->sentinels,ri->name);
            removed++;
        }
    }
    dictReleaseIterator(di);
    return removed;
}

/* Search an instance with the same runid, ip and port into a dictionary
 * of instances. Return NULL if not found, otherwise return the instance
 * pointer.
 *
 * runid or addr can be NULL. In such a case the search is performed only
 * by the non-NULL field. */
/* 在实例字典中搜索具有相同 runid、ip 和端口的实例。
 * 如果未找到，则返回 NULL，否则返回实例指针。
 *
 * runid 或 addr 可以为 NULL。在这种情况下，仅根据非 NULL 字段执行搜索。 */
sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *addr, int port, char *runid) {
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *instance = NULL;
    sentinelAddr *ri_addr = NULL;

    serverAssert(addr || runid);   /* 用户必须至少传递一个搜索参数。 */ /* User must pass at least one search param. */
    if (addr != NULL) {
        /* Try to resolve addr. If hostnames are used, we're accepting an ri_addr
         * that contains an hostname only and can still be matched based on that.
         */
        /* 尝试解析 addr。如果使用主机名，我们接受一个仅包含主机名的 ri_addr，
         * 并且仍然可以基于此进行匹配。 */
        ri_addr = createSentinelAddr(addr,port,1);
        if (!ri_addr) return NULL;
    }
    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (runid && !ri->runid) continue;
        if ((runid == NULL || strcmp(ri->runid, runid) == 0) &&
            (addr == NULL || sentinelAddrOrHostnameEqual(ri->addr, ri_addr)))
        {
            instance = ri;
            break;
        }
    }
    dictReleaseIterator(di);
    if (ri_addr != NULL)
        releaseSentinelAddr(ri_addr);

    return instance;
}

/* 按名称查找主节点 */ /* Master lookup by name */
sentinelRedisInstance *sentinelGetMasterByName(char *name) {
    sentinelRedisInstance *ri;
    sds sdsname = sdsnew(name);

    ri = dictFetchValue(sentinel.masters,sdsname);
    sdsfree(sdsname);
    return ri;
}

/* 将指定的标志添加到指定字典中的所有实例。 */ /* Add the specified flags to all the instances in the specified dictionary. */
void sentinelAddFlagsToDictOfRedisInstances(dict *instances, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags |= flags;
    }
    dictReleaseIterator(di);
}

/* 从指定字典中的所有实例中移除指定的标志。 */ /* Remove the specified flags to all the instances in the specified
 * dictionary. */
void sentinelDelFlagsToDictOfRedisInstances(dict *instances, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags &= ~flags;
    }
    dictReleaseIterator(di);
}

/* Reset the state of a monitored master:
 * 1) Remove all slaves.
 * 2) Remove all sentinels.
 * 3) Remove most of the flags resulting from runtime operations.
 * 4) Reset timers to their default value. For example after a reset it will be
 *    possible to failover again the same master ASAP, without waiting the
 *    failover timeout delay.
 * 5) In the process of doing this undo the failover if in progress.
 * 6) Disconnect the connections with the master (will reconnect automatically).
 */
/* 重置被监控主节点的状态：
 * 1) 移除所有从节点。
 * 2) 移除所有 Sentinel。
 * 3) 移除大部分由运行时操作产生的标志。
 * 4) 将计时器重置为默认值。例如，重置后可以尽快对同一主节点进行故障转移，
 *    而无需等待故障转移超时延迟。
 * 5) 如果正在进行故障转移，则在此过程中撤销故障转移。
 * 6) 断开与主节点的连接（将自动重新连接）。 */

#define SENTINEL_RESET_NO_SENTINELS (1<<0)
void sentinelResetMaster(sentinelRedisInstance *ri, int flags) {
    serverAssert(ri->flags & SRI_MASTER);
    dictRelease(ri->slaves);
    ri->slaves = dictCreate(&instancesDictType,NULL);
    if (!(flags & SENTINEL_RESET_NO_SENTINELS)) {
        dictRelease(ri->sentinels);
        ri->sentinels = dictCreate(&instancesDictType,NULL);
    }
    instanceLinkCloseConnection(ri->link,ri->link->cc);
    instanceLinkCloseConnection(ri->link,ri->link->pc);
    ri->flags &= SRI_MASTER;
    if (ri->leader) {
        sdsfree(ri->leader);
        ri->leader = NULL;
    }
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0; /* We can failover again ASAP. */
    ri->promoted_slave = NULL;
    sdsfree(ri->runid);
    sdsfree(ri->slave_master_host);
    ri->runid = NULL;
    ri->slave_master_host = NULL;
    ri->link->act_ping_time = mstime();
    ri->link->last_ping_time = 0;
    ri->link->last_avail_time = mstime();
    ri->link->last_pong_time = mstime();
    ri->role_reported_time = mstime();
    ri->role_reported = SRI_MASTER;
    if (flags & SENTINEL_GENERATE_EVENT)
        sentinelEvent(LL_WARNING,"+reset-master",ri,"%@");
}

/* Call sentinelResetMaster() on every master with a name matching the specified
 * pattern. */
/* 对每个名称与指定模式匹配的主节点调用 sentinelResetMaster()。 */
int sentinelResetMastersByPattern(char *pattern, int flags) {
    dictIterator *di;
    dictEntry *de;
    int reset = 0;

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->name) {
            if (stringmatch(pattern,ri->name,0)) {
                sentinelResetMaster(ri,flags);
                reset++;
            }
        }
    }
    dictReleaseIterator(di);
    return reset;
}

/* Reset the specified master with sentinelResetMaster(), and also change
 * the ip:port address, but take the name of the instance unmodified.
 *
 * This is used to handle the +switch-master event.
 *
 * The function returns C_ERR if the address can't be resolved for some
 * reason. Otherwise C_OK is returned.  */
/* 使用 sentinelResetMaster() 重置指定的主节点，并更改其 ip:port 地址，
 * 但保持实例名称不变。
 *
 * 该函数用于处理 +switch-master 事件。
 *
 * 如果由于某种原因无法解析地址，函数返回 C_ERR。否则返回 C_OK。 */
int sentinelResetMasterAndChangeAddress(sentinelRedisInstance *master, char *hostname, int port) {
    sentinelAddr *oldaddr, *newaddr;
    sentinelAddr **slaves = NULL;
    int numslaves = 0, j;
    dictIterator *di;
    dictEntry *de;

    newaddr = createSentinelAddr(hostname,port,0);
    if (newaddr == NULL) return C_ERR;

    /* There can be only 0 or 1 slave that has the newaddr.
     * and It can add old master 1 more slave. 
     * so It allocates dictSize(master->slaves) + 1          */
    /* 可能只有 0 或 1 个从节点具有新地址，
    * 并且它可以将旧主节点添加为一个额外的从节点。
    * 因此，它分配了 dictSize(master->slaves) + 1 的空间。 */
    slaves = zmalloc(sizeof(sentinelAddr*)*(dictSize(master->slaves) + 1));
    
    /* 不包括具有我们正在切换到的地址的从节点。 */ /* Don't include the one having the address we are switching to. */
    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        if (sentinelAddrOrHostnameEqual(slave->addr,newaddr)) continue;
        slaves[numslaves++] = dupSentinelAddr(slave->addr);
    }
    dictReleaseIterator(di);

    /* If we are switching to a different address, include the old address
     * as a slave as well, so that we'll be able to sense / reconfigure
     * the old master. */
    /* 如果我们正在切换到一个不同的地址，则将旧地址也作为从节点添加，
     * 以便我们能够感知/重新配置旧主节点。 */

    if (!sentinelAddrOrHostnameEqual(newaddr,master->addr)) {
        slaves[numslaves++] = dupSentinelAddr(master->addr);
    }

    /* 重置并切换地址。 */ /* Reset and switch address. */
    sentinelResetMaster(master,SENTINEL_RESET_NO_SENTINELS);
    oldaddr = master->addr;
    master->addr = newaddr;
    master->o_down_since_time = 0;
    master->s_down_since_time = 0;

    /* 添加从节点回去。 */ /* Add slaves back. */
    for (j = 0; j < numslaves; j++) {
        sentinelRedisInstance *slave;

        slave = createSentinelRedisInstance(NULL,SRI_SLAVE,slaves[j]->hostname,
                    slaves[j]->port, master->quorum, master);
        releaseSentinelAddr(slaves[j]);
        if (slave) sentinelEvent(LL_NOTICE,"+slave",slave,"%@");
    }
    zfree(slaves);

    /* Release the old address at the end so we are safe even if the function
     * gets the master->addr->ip and master->addr->port as arguments. */
    /* 最后释放旧地址，这样即使函数将 master->addr->ip 和 master->addr->port
     * 作为参数传递，我们也能安全操作。 */
    releaseSentinelAddr(oldaddr);
    sentinelFlushConfig();
    return C_OK;
}

/* Return non-zero if there was no SDOWN or ODOWN error associated to this
 * instance in the latest 'ms' milliseconds. */
/* 如果在最近的 'ms' 毫秒内，该实例没有与 SDOWN 或 ODOWN 错误相关联，则返回非零值。 */
int sentinelRedisInstanceNoDownFor(sentinelRedisInstance *ri, mstime_t ms) {
    mstime_t most_recent;

    most_recent = ri->s_down_since_time;
    if (ri->o_down_since_time > most_recent)
        most_recent = ri->o_down_since_time;
    return most_recent == 0 || (mstime() - most_recent) > ms;
}

/* Return the current master address, that is, its address or the address
 * of the promoted slave if already operational. */
/* 返回当前主节点地址，即其自身地址或已开始运行的被提升从节点的地址。 */
sentinelAddr *sentinelGetCurrentMasterAddress(sentinelRedisInstance *master) {
    /* If we are failing over the master, and the state is already
     * SENTINEL_FAILOVER_STATE_RECONF_SLAVES or greater, it means that we
     * already have the new configuration epoch in the master, and the
     * slave acknowledged the configuration switch. Advertise the new
     * address. */
    /* 如果我们正在对主节点进行故障转移，并且状态已经是
    * SENTINEL_FAILOVER_STATE_RECONF_SLAVES 或更高，
    * 这意味着我们已经在主节点中设置了新的配置纪元，
    * 并且从节点已经确认了配置切换。广播新的地址。 */
    if ((master->flags & SRI_FAILOVER_IN_PROGRESS) &&
        master->promoted_slave &&
        master->failover_state >= SENTINEL_FAILOVER_STATE_RECONF_SLAVES)
    {
        return master->promoted_slave->addr;
    } else {
        return master->addr;
    }
}

/* This function sets the down_after_period field value in 'master' to all
 * the slaves and sentinel instances connected to this master. */
/* 该函数将 'master' 的 down_after_period 字段值设置为
 * 所有连接到该主节点的从节点和 Sentinel 实例。 */
void sentinelPropagateDownAfterPeriod(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    int j;
    dict *d[] = {master->slaves, master->sentinels, NULL};

    for (j = 0; d[j]; j++) {
        di = dictGetIterator(d[j]);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            ri->down_after_period = master->down_after_period;
        }
        dictReleaseIterator(di);
    }
}

/* This function is used in order to send commands to Redis instances: the
 * commands we send from Sentinel may be renamed, a common case is a master
 * with CONFIG and SLAVEOF commands renamed for security concerns. In that
 * case we check the ri->renamed_command table (or if the instance is a slave,
 * we check the one of the master), and map the command that we should send
 * to the set of renamed commands. However, if the command was not renamed,
 * we just return "command" itself. */
/* 该函数用于向 Redis 实例发送命令：Sentinel 发送的命令可能会被重命名，
 * 一个常见的情况是主节点的 CONFIG 和 SLAVEOF 命令因安全原因被重命名。
 * 在这种情况下，我们会检查 ri->renamed_command 表（如果实例是从节点，
 * 则检查主节点的表），并将我们应该发送的命令映射到重命名后的命令集。
 * 然而，如果命令没有被重命名，我们直接返回原始的 "command"。 */
char *sentinelInstanceMapCommand(sentinelRedisInstance *ri, char *command) {
    sds sc = sdsnew(command);
    if (ri->master) ri = ri->master;
    char *retval = dictFetchValue(ri->renamed_commands, sc);
    sdsfree(sc);
    return retval ? retval : command;
}

/* ============================ Config handling ============================= */
/* ============================ 配置处理 ============================= */
/* Generalise handling create instance error. Use SRI_MASTER, SRI_SLAVE or
 * SRI_SENTINEL as a role value. */

/* 通用的实例创建错误处理。使用 SRI_MASTER、SRI_SLAVE 或
 * SRI_SENTINEL 作为角色值。 */
const char *sentinelCheckCreateInstanceErrors(int role) {
    switch(errno) {
    case EBUSY:
        switch (role) {
        case SRI_MASTER:
            return "Duplicate master name.";
        case SRI_SLAVE:
            return "Duplicate hostname and port for replica.";
        case SRI_SENTINEL:
            return "Duplicate runid for sentinel.";
        default:
            serverAssert(0);
            break;
        }
        break;
    case ENOENT:
        return "Can't resolve instance hostname.";
    case EINVAL:
        return "Invalid port number.";
    default:
        return "Unknown Error for creating instances.";
    }
}

/* init function for server.sentinel_config */
/* server.sentinel_config 的初始化函数 */
void initializeSentinelConfig() {
    server.sentinel_config = zmalloc(sizeof(struct sentinelConfig));
    server.sentinel_config->monitor_cfg = listCreate();
    server.sentinel_config->pre_monitor_cfg = listCreate();
    server.sentinel_config->post_monitor_cfg = listCreate();
    listSetFreeMethod(server.sentinel_config->monitor_cfg,freeSentinelLoadQueueEntry);
    listSetFreeMethod(server.sentinel_config->pre_monitor_cfg,freeSentinelLoadQueueEntry);
    listSetFreeMethod(server.sentinel_config->post_monitor_cfg,freeSentinelLoadQueueEntry);
}

/* destroy function for server.sentinel_config */
/* server.sentinel_config 的销毁函数 */
void freeSentinelConfig() {
    /* 释放这三个配置队列，因为我们将不再使用它们 */ /* release these three config queues since we will not use it anymore */
    listRelease(server.sentinel_config->pre_monitor_cfg);
    listRelease(server.sentinel_config->monitor_cfg);
    listRelease(server.sentinel_config->post_monitor_cfg);
    zfree(server.sentinel_config);
    server.sentinel_config = NULL;
}

/* Search config name in pre monitor config name array, return 1 if found,
 * 0 if not found. */
/* 在 pre_monitor 配置名称数组中搜索配置名称，如果找到返回 1，
 * 如果未找到返回 0。 */
int searchPreMonitorCfgName(const char *name) {
    for (unsigned int i = 0; i < sizeof(preMonitorCfgName)/sizeof(preMonitorCfgName[0]); i++) {
        if (!strcasecmp(preMonitorCfgName[i],name)) return 1;
    }
    return 0;
}

/* 在释放列表时用于 sentinelLoadQueueEntry 的释放方法 */ /* free method for sentinelLoadQueueEntry when release the list */
void freeSentinelLoadQueueEntry(void *item) {
    struct sentinelLoadQueueEntry *entry = item;
    sdsfreesplitres(entry->argv,entry->argc);
    sdsfree(entry->line);
    zfree(entry);
}

/* This function is used for queuing sentinel configuration, the main
 * purpose of this function is to delay parsing the sentinel config option
 * in order to avoid the order dependent issue from the config. */
/* 此函数用于将 sentinel 配置排队，主要目的是延迟解析 sentinel 配置选项，
 * 以避免配置中的顺序依赖问题。 */
void queueSentinelConfig(sds *argv, int argc, int linenum, sds line) {
    int i;
    struct sentinelLoadQueueEntry *entry;

    /* 在第一次调用时初始化 sentinel_config */ /* initialize sentinel_config for the first call */
    if (server.sentinel_config == NULL) initializeSentinelConfig();

    entry = zmalloc(sizeof(struct sentinelLoadQueueEntry));
    entry->argv = zmalloc(sizeof(char*)*argc);
    entry->argc = argc;
    entry->linenum = linenum;
    entry->line = sdsdup(line);
    for (i = 0; i < argc; i++) {
        entry->argv[i] = sdsdup(argv[i]);
    }
    /*  Separate config lines with pre monitor config, monitor config and
     *  post monitor config, in order to parsing config dependencies
     *  correctly. */
    /* 将配置行分为 pre_monitor 配置、monitor 配置和 post_monitor 配置，
     * 以便正确解析配置依赖关系 */
    if (!strcasecmp(argv[0],"monitor")) {
        listAddNodeTail(server.sentinel_config->monitor_cfg,entry);
    } else if (searchPreMonitorCfgName(argv[0])) {
        listAddNodeTail(server.sentinel_config->pre_monitor_cfg,entry);
    } else{
        listAddNodeTail(server.sentinel_config->post_monitor_cfg,entry);
    }
}

/* This function is used for loading the sentinel configuration from
 * pre_monitor_cfg, monitor_cfg and post_monitor_cfg list */
void loadSentinelConfigFromQueue(void) {
    const char *err = NULL;
    listIter li;
    listNode *ln;
    int linenum = 0;
    sds line = NULL;

    /* if there is no sentinel_config entry, we can return immediately */
    /* 如果没有 sentinel_config 条目，我们可以直接返回 */
    if (server.sentinel_config == NULL) return;

    /* loading from pre monitor config queue first to avoid dependency issues */
    /* 首先从 pre_monitor 配置队列加载，以避免依赖问题 */
    listRewind(server.sentinel_config->pre_monitor_cfg,&li);
    while((ln = listNext(&li))) {
        struct sentinelLoadQueueEntry *entry = ln->value;
        err = sentinelHandleConfiguration(entry->argv,entry->argc);
        if (err) {
            linenum = entry->linenum;
            line = entry->line;
            goto loaderr;
        }
    }

    /* 从 monitor 配置队列加载 */ /* loading from monitor config queue */
    listRewind(server.sentinel_config->monitor_cfg,&li);
    while((ln = listNext(&li))) {
        struct sentinelLoadQueueEntry *entry = ln->value;
        err = sentinelHandleConfiguration(entry->argv,entry->argc);
        if (err) {
            linenum = entry->linenum;
            line = entry->line;
            goto loaderr;
        }
    }

    /* 从 post_monitor 配置队列加载 */ /* loading from the post monitor config queue */
    listRewind(server.sentinel_config->post_monitor_cfg,&li);
    while((ln = listNext(&li))) {
        struct sentinelLoadQueueEntry *entry = ln->value;
        err = sentinelHandleConfiguration(entry->argv,entry->argc);
        if (err) {
            linenum = entry->linenum;
            line = entry->line;
            goto loaderr;
        }
    }

    /* 配置加载完成后释放 sentinel_config */ /* free sentinel_config when config loading is finished */
    freeSentinelConfig();
    return;

loaderr:
    fprintf(stderr, "\n*** FATAL CONFIG FILE ERROR (Redis %s) ***\n",
        REDIS_VERSION);
    fprintf(stderr, "Reading the configuration file, at line %d\n", linenum);
    fprintf(stderr, ">>> '%s'\n", line);
    fprintf(stderr, "%s\n", err);
    exit(1);
}

const char *sentinelHandleConfiguration(char **argv, int argc) {

    sentinelRedisInstance *ri;

    if (!strcasecmp(argv[0],"monitor") && argc == 5) {
        /* monitor <name> <host> <port> <quorum> */
        int quorum = atoi(argv[4]);

        if (quorum <= 0) return "Quorum must be 1 or greater.";
        if (createSentinelRedisInstance(argv[1],SRI_MASTER,argv[2],
                                        atoi(argv[3]),quorum,NULL) == NULL)
        {
            return sentinelCheckCreateInstanceErrors(SRI_MASTER);
        }
    } else if (!strcasecmp(argv[0],"down-after-milliseconds") && argc == 3) {
        /* down-after-milliseconds <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->down_after_period = atoi(argv[2]);
        if (ri->down_after_period <= 0)
            return "negative or zero time parameter.";
        sentinelPropagateDownAfterPeriod(ri);
    } else if (!strcasecmp(argv[0],"failover-timeout") && argc == 3) {
        /* failover-timeout <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->failover_timeout = atoi(argv[2]);
        if (ri->failover_timeout <= 0)
            return "negative or zero time parameter.";
    } else if (!strcasecmp(argv[0],"parallel-syncs") && argc == 3) {
        /* parallel-syncs <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->parallel_syncs = atoi(argv[2]);
    } else if (!strcasecmp(argv[0],"notification-script") && argc == 3) {
        /* notification-script <name> <path> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if (access(argv[2],X_OK) == -1)
            return "Notification script seems non existing or non executable.";
        ri->notification_script = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0],"client-reconfig-script") && argc == 3) {
        /* client-reconfig-script <name> <path> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if (access(argv[2],X_OK) == -1)
            return "Client reconfiguration script seems non existing or "
                   "non executable.";
        ri->client_reconfig_script = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0],"auth-pass") && argc == 3) {
        /* auth-pass <name> <password> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->auth_pass = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0],"auth-user") && argc == 3) {
        /* auth-user <name> <username> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->auth_user = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0],"current-epoch") && argc == 2) {
        /* current-epoch <epoch> */
        unsigned long long current_epoch = strtoull(argv[1],NULL,10);
        if (current_epoch > sentinel.current_epoch)
            sentinel.current_epoch = current_epoch;
    } else if (!strcasecmp(argv[0],"myid") && argc == 2) {
        if (strlen(argv[1]) != CONFIG_RUN_ID_SIZE)
            return "Malformed Sentinel id in myid option.";
        memcpy(sentinel.myid,argv[1],CONFIG_RUN_ID_SIZE);
    } else if (!strcasecmp(argv[0],"config-epoch") && argc == 3) {
        /* config-epoch <name> <epoch> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->config_epoch = strtoull(argv[2],NULL,10);
        /* The following update of current_epoch is not really useful as
         * now the current epoch is persisted on the config file, but
         * we leave this check here for redundancy. */
        /* 以下对 current_epoch 的更新实际上并没有太大作用，
         * 因为现在 current_epoch 已经持久化到配置文件中，
         * 但我们保留此检查以增加冗余性。 */
        if (ri->config_epoch > sentinel.current_epoch)
            sentinel.current_epoch = ri->config_epoch;
    } else if (!strcasecmp(argv[0],"leader-epoch") && argc == 3) {
        /* leader-epoch <name> <epoch> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->leader_epoch = strtoull(argv[2],NULL,10);
    } else if ((!strcasecmp(argv[0],"known-slave") ||
                !strcasecmp(argv[0],"known-replica")) && argc == 4)
    {
        sentinelRedisInstance *slave;

        /* known-replica <name> <ip> <port> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if ((slave = createSentinelRedisInstance(NULL,SRI_SLAVE,argv[2],
                    atoi(argv[3]), ri->quorum, ri)) == NULL)
        {
            return sentinelCheckCreateInstanceErrors(SRI_SLAVE);
        }
    } else if (!strcasecmp(argv[0],"known-sentinel") &&
               (argc == 4 || argc == 5)) {
        sentinelRedisInstance *si;

        if (argc == 5) { /* Ignore the old form without runid. */
            /* known-sentinel <name> <ip> <port> [runid] */
            ri = sentinelGetMasterByName(argv[1]);
            if (!ri) return "No such master with specified name.";
            if ((si = createSentinelRedisInstance(argv[4],SRI_SENTINEL,argv[2],
                        atoi(argv[3]), ri->quorum, ri)) == NULL)
            {
                return sentinelCheckCreateInstanceErrors(SRI_SENTINEL);
            }
            si->runid = sdsnew(argv[4]);
            sentinelTryConnectionSharing(si);
        }
    } else if (!strcasecmp(argv[0],"rename-command") && argc == 4) {
        /* rename-command <name> <command> <renamed-command> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        sds oldcmd = sdsnew(argv[2]);
        sds newcmd = sdsnew(argv[3]);
        if (dictAdd(ri->renamed_commands,oldcmd,newcmd) != DICT_OK) {
            sdsfree(oldcmd);
            sdsfree(newcmd);
            return "Same command renamed multiple times with rename-command.";
        }
    } else if (!strcasecmp(argv[0],"announce-ip") && argc == 2) {
        /* announce-ip <ip-address> */
        if (strlen(argv[1]))
            sentinel.announce_ip = sdsnew(argv[1]);
    } else if (!strcasecmp(argv[0],"announce-port") && argc == 2) {
        /* announce-port <port> */
        sentinel.announce_port = atoi(argv[1]);
    } else if (!strcasecmp(argv[0],"deny-scripts-reconfig") && argc == 2) {
        /* deny-scripts-reconfig <yes|no> */
        if ((sentinel.deny_scripts_reconfig = yesnotoi(argv[1])) == -1) {
            return "Please specify yes or no for the "
                   "deny-scripts-reconfig options.";
        }
    } else if (!strcasecmp(argv[0],"sentinel-user") && argc == 2) {
        /* sentinel-user <user-name> */
        if (strlen(argv[1]))
            sentinel.sentinel_auth_user = sdsnew(argv[1]);
    } else if (!strcasecmp(argv[0],"sentinel-pass") && argc == 2) {
        /* sentinel-pass <password> */
        if (strlen(argv[1]))
            sentinel.sentinel_auth_pass = sdsnew(argv[1]);
    } else if (!strcasecmp(argv[0],"resolve-hostnames") && argc == 2) {
        /* resolve-hostnames <yes|no> */
        if ((sentinel.resolve_hostnames = yesnotoi(argv[1])) == -1) {
            return "Please specify yes or no for the resolve-hostnames option.";
        }
    } else if (!strcasecmp(argv[0],"announce-hostnames") && argc == 2) {
        /* announce-hostnames <yes|no> */
        if ((sentinel.announce_hostnames = yesnotoi(argv[1])) == -1) {
            return "Please specify yes or no for the announce-hostnames option.";
        }
    } else {
        return "Unrecognized sentinel configuration statement.";
    }
    return NULL;
}

/* Implements CONFIG REWRITE for "sentinel" option.
 * This is used not just to rewrite the configuration given by the user
 * (the configured masters) but also in order to retain the state of
 * Sentinel across restarts: config epoch of masters, associated slaves
 * and sentinel instances, and so forth. */
/* 实现 "sentinel" 选项的 CONFIG REWRITE。
 * 这不仅用于重写用户提供的配置（配置的主节点），
 * 还用于在重启后保留 Sentinel 的状态：
 * 主节点的配置纪元、关联的从节点和 Sentinel 实例等。 */
void rewriteConfigSentinelOption(struct rewriteConfigState *state) {
    dictIterator *di, *di2;
    dictEntry *de;
    sds line;

    /* sentinel 唯一 ID。 */ /* sentinel unique ID. */
    line = sdscatprintf(sdsempty(), "sentinel myid %s", sentinel.myid);
    rewriteConfigRewriteLine(state,"sentinel myid",line,1);

    /* sentinel deny-scripts-reconfig 配置项。 */ /* sentinel deny-scripts-reconfig. */
    line = sdscatprintf(sdsempty(), "sentinel deny-scripts-reconfig %s",
        sentinel.deny_scripts_reconfig ? "yes" : "no");
    rewriteConfigRewriteLine(state,"sentinel deny-scripts-reconfig",line,
        sentinel.deny_scripts_reconfig != SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG);

    /* sentinel resolve-hostnames.
     * This must be included early in the file so it is already in effect
     * when reading the file.
     */
    /* sentinel resolve-hostnames 配置项。
     * 这必须在文件的开头包含，以便在读取文件时生效。 */
    line = sdscatprintf(sdsempty(), "sentinel resolve-hostnames %s",
                        sentinel.resolve_hostnames ? "yes" : "no");
    rewriteConfigRewriteLine(state,"sentinel resolve-hostnames",line,
                             sentinel.resolve_hostnames != SENTINEL_DEFAULT_RESOLVE_HOSTNAMES);

    /* sentinel announce-hostnames 配置项。 */ /* sentinel announce-hostnames. */
    line = sdscatprintf(sdsempty(), "sentinel announce-hostnames %s",
                        sentinel.announce_hostnames ? "yes" : "no");
    rewriteConfigRewriteLine(state,"sentinel announce-hostnames",line,
                             sentinel.announce_hostnames != SENTINEL_DEFAULT_ANNOUNCE_HOSTNAMES);

    /* 为每个主节点生成一个 "sentinel monitor" 配置条目。 */ /* For every master emit a "sentinel monitor" config entry. */
    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *master, *ri;
        sentinelAddr *master_addr;

        /* 哨兵监控 sentinel monitor */
        master = dictGetVal(de);
        master_addr = sentinelGetCurrentMasterAddress(master);
        line = sdscatprintf(sdsempty(),"sentinel monitor %s %s %d %d",
            master->name, announceSentinelAddr(master_addr), master_addr->port,
            master->quorum);
        rewriteConfigRewriteLine(state,"sentinel monitor",line,1);
        /* rewriteConfigMarkAsProcessed is handled after the loop */

        /* sentinel down-after-milliseconds 配置项 */ /* sentinel down-after-milliseconds */
        if (master->down_after_period != SENTINEL_DEFAULT_DOWN_AFTER) {
            line = sdscatprintf(sdsempty(),
                "sentinel down-after-milliseconds %s %ld",
                master->name, (long) master->down_after_period);
            rewriteConfigRewriteLine(state,"sentinel down-after-milliseconds",line,1);
            /* rewriteConfigMarkAsProcessed 在循环结束后处理 */ /* rewriteConfigMarkAsProcessed is handled after the loop */
        }

        /* sentinel failover-timeout 配置项 */ /* sentinel failover-timeout */
        if (master->failover_timeout != SENTINEL_DEFAULT_FAILOVER_TIMEOUT) {
            line = sdscatprintf(sdsempty(),
                "sentinel failover-timeout %s %ld",
                master->name, (long) master->failover_timeout);
            rewriteConfigRewriteLine(state,"sentinel failover-timeout",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */

        }

        /* sentinel parallel-syncs 配置项 */ /* sentinel parallel-syncs */
        if (master->parallel_syncs != SENTINEL_DEFAULT_PARALLEL_SYNCS) {
            line = sdscatprintf(sdsempty(),
                "sentinel parallel-syncs %s %d",
                master->name, master->parallel_syncs);
            rewriteConfigRewriteLine(state,"sentinel parallel-syncs",line,1);
            /* rewriteConfigMarkAsProcessed 在循环结束后处理 */ /* rewriteConfigMarkAsProcessed is handled after the loop */
        }

        /* sentinel notification-script 配置项 */ /* sentinel notification-script */
        if (master->notification_script) {
            line = sdscatprintf(sdsempty(),
                "sentinel notification-script %s %s",
                master->name, master->notification_script);
            rewriteConfigRewriteLine(state,"sentinel notification-script",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */
        }

        /* sentinel client-reconfig-script 配置项 */ /* sentinel client-reconfig-script */
        if (master->client_reconfig_script) {
            line = sdscatprintf(sdsempty(),
                "sentinel client-reconfig-script %s %s",
                master->name, master->client_reconfig_script);
            rewriteConfigRewriteLine(state,"sentinel client-reconfig-script",line,1);
            /* rewriteConfigMarkAsProcessed 在循环结束后处理 */ /* rewriteConfigMarkAsProcessed is handled after the loop */
        }

        /* sentinel auth-pass 和 auth-user 配置项 */ /* sentinel auth-pass & auth-user */
        if (master->auth_pass) {
            line = sdscatprintf(sdsempty(),
                "sentinel auth-pass %s %s",
                master->name, master->auth_pass);
            rewriteConfigRewriteLine(state,"sentinel auth-pass",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */
        }

        if (master->auth_user) {
            line = sdscatprintf(sdsempty(),
                "sentinel auth-user %s %s",
                master->name, master->auth_user);
            rewriteConfigRewriteLine(state,"sentinel auth-user",line,1);
            /* rewriteConfigMarkAsProcessed is handled after the loop */
        }

        /* sentinel config-epoch 配置项 */ /* sentinel config-epoch */
        line = sdscatprintf(sdsempty(),
            "sentinel config-epoch %s %llu",
            master->name, (unsigned long long) master->config_epoch);
        rewriteConfigRewriteLine(state,"sentinel config-epoch",line,1);
        /* rewriteConfigMarkAsProcessed 在循环结束后处理 */ /* rewriteConfigMarkAsProcessed is handled after the loop */


        /* sentinel leader-epoch 配置项 */ /* sentinel leader-epoch */
        line = sdscatprintf(sdsempty(),
            "sentinel leader-epoch %s %llu",
            master->name, (unsigned long long) master->leader_epoch);
        rewriteConfigRewriteLine(state,"sentinel leader-epoch",line,1);
        /* rewriteConfigMarkAsProcessed 在循环结束后处理 */ /* rewriteConfigMarkAsProcessed is handled after the loop */

        /* sentinel known-slave 配置项 */ /* sentinel known-slave */
        di2 = dictGetIterator(master->slaves);
        while((de = dictNext(di2)) != NULL) {
            sentinelAddr *slave_addr;

            ri = dictGetVal(de);
            slave_addr = ri->addr;

            /* If master_addr (obtained using sentinelGetCurrentMasterAddress()
             * so it may be the address of the promoted slave) is equal to this
             * slave's address, a failover is in progress and the slave was
             * already successfully promoted. So as the address of this slave
             * we use the old master address instead. */
            /* 如果 master_addr（通过 sentinelGetCurrentMasterAddress() 获取，
            * 因此它可能是被提升的从节点的地址）与该从节点的地址相同，
            * 则表示正在进行故障转移，并且该从节点已成功提升。
            * 因此，对于该从节点的地址，我们使用旧的主节点地址。 */
            if (sentinelAddrOrHostnameEqual(slave_addr,master_addr))
                slave_addr = master->addr;
            line = sdscatprintf(sdsempty(),
                "sentinel known-replica %s %s %d",
                master->name, announceSentinelAddr(slave_addr), slave_addr->port);
            rewriteConfigRewriteLine(state,"sentinel known-replica",line,1);
            /* rewriteConfigMarkAsProcessed 在循环结束后处理 */ /* rewriteConfigMarkAsProcessed is handled after the loop */
        }
        dictReleaseIterator(di2);

        /* sentinel known-sentinel 配置项 */ /* sentinel known-sentinel */
        di2 = dictGetIterator(master->sentinels);
        while((de = dictNext(di2)) != NULL) {
            ri = dictGetVal(de);
            if (ri->runid == NULL) continue;
            line = sdscatprintf(sdsempty(),
                "sentinel known-sentinel %s %s %d %s",
                master->name, announceSentinelAddr(ri->addr), ri->addr->port, ri->runid);
            rewriteConfigRewriteLine(state,"sentinel known-sentinel",line,1);
            /* rewriteConfigMarkAsProcessed 在循环结束后处理 */ /* rewriteConfigMarkAsProcessed is handled after the loop */
        }
        dictReleaseIterator(di2);

        /* sentinel rename-command 配置项 */ /* sentinel rename-command */
        di2 = dictGetIterator(master->renamed_commands);
        while((de = dictNext(di2)) != NULL) {
            sds oldname = dictGetKey(de);
            sds newname = dictGetVal(de);
            line = sdscatprintf(sdsempty(),
                "sentinel rename-command %s %s %s",
                master->name, oldname, newname);
            rewriteConfigRewriteLine(state,"sentinel rename-command",line,1);
            /* rewriteConfigMarkAsProcessed 在循环结束后处理 */ /* rewriteConfigMarkAsProcessed is handled after the loop */
        }
        dictReleaseIterator(di2);
    }

    /* sentinel current-epoch 是一个对所有主节点都有效的全局状态。 */ /* sentinel current-epoch is a global state valid for all the masters. */
    line = sdscatprintf(sdsempty(),
        "sentinel current-epoch %llu", (unsigned long long) sentinel.current_epoch);
    rewriteConfigRewriteLine(state,"sentinel current-epoch",line,1);

    /* sentinel announce-ip 配置项。 */ /* sentinel announce-ip. */
    if (sentinel.announce_ip) {
        line = sdsnew("sentinel announce-ip ");
        line = sdscatrepr(line, sentinel.announce_ip, sdslen(sentinel.announce_ip));
        rewriteConfigRewriteLine(state,"sentinel announce-ip",line,1);
    } else {
        rewriteConfigMarkAsProcessed(state,"sentinel announce-ip");
    }

    /* sentinel announce-port 配置项。 */ /* sentinel announce-port. */
    if (sentinel.announce_port) {
        line = sdscatprintf(sdsempty(),"sentinel announce-port %d",
                            sentinel.announce_port);
        rewriteConfigRewriteLine(state,"sentinel announce-port",line,1);
    } else {
        rewriteConfigMarkAsProcessed(state,"sentinel announce-port");
    }

    /* sentinel sentinel-user 配置项。 */ /* sentinel sentinel-user. */
    if (sentinel.sentinel_auth_user) {
        line = sdscatprintf(sdsempty(), "sentinel sentinel-user %s", sentinel.sentinel_auth_user);
        rewriteConfigRewriteLine(state,"sentinel sentinel-user",line,1);
    } else {
        rewriteConfigMarkAsProcessed(state,"sentinel sentinel-user");
    }

    /* sentinel sentinel-pass 配置项。 */ /* sentinel sentinel-pass. */
    if (sentinel.sentinel_auth_pass) {
        line = sdscatprintf(sdsempty(), "sentinel sentinel-pass %s", sentinel.sentinel_auth_pass);
        rewriteConfigRewriteLine(state,"sentinel sentinel-pass",line,1);
    } else {
        rewriteConfigMarkAsProcessed(state,"sentinel sentinel-pass");  
    }

    dictReleaseIterator(di);

    /* NOTE: the purpose here is in case due to the state change, the config rewrite 
     does not handle the configs, however, previously the config was set in the config file, 
     rewriteConfigMarkAsProcessed should be put here to mark it as processed in order to 
     delete the old config entry.
    */
   /* 注意：这里的目的是防止由于状态变化导致配置重写未处理某些配置，
    * 然而之前的配置已经写入了配置文件，此时需要调用
    * rewriteConfigMarkAsProcessed 将其标记为已处理，以便删除旧的配置条目。
    */
    rewriteConfigMarkAsProcessed(state,"sentinel monitor");
    rewriteConfigMarkAsProcessed(state,"sentinel down-after-milliseconds");
    rewriteConfigMarkAsProcessed(state,"sentinel failover-timeout");
    rewriteConfigMarkAsProcessed(state,"sentinel parallel-syncs");
    rewriteConfigMarkAsProcessed(state,"sentinel notification-script");
    rewriteConfigMarkAsProcessed(state,"sentinel client-reconfig-script");
    rewriteConfigMarkAsProcessed(state,"sentinel auth-pass");
    rewriteConfigMarkAsProcessed(state,"sentinel auth-user");
    rewriteConfigMarkAsProcessed(state,"sentinel config-epoch");
    rewriteConfigMarkAsProcessed(state,"sentinel leader-epoch");
    rewriteConfigMarkAsProcessed(state,"sentinel known-replica");
    rewriteConfigMarkAsProcessed(state,"sentinel known-sentinel");
    rewriteConfigMarkAsProcessed(state,"sentinel rename-command");
}

/* This function uses the config rewriting Redis engine in order to persist
 * the state of the Sentinel in the current configuration file.
 *
 * Before returning the function calls fsync() against the generated
 * configuration file to make sure changes are committed to disk.
 *
 * On failure the function logs a warning on the Redis log. */
/* 此函数使用 Redis 的配置重写引擎将 Sentinel 的状态持久化到当前配置文件中。
 *
 * 在返回之前，函数会对生成的配置文件调用 fsync()，
 * 以确保更改已提交到磁盘。
 *
 * 如果失败，函数会在 Redis 日志中记录一条警告。 */
void sentinelFlushConfig(void) {
    int fd = -1;
    int saved_hz = server.hz;
    int rewrite_status;

    server.hz = CONFIG_DEFAULT_HZ;
    rewrite_status = rewriteConfig(server.configfile, 0);
    server.hz = saved_hz;

    if (rewrite_status == -1) goto werr;
    if ((fd = open(server.configfile,O_RDONLY)) == -1) goto werr;
    if (fsync(fd) == -1) goto werr;
    if (close(fd) == EOF) goto werr;
    return;

werr:
    serverLog(LL_WARNING,"WARNING: Sentinel was not able to save the new configuration on disk!!!: %s", strerror(errno));
    if (fd != -1) close(fd);
}

/* ====================== hiredis 连接处理 ======================= */ /* ====================== hiredis connection handling ======================= */

/* Send the AUTH command with the specified master password if needed.
 * Note that for slaves the password set for the master is used.
 *
 * In case this Sentinel requires a password as well, via the "requirepass"
 * configuration directive, we assume we should use the local password in
 * order to authenticate when connecting with the other Sentinels as well.
 * So basically all the Sentinels share the same password and use it to
 * authenticate reciprocally.
 *
 * We don't check at all if the command was successfully transmitted
 * to the instance as if it fails Sentinel will detect the instance down,
 * will disconnect and reconnect the link and so forth. */
/* 如果需要，使用指定的主节点密码发送 AUTH 命令。
 * 注意，对于从节点，使用为主节点设置的密码。
 *
 * 如果该 Sentinel 也需要密码（通过 "requirepass" 配置指令设置），
 * 我们假设应该使用本地密码来与其他 Sentinel 进行身份验证。
 * 因此，基本上所有 Sentinel 共享相同的密码，并相互验证。
 *
 * 我们完全不检查命令是否成功传输到实例，
 * 因为如果失败，Sentinel 会检测到实例宕机，
 * 断开连接并重新连接链接等。 */
void sentinelSendAuthIfNeeded(sentinelRedisInstance *ri, redisAsyncContext *c) {
    char *auth_pass = NULL;
    char *auth_user = NULL;

    if (ri->flags & SRI_MASTER) {
        auth_pass = ri->auth_pass;
        auth_user = ri->auth_user;
    } else if (ri->flags & SRI_SLAVE) {
        auth_pass = ri->master->auth_pass;
        auth_user = ri->master->auth_user;
    } else if (ri->flags & SRI_SENTINEL) {
        /* 如果 sentinel_auth_user 为 NULL，AUTH 将使用默认用户
         * 和 sentinel_auth_pass 进行身份验证 */ /* If sentinel_auth_user is NULL, AUTH will use default user
           with sentinel_auth_pass to authenticate */
        if (sentinel.sentinel_auth_pass) {
            auth_pass = sentinel.sentinel_auth_pass;
            auth_user = sentinel.sentinel_auth_user;
        } else {
            /* Compatibility with old configs. requirepass is used
             * for both incoming and outgoing authentication. */
            /* 兼容旧配置。requirepass 用于传入和传出的身份验证。 */
            auth_pass = server.requirepass;
            auth_user = NULL;
        }
    }

    if (auth_pass && auth_user == NULL) {
        if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri, "%s %s",
            sentinelInstanceMapCommand(ri,"AUTH"),
            auth_pass) == C_OK) ri->link->pending_commands++;
    } else if (auth_pass && auth_user) {
        /* If we also have an username, use the ACL-style AUTH command
         * with two arguments, username and password. */
        /* 如果我们也有用户名，则使用带有两个参数（用户名和密码）的
         * ACL 风格的 AUTH 命令。 */
        if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri, "%s %s %s",
            sentinelInstanceMapCommand(ri,"AUTH"),
            auth_user, auth_pass) == C_OK) ri->link->pending_commands++;
    }
}

/* Use CLIENT SETNAME to name the connection in the Redis instance as
 * sentinel-<first_8_chars_of_runid>-<connection_type>
 * The connection type is "cmd" or "pubsub" as specified by 'type'.
 *
 * This makes it possible to list all the sentinel instances connected
 * to a Redis server with CLIENT LIST, grepping for a specific name format. */
/* 使用 CLIENT SETNAME 为 Redis 实例中的连接命名为
 * sentinel-<runid 的前 8 个字符>-<连接类型>
 * 连接类型由 'type' 指定，可以是 "cmd" 或 "pubsub"。
 *
 * 这使得可以通过 CLIENT LIST 列出所有连接到 Redis 服务器的 Sentinel 实例，
 * 并通过特定的名称格式进行筛选。 */
void sentinelSetClientName(sentinelRedisInstance *ri, redisAsyncContext *c, char *type) {
    char name[64];

    snprintf(name,sizeof(name),"sentinel-%.8s-%s",sentinel.myid,type);
    if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri,
        "%s SETNAME %s",
        sentinelInstanceMapCommand(ri,"CLIENT"),
        name) == C_OK)
    {
        ri->link->pending_commands++;
    }
}

static int instanceLinkNegotiateTLS(redisAsyncContext *context) {
#ifndef USE_OPENSSL
    (void) context;
#else
    if (!redis_tls_ctx) return C_ERR;
    SSL *ssl = SSL_new(redis_tls_client_ctx ? redis_tls_client_ctx : redis_tls_ctx);
    if (!ssl) return C_ERR;

    if (redisInitiateSSL(&context->c, ssl) == REDIS_ERR) {
        SSL_free(ssl);
        return C_ERR;
    }
#endif
    return C_OK;
}

/* Create the async connections for the instance link if the link
 * is disconnected. Note that link->disconnected is true even if just
 * one of the two links (commands and pub/sub) is missing. */
 /* 如果链接断开，则为实例链接创建异步连接。
 * 注意，即使两个链接（命令和发布/订阅）中只有一个丢失，
 * link->disconnected 也会为 true。 */
void sentinelReconnectInstance(sentinelRedisInstance *ri) {

    if (ri->link->disconnected == 0) return;
    if (ri->addr->port == 0) return; /* port == 0 表示地址无效。 */ /* port == 0 means invalid address. */
    instanceLink *link = ri->link;
    mstime_t now = mstime();

    if (now - ri->link->last_reconn_time < SENTINEL_PING_PERIOD) return;
    ri->link->last_reconn_time = now;

    /* 命令连接。 */ /* Commands connection. */
    if (link->cc == NULL) {

        /* It might be that the instance is disconnected because it wasn't available earlier when the instance
         * allocated, say during failover, and therefore we failed to resolve its ip.
         * Another scenario is that the instance restarted with new ip, and we should resolve its new ip based on
         * its hostname */
        /* 实例可能由于以下原因断开连接：
        * 例如，在实例分配期间（如故障转移期间），实例之前不可用，
        * 因此我们未能解析其 IP。
        * 另一种情况是实例重新启动并使用了新的 IP，
        * 我们应该根据其主机名解析新的 IP。 */
        if (sentinel.resolve_hostnames) {
            sentinelAddr *tryResolveAddr = createSentinelAddr(ri->addr->hostname, ri->addr->port, 0);
            if (tryResolveAddr != NULL) {
                releaseSentinelAddr(ri->addr);
                ri->addr = tryResolveAddr;
            }
        }

        link->cc = redisAsyncConnectBind(ri->addr->ip,ri->addr->port,NET_FIRST_BIND_ADDR);

        if (link->cc && !link->cc->err) anetCloexec(link->cc->c.fd);
        if (!link->cc) {
            sentinelEvent(LL_DEBUG,"-cmd-link-reconnection",ri,"%@ #Failed to establish connection");
        } else if (!link->cc->err && server.tls_replication &&
                (instanceLinkNegotiateTLS(link->cc) == C_ERR)) {
            sentinelEvent(LL_DEBUG,"-cmd-link-reconnection",ri,"%@ #Failed to initialize TLS");
            instanceLinkCloseConnection(link,link->cc);
        } else if (link->cc->err) {
            sentinelEvent(LL_DEBUG,"-cmd-link-reconnection",ri,"%@ #%s",
                link->cc->errstr);
            instanceLinkCloseConnection(link,link->cc);
        } else {
            link->pending_commands = 0;
            link->cc_conn_time = mstime();
            link->cc->data = link;
            redisAeAttach(server.el,link->cc);
            redisAsyncSetConnectCallback(link->cc,
                    sentinelLinkEstablishedCallback);
            redisAsyncSetDisconnectCallback(link->cc,
                    sentinelDisconnectCallback);
            sentinelSendAuthIfNeeded(ri,link->cc);
            sentinelSetClientName(ri,link->cc,"cmd");

            /* 重新连接时尽快发送 PING。 */ /* Send a PING ASAP when reconnecting. */
            sentinelSendPing(ri);
        }
    }
    /* 发布/订阅 */ /* Pub / Sub */
    if ((ri->flags & (SRI_MASTER|SRI_SLAVE)) && link->pc == NULL) {
        link->pc = redisAsyncConnectBind(ri->addr->ip,ri->addr->port,NET_FIRST_BIND_ADDR);
        if (link->pc && !link->pc->err) anetCloexec(link->pc->c.fd);
        if (!link->pc) {
            sentinelEvent(LL_DEBUG,"-pubsub-link-reconnection",ri,"%@ #Failed to establish connection");
        } else if (!link->pc->err && server.tls_replication &&
                (instanceLinkNegotiateTLS(link->pc) == C_ERR)) {
            sentinelEvent(LL_DEBUG,"-pubsub-link-reconnection",ri,"%@ #Failed to initialize TLS");
        } else if (link->pc->err) {
            sentinelEvent(LL_DEBUG,"-pubsub-link-reconnection",ri,"%@ #%s",
                link->pc->errstr);
            instanceLinkCloseConnection(link,link->pc);
        } else {
            int retval;
            link->pc_conn_time = mstime();
            link->pc->data = link;
            redisAeAttach(server.el,link->pc);
            redisAsyncSetConnectCallback(link->pc,
                    sentinelLinkEstablishedCallback);
            redisAsyncSetDisconnectCallback(link->pc,
                    sentinelDisconnectCallback);
            sentinelSendAuthIfNeeded(ri,link->pc);
            sentinelSetClientName(ri,link->pc,"pubsub");
            /* 现在我们订阅 Sentinels 的 "Hello" 频道。 */ /* Now we subscribe to the Sentinels "Hello" channel. */
            retval = redisAsyncCommand(link->pc,
                sentinelReceiveHelloMessages, ri, "%s %s",
                sentinelInstanceMapCommand(ri,"SUBSCRIBE"),
                SENTINEL_HELLO_CHANNEL);
            if (retval != C_OK) {
                /* 如果我们无法订阅，Pub/Sub 连接将毫无用处，
                 * 我们可以简单地断开连接并重试。 */ /* If we can't subscribe, the Pub/Sub connection is useless
                 * and we can simply disconnect it and try again. */
                instanceLinkCloseConnection(link,link->pc);
                return;
            }
        }
    }
    /* Clear the disconnected status only if we have both the connections
     * (or just the commands connection if this is a sentinel instance). */
    /* 仅当我们拥有两个连接（或者如果这是一个 Sentinel 实例，仅命令连接）时，
     * 才清除断开连接的状态。 */
    if (link->cc && (ri->flags & SRI_SENTINEL || link->pc))
        link->disconnected = 0;
}

/* ======================== Redis 实例的 PING 操作 ======================== */ /* ======================== Redis instances pinging  ======================== */

/* Return true if master looks "sane", that is:
 * 1) It is actually a master in the current configuration.
 * 2) It reports itself as a master.
 * 3) It is not SDOWN or ODOWN.
 * 4) We obtained last INFO no more than two times the INFO period time ago. */
/* 如果主节点看起来“正常”，返回 true，具体条件如下：
 * 1) 它在当前配置中确实是一个主节点。
 * 2) 它报告自己是一个主节点。
 * 3) 它没有处于 SDOWN 或 ODOWN 状态。
 * 4) 我们获取最后一次 INFO 的时间不超过 INFO 周期时间的两倍。 */
int sentinelMasterLooksSane(sentinelRedisInstance *master) {
    return
        master->flags & SRI_MASTER &&
        master->role_reported == SRI_MASTER &&
        (master->flags & (SRI_S_DOWN|SRI_O_DOWN)) == 0 &&
        (mstime() - master->info_refresh) < SENTINEL_INFO_PERIOD*2;
}

/* 处理来自主节点的 INFO 输出。 */ /* Process the INFO output from masters. */
void sentinelRefreshInstanceInfo(sentinelRedisInstance *ri, const char *info) {
    sds *lines;
    int numlines, j;
    int role = 0;

    /* 缓存实例的完整 INFO 输出 */ /* cache full INFO output for instance */
    sdsfree(ri->info);
    ri->info = sdsnew(info);

    /* The following fields must be reset to a given value in the case they
     * are not found at all in the INFO output. */
     /* 如果 INFO 输出中未找到以下字段，则将其重置为给定值。 *
    ri->master_link_down_time = 0;

    /* 按行处理。 */ /* Process line by line. */
    lines = sdssplitlen(info,strlen(info),"\r\n",2,&numlines);
    for (j = 0; j < numlines; j++) {
        sentinelRedisInstance *slave;
        sds l = lines[j];

        /* run_id:<40 个十六进制字符> */ /* run_id:<40 hex chars>*/
        if (sdslen(l) >= 47 && !memcmp(l,"run_id:",7)) {
            if (ri->runid == NULL) {
                ri->runid = sdsnewlen(l+7,40);
            } else {
                if (strncmp(ri->runid,l+7,40) != 0) {
                    sentinelEvent(LL_NOTICE,"+reboot",ri,"%@");
                    sdsfree(ri->runid);
                    ri->runid = sdsnewlen(l+7,40);
                }
            }
        }

        /* old versions: slave0:<ip>,<port>,<state>
         * new versions: slave0:ip=127.0.0.1,port=9999,... */
        /* 旧版本：slave0:<ip>,<port>,<state>
         * 新版本：slave0:ip=127.0.0.1,port=9999,... */
        if ((ri->flags & SRI_MASTER) &&
            sdslen(l) >= 7 &&
            !memcmp(l,"slave",5) && isdigit(l[5]))
        {
            char *ip, *port, *end;

            if (strstr(l,"ip=") == NULL) {
                /* Old format. */
                ip = strchr(l,':'); if (!ip) continue;
                ip++; /* Now ip points to start of ip address. */
                port = strchr(ip,','); if (!port) continue;
                *port = '\0'; /* nul term for easy access. */
                port++; /* Now port points to start of port number. */
                end = strchr(port,','); if (!end) continue;
                *end = '\0'; /* nul term for easy access. */
            } else {
                /* New format. */
                ip = strstr(l,"ip="); if (!ip) continue;
                ip += 3; /* Now ip points to start of ip address. */
                port = strstr(l,"port="); if (!port) continue;
                port += 5; /* Now port points to start of port number. */
                /* Nul term both fields for easy access. */
                end = strchr(ip,','); if (end) *end = '\0';
                end = strchr(port,','); if (end) *end = '\0';
            }

            /* Check if we already have this slave into our table,
             * otherwise add it. */
            /* 检查我们是否已经在表中有这个从节点，
             * 如果没有，则添加它。 */
            if (sentinelRedisInstanceLookupSlave(ri,ip,atoi(port)) == NULL) {
                if ((slave = createSentinelRedisInstance(NULL,SRI_SLAVE,ip,
                            atoi(port), ri->quorum, ri)) != NULL)
                {
                    sentinelEvent(LL_NOTICE,"+slave",slave,"%@");
                    sentinelFlushConfig();
                }
            }
        }

        /* master_link_down_since_seconds:<seconds> */
        if (sdslen(l) >= 32 &&
            !memcmp(l,"master_link_down_since_seconds",30))
        {
            ri->master_link_down_time = strtoll(l+31,NULL,10)*1000;
        }

        /* role:<role> */
        if (sdslen(l) >= 11 && !memcmp(l,"role:master",11)) role = SRI_MASTER;
        else if (sdslen(l) >= 10 && !memcmp(l,"role:slave",10)) role = SRI_SLAVE;

        if (role == SRI_SLAVE) {
            /* master_host:<host> */
            if (sdslen(l) >= 12 && !memcmp(l,"master_host:",12)) {
                if (ri->slave_master_host == NULL ||
                    strcasecmp(l+12,ri->slave_master_host))
                {
                    sdsfree(ri->slave_master_host);
                    ri->slave_master_host = sdsnew(l+12);
                    ri->slave_conf_change_time = mstime();
                }
            }

            /* master_port:<port> */
            if (sdslen(l) >= 12 && !memcmp(l,"master_port:",12)) {
                int slave_master_port = atoi(l+12);

                if (ri->slave_master_port != slave_master_port) {
                    ri->slave_master_port = slave_master_port;
                    ri->slave_conf_change_time = mstime();
                }
            }

            /* master_link_status:<status> */
            if (sdslen(l) >= 19 && !memcmp(l,"master_link_status:",19)) {
                ri->slave_master_link_status =
                    (strcasecmp(l+19,"up") == 0) ?
                    SENTINEL_MASTER_LINK_STATUS_UP :
                    SENTINEL_MASTER_LINK_STATUS_DOWN;
            }

            /* slave_priority:<priority> */
            if (sdslen(l) >= 15 && !memcmp(l,"slave_priority:",15))
                ri->slave_priority = atoi(l+15);

            /* slave_repl_offset:<offset> */
            if (sdslen(l) >= 18 && !memcmp(l,"slave_repl_offset:",18))
                ri->slave_repl_offset = strtoull(l+18,NULL,10);

            /* replica_announced:<announcement> */
            if (sdslen(l) >= 18 && !memcmp(l,"replica_announced:",18))
                ri->replica_announced = atoi(l+18);
        }
    }
    ri->info_refresh = mstime();
    sdsfreesplitres(lines,numlines);

    /* ---------------------------- Acting half -----------------------------
     * Some things will not happen if sentinel.tilt is true, but some will
     * still be processed. */
    /* ---------------------------- 执行部分 -----------------------------
     * 如果 sentinel.tilt 为 true，则某些操作不会执行，但有些仍会处理。 */

    /* 记录角色更改的时间。 */ /* Remember when the role changed. */
    if (role != ri->role_reported) {
        ri->role_reported_time = mstime();
        ri->role_reported = role;
        if (role == SRI_SLAVE) ri->slave_conf_change_time = mstime();
        /* Log the event with +role-change if the new role is coherent or
         * with -role-change if there is a mismatch with the current config. */
        /* 如果新角色与当前配置一致，则记录 +role-change，
         * 如果不一致，则记录 -role-change。 */
        sentinelEvent(LL_VERBOSE,
            ((ri->flags & (SRI_MASTER|SRI_SLAVE)) == role) ?
            "+role-change" : "-role-change",
            ri, "%@ new reported role is %s",
            role == SRI_MASTER ? "master" : "slave");
    }

    /* None of the following conditions are processed when in tilt mode, so
     * return asap. */
      /* 如果处于倾斜模式，则不处理以下任何条件，尽快返回。 */
    if (sentinel.tilt) return;

    /* 处理主节点 -> 从节点角色切换。 */ /* Handle master -> slave role switch. */
    if ((ri->flags & SRI_MASTER) && role == SRI_SLAVE) {
        /* Nothing to do, but masters claiming to be slaves are
         * considered to be unreachable by Sentinel, so eventually
         * a failover will be triggered. */
        /* 无需操作，但声称是从节点的主节点会被 Sentinel 视为不可达，
         * 因此最终会触发故障转移。 */
    }

    /* 处理从节点 -> 主节点角色切换。 */ /* Handle slave -> master role switch. */
    if ((ri->flags & SRI_SLAVE) && role == SRI_MASTER) {
        /* If this is a promoted slave we can change state to the
         * failover state machine. */
        /* 如果这是一个被提升的从节点，我们可以将状态更改为
         * 故障转移状态机。 */
        if ((ri->flags & SRI_PROMOTED) &&
            (ri->master->flags & SRI_FAILOVER_IN_PROGRESS) &&
            (ri->master->failover_state ==
                SENTINEL_FAILOVER_STATE_WAIT_PROMOTION))
        {
            /* Now that we are sure the slave was reconfigured as a master
             * set the master configuration epoch to the epoch we won the
             * election to perform this failover. This will force the other
             * Sentinels to update their config (assuming there is not
             * a newer one already available). */
            /* 现在我们确定从节点已重新配置为主节点，
             * 将主节点的配置纪元设置为我们赢得选举以执行此故障转移的纪元。
             * 这将强制其他 Sentinel 更新其配置（假设没有更新的配置可用）。 */
            ri->master->config_epoch = ri->master->failover_epoch;
            ri->master->failover_state = SENTINEL_FAILOVER_STATE_RECONF_SLAVES;
            ri->master->failover_state_change_time = mstime();
            sentinelFlushConfig();
            sentinelEvent(LL_WARNING,"+promoted-slave",ri,"%@");
            if (sentinel.simfailure_flags &
                SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION)
                sentinelSimFailureCrash();
            sentinelEvent(LL_WARNING,"+failover-state-reconf-slaves",
                ri->master,"%@");
            sentinelCallClientReconfScript(ri->master,SENTINEL_LEADER,
                "start",ri->master->addr,ri->addr);
            sentinelForceHelloUpdateForMaster(ri->master);
        } else {
            /* A slave turned into a master. We want to force our view and
             * reconfigure as slave. Wait some time after the change before
             * going forward, to receive new configs if any. */
            /* 一个从节点变成了主节点。我们希望强制我们的视图并
             * 重新配置为从节点。在更改后等待一段时间再继续，
             * 以接收新的配置（如果有）。 */
            mstime_t wait_time = SENTINEL_PUBLISH_PERIOD*4;

            if (!(ri->flags & SRI_PROMOTED) &&
                 sentinelMasterLooksSane(ri->master) &&
                 sentinelRedisInstanceNoDownFor(ri,wait_time) &&
                 mstime() - ri->role_reported_time > wait_time)
            {
                int retval = sentinelSendSlaveOf(ri,ri->master->addr);
                if (retval == C_OK)
                    sentinelEvent(LL_NOTICE,"+convert-to-slave",ri,"%@");
            }
        }
    }

    /* 处理正在复制到不同主节点地址的从节点。 */ /* Handle slaves replicating to a different master address. */
    if ((ri->flags & SRI_SLAVE) &&
        role == SRI_SLAVE &&
        (ri->slave_master_port != ri->master->addr->port ||
         !sentinelAddrEqualsHostname(ri->master->addr, ri->slave_master_host)))
    {
        mstime_t wait_time = ri->master->failover_timeout;

        /* Make sure the master is sane before reconfiguring this instance
         * into a slave. */
        /* 在将此实例重新配置为从节点之前，确保主节点是正常的。 */
        if (sentinelMasterLooksSane(ri->master) &&
            sentinelRedisInstanceNoDownFor(ri,wait_time) &&
            mstime() - ri->slave_conf_change_time > wait_time)
        {
            int retval = sentinelSendSlaveOf(ri,ri->master->addr);
            if (retval == C_OK)
                sentinelEvent(LL_NOTICE,"+fix-slave-config",ri,"%@");
        }
    }

    /* 检测正在重新配置的从节点是否更改了状态。 */ /* Detect if the slave that is in the process of being reconfigured
     * changed state. */
    if ((ri->flags & SRI_SLAVE) && role == SRI_SLAVE &&
        (ri->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG)))
    {
        /* SRI_RECONF_SENT -> SRI_RECONF_INPROG. */
        if ((ri->flags & SRI_RECONF_SENT) &&
            ri->slave_master_host &&
            sentinelAddrEqualsHostname(ri->master->promoted_slave->addr,
                ri->slave_master_host) &&
            ri->slave_master_port == ri->master->promoted_slave->addr->port)
        {
            ri->flags &= ~SRI_RECONF_SENT;
            ri->flags |= SRI_RECONF_INPROG;
            sentinelEvent(LL_NOTICE,"+slave-reconf-inprog",ri,"%@");
        }

        /* SRI_RECONF_INPROG -> SRI_RECONF_DONE */
        if ((ri->flags & SRI_RECONF_INPROG) &&
            ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP)
        {
            ri->flags &= ~SRI_RECONF_INPROG;
            ri->flags |= SRI_RECONF_DONE;
            sentinelEvent(LL_NOTICE,"+slave-reconf-done",ri,"%@");
        }
    }
}

void sentinelInfoReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    if (r->type == REDIS_REPLY_STRING)
        sentinelRefreshInstanceInfo(ri,r->str);
}

/* Just discard the reply. We use this when we are not monitoring the return
 * value of the command but its effects directly. */
/* 直接丢弃回复。当我们不监控命令的返回值，而是直接监控其效果时使用此方法。 */
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    instanceLink *link = c->data;
    UNUSED(reply);
    UNUSED(privdata);

    if (link) link->pending_commands--;
}

void sentinelPingReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    if (r->type == REDIS_REPLY_STATUS ||
        r->type == REDIS_REPLY_ERROR) {
        /* Update the "instance available" field only if this is an
         * acceptable reply. */
        /* 仅当这是一个可接受的回复时，才更新“实例可用”字段。 */
        if (strncmp(r->str,"PONG",4) == 0 ||
            strncmp(r->str,"LOADING",7) == 0 ||
            strncmp(r->str,"MASTERDOWN",10) == 0)
        {
            link->last_avail_time = mstime();
            link->act_ping_time = 0; /* Flag the pong as received. */
        } else {
            /* Send a SCRIPT KILL command if the instance appears to be
             * down because of a busy script. */
            /* 如果实例由于繁忙脚本而似乎宕机，则发送 SCRIPT KILL 命令。 */
            if (strncmp(r->str,"BUSY",4) == 0 &&
                (ri->flags & SRI_S_DOWN) &&
                !(ri->flags & SRI_SCRIPT_KILL_SENT))
            {
                if (redisAsyncCommand(ri->link->cc,
                        sentinelDiscardReplyCallback, ri,
                        "%s KILL",
                        sentinelInstanceMapCommand(ri,"SCRIPT")) == C_OK)
                {
                    ri->link->pending_commands++;
                }
                ri->flags |= SRI_SCRIPT_KILL_SENT;
            }
        }
    }
    link->last_pong_time = mstime();
}

/* This is called when we get the reply about the PUBLISH command we send
 * to the master to advertise this sentinel. */
/* 当我们收到关于发送给主节点的 PUBLISH 命令的回复时调用此函数，
 * 用于宣传此 Sentinel。 */
void sentinelPublishReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    /* Only update pub_time if we actually published our message. Otherwise
     * we'll retry again in 100 milliseconds. */
    /* 仅当我们实际发布了消息时才更新 pub_time。否则我们将在 100 毫秒后重试。 */
    if (r->type != REDIS_REPLY_ERROR)
        ri->last_pub_time = mstime();
}

/* Process a hello message received via Pub/Sub in master or slave instance,
 * or sent directly to this sentinel via the (fake) PUBLISH command of Sentinel.
 *
 * If the master name specified in the message is not known, the message is
 * discarded. */
/* 处理通过主节点或从节点实例中的 Pub/Sub 接收到的 Hello 消息，
 * 或通过 Sentinel 的（伪造的）PUBLISH 命令直接发送到此 Sentinel 的消息。
 *
 * 如果消息中指定的主节点名称未知，则丢弃该消息。 */
void sentinelProcessHelloMessage(char *hello, int hello_len) {
    /* Format is composed of 8 tokens:
     * 0=ip,1=port,2=runid,3=current_epoch,4=master_name,
     * 5=master_ip,6=master_port,7=master_config_epoch. */
    /* 格式由 8 个标记组成：
    * 0=ip,1=port,2=runid,3=current_epoch,4=master_name,
    * 5=master_ip,6=master_port,7=master_config_epoch。 */
    int numtokens, port, removed, master_port;
    uint64_t current_epoch, master_config_epoch;
    char **token = sdssplitlen(hello, hello_len, ",", 1, &numtokens);
    sentinelRedisInstance *si, *master;

    if (numtokens == 8) {
        /* 如果没有匹配的主节点，则跳过该消息。 */ /* Obtain a reference to the master this hello message is about */
        master = sentinelGetMasterByName(token[4]);
        if (!master) goto cleanup; /* Unknown master, skip the message. */

        /* 首先，尝试查看我们是否已经有这个 Sentinel。 */ /* First, try to see if we already have this sentinel. */
        port = atoi(token[1]);
        master_port = atoi(token[6]);
        si = getSentinelRedisInstanceByAddrAndRunID(
                        master->sentinels,token[0],port,token[2]);
        current_epoch = strtoull(token[3],NULL,10);
        master_config_epoch = strtoull(token[7],NULL,10);

        if (!si) {
            /* If not, remove all the sentinels that have the same runid
             * because there was an address change, and add the same Sentinel
             * with the new address back. */
            /* 如果没有，移除所有具有相同 runid 的 Sentinel，
             * 因为地址发生了变化，并使用新地址重新添加相同的 Sentinel。 */
            removed = removeMatchingSentinelFromMaster(master,token[2]);
            if (removed) {
                sentinelEvent(LL_NOTICE,"+sentinel-address-switch",master,
                    "%@ ip %s port %d for %s", token[0],port,token[2]);
            } else {
                /* Check if there is another Sentinel with the same address this
                 * new one is reporting. What we do if this happens is to set its
                 * port to 0, to signal the address is invalid. We'll update it
                 * later if we get an HELLO message. */
                /* 如果有另一个 Sentinel 报告了相同的地址，
                * 我们将其端口设置为 0，表示地址无效。
                * 如果稍后收到 HELLO 消息，我们将更新它。 */
                sentinelRedisInstance *other =
                    getSentinelRedisInstanceByAddrAndRunID(
                        master->sentinels, token[0],port,NULL);
                if (other) {
                    sentinelEvent(LL_NOTICE,"+sentinel-invalid-addr",other,"%@");
                    other->addr->port = 0; /* It means: invalid address. */
                    sentinelUpdateSentinelAddressInAllMasters(other);
                }
            }

            /* 添加新的 Sentinel。 */ /* Add the new sentinel. */
            si = createSentinelRedisInstance(token[2],SRI_SENTINEL,
                            token[0],port,master->quorum,master);

            if (si) {
                if (!removed) sentinelEvent(LL_NOTICE,"+sentinel",si,"%@");
                /* The runid is NULL after a new instance creation and
                 * for Sentinels we don't have a later chance to fill it,
                 * so do it now. */
                /* 在新实例创建后，runid 为 NULL，
                * 对于 Sentinel，我们之后没有机会填充它，
                * 所以现在就填充它。 */
                si->runid = sdsnew(token[2]);
                sentinelTryConnectionSharing(si);
                if (removed) sentinelUpdateSentinelAddressInAllMasters(si);
                sentinelFlushConfig();
            }
        }

        /* 如果接收到的 current_epoch 大于本地 current_epoch，则更新本地 current_epoch。 */ /* Update local current_epoch if received current_epoch is greater.*/
        if (current_epoch > sentinel.current_epoch) {
            sentinel.current_epoch = current_epoch;
            sentinelFlushConfig();
            sentinelEvent(LL_WARNING,"+new-epoch",master,"%llu",
                (unsigned long long) sentinel.current_epoch);
        }

        /* 如果接收到的配置更新比当前的更新，则更新主节点信息。 */ /* Update master info if received configuration is newer. */
        if (si && master->config_epoch < master_config_epoch) {
            master->config_epoch = master_config_epoch;
            if (master_port != master->addr->port ||
                !sentinelAddrEqualsHostname(master->addr, token[5]))
            {
                sentinelAddr *old_addr;

                sentinelEvent(LL_WARNING,"+config-update-from",si,"%@");
                sentinelEvent(LL_WARNING,"+switch-master",
                    master,"%s %s %d %s %d",
                    master->name,
                    announceSentinelAddr(master->addr), master->addr->port,
                    token[5], master_port);

                old_addr = dupSentinelAddr(master->addr);
                sentinelResetMasterAndChangeAddress(master, token[5], master_port);
                sentinelCallClientReconfScript(master,
                    SENTINEL_OBSERVER,"start",
                    old_addr,master->addr);
                releaseSentinelAddr(old_addr);
            }
        }

        /* 更新 Sentinel 的状态。 */ /* Update the state of the Sentinel. */
        if (si) si->last_hello_time = mstime();
    }

cleanup:
    sdsfreesplitres(token,numtokens);
}


/* This is our Pub/Sub callback for the Hello channel. It's useful in order
 * to discover other sentinels attached at the same master. */
/* 这是我们用于 Hello 频道的 Pub/Sub 回调。
 * 它用于发现连接到同一主节点的其他 Sentinel。 */
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    redisReply *r;
    UNUSED(c);

    if (!reply || !ri) return;
    r = reply;

    /* Update the last activity in the pubsub channel. Note that since we
     * receive our messages as well this timestamp can be used to detect
     * if the link is probably disconnected even if it seems otherwise. */
    /* 更新 pubsub 频道中的最后活动时间。
    * 注意，由于我们也会接收自己的消息，因此即使看起来连接正常，
    * 也可以使用此时间戳来检测连接是否可能已断开。 */
    ri->link->pc_last_activity = mstime();

    /* Sanity check in the reply we expect, so that the code that follows
     * can avoid to check for details. */
    /* 检查我们期望的回复的正确性，以便后续代码可以避免检查细节。 */
    if (r->type != REDIS_REPLY_ARRAY ||
        r->elements != 3 ||
        r->element[0]->type != REDIS_REPLY_STRING ||
        r->element[1]->type != REDIS_REPLY_STRING ||
        r->element[2]->type != REDIS_REPLY_STRING ||
        strcmp(r->element[0]->str,"message") != 0) return;

    /* We are not interested in meeting ourselves */
    /* 我们对遇到自己的消息不感兴趣。 */
    if (strstr(r->element[2]->str,sentinel.myid) != NULL) return;

    sentinelProcessHelloMessage(r->element[2]->str, r->element[2]->len);
}

/* Send a "Hello" message via Pub/Sub to the specified 'ri' Redis
 * instance in order to broadcast the current configuration for this
 * master, and to advertise the existence of this Sentinel at the same time.
 *
 * The message has the following format:
 *
 * sentinel_ip,sentinel_port,sentinel_runid,current_epoch,
 * master_name,master_ip,master_port,master_config_epoch.
 *
 * Returns C_OK if the PUBLISH was queued correctly, otherwise
 * C_ERR is returned. */
/* 通过 Pub/Sub 向指定的 'ri' Redis 实例发送一条 "Hello" 消息，
 * 以广播当前主节点的配置，同时宣传此 Sentinel 的存在。
 *
 * 消息的格式如下：
 *
 * sentinel_ip,sentinel_port,sentinel_runid,current_epoch,
 * master_name,master_ip,master_port,master_config_epoch。
 *
 * 如果 PUBLISH 命令正确排队，则返回 C_OK，否则返回 C_ERR。 */
int sentinelSendHello(sentinelRedisInstance *ri) {
    char ip[NET_IP_STR_LEN];
    char payload[NET_IP_STR_LEN+1024];
    int retval;
    char *announce_ip;
    int announce_port;
    sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ? ri : ri->master;
    sentinelAddr *master_addr = sentinelGetCurrentMasterAddress(master);

    if (ri->link->disconnected) return C_ERR;

    /* Use the specified announce address if specified, otherwise try to
     * obtain our own IP address. */
    /* 如果指定了 announce 地址，则使用指定的地址，否则尝试获取我们自己的 IP 地址。 */
    if (sentinel.announce_ip) {
        announce_ip = sentinel.announce_ip;
    } else {
        if (anetFdToString(ri->link->cc->c.fd,ip,sizeof(ip),NULL,FD_TO_SOCK_NAME) == -1)
            return C_ERR;
        announce_ip = ip;
    }
    if (sentinel.announce_port) announce_port = sentinel.announce_port;
    else if (server.tls_replication && server.tls_port) announce_port = server.tls_port;
    else announce_port = server.port;

    /* 格式化并发送 Hello 消息。 */ /* Format and send the Hello message. */
    snprintf(payload,sizeof(payload),
        "%s,%d,%s,%llu," /* 关于此 Sentinel 的信息。 */ /* Info about this sentinel. */
        "%s,%s,%d,%llu", /* 关于当前主节点的信息。 */ /* Info about current master. */
        announce_ip, announce_port, sentinel.myid,
        (unsigned long long) sentinel.current_epoch,
        /* --- */
        master->name,announceSentinelAddr(master_addr),master_addr->port,
        (unsigned long long) master->config_epoch);
    retval = redisAsyncCommand(ri->link->cc,
        sentinelPublishReplyCallback, ri, "%s %s %s",
        sentinelInstanceMapCommand(ri,"PUBLISH"),
        SENTINEL_HELLO_CHANNEL,payload);
    if (retval != C_OK) return C_ERR;
    ri->link->pending_commands++;
    return C_OK;
}

/* Reset last_pub_time in all the instances in the specified dictionary
 * in order to force the delivery of a Hello update ASAP. */
/* 重置指定字典中所有实例的 last_pub_time，
 * 以便强制尽快发送 Hello 更新。 */
void sentinelForceHelloUpdateDictOfRedisInstances(dict *instances) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        if (ri->last_pub_time >= (SENTINEL_PUBLISH_PERIOD+1))
            ri->last_pub_time -= (SENTINEL_PUBLISH_PERIOD+1);
    }
    dictReleaseIterator(di);
}

/* This function forces the delivery of a "Hello" message (see
 * sentinelSendHello() top comment for further information) to all the Redis
 * and Sentinel instances related to the specified 'master'.
 *
 * It is technically not needed since we send an update to every instance
 * with a period of SENTINEL_PUBLISH_PERIOD milliseconds, however when a
 * Sentinel upgrades a configuration it is a good idea to deliver an update
 * to the other Sentinels ASAP. */
/* 该函数强制向与指定 'master' 相关的所有 Redis 和 Sentinel 实例
 * 发送一条 "Hello" 消息（有关更多信息，请参阅 sentinelSendHello() 的顶部注释）。
 *
 * 从技术上讲，这不是必需的，因为我们会以 SENTINEL_PUBLISH_PERIOD 毫秒的周期
 * 向每个实例发送一次更新，但是当一个 Sentinel 升级配置时，
 * 最好尽快向其他 Sentinel 发送更新。 */
int sentinelForceHelloUpdateForMaster(sentinelRedisInstance *master) {
    if (!(master->flags & SRI_MASTER)) return C_ERR;
    if (master->last_pub_time >= (SENTINEL_PUBLISH_PERIOD+1))
        master->last_pub_time -= (SENTINEL_PUBLISH_PERIOD+1);
    sentinelForceHelloUpdateDictOfRedisInstances(master->sentinels);
    sentinelForceHelloUpdateDictOfRedisInstances(master->slaves);
    return C_OK;
}

/* Send a PING to the specified instance and refresh the act_ping_time
 * if it is zero (that is, if we received a pong for the previous ping).
 *
 * On error zero is returned, and we can't consider the PING command
 * queued in the connection. */
/* 向指定的实例发送一个 PING，如果 act_ping_time 为 0（即我们已经
 * 收到了上一个 PING 的 PONG 回复），则刷新 act_ping_time。
 *
 * 如果出错，则返回 0，此时我们不能认为 PING 命令已排队到连接中。 */
int sentinelSendPing(sentinelRedisInstance *ri) {
    int retval = redisAsyncCommand(ri->link->cc,
        sentinelPingReplyCallback, ri, "%s",
        sentinelInstanceMapCommand(ri,"PING"));
    if (retval == C_OK) {
        ri->link->pending_commands++;
        ri->link->last_ping_time = mstime();
        /* We update the active ping time only if we received the pong for
         * the previous ping, otherwise we are technically waiting since the
         * first ping that did not receive a reply. */
        /* 只有在我们收到上一个 PING 的 PONG 回复时才更新 act_ping_time，
         * 否则从第一个未收到回复的 PING 开始，我们实际上一直在等待。 */
        if (ri->link->act_ping_time == 0)
            ri->link->act_ping_time = ri->link->last_ping_time;
        return 1;
    } else {
        return 0;
    }
}

/* Send periodic PING, INFO, and PUBLISH to the Hello channel to
 * the specified master or slave instance. */
/* 向指定的主节点或从节点实例发送周期性的 PING、INFO 和 PUBLISH 到 Hello 频道。 */
void sentinelSendPeriodicCommands(sentinelRedisInstance *ri) {
    mstime_t now = mstime();
    mstime_t info_period, ping_period;
    int retval;

    /* Return ASAP if we have already a PING or INFO already pending, or
     * in the case the instance is not properly connected. */
    /* 如果已经有一个 PING 或 INFO 命令正在等待，或者实例未正确连接，则尽快返回。 */
    if (ri->link->disconnected) return;

    /* For INFO, PING, PUBLISH that are not critical commands to send we
     * also have a limit of SENTINEL_MAX_PENDING_COMMANDS. We don't
     * want to use a lot of memory just because a link is not working
     * properly (note that anyway there is a redundant protection about this,
     * that is, the link will be disconnected and reconnected if a long
     * timeout condition is detected. */
    /* 对于 INFO、PING、PUBLISH 等非关键命令，我们也有一个 SENTINEL_MAX_PENDING_COMMANDS 的限制。
     * 我们不希望因为连接不正常而占用大量内存（注意，无论如何这里有一个冗余保护，
     * 即如果检测到长时间超时条件，连接将被断开并重新连接）。 */
    if (ri->link->pending_commands >=
        SENTINEL_MAX_PENDING_COMMANDS * ri->link->refcount) return;

    /* If this is a slave of a master in O_DOWN condition we start sending
     * it INFO every second, instead of the usual SENTINEL_INFO_PERIOD
     * period. In this state we want to closely monitor slaves in case they
     * are turned into masters by another Sentinel, or by the sysadmin.
     *
     * Similarly we monitor the INFO output more often if the slave reports
     * to be disconnected from the master, so that we can have a fresh
     * disconnection time figure. */
    /* 如果这是一个处于 O_DOWN 状态的主节点的从节点，我们会开始每秒发送一次 INFO，
     * 而不是通常的 SENTINEL_INFO_PERIOD 周期。在这种状态下，我们希望密切监控从节点，
     * 以防它们被其他 Sentinel 或系统管理员提升为主节点。
     *
     * 类似地，如果从节点报告与主节点断开连接，我们会更频繁地监控 INFO 输出，
     * 以便获取最新的断开连接时间信息。 */
    if ((ri->flags & SRI_SLAVE) &&
        ((ri->master->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS)) ||
         (ri->master_link_down_time != 0)))
    {
        info_period = 1000;
    } else {
        info_period = SENTINEL_INFO_PERIOD;
    }

    /* We ping instances every time the last received pong is older than
     * the configured 'down-after-milliseconds' time, but every second
     * anyway if 'down-after-milliseconds' is greater than 1 second. */
    /* 我们会在最后一次收到 PONG 的时间超过配置的 'down-after-milliseconds' 时间时对实例发送 PING，
     * 但如果 'down-after-milliseconds' 大于 1 秒，则无论如何每秒都会发送一次。 */
    ping_period = ri->down_after_period;
    if (ping_period > SENTINEL_PING_PERIOD) ping_period = SENTINEL_PING_PERIOD;

    /* 向主节点和从节点发送 INFO，不向 Sentinel 发送。 */ /* Send INFO to masters and slaves, not sentinels. */
    if ((ri->flags & SRI_SENTINEL) == 0 &&
        (ri->info_refresh == 0 ||
        (now - ri->info_refresh) > info_period))
    {
        retval = redisAsyncCommand(ri->link->cc,
            sentinelInfoReplyCallback, ri, "%s",
            sentinelInstanceMapCommand(ri,"INFO"));
        if (retval == C_OK) ri->link->pending_commands++;
    }

     /* 向所有三种类型的实例发送 PING。 */ /* Send PING to all the three kinds of instances. */
    if ((now - ri->link->last_pong_time) > ping_period &&
               (now - ri->link->last_ping_time) > ping_period/2) {
        sentinelSendPing(ri);
    }

    //* 向所有三种类型的实例发布 hello 消息。 */ * PUBLISH hello messages to all the three kinds of instances. */
    if ((now - ri->last_pub_time) > SENTINEL_PUBLISH_PERIOD) {
        sentinelSendHello(ri);
    }
}

/* =========================== SENTINEL command ============================= */
/* =========================== SENTINEL 命令 ============================= */
/* SENTINEL CONFIG SET <option> */
void sentinelConfigSetCommand(client *c) {
    robj *o = c->argv[3];
    robj *val = c->argv[4];
    long long numval;
    int drop_conns = 0;

    if (!strcasecmp(o->ptr, "resolve-hostnames")) {
        if ((numval = yesnotoi(val->ptr)) == -1) goto badfmt;
        sentinel.resolve_hostnames = numval;
    } else if (!strcasecmp(o->ptr, "announce-hostnames")) {
        if ((numval = yesnotoi(val->ptr)) == -1) goto badfmt;
        sentinel.announce_hostnames = numval;
    } else if (!strcasecmp(o->ptr, "announce-ip")) {
        if (sentinel.announce_ip) sdsfree(sentinel.announce_ip);
        sentinel.announce_ip = sdsnew(val->ptr);
    } else if (!strcasecmp(o->ptr, "announce-port")) {
        if (getLongLongFromObject(val, &numval) == C_ERR ||
            numval < 0 || numval > 65535)
            goto badfmt;
        sentinel.announce_port = numval;
    } else if (!strcasecmp(o->ptr, "sentinel-user")) {
        sdsfree(sentinel.sentinel_auth_user);
        sentinel.sentinel_auth_user = sdslen(val->ptr) == 0 ?
            NULL : sdsdup(val->ptr);
        drop_conns = 1;
    } else if (!strcasecmp(o->ptr, "sentinel-pass")) {
        sdsfree(sentinel.sentinel_auth_pass);
        sentinel.sentinel_auth_pass = sdslen(val->ptr) == 0 ?
            NULL : sdsdup(val->ptr);
        drop_conns = 1;
    } else {
        addReplyErrorFormat(c, "Invalid argument '%s' to SENTINEL CONFIG SET",
                            (char *) o->ptr);
        return;
    }

    sentinelFlushConfig();
    addReply(c, shared.ok);

    /* 断开 Sentinel 连接以在需要时启动重新连接。 */ /* Drop Sentinel connections to initiate a reconnect if needed. */
    if (drop_conns)
        sentinelDropConnections();

    return;

badfmt:
    addReplyErrorFormat(c, "Invalid value '%s' to SENTINEL CONFIG SET '%s'",
                        (char *) val->ptr, (char *) o->ptr);
}

/* SENTINEL 配置获取 <选项> */ /* SENTINEL CONFIG GET <option> */
void sentinelConfigGetCommand(client *c) {
    robj *o = c->argv[3];
    const char *pattern = o->ptr;
    void *replylen = addReplyDeferredLen(c);
    int matches = 0;

    if (stringmatch(pattern,"resolve-hostnames",1)) {
        addReplyBulkCString(c,"resolve-hostnames");
        addReplyBulkCString(c,sentinel.resolve_hostnames ? "yes" : "no");
        matches++;
    }

    if (stringmatch(pattern, "announce-hostnames", 1)) {
        addReplyBulkCString(c,"announce-hostnames");
        addReplyBulkCString(c,sentinel.announce_hostnames ? "yes" : "no");
        matches++;
    }

    if (stringmatch(pattern, "announce-ip", 1)) {
        addReplyBulkCString(c,"announce-ip");
        addReplyBulkCString(c,sentinel.announce_ip ? sentinel.announce_ip : "");
        matches++;
    }

    if (stringmatch(pattern, "announce-port", 1)) {
        addReplyBulkCString(c, "announce-port");
        addReplyBulkLongLong(c, sentinel.announce_port);
        matches++;
    }

    if (stringmatch(pattern, "sentinel-user", 1)) {
        addReplyBulkCString(c, "sentinel-user");
        addReplyBulkCString(c, sentinel.sentinel_auth_user ? sentinel.sentinel_auth_user : "");
        matches++;
    }

    if (stringmatch(pattern, "sentinel-pass", 1)) {
        addReplyBulkCString(c, "sentinel-pass");
        addReplyBulkCString(c, sentinel.sentinel_auth_pass ? sentinel.sentinel_auth_pass : "");
        matches++;
    }

    setDeferredMapLen(c, replylen, matches);
}

const char *sentinelFailoverStateStr(int state) {
    switch(state) {
    case SENTINEL_FAILOVER_STATE_NONE: return "none";
    case SENTINEL_FAILOVER_STATE_WAIT_START: return "wait_start";
    case SENTINEL_FAILOVER_STATE_SELECT_SLAVE: return "select_slave";
    case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE: return "send_slaveof_noone";
    case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION: return "wait_promotion";
    case SENTINEL_FAILOVER_STATE_RECONF_SLAVES: return "reconf_slaves";
    case SENTINEL_FAILOVER_STATE_UPDATE_CONFIG: return "update_config";
    default: return "unknown";
    }
}

/* Redis 实例到 Redis 协议的表示。 */ /* Redis instance to Redis protocol representation. */
void addReplySentinelRedisInstance(client *c, sentinelRedisInstance *ri) {
    char *flags = sdsempty();
    void *mbl;
    int fields = 0;

    mbl = addReplyDeferredLen(c);

    addReplyBulkCString(c,"name");
    addReplyBulkCString(c,ri->name);
    fields++;

    addReplyBulkCString(c,"ip");
    addReplyBulkCString(c,announceSentinelAddr(ri->addr));
    fields++;

    addReplyBulkCString(c,"port");
    addReplyBulkLongLong(c,ri->addr->port);
    fields++;

    addReplyBulkCString(c,"runid");
    addReplyBulkCString(c,ri->runid ? ri->runid : "");
    fields++;

    addReplyBulkCString(c,"flags");
    if (ri->flags & SRI_S_DOWN) flags = sdscat(flags,"s_down,");
    if (ri->flags & SRI_O_DOWN) flags = sdscat(flags,"o_down,");
    if (ri->flags & SRI_MASTER) flags = sdscat(flags,"master,");
    if (ri->flags & SRI_SLAVE) flags = sdscat(flags,"slave,");
    if (ri->flags & SRI_SENTINEL) flags = sdscat(flags,"sentinel,");
    if (ri->link->disconnected) flags = sdscat(flags,"disconnected,");
    if (ri->flags & SRI_MASTER_DOWN) flags = sdscat(flags,"master_down,");
    if (ri->flags & SRI_FAILOVER_IN_PROGRESS)
        flags = sdscat(flags,"failover_in_progress,");
    if (ri->flags & SRI_PROMOTED) flags = sdscat(flags,"promoted,");
    if (ri->flags & SRI_RECONF_SENT) flags = sdscat(flags,"reconf_sent,");
    if (ri->flags & SRI_RECONF_INPROG) flags = sdscat(flags,"reconf_inprog,");
    if (ri->flags & SRI_RECONF_DONE) flags = sdscat(flags,"reconf_done,");
    if (ri->flags & SRI_FORCE_FAILOVER) flags = sdscat(flags,"force_failover,");
    if (ri->flags & SRI_SCRIPT_KILL_SENT) flags = sdscat(flags,"script_kill_sent,");

    if (sdslen(flags) != 0) sdsrange(flags,0,-2); /* remove last "," */
    addReplyBulkCString(c,flags);
    sdsfree(flags);
    fields++;

    addReplyBulkCString(c,"link-pending-commands");
    addReplyBulkLongLong(c,ri->link->pending_commands);
    fields++;

    addReplyBulkCString(c,"link-refcount");
    addReplyBulkLongLong(c,ri->link->refcount);
    fields++;

    if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
        addReplyBulkCString(c,"failover-state");
        addReplyBulkCString(c,(char*)sentinelFailoverStateStr(ri->failover_state));
        fields++;
    }

    addReplyBulkCString(c,"last-ping-sent");
    addReplyBulkLongLong(c,
        ri->link->act_ping_time ? (mstime() - ri->link->act_ping_time) : 0);
    fields++;

    addReplyBulkCString(c,"last-ok-ping-reply");
    addReplyBulkLongLong(c,mstime() - ri->link->last_avail_time);
    fields++;

    addReplyBulkCString(c,"last-ping-reply");
    addReplyBulkLongLong(c,mstime() - ri->link->last_pong_time);
    fields++;

    if (ri->flags & SRI_S_DOWN) {
        addReplyBulkCString(c,"s-down-time");
        addReplyBulkLongLong(c,mstime()-ri->s_down_since_time);
        fields++;
    }

    if (ri->flags & SRI_O_DOWN) {
        addReplyBulkCString(c,"o-down-time");
        addReplyBulkLongLong(c,mstime()-ri->o_down_since_time);
        fields++;
    }

    addReplyBulkCString(c,"down-after-milliseconds");
    addReplyBulkLongLong(c,ri->down_after_period);
    fields++;

    /* Masters and Slaves */
    if (ri->flags & (SRI_MASTER|SRI_SLAVE)) {
        addReplyBulkCString(c,"info-refresh");
        addReplyBulkLongLong(c,
            ri->info_refresh ? (mstime() - ri->info_refresh) : 0);
        fields++;

        addReplyBulkCString(c,"role-reported");
        addReplyBulkCString(c, (ri->role_reported == SRI_MASTER) ? "master" :
                                                                   "slave");
        fields++;

        addReplyBulkCString(c,"role-reported-time");
        addReplyBulkLongLong(c,mstime() - ri->role_reported_time);
        fields++;
    }

    /* Only masters */
    if (ri->flags & SRI_MASTER) {
        addReplyBulkCString(c,"config-epoch");
        addReplyBulkLongLong(c,ri->config_epoch);
        fields++;

        addReplyBulkCString(c,"num-slaves");
        addReplyBulkLongLong(c,dictSize(ri->slaves));
        fields++;

        addReplyBulkCString(c,"num-other-sentinels");
        addReplyBulkLongLong(c,dictSize(ri->sentinels));
        fields++;

        addReplyBulkCString(c,"quorum");
        addReplyBulkLongLong(c,ri->quorum);
        fields++;

        addReplyBulkCString(c,"failover-timeout");
        addReplyBulkLongLong(c,ri->failover_timeout);
        fields++;

        addReplyBulkCString(c,"parallel-syncs");
        addReplyBulkLongLong(c,ri->parallel_syncs);
        fields++;

        if (ri->notification_script) {
            addReplyBulkCString(c,"notification-script");
            addReplyBulkCString(c,ri->notification_script);
            fields++;
        }

        if (ri->client_reconfig_script) {
            addReplyBulkCString(c,"client-reconfig-script");
            addReplyBulkCString(c,ri->client_reconfig_script);
            fields++;
        }
    }

    /* Only slaves */
    if (ri->flags & SRI_SLAVE) {
        addReplyBulkCString(c,"master-link-down-time");
        addReplyBulkLongLong(c,ri->master_link_down_time);
        fields++;

        addReplyBulkCString(c,"master-link-status");
        addReplyBulkCString(c,
            (ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP) ?
            "ok" : "err");
        fields++;

        addReplyBulkCString(c,"master-host");
        addReplyBulkCString(c,
            ri->slave_master_host ? ri->slave_master_host : "?");
        fields++;

        addReplyBulkCString(c,"master-port");
        addReplyBulkLongLong(c,ri->slave_master_port);
        fields++;

        addReplyBulkCString(c,"slave-priority");
        addReplyBulkLongLong(c,ri->slave_priority);
        fields++;

        addReplyBulkCString(c,"slave-repl-offset");
        addReplyBulkLongLong(c,ri->slave_repl_offset);
        fields++;

        addReplyBulkCString(c,"replica-announced");
        addReplyBulkLongLong(c,ri->replica_announced);
        fields++;
    }

    /* 仅限 Sentinels */ /* Only sentinels */
    if (ri->flags & SRI_SENTINEL) {
        addReplyBulkCString(c,"last-hello-message");
        addReplyBulkLongLong(c,mstime() - ri->last_hello_time);
        fields++;

        addReplyBulkCString(c,"voted-leader");
        addReplyBulkCString(c,ri->leader ? ri->leader : "?");
        fields++;

        addReplyBulkCString(c,"voted-leader-epoch");
        addReplyBulkLongLong(c,ri->leader_epoch);
        fields++;
    }

    setDeferredMapLen(c,mbl,fields);
}

/* Output a number of instances contained inside a dictionary as
 * Redis protocol. */
/* 以 Redis 协议的形式输出字典中包含的多个实例。 */
void addReplyDictOfRedisInstances(client *c, dict *instances) {
    dictIterator *di;
    dictEntry *de;
    long slaves = 0;
    void *replylen = addReplyDeferredLen(c);

    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        /* 不公布未宣布的副本 */ /* don't announce unannounced replicas */
        if (ri->flags & SRI_SLAVE && !ri->replica_announced) continue;
        addReplySentinelRedisInstance(c,ri);
        slaves++;
    }
    dictReleaseIterator(di);
    setDeferredArrayLen(c, replylen, slaves);
}

/* Lookup the named master into sentinel.masters.
 * If the master is not found reply to the client with an error and returns
 * NULL. */
/* 在 sentinel.masters 中查找指定名称的主节点。
 * 如果未找到主节点，则向客户端返回错误并返回 NULL。 */
sentinelRedisInstance *sentinelGetMasterByNameOrReplyError(client *c,
                        robj *name)
{
    sentinelRedisInstance *ri;

    ri = dictFetchValue(sentinel.masters,name->ptr);
    if (!ri) {
        addReplyError(c,"No such master with that name");
        return NULL;
    }
    return ri;
}

#define SENTINEL_ISQR_OK 0
#define SENTINEL_ISQR_NOQUORUM (1<<0)
#define SENTINEL_ISQR_NOAUTH (1<<1)
int sentinelIsQuorumReachable(sentinelRedisInstance *master, int *usableptr) {
    dictIterator *di;
    dictEntry *de;
    int usable = 1;           /* 可用的 Sentinels 数量。初始化为 1 以包括自身。 */ /* Number of usable Sentinels. Init to 1 to count myself. */
    int result = SENTINEL_ISQR_OK;
    int voters = dictSize(master->sentinels)+1; /* 已知的 Sentinels 数量 + 自身。 */ /* Known Sentinels + myself. */

    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->flags & (SRI_S_DOWN|SRI_O_DOWN)) continue;
        usable++;
    }
    dictReleaseIterator(di);

    if (usable < (int)master->quorum) result |= SENTINEL_ISQR_NOQUORUM;
    if (usable < voters/2+1) result |= SENTINEL_ISQR_NOAUTH;
    if (usableptr) *usableptr = usable;
    return result;
}

void sentinelCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"CKQUORUM <master-name>",
"    Check if the current Sentinel configuration is able to reach the quorum",
"    needed to failover a master and the majority needed to authorize the",
"    failover.",
"CONFIG SET <param> <value>",
"    Set a global Sentinel configuration parameter.",
"CONFIG GET <param>",
"    Get global Sentinel configuration parameter.",
"GET-MASTER-ADDR-BY-NAME <master-name>",
"    Return the ip and port number of the master with that name.",
"FAILOVER <master-name>",
"    Manually failover a master node without asking for agreement from other",
"    Sentinels",
"FLUSHCONFIG",
"    Force Sentinel to rewrite its configuration on disk, including the current",
"    Sentinel state.",
"INFO-CACHE <master-name>",
"    Return last cached INFO output from masters and all its replicas.",
"IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>",
"    Check if the master specified by ip:port is down from current Sentinel's",
"    point of view.",
"MASTER <master-name>",
"    Show the state and info of the specified master.",
"MASTERS",
"    Show a list of monitored masters and their state.",
"MONITOR <name> <ip> <port> <quorum>",
"    Start monitoring a new master with the specified name, ip, port and quorum.",
"MYID",
"    Return the ID of the Sentinel instance.",
"PENDING-SCRIPTS",
"    Get pending scripts information.",
"REMOVE <master-name>",
"    Remove master from Sentinel's monitor list.",
"REPLICAS <master-name>",
"    Show a list of replicas for this master and their state.",
"RESET <pattern>",
"    Reset masters for specific master name matching this pattern.",
"SENTINELS <master-name>",
"    Show a list of Sentinel instances for this master and their state.",
"SET <master-name> <option> <value>",
"    Set configuration paramters for certain masters.",
"SIMULATE-FAILURE (CRASH-AFTER-ELECTION|CRASH-AFTER-PROMOTION|HELP)",
"    Simulate a Sentinel crash.",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"masters")) {
        /* SENTINEL MASTERS */
        if (c->argc != 2) goto numargserr;
        addReplyDictOfRedisInstances(c,sentinel.masters);
    } else if (!strcasecmp(c->argv[1]->ptr,"master")) {
        /* SENTINEL MASTER <name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2]))
            == NULL) return;
        addReplySentinelRedisInstance(c,ri);
    } else if (!strcasecmp(c->argv[1]->ptr,"slaves") ||
               !strcasecmp(c->argv[1]->ptr,"replicas"))
    {
        /* SENTINEL REPLICAS <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
            return;
        addReplyDictOfRedisInstances(c,ri->slaves);
    } else if (!strcasecmp(c->argv[1]->ptr,"sentinels")) {
        /* SENTINEL SENTINELS <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
            return;
        addReplyDictOfRedisInstances(c,ri->sentinels);
    } else if (!strcasecmp(c->argv[1]->ptr,"myid") && c->argc == 2) {
        /* SENTINEL MYID */
        addReplyBulkCBuffer(c,sentinel.myid,CONFIG_RUN_ID_SIZE);
    } else if (!strcasecmp(c->argv[1]->ptr,"is-master-down-by-addr")) {
        /* SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>
         *
         * Arguments:
         *
         * ip and port are the ip and port of the master we want to be
         * checked by Sentinel. Note that the command will not check by
         * name but just by master, in theory different Sentinels may monitor
         * different masters with the same name.
         *
         * current-epoch is needed in order to understand if we are allowed
         * to vote for a failover leader or not. Each Sentinel can vote just
         * one time per epoch.
         *
         * runid is "*" if we are not seeking for a vote from the Sentinel
         * in order to elect the failover leader. Otherwise it is set to the
         * runid we want the Sentinel to vote if it did not already voted.
         */
        /* SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>
        *
        * 参数:
        *
        * ip 和 port 是我们希望 Sentinel 检查的主节点的 IP 和端口。
        * 注意，该命令不会通过名称检查，而是仅通过主节点检查，
        * 理论上，不同的 Sentinel 可能会监控具有相同名称的不同主节点。
        *
        * current-epoch 用于判断我们是否被允许为故障转移领导者投票。
        * 每个 Sentinel 在每个纪元中只能投票一次。
        *
        * runid 如果我们不需要 Sentinel 投票选举故障转移领导者，则为 "*"。
        * 否则，它被设置为我们希望 Sentinel 投票的 runid（如果尚未投票）。
        */
        sentinelRedisInstance *ri;
        long long req_epoch;
        uint64_t leader_epoch = 0;
        char *leader = NULL;
        long port;
        int isdown = 0;

        if (c->argc != 6) goto numargserr;
        if (getLongFromObjectOrReply(c,c->argv[3],&port,NULL) != C_OK ||
            getLongLongFromObjectOrReply(c,c->argv[4],&req_epoch,NULL)
                                                              != C_OK)
            return;
        ri = getSentinelRedisInstanceByAddrAndRunID(sentinel.masters,
            c->argv[2]->ptr,port,NULL);

        /* 它存在吗？它实际上是一个主节点吗？它是主观下线状态吗？如果是，则标记为下线。
         * 注意：如果我们处于 TILT 模式，我们始终回复 "0"。 */ /* It exists? Is actually a master? Is subjectively down? It's down.
         * Note: if we are in tilt mode we always reply with "0". */
        if (!sentinel.tilt && ri && (ri->flags & SRI_S_DOWN) &&
                                    (ri->flags & SRI_MASTER))
            isdown = 1;

        /* Vote for the master (or fetch the previous vote) if the request
         * includes a runid, otherwise the sender is not seeking for a vote. */
        /* 如果请求包含一个 runid，则为主节点投票（或获取之前的投票），
         * 否则发送方并未寻求投票。 */
        if (ri && ri->flags & SRI_MASTER && strcasecmp(c->argv[5]->ptr,"*")) {
            leader = sentinelVoteLeader(ri,(uint64_t)req_epoch,
                                            c->argv[5]->ptr,
                                            &leader_epoch);
        }

        /* Reply with a three-elements multi-bulk reply:
         * down state, leader, vote epoch. */
        /* 使用包含三个元素的多批量回复：
         * 下线状态、领导者、投票纪元。 */
        addReplyArrayLen(c,3);
        addReply(c, isdown ? shared.cone : shared.czero);
        addReplyBulkCString(c, leader ? leader : "*");
        addReplyLongLong(c, (long long)leader_epoch);
        if (leader) sdsfree(leader);
    } else if (!strcasecmp(c->argv[1]->ptr,"reset")) {
        /* SENTINEL RESET <pattern> */
        if (c->argc != 3) goto numargserr;
        addReplyLongLong(c,sentinelResetMastersByPattern(c->argv[2]->ptr,SENTINEL_GENERATE_EVENT));
    } else if (!strcasecmp(c->argv[1]->ptr,"get-master-addr-by-name")) {
        /* SENTINEL GET-MASTER-ADDR-BY-NAME <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        ri = sentinelGetMasterByName(c->argv[2]->ptr);
        if (ri == NULL) {
            addReplyNullArray(c);
        } else {
            sentinelAddr *addr = sentinelGetCurrentMasterAddress(ri);

            addReplyArrayLen(c,2);
            addReplyBulkCString(c,announceSentinelAddr(addr));
            addReplyBulkLongLong(c,addr->port);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"failover")) {
        /* SENTINEL FAILOVER <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
            return;
        if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
            addReplyError(c,"-INPROG Failover already in progress");
            return;
        }
        if (sentinelSelectSlave(ri) == NULL) {
            addReplyError(c,"-NOGOODSLAVE No suitable replica to promote");
            return;
        }
        serverLog(LL_WARNING,"Executing user requested FAILOVER of '%s'",
            ri->name);
        sentinelStartFailover(ri);
        ri->flags |= SRI_FORCE_FAILOVER;
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"pending-scripts")) {
        /* SENTINEL PENDING-SCRIPTS */

        if (c->argc != 2) goto numargserr;
        sentinelPendingScriptsCommand(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"monitor")) {
        /* SENTINEL MONITOR <name> <ip> <port> <quorum> */
        sentinelRedisInstance *ri;
        long quorum, port;
        char ip[NET_IP_STR_LEN];

        if (c->argc != 6) goto numargserr;
        if (getLongFromObjectOrReply(c,c->argv[5],&quorum,"Invalid quorum")
            != C_OK) return;
        if (getLongFromObjectOrReply(c,c->argv[4],&port,"Invalid port")
            != C_OK) return;

        if (quorum <= 0) {
            addReplyError(c, "Quorum must be 1 or greater.");
            return;
        }

        /* If resolve-hostnames is used, actual DNS resolution may take place.
         * Otherwise just validate address.
         */
        /* 如果使用了 resolve-hostnames，则可能会进行实际的 DNS 解析。
         * 否则只验证地址。
         */
        if (anetResolve(NULL,c->argv[3]->ptr,ip,sizeof(ip),
                        sentinel.resolve_hostnames ? ANET_NONE : ANET_IP_ONLY) == ANET_ERR) {
            addReplyError(c, "Invalid IP address or hostname specified");
            return;
        }

        /* 参数有效。尝试创建主节点实例。 */ /* Parameters are valid. Try to create the master instance. */
        ri = createSentinelRedisInstance(c->argv[2]->ptr,SRI_MASTER,
                c->argv[3]->ptr,port,quorum,NULL);
        if (ri == NULL) {
            addReplyError(c,sentinelCheckCreateInstanceErrors(SRI_MASTER));
        } else {
            sentinelFlushConfig();
            sentinelEvent(LL_WARNING,"+monitor",ri,"%@ quorum %d",ri->quorum);
            addReply(c,shared.ok);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"flushconfig")) {
        if (c->argc != 2) goto numargserr;
        sentinelFlushConfig();
        addReply(c,shared.ok);
        return;
    } else if (!strcasecmp(c->argv[1]->ptr,"remove")) {
        /* SENTINEL REMOVE <name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2]))
            == NULL) return;
        sentinelEvent(LL_WARNING,"-monitor",ri,"%@");
        dictDelete(sentinel.masters,c->argv[2]->ptr);
        sentinelFlushConfig();
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"ckquorum")) {
        /* SENTINEL CKQUORUM <name> */
        sentinelRedisInstance *ri;
        int usable;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2]))
            == NULL) return;
        int result = sentinelIsQuorumReachable(ri,&usable);
        if (result == SENTINEL_ISQR_OK) {
            addReplySds(c, sdscatfmt(sdsempty(),
                "+OK %i usable Sentinels. Quorum and failover authorization "
                "can be reached\r\n",usable));
        } else {
            sds e = sdscatfmt(sdsempty(),
                "-NOQUORUM %i usable Sentinels. ",usable);
            if (result & SENTINEL_ISQR_NOQUORUM)
                e = sdscat(e,"Not enough available Sentinels to reach the"
                             " specified quorum for this master");
            if (result & SENTINEL_ISQR_NOAUTH) {
                if (result & SENTINEL_ISQR_NOQUORUM) e = sdscat(e,". ");
                e = sdscat(e, "Not enough available Sentinels to reach the"
                              " majority and authorize a failover");
            }
            addReplyErrorSds(c,e);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"set")) {
        if (c->argc < 3) goto numargserr;
        sentinelSetCommand(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"config")) {
        if (c->argc < 3) goto numargserr;
        if (!strcasecmp(c->argv[2]->ptr,"set") && c->argc == 5)
            sentinelConfigSetCommand(c);
        else if (!strcasecmp(c->argv[2]->ptr,"get") && c->argc == 4)
            sentinelConfigGetCommand(c);
        else
            addReplyError(c, "Only SENTINEL CONFIG GET <option> / SET <option> <value> are supported.");
    } else if (!strcasecmp(c->argv[1]->ptr,"info-cache")) {
        /* SENTINEL INFO-CACHE <name> */
        if (c->argc < 2) goto numargserr;
        mstime_t now = mstime();

        /* Create an ad-hoc dictionary type so that we can iterate
         * a dictionary composed of just the master groups the user
         * requested. */
        /* 创建一个临时字典类型，以便我们可以迭代
         * 一个仅包含用户请求的主节点组的字典。 */
        dictType copy_keeper = instancesDictType;
        copy_keeper.valDestructor = NULL;
        dict *masters_local = sentinel.masters;
        if (c->argc > 2) {
            masters_local = dictCreate(&copy_keeper, NULL);

            for (int i = 2; i < c->argc; i++) {
                sentinelRedisInstance *ri;
                ri = sentinelGetMasterByName(c->argv[i]->ptr);
                if (!ri) continue; /* 忽略不存在的名称 */ /* ignore non-existing names */
                dictAdd(masters_local, ri->name, ri);
            }
        }

        /* Reply format:
         *   1.) master name
         *   2.) 1.) info from master
         *       2.) info from replica
         *       ...
         *   3.) other master name
         *   ...
         */
        /* 回复格式:
         *   1.) 主节点名称
         *   2.) 1.) 主节点的信息
         *       2.) 副本的信息
         *       ...
         *   3.) 另一个主节点名称
         *   ...
         */
        addReplyArrayLen(c,dictSize(masters_local) * 2);

        dictIterator  *di;
        dictEntry *de;
        di = dictGetIterator(masters_local);
        while ((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            addReplyBulkCBuffer(c,ri->name,strlen(ri->name));
            addReplyArrayLen(c,dictSize(ri->slaves) + 1); /* +1 for self */
            addReplyArrayLen(c,2);
            addReplyLongLong(c,
                ri->info_refresh ? (now - ri->info_refresh) : 0);
            if (ri->info)
                addReplyBulkCBuffer(c,ri->info,sdslen(ri->info));
            else
                addReplyNull(c);

            dictIterator *sdi;
            dictEntry *sde;
            sdi = dictGetIterator(ri->slaves);
            while ((sde = dictNext(sdi)) != NULL) {
                sentinelRedisInstance *sri = dictGetVal(sde);
                addReplyArrayLen(c,2);
                addReplyLongLong(c,
                    ri->info_refresh ? (now - sri->info_refresh) : 0);
                if (sri->info)
                    addReplyBulkCBuffer(c,sri->info,sdslen(sri->info));
                else
                    addReplyNull(c);
            }
            dictReleaseIterator(sdi);
        }
        dictReleaseIterator(di);
        if (masters_local != sentinel.masters) dictRelease(masters_local);
    } else if (!strcasecmp(c->argv[1]->ptr,"simulate-failure")) {
        /* SENTINEL SIMULATE-FAILURE <flag> <flag> ... <flag> */
        int j;

        sentinel.simfailure_flags = SENTINEL_SIMFAILURE_NONE;
        for (j = 2; j < c->argc; j++) {
            if (!strcasecmp(c->argv[j]->ptr,"crash-after-election")) {
                sentinel.simfailure_flags |=
                    SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION;
                serverLog(LL_WARNING,"Failure simulation: this Sentinel "
                    "will crash after being successfully elected as failover "
                    "leader");
            } else if (!strcasecmp(c->argv[j]->ptr,"crash-after-promotion")) {
                sentinel.simfailure_flags |=
                    SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION;
                serverLog(LL_WARNING,"Failure simulation: this Sentinel "
                    "will crash after promoting the selected replica to master");
            } else if (!strcasecmp(c->argv[j]->ptr,"help")) {
                addReplyArrayLen(c,2);
                addReplyBulkCString(c,"crash-after-election");
                addReplyBulkCString(c,"crash-after-promotion");
            } else {
                addReplyError(c,"Unknown failure simulation specified");
                return;
            }
        }
        addReply(c,shared.ok);
    } else {
        addReplySubcommandSyntaxError(c);
    }
    return;

numargserr:
    addReplyErrorFormat(c,"Wrong number of arguments for 'sentinel %s'",
                          (char*)c->argv[1]->ptr);
}

#define info_section_from_redis(section_name) do { \
    if (defsections || allsections || !strcasecmp(section,section_name)) { \
        sds redissection; \
        if (sections++) info = sdscat(info,"\r\n"); \
        redissection = genRedisInfoString(section_name); \
        info = sdscatlen(info,redissection,sdslen(redissection)); \
        sdsfree(redissection); \
    } \
} while(0)

/* SENTINEL INFO [section] */
void sentinelInfoCommand(client *c) {
    if (c->argc > 2) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    int defsections = 0, allsections = 0;
    char *section = c->argc == 2 ? c->argv[1]->ptr : NULL;
    if (section) {
        allsections = !strcasecmp(section,"all");
        defsections = !strcasecmp(section,"default");
    } else {
        defsections = 1;
    }

    int sections = 0;
    sds info = sdsempty();

    info_section_from_redis("server");
    info_section_from_redis("clients");
    info_section_from_redis("cpu");
    info_section_from_redis("stats");

    if (defsections || allsections || !strcasecmp(section,"sentinel")) {
        dictIterator *di;
        dictEntry *de;
        int master_id = 0;

        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Sentinel\r\n"
            "sentinel_masters:%lu\r\n"
            "sentinel_tilt:%d\r\n"
            "sentinel_running_scripts:%d\r\n"
            "sentinel_scripts_queue_length:%ld\r\n"
            "sentinel_simulate_failure_flags:%lu\r\n",
            dictSize(sentinel.masters),
            sentinel.tilt,
            sentinel.running_scripts,
            listLength(sentinel.scripts_queue),
            sentinel.simfailure_flags);

        di = dictGetIterator(sentinel.masters);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            char *status = "ok";

            if (ri->flags & SRI_O_DOWN) status = "odown";
            else if (ri->flags & SRI_S_DOWN) status = "sdown";
            info = sdscatprintf(info,
                "master%d:name=%s,status=%s,address=%s:%d,"
                "slaves=%lu,sentinels=%lu\r\n",
                master_id++, ri->name, status,
                announceSentinelAddr(ri->addr), ri->addr->port,
                dictSize(ri->slaves),
                dictSize(ri->sentinels)+1);
        }
        dictReleaseIterator(di);
    }

    addReplyBulkSds(c, info);
}

/* Implements Sentinel version of the ROLE command. The output is
 * "sentinel" and the list of currently monitored master names. */
/* 实现 Sentinel 版本的 ROLE 命令。输出为
 * "sentinel" 和当前监控的主节点名称列表。
 */
void sentinelRoleCommand(client *c) {
    dictIterator *di;
    dictEntry *de;

    addReplyArrayLen(c,2);
    addReplyBulkCBuffer(c,"sentinel",8);
    addReplyArrayLen(c,dictSize(sentinel.masters));

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        addReplyBulkCString(c,ri->name);
    }
    dictReleaseIterator(di);
}

/* SENTINEL SET <mastername> [<option> <value> ...] */
void sentinelSetCommand(client *c) {
    sentinelRedisInstance *ri;
    int j, changes = 0;
    int badarg = 0; /* 错误参数位置，用于错误报告。 */ /* Bad argument position for error reporting. */
    char *option;
    int redacted;

    if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2]))
        == NULL) return;

    /* 处理选项 - 值对。 */ /* Process option - value pairs. */
    for (j = 3; j < c->argc; j++) {
        int moreargs = (c->argc-1) - j;
        option = c->argv[j]->ptr;
        long long ll;
        int old_j = j; /* 用于确定记录什么作为事件。 */ /* Used to know what to log as an event. */
        redacted = 0;

        if (!strcasecmp(option,"down-after-milliseconds") && moreargs > 0) {
            /* down-after-millisecodns <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->down_after_period = ll;
            sentinelPropagateDownAfterPeriod(ri);
            changes++;
        } else if (!strcasecmp(option,"failover-timeout") && moreargs > 0) {
            /* failover-timeout <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->failover_timeout = ll;
            changes++;
        } else if (!strcasecmp(option,"parallel-syncs") && moreargs > 0) {
            /* parallel-syncs <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->parallel_syncs = ll;
            changes++;
        } else if (!strcasecmp(option,"notification-script") && moreargs > 0) {
            /* notification-script <path> */
            char *value = c->argv[++j]->ptr;
            if (sentinel.deny_scripts_reconfig) {
                addReplyError(c,
                    "Reconfiguration of scripts path is denied for "
                    "security reasons. Check the deny-scripts-reconfig "
                    "configuration directive in your Sentinel configuration");
                goto seterr;
            }

            if (strlen(value) && access(value,X_OK) == -1) {
                addReplyError(c,
                    "Notification script seems non existing or non executable");
                goto seterr;
            }
            sdsfree(ri->notification_script);
            ri->notification_script = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"client-reconfig-script") && moreargs > 0) {
            /* client-reconfig-script <path> */
            char *value = c->argv[++j]->ptr;
            if (sentinel.deny_scripts_reconfig) {
                addReplyError(c,
                    "Reconfiguration of scripts path is denied for "
                    "security reasons. Check the deny-scripts-reconfig "
                    "configuration directive in your Sentinel configuration");
                goto seterr;
            }

            if (strlen(value) && access(value,X_OK) == -1) {
                addReplyError(c,
                    "Client reconfiguration script seems non existing or "
                    "non executable");
                goto seterr;
            }
            sdsfree(ri->client_reconfig_script);
            ri->client_reconfig_script = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"auth-pass") && moreargs > 0) {
            /* auth-pass <password> */
            char *value = c->argv[++j]->ptr;
            sdsfree(ri->auth_pass);
            ri->auth_pass = strlen(value) ? sdsnew(value) : NULL;
            changes++;
            redacted = 1;
        } else if (!strcasecmp(option,"auth-user") && moreargs > 0) {
            /* auth-user <username> */
            char *value = c->argv[++j]->ptr;
            sdsfree(ri->auth_user);
            ri->auth_user = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"quorum") && moreargs > 0) {
            /* quorum <count> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->quorum = ll;
            changes++;
        } else if (!strcasecmp(option,"rename-command") && moreargs > 1) {
            /* rename-command <oldname> <newname> */
            sds oldname = c->argv[++j]->ptr;
            sds newname = c->argv[++j]->ptr;

            if ((sdslen(oldname) == 0) || (sdslen(newname) == 0)) {
                badarg = sdslen(newname) ? j-1 : j;
                goto badfmt;
            }

            /* 删除此命令的任何旧重命名。 */ /* Remove any older renaming for this command. */
            dictDelete(ri->renamed_commands,oldname);

            /* 如果目标名称与源名称相同，则无需添加映射到自身的条目。 */ /* If the target name is the same as the source name there
             * is no need to add an entry mapping to itself. */
            if (!dictSdsKeyCaseCompare(NULL,oldname,newname)) {
                oldname = sdsdup(oldname);
                newname = sdsdup(newname);
                dictAdd(ri->renamed_commands,oldname,newname);
            }
            changes++;
        } else {
            addReplyErrorFormat(c,"Unknown option or number of arguments for "
                                  "SENTINEL SET '%s'", option);
            goto seterr;
        }

        /* 记录事件。 */ /* Log the event. */
        int numargs = j-old_j+1;
        switch(numargs) {
        case 2:
            sentinelEvent(LL_WARNING,"+set",ri,"%@ %s %s",(char*)c->argv[old_j]->ptr,
                                                          redacted ? "******" : (char*)c->argv[old_j+1]->ptr);
            break;
        case 3:
            sentinelEvent(LL_WARNING,"+set",ri,"%@ %s %s %s",(char*)c->argv[old_j]->ptr,
                                                             (char*)c->argv[old_j+1]->ptr,
                                                             (char*)c->argv[old_j+2]->ptr);
            break;
        default:
            sentinelEvent(LL_WARNING,"+set",ri,"%@ %s",(char*)c->argv[old_j]->ptr);
            break;
        }
    }

    if (changes) sentinelFlushConfig();
    addReply(c,shared.ok);
    return;

badfmt: /* 格式错误 */ /* Bad format errors */
    addReplyErrorFormat(c,"Invalid argument '%s' for SENTINEL SET '%s'",
        (char*)c->argv[badarg]->ptr,option);
seterr:
    if (changes) sentinelFlushConfig();
    return;
}

/* Our fake PUBLISH command: it is actually useful only to receive hello messages
 * from the other sentinel instances, and publishing to a channel other than
 * SENTINEL_HELLO_CHANNEL is forbidden.
 *
 * Because we have a Sentinel PUBLISH, the code to send hello messages is the same
 * for all the three kind of instances: masters, slaves, sentinels. */
/* 我们的伪 PUBLISH 命令：它实际上仅用于接收其他 Sentinel 实例的 hello 消息，
 * 并且禁止发布到 SENTINEL_HELLO_CHANNEL 以外的频道。
 *
 * 由于我们有一个 Sentinel PUBLISH，发送 hello 消息的代码对三种实例类型：
 * 主节点、从节点、Sentinel，都是相同的。 */
void sentinelPublishCommand(client *c) {
    if (strcmp(c->argv[1]->ptr,SENTINEL_HELLO_CHANNEL)) {
        addReplyError(c, "Only HELLO messages are accepted by Sentinel instances.");
        return;
    }
    sentinelProcessHelloMessage(c->argv[2]->ptr,sdslen(c->argv[2]->ptr));
    addReplyLongLong(c,1);
}

/* ===================== SENTINEL 可用性检查 ======================= */ /* ===================== SENTINEL availability checks ======================= */

/* 从我们的角度来看，这个实例是否下线了？ */ /* Is this instance down from our point of view? */
void sentinelCheckSubjectivelyDown(sentinelRedisInstance *ri) {
    mstime_t elapsed = 0;

    if (ri->link->act_ping_time)
        elapsed = mstime() - ri->link->act_ping_time;
    else if (ri->link->disconnected)
        elapsed = mstime() - ri->link->last_avail_time;

    /* Check if we are in need for a reconnection of one of the
     * links, because we are detecting low activity.
     *
     * 1) Check if the command link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have a
     *    pending ping for more than half the timeout. */
    /* 检查我们是否需要重新连接某个链接，因为我们检测到活动较低。
     *
     * 1) 检查命令链接是否看起来已连接，连接时间不少于 SENTINEL_MIN_LINK_RECONNECT_PERIOD，
     *    但仍然有一个挂起的 ping 超过超时的一半。 */
    if (ri->link->cc &&
        (mstime() - ri->link->cc_conn_time) >
        SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        ri->link->act_ping_time != 0 && /* There is a pending ping... */
        /* 挂起的 ping 被延迟，并且我们也没有收到错误回复。 */ /* The pending ping is delayed, and we did not receive
         * error replies as well. */
        (mstime() - ri->link->act_ping_time) > (ri->down_after_period/2) &&
        (mstime() - ri->link->last_pong_time) > (ri->down_after_period/2))
    {
        instanceLinkCloseConnection(ri->link,ri->link->cc);
    }

    /* 2) Check if the pubsub link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have no
     *    activity in the Pub/Sub channel for more than
     *    SENTINEL_PUBLISH_PERIOD * 3.
     */
    /* 2) 检查 pubsub 链接是否看起来已连接，连接时间不少于 SENTINEL_MIN_LINK_RECONNECT_PERIOD，
     *    但在 Pub/Sub 频道中超过 SENTINEL_PUBLISH_PERIOD * 3 的时间没有活动。 */
    if (ri->link->pc &&
        (mstime() - ri->link->pc_conn_time) >
         SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        (mstime() - ri->link->pc_last_activity) > (SENTINEL_PUBLISH_PERIOD*3))
    {
        instanceLinkCloseConnection(ri->link,ri->link->pc);
    }

    /* Update the SDOWN flag. We believe the instance is SDOWN if:
     *
     * 1) It is not replying.
     * 2) We believe it is a master, it reports to be a slave for enough time
     *    to meet the down_after_period, plus enough time to get two times
     *    INFO report from the instance. */
    /* 更新 SDOWN 标志。我们认为实例是 SDOWN 的，如果：
     *
     * 1) 它没有回复。
     * 2) 我们认为它是主节点，但它报告为从节点的时间足够长，
     *    达到了 down_after_period，加上足够的时间以从实例获取两次 INFO 报告。 */
    if (elapsed > ri->down_after_period ||
        (ri->flags & SRI_MASTER &&
         ri->role_reported == SRI_SLAVE &&
         mstime() - ri->role_reported_time >
          (ri->down_after_period+SENTINEL_INFO_PERIOD*2)))
    {
        /* 主观下线 */ /* Is subjectively down */
        if ((ri->flags & SRI_S_DOWN) == 0) {
            sentinelEvent(LL_WARNING,"+sdown",ri,"%@");
            ri->s_down_since_time = mstime();
            ri->flags |= SRI_S_DOWN;
        }
    } else {
       /* 主观上线 */ /* Is subjectively up */
        if (ri->flags & SRI_S_DOWN) {
            sentinelEvent(LL_WARNING,"-sdown",ri,"%@");
            ri->flags &= ~(SRI_S_DOWN|SRI_SCRIPT_KILL_SENT);
        }
    }
}

/* Is this instance down according to the configured quorum?
 *
 * Note that ODOWN is a weak quorum, it only means that enough Sentinels
 * reported in a given time range that the instance was not reachable.
 * However messages can be delayed so there are no strong guarantees about
 * N instances agreeing at the same time about the down state. */
/* 根据配置的法定人数，该实例是否下线？
 *
 * 注意，ODOWN 是一个弱法定人数，它仅意味着在给定的时间范围内，
 * 有足够多的 Sentinel 报告实例不可达。
 * 然而，消息可能会延迟，因此无法强保证 N 个实例同时同意下线状态。 */
void sentinelCheckObjectivelyDown(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    unsigned int quorum = 0, odown = 0;

    if (master->flags & SRI_S_DOWN) {
        /* 是否有足够的 Sentinel 认为主节点下线？ */ /* Is down for enough sentinels? */
        quorum = 1; /* the current sentinel. */
        /* Count all the other sentinels. */
        /* 统计所有其他 Sentinel。 */
        di = dictGetIterator(master->sentinels);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);

            if (ri->flags & SRI_MASTER_DOWN) quorum++;
        }
        dictReleaseIterator(di);
        if (quorum >= master->quorum) odown = 1;
    }

    /* 根据结果设置标志。 */ /* Set the flag accordingly to the outcome. */
    if (odown) {
        if ((master->flags & SRI_O_DOWN) == 0) {
            sentinelEvent(LL_WARNING,"+odown",master,"%@ #quorum %d/%d",
                quorum, master->quorum);
            master->flags |= SRI_O_DOWN;
            master->o_down_since_time = mstime();
        }
    } else {
        if (master->flags & SRI_O_DOWN) {
            sentinelEvent(LL_WARNING,"-odown",master,"%@");
            master->flags &= ~SRI_O_DOWN;
        }
    }
}

/* Receive the SENTINEL is-master-down-by-addr reply, see the
 * sentinelAskMasterStateToOtherSentinels() function for more information. */
/* 接收 SENTINEL is-master-down-by-addr 的回复，
 * 有关更多信息，请参阅 sentinelAskMasterStateToOtherSentinels() 函数。 */
void sentinelReceiveIsMasterDownReply(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    /* Ignore every error or unexpected reply.
     * Note that if the command returns an error for any reason we'll
     * end clearing the SRI_MASTER_DOWN flag for timeout anyway. */
    /* 忽略所有错误或意外的回复。
     * 注意，如果由于任何原因命令返回错误，我们最终仍会因为超时清除 SRI_MASTER_DOWN 标志。 */
    if (r->type == REDIS_REPLY_ARRAY && r->elements == 3 &&
        r->element[0]->type == REDIS_REPLY_INTEGER &&
        r->element[1]->type == REDIS_REPLY_STRING &&
        r->element[2]->type == REDIS_REPLY_INTEGER)
    {
        ri->last_master_down_reply_time = mstime();
        if (r->element[0]->integer == 1) {
            ri->flags |= SRI_MASTER_DOWN;
        } else {
            ri->flags &= ~SRI_MASTER_DOWN;
        }
        if (strcmp(r->element[1]->str,"*")) {
            /* 如果回复中的 runid 不是 "*"，则 Sentinel 实际上回复了一个投票。 */ /* If the runid in the reply is not "*" the Sentinel actually
             * replied with a vote. */
            sdsfree(ri->leader);
            if ((long long)ri->leader_epoch != r->element[2]->integer)
                serverLog(LL_WARNING,
                    "%s voted for %s %llu", ri->name,
                    r->element[1]->str,
                    (unsigned long long) r->element[2]->integer);
            ri->leader = sdsnew(r->element[1]->str);
            ri->leader_epoch = r->element[2]->integer;
        }
    }
}

/* If we think the master is down, we start sending
 * SENTINEL IS-MASTER-DOWN-BY-ADDR requests to other sentinels
 * in order to get the replies that allow to reach the quorum
 * needed to mark the master in ODOWN state and trigger a failover. */
/* 如果我们认为主节点下线，我们开始向其他 Sentinel 发送
 * SENTINEL IS-MASTER-DOWN-BY-ADDR 请求，以获取允许达到法定人数的回复，
 * 从而将主节点标记为 ODOWN 状态并触发故障转移。 */
#define SENTINEL_ASK_FORCED (1<<0)
void sentinelAskMasterStateToOtherSentinels(sentinelRedisInstance *master, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        mstime_t elapsed = mstime() - ri->last_master_down_reply_time;
        char port[32];
        int retval;

        /* 如果来自其他 Sentinel 的主节点状态过旧，则清除它。 */ /* If the master state from other sentinel is too old, we clear it. */
        if (elapsed > SENTINEL_ASK_PERIOD*5) {
            ri->flags &= ~SRI_MASTER_DOWN;
            sdsfree(ri->leader);
            ri->leader = NULL;
        }

        /* Only ask if master is down to other sentinels if:
         *
         * 1) We believe it is down, or there is a failover in progress.
         * 2) Sentinel is connected.
         * 3) We did not receive the info within SENTINEL_ASK_PERIOD ms. */
        /* 仅在以下情况下向其他 Sentinel 询问主节点是否下线：
         *
         * 1) 我们认为它下线了，或者正在进行故障转移。
         * 2) Sentinel 已连接。
         * 3) 我们在 SENTINEL_ASK_PERIOD 毫秒内未收到信息。 */
        if ((master->flags & SRI_S_DOWN) == 0) continue;
        if (ri->link->disconnected) continue;
        if (!(flags & SENTINEL_ASK_FORCED) &&
            mstime() - ri->last_master_down_reply_time < SENTINEL_ASK_PERIOD)
            continue;

        /* Ask */
        ll2string(port,sizeof(port),master->addr->port);
        retval = redisAsyncCommand(ri->link->cc,
                    sentinelReceiveIsMasterDownReply, ri,
                    "%s is-master-down-by-addr %s %s %llu %s",
                    sentinelInstanceMapCommand(ri,"SENTINEL"),
                    announceSentinelAddr(master->addr), port,
                    sentinel.current_epoch,
                    (master->failover_state > SENTINEL_FAILOVER_STATE_NONE) ?
                    sentinel.myid : "*");
        if (retval == C_OK) ri->link->pending_commands++;
    }
    dictReleaseIterator(di);
}

/* =============================== 故障转移 ================================= */ /* =============================== FAILOVER ================================= */

/* 因用户通过 SENTINEL simulate-failure 命令请求而崩溃。 */ /* Crash because of user request via SENTINEL simulate-failure command. */
void sentinelSimFailureCrash(void) {
    serverLog(LL_WARNING,
        "Sentinel CRASH because of SENTINEL simulate-failure");
    exit(99);
}

/* Vote for the sentinel with 'req_runid' or return the old vote if already
 * voted for the specified 'req_epoch' or one greater.
 *
 * If a vote is not available returns NULL, otherwise return the Sentinel
 * runid and populate the leader_epoch with the epoch of the vote. */
/* 为带有 'req_runid' 的 Sentinel 投票，或者如果已经为指定的 'req_epoch' 或更大的纪元投票，则返回旧票。
 *
 * 如果没有可用的投票，则返回 NULL，否则返回 Sentinel 的 runid，并用投票的纪元填充 leader_epoch。 */
char *sentinelVoteLeader(sentinelRedisInstance *master, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch) {
    if (req_epoch > sentinel.current_epoch) {
        sentinel.current_epoch = req_epoch;
        sentinelFlushConfig();
        sentinelEvent(LL_WARNING,"+new-epoch",master,"%llu",
            (unsigned long long) sentinel.current_epoch);
    }

    if (master->leader_epoch < req_epoch && sentinel.current_epoch <= req_epoch)
    {
        sdsfree(master->leader);
        master->leader = sdsnew(req_runid);
        master->leader_epoch = sentinel.current_epoch;
        sentinelFlushConfig();
        sentinelEvent(LL_WARNING,"+vote-for-leader",master,"%s %llu",
            master->leader, (unsigned long long) master->leader_epoch);
        /* If we did not voted for ourselves, set the master failover start
         * time to now, in order to force a delay before we can start a
         * failover for the same master. */
        /* 如果我们没有为自己投票，则将主节点故障转移的开始时间设置为当前时间，
         * 以强制在我们可以为同一主节点启动故障转移之前延迟一段时间。 */
        if (strcasecmp(master->leader,sentinel.myid))
            master->failover_start_time = mstime()+rand()%SENTINEL_MAX_DESYNC;
    }

    *leader_epoch = master->leader_epoch;
    return master->leader ? sdsnew(master->leader) : NULL;
}

struct sentinelLeader {
    char *runid;
    unsigned long votes;
};

/* Helper function for sentinelGetLeader, increment the counter
 * relative to the specified runid. */
/* 辅助函数，用于 sentinelGetLeader，增加与指定 runid 相关的计数器。 */
int sentinelLeaderIncr(dict *counters, char *runid) {
    dictEntry *existing, *de;
    uint64_t oldval;

    de = dictAddRaw(counters,runid,&existing);
    if (existing) {
        oldval = dictGetUnsignedIntegerVal(existing);
        dictSetUnsignedIntegerVal(existing,oldval+1);
        return oldval+1;
    } else {
        serverAssert(de != NULL);
        dictSetUnsignedIntegerVal(de,1);
        return 1;
    }
}

/* Scan all the Sentinels attached to this master to check if there
 * is a leader for the specified epoch.
 *
 * To be a leader for a given epoch, we should have the majority of
 * the Sentinels we know (ever seen since the last SENTINEL RESET) that
 * reported the same instance as leader for the same epoch. */
/* 扫描附加到该主节点的所有 Sentinel，以检查是否存在指定纪元的领导者。
 *
 * 要成为给定纪元的领导者，我们需要已知的（自上次 SENTINEL RESET 以来见过的）
 * 大多数 Sentinel 报告相同的实例为该纪元的领导者。 */
char *sentinelGetLeader(sentinelRedisInstance *master, uint64_t epoch) {
    dict *counters;
    dictIterator *di;
    dictEntry *de;
    unsigned int voters = 0, voters_quorum;
    char *myvote;
    char *winner = NULL;
    uint64_t leader_epoch;
    uint64_t max_votes = 0;

    serverAssert(master->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS));
    counters = dictCreate(&leaderVotesDictType,NULL);

    voters = dictSize(master->sentinels)+1; /* All the other sentinels and me.*/

    /* 统计所有其他 Sentinel 的投票 */ /* Count other sentinels votes */
    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        if (ri->leader != NULL && ri->leader_epoch == sentinel.current_epoch)
            sentinelLeaderIncr(counters,ri->leader);
    }
    dictReleaseIterator(di);

    /* Check what's the winner. For the winner to win, it needs two conditions:
     * 1) Absolute majority between voters (50% + 1).
     * 2) And anyway at least master->quorum votes. */
    /* 检查获胜者。获胜者需要满足两个条件：
     * 1) 在投票者中获得绝对多数（50% + 1）。
     * 2) 至少获得 master->quorum 的投票数。 */
    di = dictGetIterator(counters);
    while((de = dictNext(di)) != NULL) {
        uint64_t votes = dictGetUnsignedIntegerVal(de);

        if (votes > max_votes) {
            max_votes = votes;
            winner = dictGetKey(de);
        }
    }
    dictReleaseIterator(di);

    /* Count this Sentinel vote:
     * if this Sentinel did not voted yet, either vote for the most
     * common voted sentinel, or for itself if no vote exists at all. */
    /* 统计当前 Sentinel 的投票：
     * 如果当前 Sentinel 尚未投票，则投票给最常见的 Sentinel，
     * 或者如果没有投票，则投票给自己。 */
    if (winner)
        myvote = sentinelVoteLeader(master,epoch,winner,&leader_epoch);
    else
        myvote = sentinelVoteLeader(master,epoch,sentinel.myid,&leader_epoch);

    if (myvote && leader_epoch == epoch) {
        uint64_t votes = sentinelLeaderIncr(counters,myvote);

        if (votes > max_votes) {
            max_votes = votes;
            winner = myvote;
        }
    }

    voters_quorum = voters/2+1;
    if (winner && (max_votes < voters_quorum || max_votes < master->quorum))
        winner = NULL;

    winner = winner ? sdsnew(winner) : NULL;
    sdsfree(myvote);
    dictRelease(counters);
    return winner;
}

/* Send SLAVEOF to the specified instance, always followed by a
 * CONFIG REWRITE command in order to store the new configuration on disk
 * when possible (that is, if the Redis instance is recent enough to support
 * config rewriting, and if the server was started with a configuration file).
 *
 * If Host is NULL the function sends "SLAVEOF NO ONE".
 *
 * The command returns C_OK if the SLAVEOF command was accepted for
 * (later) delivery otherwise C_ERR. The command replies are just
 * discarded. */
/* 发送 SLAVEOF 给指定实例，始终跟随 CONFIG REWRITE 命令，
 * 以便在可能的情况下将新配置存储到磁盘上
 * （即，如果 Redis 实例足够新以支持配置重写，并且服务器是使用配置文件启动的）。
 *
 * 如果 Host 为 NULL，则函数发送 "SLAVEOF NO ONE"。
 *
 * 如果 SLAVEOF 命令被接受（稍后）发送，则命令返回 C_OK，否则返回 C_ERR。
 * 命令的回复将被忽略。 */
int sentinelSendSlaveOf(sentinelRedisInstance *ri, const sentinelAddr *addr) {
    char portstr[32];
    const char *host;
    int retval;

    /* If host is NULL we send SLAVEOF NO ONE that will turn the instance
    * into a master. */
    /* 如果 host 为 NULL，我们发送 SLAVEOF NO ONE，将实例转换为主节点。 */
    if (!addr) {
        host = "NO";
        memcpy(portstr,"ONE",4);
    } else {
        host = announceSentinelAddr(addr);
        ll2string(portstr,sizeof(portstr),addr->port);
    }

    /* In order to send SLAVEOF in a safe way, we send a transaction performing
     * the following tasks:
     * 1) Reconfigure the instance according to the specified host/port params.
     * 2) Rewrite the configuration.
     * 3) Disconnect all clients (but this one sending the command) in order
     *    to trigger the ask-master-on-reconnection protocol for connected
     *    clients.
     *
     * Note that we don't check the replies returned by commands, since we
     * will observe instead the effects in the next INFO output. */
    /* 为了以安全的方式发送 SLAVEOF，我们发送一个事务，执行以下任务：
    * 1) 根据指定的 host/port 参数重新配置实例。
    * 2) 重写配置。
    * 3) 断开所有客户端（除了发送命令的客户端），以触发连接客户端的 ask-master-on-reconnection 协议。
    *
    * 注意，我们不会检查命令返回的回复，因为我们会在下一个 INFO 输出中观察其效果。 */
    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s",
        sentinelInstanceMapCommand(ri,"MULTI"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s %s %s",
        sentinelInstanceMapCommand(ri,"SLAVEOF"),
        host, portstr);
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s REWRITE",
        sentinelInstanceMapCommand(ri,"CONFIG"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    /* CLIENT KILL TYPE <type> is only supported starting from Redis 2.8.12,
     * however sending it to an instance not understanding this command is not
     * an issue because CLIENT is variadic command, so Redis will not
     * recognized as a syntax error, and the transaction will not fail (but
     * only the unsupported command will fail). */
    /* CLIENT KILL TYPE <type> 从 Redis 2.8.12 开始支持，
    * 但是将其发送到不支持该命令的实例不会有问题，
    * 因为 CLIENT 是一个可变参数命令，因此 Redis 不会将其识别为语法错误，
    * 并且事务不会失败（只有不支持的命令会失败）。 */
    for (int type = 0; type < 2; type++) {
        retval = redisAsyncCommand(ri->link->cc,
            sentinelDiscardReplyCallback, ri, "%s KILL TYPE %s",
            sentinelInstanceMapCommand(ri,"CLIENT"),
            type == 0 ? "normal" : "pubsub");
        if (retval == C_ERR) return retval;
        ri->link->pending_commands++;
    }

    retval = redisAsyncCommand(ri->link->cc,
        sentinelDiscardReplyCallback, ri, "%s",
        sentinelInstanceMapCommand(ri,"EXEC"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    return C_OK;
}

/* 设置主节点状态以启动故障转移。 */ /* Setup the master state to start a failover. */
void sentinelStartFailover(sentinelRedisInstance *master) {
    serverAssert(master->flags & SRI_MASTER);

    master->failover_state = SENTINEL_FAILOVER_STATE_WAIT_START;
    master->flags |= SRI_FAILOVER_IN_PROGRESS;
    master->failover_epoch = ++sentinel.current_epoch;
    sentinelEvent(LL_WARNING,"+new-epoch",master,"%llu",
        (unsigned long long) sentinel.current_epoch);
    sentinelEvent(LL_WARNING,"+try-failover",master,"%@");
    master->failover_start_time = mstime()+rand()%SENTINEL_MAX_DESYNC;
    master->failover_state_change_time = mstime();
}

/* This function checks if there are the conditions to start the failover,
 * that is:
 *
 * 1) Master must be in ODOWN condition.
 * 2) No failover already in progress.
 * 3) No failover already attempted recently.
 *
 * We still don't know if we'll win the election so it is possible that we
 * start the failover but that we'll not be able to act.
 *
 * Return non-zero if a failover was started. */
/* 此函数检查是否满足启动故障转移的条件，即：
 *
 * 1) 主节点必须处于 ODOWN 状态。
 * 2) 当前没有正在进行的故障转移。
 * 3) 最近没有尝试过故障转移。
 *
 * 我们仍然不知道是否会赢得选举，因此可能会启动故障转移，
 * 但无法采取进一步操作。
 *
 * 如果启动了故障转移，则返回非零值。 */
int sentinelStartFailoverIfNeeded(sentinelRedisInstance *master) {
    /* 如果主节点未处于 O_DOWN 状态，则无法进行故障转移。 */ /* We can't failover if the master is not in O_DOWN state. */
    if (!(master->flags & SRI_O_DOWN)) return 0;

    /* 是否已经有故障转移正在进行？ */ /* Failover already in progress? */
    if (master->flags & SRI_FAILOVER_IN_PROGRESS) return 0;

    /* 上次故障转移尝试是否发生在很短的时间之前？ */ /* Last failover attempt started too little time ago? */
    if (mstime() - master->failover_start_time <
        master->failover_timeout*2)
    {
        if (master->failover_delay_logged != master->failover_start_time) {
            time_t clock = (master->failover_start_time +
                            master->failover_timeout*2) / 1000;
            char ctimebuf[26];

            ctime_r(&clock,ctimebuf);
            ctimebuf[24] = '\0';      /* 移除换行符。 */ /* Remove newline. */
            master->failover_delay_logged = master->failover_start_time;
            serverLog(LL_WARNING,
                "Next failover delay: I will not start a failover before %s",
                ctimebuf);
        }
        return 0;
    }

    sentinelStartFailover(master);
    return 1;
}

/* Select a suitable slave to promote. The current algorithm only uses
 * the following parameters:
 *
 * 1) None of the following conditions: S_DOWN, O_DOWN, DISCONNECTED.
 * 2) Last time the slave replied to ping no more than 5 times the PING period.
 * 3) info_refresh not older than 3 times the INFO refresh period.
 * 4) master_link_down_time no more than:
 *     (now - master->s_down_since_time) + (master->down_after_period * 10).
 *    Basically since the master is down from our POV, the slave reports
 *    to be disconnected no more than 10 times the configured down-after-period.
 *    This is pretty much black magic but the idea is, the master was not
 *    available so the slave may be lagging, but not over a certain time.
 *    Anyway we'll select the best slave according to replication offset.
 * 5) Slave priority can't be zero, otherwise the slave is discarded.
 *
 * Among all the slaves matching the above conditions we select the slave
 * with, in order of sorting key:
 *
 * - lower slave_priority.
 * - bigger processed replication offset.
 * - lexicographically smaller runid.
 *
 * Basically if runid is the same, the slave that processed more commands
 * from the master is selected.
 *
 * The function returns the pointer to the selected slave, otherwise
 * NULL if no suitable slave was found.
 */
/* 此函数检查是否满足启动故障转移的条件，即：
 *
 * 1) 主节点必须处于 ODOWN 状态。
 * 2) 当前没有正在进行的故障转移。
 * 3) 最近没有尝试过故障转移。
 *
 * 我们仍然不知道是否会赢得选举，因此可能会启动故障转移，
 * 但无法采取进一步操作。
 *
 * 如果启动了故障转移，则返回非零值。 */

/* 选择一个合适的从节点进行提升。当前算法仅使用以下参数：
 *
 * 1) 不满足以下任何条件：S_DOWN、O_DOWN、DISCONNECTED。
 * 2) 从节点最后一次回复 ping 的时间不超过 PING 周期的 5 倍。
 * 3) info_refresh 的时间不早于 INFO 刷新周期的 3 倍。
 * 4) master_link_down_time 不超过：
 *     (当前时间 - master->s_down_since_time) + (master->down_after_period * 10)。
 *    基本上，从我们的角度来看，主节点下线后，从节点报告断开连接的时间
 *    不超过配置的 down-after-period 的 10 倍。
 *    这有点像黑魔法，但基本思想是，主节点不可用，因此从节点可能存在延迟，
 *    但不能超过一定时间。
 *    无论如何，我们将根据复制偏移量选择最佳从节点。
 * 5) 从节点的优先级不能为零，否则将被丢弃。
 *
 * 在所有符合上述条件的从节点中，我们按以下排序键选择从节点：
 *
 * - 较低的 slave_priority。
 * - 较大的已处理复制偏移量。
 * - 字典序较小的 runid。
 *
 * 基本上，如果 runid 相同，则选择从主节点处理更多命令的从节点。
 *
 * 如果找到合适的从节点，函数返回指向该从节点的指针，否则返回 NULL。 */

/* sentinelSelectSlave() 的辅助函数。此函数由 qsort() 使用，
 * 以“更优先的从节点排在前面”的顺序对合适的从节点进行排序，
 * 以便选择列表中的第一个从节点。 */ /* Helper for sentinelSelectSlave(). This is used by qsort() in order to
 * sort suitable slaves in a "better first" order, to take the first of
 * the list. */
int compareSlavesForPromotion(const void *a, const void *b) {
    sentinelRedisInstance **sa = (sentinelRedisInstance **)a,
                          **sb = (sentinelRedisInstance **)b;
    char *sa_runid, *sb_runid;

    if ((*sa)->slave_priority != (*sb)->slave_priority)
        return (*sa)->slave_priority - (*sb)->slave_priority;

    /* If priority is the same, select the slave with greater replication
     * offset (processed more data from the master). */
    /* 如果优先级相同，则选择复制偏移量更大的从节点（处理了更多来自主节点的数据）。 */
    if ((*sa)->slave_repl_offset > (*sb)->slave_repl_offset) {
        return -1; /* a < b */
    } else if ((*sa)->slave_repl_offset < (*sb)->slave_repl_offset) {
        return 1; /* a > b */
    }

    /* If the replication offset is the same select the slave with that has
     * the lexicographically smaller runid. Note that we try to handle runid
     * == NULL as there are old Redis versions that don't publish runid in
     * INFO. A NULL runid is considered bigger than any other runid. */
    /* 如果复制偏移量相同，则选择具有字典序较小的 runid 的从节点。
    * 注意，我们尝试处理 runid == NULL 的情况，因为旧版本的 Redis 不会在 INFO 中发布 runid。
    * NULL runid 被认为比任何其他 runid 都大。 */
    sa_runid = (*sa)->runid;
    sb_runid = (*sb)->runid;
    if (sa_runid == NULL && sb_runid == NULL) return 0;
    else if (sa_runid == NULL) return 1;  /* a > b */
    else if (sb_runid == NULL) return -1; /* a < b */
    return strcasecmp(sa_runid, sb_runid);
}

sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master) {
    sentinelRedisInstance **instance =
        zmalloc(sizeof(instance[0])*dictSize(master->slaves));
    sentinelRedisInstance *selected = NULL;
    int instances = 0;
    dictIterator *di;
    dictEntry *de;
    mstime_t max_master_down_time = 0;

    if (master->flags & SRI_S_DOWN)
        max_master_down_time += mstime() - master->s_down_since_time;
    max_master_down_time += master->down_after_period * 10;

    di = dictGetIterator(master->slaves);

    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);
        mstime_t info_validity_time;

        if (slave->flags & (SRI_S_DOWN|SRI_O_DOWN)) continue;
        if (slave->link->disconnected) continue;
        if (mstime() - slave->link->last_avail_time > SENTINEL_PING_PERIOD*5) continue;
        if (slave->slave_priority == 0) continue;

        /* If the master is in SDOWN state we get INFO for slaves every second.
         * Otherwise we get it with the usual period so we need to account for
         * a larger delay. */
        /* 如果主节点处于 SDOWN 状态，我们每秒获取一次从节点的 INFO。
         * 否则，我们按照通常的周期获取，因此需要考虑更大的延迟。 */
        if (master->flags & SRI_S_DOWN)
            info_validity_time = SENTINEL_PING_PERIOD*5;
        else
            info_validity_time = SENTINEL_INFO_PERIOD*3;
        if (mstime() - slave->info_refresh > info_validity_time) continue;
        if (slave->master_link_down_time > max_master_down_time) continue;
        instance[instances++] = slave;
    }
    dictReleaseIterator(di);
    if (instances) {
        qsort(instance,instances,sizeof(sentinelRedisInstance*),
            compareSlavesForPromotion);
        selected = instance[0];
    }
    zfree(instance);
    return selected;
}

/* ---------------- 故障转移状态机实现 ------------------- */ /* ---------------- Failover state machine implementation ------------------- */
void sentinelFailoverWaitStart(sentinelRedisInstance *ri) {
    char *leader;
    int isleader;

    /* 检查我们是否是当前故障转移纪元的领导者。 */ /* Check if we are the leader for the failover epoch. */
    leader = sentinelGetLeader(ri, ri->failover_epoch);
    isleader = leader && strcasecmp(leader,sentinel.myid) == 0;
    sdsfree(leader);

    /* If I'm not the leader, and it is not a forced failover via
     * SENTINEL FAILOVER, then I can't continue with the failover. */
    /* 如果我不是领导者，并且这不是通过 SENTINEL FAILOVER 强制执行的故障转移，
     * 那么我无法继续进行故障转移。 */
    if (!isleader && !(ri->flags & SRI_FORCE_FAILOVER)) {
        int election_timeout = SENTINEL_ELECTION_TIMEOUT;

        /* The election timeout is the MIN between SENTINEL_ELECTION_TIMEOUT
         * and the configured failover timeout. */
        /* 选举超时是 SENTINEL_ELECTION_TIMEOUT 和配置的故障转移超时之间的最小值。 */
        if (election_timeout > ri->failover_timeout)
            election_timeout = ri->failover_timeout;
        /* 如果在一段时间后我仍然不是领导者，则中止故障转移。 */ /* Abort the failover if I'm not the leader after some time. */
        if (mstime() - ri->failover_start_time > election_timeout) {
            sentinelEvent(LL_WARNING,"-failover-abort-not-elected",ri,"%@");
            sentinelAbortFailover(ri);
        }
        return;
    }
    sentinelEvent(LL_WARNING,"+elected-leader",ri,"%@");
    if (sentinel.simfailure_flags & SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION)
        sentinelSimFailureCrash();
    ri->failover_state = SENTINEL_FAILOVER_STATE_SELECT_SLAVE;
    ri->failover_state_change_time = mstime();
    sentinelEvent(LL_WARNING,"+failover-state-select-slave",ri,"%@");
}

void sentinelFailoverSelectSlave(sentinelRedisInstance *ri) {
    sentinelRedisInstance *slave = sentinelSelectSlave(ri);

    /* We don't handle the timeout in this state as the function aborts
     * the failover or go forward in the next state. */
    /* 我们不会在此状态下处理超时，因为函数会中止故障转移或进入下一个状态。 */
    if (slave == NULL) {
        sentinelEvent(LL_WARNING,"-failover-abort-no-good-slave",ri,"%@");
        sentinelAbortFailover(ri);
    } else {
        sentinelEvent(LL_WARNING,"+selected-slave",slave,"%@");
        slave->flags |= SRI_PROMOTED;
        ri->promoted_slave = slave;
        ri->failover_state = SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE;
        ri->failover_state_change_time = mstime();
        sentinelEvent(LL_NOTICE,"+failover-state-send-slaveof-noone",
            slave, "%@");
    }
}

void sentinelFailoverSendSlaveOfNoOne(sentinelRedisInstance *ri) {
    int retval;

    /* We can't send the command to the promoted slave if it is now
     * disconnected. Retry again and again with this state until the timeout
     * is reached, then abort the failover. */
    /* 如果提升的从节点现在断开连接，我们无法向其发送命令。
     * 在超时之前会不断重试，然后中止故障转移。 */
    if (ri->promoted_slave->link->disconnected) {
        if (mstime() - ri->failover_state_change_time > ri->failover_timeout) {
            sentinelEvent(LL_WARNING,"-failover-abort-slave-timeout",ri,"%@");
            sentinelAbortFailover(ri);
        }
        return;
    }

    /* Send SLAVEOF NO ONE command to turn the slave into a master.
     * We actually register a generic callback for this command as we don't
     * really care about the reply. We check if it worked indirectly observing
     * if INFO returns a different role (master instead of slave). */
    /* 发送 SLAVEOF NO ONE 命令，将从节点转换为主节点。
     * 我们实际上为此命令注册了一个通用回调，因为我们并不真正关心回复。
     * 我们通过观察 INFO 是否返回不同的角色（从从节点变为主节点）间接检查它是否成功。 */
    retval = sentinelSendSlaveOf(ri->promoted_slave,NULL);
    if (retval != C_OK) return;
    sentinelEvent(LL_NOTICE, "+failover-state-wait-promotion",
        ri->promoted_slave,"%@");
    ri->failover_state = SENTINEL_FAILOVER_STATE_WAIT_PROMOTION;
    ri->failover_state_change_time = mstime();
}

/* We actually wait for promotion indirectly checking with INFO when the
 * slave turns into a master. */
/* 我们实际上是通过检查 INFO 命令，间接等待从节点变为主节点。 */
void sentinelFailoverWaitPromotion(sentinelRedisInstance *ri) {
    /* Just handle the timeout. Switching to the next state is handled
     * by the function parsing the INFO command of the promoted slave. */
    /* 仅处理超时。切换到下一个状态由解析提升从节点的 INFO 命令的函数处理。 */
    if (mstime() - ri->failover_state_change_time > ri->failover_timeout) {
        sentinelEvent(LL_WARNING,"-failover-abort-slave-timeout",ri,"%@");
        sentinelAbortFailover(ri);
    }
}

void sentinelFailoverDetectEnd(sentinelRedisInstance *master) {
    int not_reconfigured = 0, timeout = 0;
    dictIterator *di;
    dictEntry *de;
    mstime_t elapsed = mstime() - master->failover_state_change_time;

    /* 如果提升的从节点不可达，我们不能认为故障转移已完成。 */ /* We can't consider failover finished if the promoted slave is
     * not reachable. */
    if (master->promoted_slave == NULL ||
        master->promoted_slave->flags & SRI_S_DOWN) return;

    /* 一旦所有可达的从节点都正确配置，故障转移就会终止。 */ /* The failover terminates once all the reachable slaves are properly
     * configured. */
    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE)) continue;
        if (slave->flags & SRI_S_DOWN) continue;
        not_reconfigured++;
    }
    dictReleaseIterator(di);

    /* 在超时时强制结束故障转移。 */ /* Force end of failover on timeout. */
    if (elapsed > master->failover_timeout) {
        not_reconfigured = 0;
        timeout = 1;
        sentinelEvent(LL_WARNING,"+failover-end-for-timeout",master,"%@");
    }

    if (not_reconfigured == 0) {
        sentinelEvent(LL_WARNING,"+failover-end",master,"%@");
        master->failover_state = SENTINEL_FAILOVER_STATE_UPDATE_CONFIG;
        master->failover_state_change_time = mstime();
    }

    /* If I'm the leader it is a good idea to send a best effort SLAVEOF
     * command to all the slaves still not reconfigured to replicate with
     * the new master. */
    /* 如果我是领导者，最好向所有尚未重新配置的从节点发送尽力而为的 SLAVEOF
     * 命令，以使其复制新的主节点。 */
    if (timeout) {
        dictIterator *di;
        dictEntry *de;

        di = dictGetIterator(master->slaves);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *slave = dictGetVal(de);
            int retval;

            if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE|SRI_RECONF_SENT)) continue;
            if (slave->link->disconnected) continue;

            retval = sentinelSendSlaveOf(slave,master->promoted_slave->addr);
            if (retval == C_OK) {
                sentinelEvent(LL_NOTICE,"+slave-reconf-sent-be",slave,"%@");
                slave->flags |= SRI_RECONF_SENT;
            }
        }
        dictReleaseIterator(di);
    }
}

/* Send SLAVE OF <new master address> to all the remaining slaves that
 * still don't appear to have the configuration updated. */
/* 向所有仍未更新配置的剩余从节点发送 SLAVE OF <新主节点地址>。 */
void sentinelFailoverReconfNextSlave(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    int in_progress = 0;

    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        if (slave->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG))
            in_progress++;
    }
    dictReleaseIterator(di);

    di = dictGetIterator(master->slaves);
    while(in_progress < master->parallel_syncs &&
          (de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *slave = dictGetVal(de);
        int retval;

        /* 跳过提升的从节点和已配置的从节点。 */ /* Skip the promoted slave, and already configured slaves. */
        if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE)) continue;

        /* If too much time elapsed without the slave moving forward to
         * the next state, consider it reconfigured even if it is not.
         * Sentinels will detect the slave as misconfigured and fix its
         * configuration later. */
        /* 如果从节点在长时间内未进入下一个状态，即使它没有完成，
         * 也将其视为已重新配置。
         * Sentinel 将检测到从节点配置错误，并稍后修复其配置。 */
        if ((slave->flags & SRI_RECONF_SENT) &&
            (mstime() - slave->slave_reconf_sent_time) >
            SENTINEL_SLAVE_RECONF_TIMEOUT)
        {
            sentinelEvent(LL_NOTICE,"-slave-reconf-sent-timeout",slave,"%@");
            slave->flags &= ~SRI_RECONF_SENT;
            slave->flags |= SRI_RECONF_DONE;
        }

        /* 对于已断开连接或已处于 RECONF_SENT 状态的实例，无需操作。 */ /* Nothing to do for instances that are disconnected or already
         * in RECONF_SENT state. */
        if (slave->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG)) continue;
        if (slave->link->disconnected) continue;

        /* 发送 SLAVEOF <新主节点>。 */ /* Send SLAVEOF <new master>. */
        retval = sentinelSendSlaveOf(slave,master->promoted_slave->addr);
        if (retval == C_OK) {
            slave->flags |= SRI_RECONF_SENT;
            slave->slave_reconf_sent_time = mstime();
            sentinelEvent(LL_NOTICE,"+slave-reconf-sent",slave,"%@");
            in_progress++;
        }
    }
    dictReleaseIterator(di);

    /* 检查是否所有从节点都已重新配置并处理超时。 */ /* Check if all the slaves are reconfigured and handle timeout. */
    sentinelFailoverDetectEnd(master);
}

/* This function is called when the slave is in
 * SENTINEL_FAILOVER_STATE_UPDATE_CONFIG state. In this state we need
 * to remove it from the master table and add the promoted slave instead. */
/* 当从节点处于 SENTINEL_FAILOVER_STATE_UPDATE_CONFIG 状态时调用此函数。
 * 在此状态下，我们需要将其从主节点表中移除，并添加被提升的从节点。 */
void sentinelFailoverSwitchToPromotedSlave(sentinelRedisInstance *master) {
    sentinelRedisInstance *ref = master->promoted_slave ?
                                 master->promoted_slave : master;

    sentinelEvent(LL_WARNING,"+switch-master",master,"%s %s %d %s %d",
        master->name, announceSentinelAddr(master->addr), master->addr->port,
        announceSentinelAddr(ref->addr), ref->addr->port);

    sentinelResetMasterAndChangeAddress(master,ref->addr->hostname,ref->addr->port);
}

void sentinelFailoverStateMachine(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_MASTER);

    if (!(ri->flags & SRI_FAILOVER_IN_PROGRESS)) return;

    switch(ri->failover_state) {
        case SENTINEL_FAILOVER_STATE_WAIT_START:
            sentinelFailoverWaitStart(ri);
            break;
        case SENTINEL_FAILOVER_STATE_SELECT_SLAVE:
            sentinelFailoverSelectSlave(ri);
            break;
        case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE:
            sentinelFailoverSendSlaveOfNoOne(ri);
            break;
        case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION:
            sentinelFailoverWaitPromotion(ri);
            break;
        case SENTINEL_FAILOVER_STATE_RECONF_SLAVES:
            sentinelFailoverReconfNextSlave(ri);
            break;
    }
}

/* Abort a failover in progress:
 *
 * This function can only be called before the promoted slave acknowledged
 * the slave -> master switch. Otherwise the failover can't be aborted and
 * will reach its end (possibly by timeout). */
/* 中止正在进行的故障转移：
 *
 * 此函数只能在被提升的从节点确认从节点 -> 主节点切换之前调用。
 * 否则，故障转移无法中止并将完成（可能因超时）。 */

void sentinelAbortFailover(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_FAILOVER_IN_PROGRESS);
    serverAssert(ri->failover_state <= SENTINEL_FAILOVER_STATE_WAIT_PROMOTION);

    ri->flags &= ~(SRI_FAILOVER_IN_PROGRESS|SRI_FORCE_FAILOVER);
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = mstime();
    if (ri->promoted_slave) {
        ri->promoted_slave->flags &= ~SRI_PROMOTED;
        ri->promoted_slave = NULL;
    }
}

/* ======================== SENTINEL timer handler ==========================
 * This is the "main" our Sentinel, being sentinel completely non blocking
 * in design. The function is called every second.
 * -------------------------------------------------------------------------- */
/* ======================== SENTINEL 定时器处理程序 ==========================
 * 这是 Sentinel 的“主函数”，因为 Sentinel 的设计完全是非阻塞的。
 * 该函数每秒调用一次。
 * -------------------------------------------------------------------------- */

/* 为指定的 Redis 实例执行计划的操作。 */ /* Perform scheduled operations for the specified Redis instance. */
void sentinelHandleRedisInstance(sentinelRedisInstance *ri) {
    /* ========== 监控部分 ============ */ /* ========== MONITORING HALF ============ */
    /* 每种类型的实例 */ /* Every kind of instance */
    sentinelReconnectInstance(ri);
    sentinelSendPeriodicCommands(ri);

    /* ============== ACTING HALF ============= */
    /* We don't proceed with the acting half if we are in TILT mode.
     * TILT happens when we find something odd with the time, like a
     * sudden change in the clock. */
    /* ============== 执行部分 ============= */
    /* 如果我们处于 TILT 模式，则不会继续执行部分。
     * TILT 模式发生在我们发现时间异常时，比如时钟突然变化。 */
    if (sentinel.tilt) {
        if (mstime()-sentinel.tilt_start_time < SENTINEL_TILT_PERIOD) return;
        sentinel.tilt = 0;
        sentinelEvent(LL_WARNING,"-tilt",NULL,"#tilt mode exited");
    }

    /* 每种类型的实例 */ /* Every kind of instance */
    sentinelCheckSubjectivelyDown(ri);

    /* 主节点和从节点 */ /* Masters and slaves */
    if (ri->flags & (SRI_MASTER|SRI_SLAVE)) {
        /* Nothing so far. */
    }

    /* 仅主节点 */  /* Only masters */
    if (ri->flags & SRI_MASTER) {
        sentinelCheckObjectivelyDown(ri);
        if (sentinelStartFailoverIfNeeded(ri))
            sentinelAskMasterStateToOtherSentinels(ri,SENTINEL_ASK_FORCED);
        sentinelFailoverStateMachine(ri);
        sentinelAskMasterStateToOtherSentinels(ri,SENTINEL_NO_FLAGS);
    }
}

/* Perform scheduled operations for all the instances in the dictionary.
 * Recursively call the function against dictionaries of slaves. */
/* 为字典中的所有实例执行计划的操作。
 * 递归调用该函数以处理从节点的字典。 */
void sentinelHandleDictOfRedisInstances(dict *instances) {
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *switch_to_promoted = NULL;

    /* 我们需要对每个主节点执行一些操作。 */ /* There are a number of things we need to perform against every master. */
    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        sentinelHandleRedisInstance(ri);
        if (ri->flags & SRI_MASTER) {
            sentinelHandleDictOfRedisInstances(ri->slaves);
            sentinelHandleDictOfRedisInstances(ri->sentinels);
            if (ri->failover_state == SENTINEL_FAILOVER_STATE_UPDATE_CONFIG) {
                switch_to_promoted = ri;
            }
        }
    }
    if (switch_to_promoted)
        sentinelFailoverSwitchToPromotedSlave(switch_to_promoted);
    dictReleaseIterator(di);
}

/* This function checks if we need to enter the TITL mode.
 *
 * The TILT mode is entered if we detect that between two invocations of the
 * timer interrupt, a negative amount of time, or too much time has passed.
 * Note that we expect that more or less just 100 milliseconds will pass
 * if everything is fine. However we'll see a negative number or a
 * difference bigger than SENTINEL_TILT_TRIGGER milliseconds if one of the
 * following conditions happen:
 *
 * 1) The Sentinel process for some time is blocked, for every kind of
 * random reason: the load is huge, the computer was frozen for some time
 * in I/O or alike, the process was stopped by a signal. Everything.
 * 2) The system clock was altered significantly.
 *
 * Under both this conditions we'll see everything as timed out and failing
 * without good reasons. Instead we enter the TILT mode and wait
 * for SENTINEL_TILT_PERIOD to elapse before starting to act again.
 *
 * During TILT time we still collect information, we just do not act. */

/* 此函数检查是否需要进入 TILT 模式。 */
/* 如果我们检测到两次定时器中断调用之间经过了负时间或过多时间，则会进入 TILT 模式。
 * 注意，如果一切正常，我们期望大约 100 毫秒的时间间隔。
 * 然而，如果发生以下情况之一，我们会看到负数或大于 SENTINEL_TILT_TRIGGER 毫秒的时间差： */
/* 1) Sentinel 进程由于各种随机原因被阻塞了一段时间：负载过大、计算机因 I/O 等原因冻结了一段时间、
 * 进程被信号停止等。任何情况都有可能。 */
/* 2) 系统时钟被显著修改。 */
/* 在这两种情况下，我们会将所有内容视为超时和失败，尽管没有充分的理由。
 * 相反，我们会进入 TILT 模式，并等待 SENTINEL_TILT_PERIOD 时间过去后再重新开始操作。 */
/* 在 TILT 模式期间，我们仍然会收集信息，但不会执行任何操作。 */
void sentinelCheckTiltCondition(void) {
    mstime_t now = mstime();
    mstime_t delta = now - sentinel.previous_time;

    if (delta < 0 || delta > SENTINEL_TILT_TRIGGER) {
        sentinel.tilt = 1;
        sentinel.tilt_start_time = mstime();
        sentinelEvent(LL_WARNING,"+tilt",NULL,"#tilt mode entered");
    }
    sentinel.previous_time = mstime();
}

void sentinelTimer(void) {
    sentinelCheckTiltCondition();
    sentinelHandleDictOfRedisInstances(sentinel.masters);
    sentinelRunPendingScripts();
    sentinelCollectTerminatedScripts();
    sentinelKillTimedoutScripts();

    /* We continuously change the frequency of the Redis "timer interrupt"
     * in order to desynchronize every Sentinel from every other.
     * This non-determinism avoids that Sentinels started at the same time
     * exactly continue to stay synchronized asking to be voted at the
     * same time again and again (resulting in nobody likely winning the
     * election because of split brain voting). */
    /* 我们会不断更改 Redis “定时器中断”的频率，以使每个 Sentinel 之间不同步。
     * 这种非确定性可以避免同时启动的 Sentinels 始终保持同步，并在同一时间反复请求投票
     * （这可能导致由于脑裂投票而没有人能够赢得选举）。 */
    server.hz = CONFIG_DEFAULT_HZ + rand() % CONFIG_DEFAULT_HZ;
}
