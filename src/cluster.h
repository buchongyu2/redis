#ifndef __CLUSTER_H
#define __CLUSTER_H

/*-----------------------------------------------------------------------------
 * Redis 集群数据结构、定义、导出的 API。 Redis cluster data structures, defines, exported API.
 *----------------------------------------------------------------------------*/

#define CLUSTER_SLOTS 16384
#define CLUSTER_OK 0             /* 一切正常 */ /* Everything looks ok */
#define CLUSTER_FAIL 1           /* 集群无法工作 */ /* The cluster can't work */
#define CLUSTER_NAMELEN 40       /* sha1 十六进制长度 */ /* sha1 hex length */
#define CLUSTER_PORT_INCR 10000  /* 集群端口 = 基础端口 + PORT_INCR */ /* Cluster port = baseport + PORT_INCR */

/* The following defines are amount of time, sometimes expressed as
 * multiplicators of the node timeout value (when ending with MULT). */
/* 以下定义的是时间量，有时以节点超时时间的倍数表示（以 MULT 结尾）。 */
#define CLUSTER_FAIL_REPORT_VALIDITY_MULT 2 /* 故障报告有效期。 */ /* Fail report validity. */
#define CLUSTER_FAIL_UNDO_TIME_MULT 2       /* 如果主节点恢复，撤销故障。 */ /* Undo fail if master is back. */
#define CLUSTER_FAIL_UNDO_TIME_ADD 10       /* 一些额外时间。 */ /* Some additional time. */
#define CLUSTER_FAILOVER_DELAY 5            /* 秒数 */ /* Seconds */
#define CLUSTER_MF_TIMEOUT 5000             /* 手动故障转移的毫秒数。 */ /* Milliseconds to do a manual failover. */
#define CLUSTER_MF_PAUSE_MULT 2             /* 主节点暂停手动故障转移的倍数。 */ /* Master pause manual failover mult. */
#define CLUSTER_SLAVE_MIGRATION_DELAY 5000  /* 从节点迁移的延迟。 */ /* Delay for slave migration. */

/* getNodeByQuery() 返回的重定向错误。 */ /* Redirection errors returned by getNodeByQuery(). */
#define CLUSTER_REDIR_NONE 0           /* 节点可以处理请求。 */ /* Node can serve the request. */
#define CLUSTER_REDIR_CROSS_SLOT 1     /* -CROSSSLOT 请求。 */ /* -CROSSSLOT request. */
#define CLUSTER_REDIR_UNSTABLE 2       /* 需要 -TRYAGAIN 重定向 */ /* -TRYAGAIN redirection required */
#define CLUSTER_REDIR_ASK 3            /* 需要 -ASK 重定向。 */ /* -ASK redirection required. */
#define CLUSTER_REDIR_MOVED 4          /* 需要 -MOVED 重定向。 */ /* -MOVED redirection required. */
#define CLUSTER_REDIR_DOWN_STATE 5     /* -CLUSTERDOWN，全局状态。 */ /* -CLUSTERDOWN, global state. */
#define CLUSTER_REDIR_DOWN_UNBOUND 6   /* -CLUSTERDOWN，未绑定槽。 */ /* -CLUSTERDOWN, unbound slot. */
#define CLUSTER_REDIR_DOWN_RO_STATE 7  /* -CLUSTERDOWN，允许读操作。 */ /* -CLUSTERDOWN, allow reads. */

struct clusterNode;

/* clusterLink encapsulates everything needed to talk with a remote node. */
/* clusterLink 封装了与远程节点通信所需的一切。 */
typedef struct clusterLink {
    mstime_t ctime;             /* 链接创建时间 */ /* Link creation time */
    connection *conn;           /* 到远程节点的连接 */ /* Connection to remote node */
    sds sndbuf;                 /* 数据包发送缓冲区 */ /* Packet send buffer */
    char *rcvbuf;               /* 数据包接收缓冲区 */ /* Packet reception buffer */
    size_t rcvbuf_len;          /* rcvbuf 已使用的大小 *//* Used size of rcvbuf */
    size_t rcvbuf_alloc;        /* rcvbuf 分配的大小 */ /* Allocated size of rcvbuf */
    struct clusterNode *node;    /* 与此链接相关的节点，如果有，否则为 NULL */ /* Node related to this link if any, or NULL */
} clusterLink;

/* 集群节点标志和宏。 */ /* Cluster node flags and macros. */
#define CLUSTER_NODE_MASTER 1     /* 该节点是主节点 */ /* The node is a master */
#define CLUSTER_NODE_SLAVE 2      /* 该节点是从节点 */ /* The node is a slave */
#define CLUSTER_NODE_PFAIL 4      /* 故障？需要确认 */ /* Failure? Need acknowledge */
#define CLUSTER_NODE_FAIL 8       /* 该节点被认为发生故障*/ /* The node is believed to be malfunctioning */
#define CLUSTER_NODE_MYSELF 16    /* 该节点是自身*/ /* This node is myself */
#define CLUSTER_NODE_HANDSHAKE 32 /* 尚未完成首次 ping 握*/ /* We have still to exchange the first ping */
#define CLUSTER_NODE_NOADDR   64  /* 向该节点发送 MEET 消息 不知道该节点的地 We don't know the address of this node */
#define CLUSTER_NODE_MEET 128     /* 主节点可用于副本迁移 Send a MEET message to this node */
#define CLUSTER_NODE_MIGRATE_TO 256 /* 从节点不会尝试故障转移  Master eligible for replica migration. */
#define CLUSTER_NODE_NOFAILOVER 512 /* 空节点名称 Slave will not try to failover. */
#define CLUSTER_NODE_NULL_NAME "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"

#define nodeIsMaster(n) ((n)->flags & CLUSTER_NODE_MASTER)
#define nodeIsSlave(n) ((n)->flags & CLUSTER_NODE_SLAVE)
#define nodeInHandshake(n) ((n)->flags & CLUSTER_NODE_HANDSHAKE)
#define nodeHasAddr(n) (!((n)->flags & CLUSTER_NODE_NOADDR))
#define nodeWithoutAddr(n) ((n)->flags & CLUSTER_NODE_NOADDR)
#define nodeTimedOut(n) ((n)->flags & CLUSTER_NODE_PFAIL)
#define nodeFailed(n) ((n)->flags & CLUSTER_NODE_FAIL)
#define nodeCantFailover(n) ((n)->flags & CLUSTER_NODE_NOFAILOVER)

/* 从属节点无法进行故障转移的原因。 */ /* Reasons why a slave is not able to failover. */
#define CLUSTER_CANT_FAILOVER_NONE 0
#define CLUSTER_CANT_FAILOVER_DATA_AGE 1
#define CLUSTER_CANT_FAILOVER_WAITING_DELAY 2
#define CLUSTER_CANT_FAILOVER_EXPIRED 3
#define CLUSTER_CANT_FAILOVER_WAITING_VOTES 4
#define CLUSTER_CANT_FAILOVER_RELOG_PERIOD (60*5) /* seconds. */

/* clusterState todo_before_sleep 标志位。 */ /* clusterState todo_before_sleep flags. */
#define CLUSTER_TODO_HANDLE_FAILOVER (1<<0)
#define CLUSTER_TODO_UPDATE_STATE (1<<1)
#define CLUSTER_TODO_SAVE_CONFIG (1<<2)
#define CLUSTER_TODO_FSYNC_CONFIG (1<<3)
#define CLUSTER_TODO_HANDLE_MANUALFAILOVER (1<<4)

/* Message types.
 *
 * Note that the PING, PONG and MEET messages are actually the same exact
 * kind of packet. PONG is the reply to ping, in the exact format as a PING,
 * while MEET is a special PING that forces the receiver to add the sender
 * as a node (if it is not already in the list). */
/* 消息类型。
 *
 * 注意 PING、PONG 和 MEET 实际上是同一种数据包。PONG 是对 PING 的回复，格式与 PING 完全一致，
 * 而 MEET 是一种特殊的 PING，会强制接收方将发送方加入节点列表（如果尚未在列表中）。 */
#define CLUSTERMSG_TYPE_PING 0          /* Ping */
#define CLUSTERMSG_TYPE_PONG 1          /* Pong (reply to Ping) */
#define CLUSTERMSG_TYPE_MEET 2          /* Meet "let's join" message */
#define CLUSTERMSG_TYPE_FAIL 3          /* Mark node xxx as failing */
#define CLUSTERMSG_TYPE_PUBLISH 4       /* Pub/Sub Publish propagation */
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST 5 /* May I failover? */
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK 6     /* Yes, you have my vote */
#define CLUSTERMSG_TYPE_UPDATE 7        /* Another node slots configuration */
#define CLUSTERMSG_TYPE_MFSTART 8       /* Pause clients for manual failover */
#define CLUSTERMSG_TYPE_MODULE 9        /* Module cluster API message. */
#define CLUSTERMSG_TYPE_COUNT 10        /* Total number of message types. */

/* Flags that a module can set in order to prevent certain Redis Cluster
 * features to be enabled. Useful when implementing a different distributed
 * system on top of Redis Cluster message bus, using modules. */
/* 模块可以设置的标志，用于阻止某些 Redis Cluster 特性被启用。
 * 当在 Redis Cluster 消息总线上实现不同的分布式系统时很有用。 */
#define CLUSTER_MODULE_FLAG_NONE 0
#define CLUSTER_MODULE_FLAG_NO_FAILOVER (1<<1)
#define CLUSTER_MODULE_FLAG_NO_REDIRECTION (1<<2)

/* This structure represent elements of node->fail_reports. */
/* 该结构体表示 node->fail_reports 的元素。 */
typedef struct clusterNodeFailReport {
    struct clusterNode *node;  /* 报告故障的节点。*/ /* Node reporting the failure condition. */
    mstime_t time;             /* 该节点最后一次报告故障的时间。*/  /* Time of the last report from this node. */
} clusterNodeFailReport;

typedef struct clusterNode {
    mstime_t ctime; /* 节点对象创建时间。*/ /* Node object creation time. */
    char name[CLUSTER_NAMELEN]; /* 节点名称，十六进制字符串，sha1 大小。*/ /* Node name, hex string, sha1-size */
    int flags;      /* CLUSTER_NODE_... 标志位。*/ /* CLUSTER_NODE_... */
    uint64_t configEpoch; /* 该节点最后一次观察到的 configEpoch。*/ /* Last configEpoch observed for this node */
    unsigned char slots[CLUSTER_SLOTS/8]; /* 该节点负责的槽位。*/ /* slots handled by this node */
    sds slots_info; /* 用字符串表示的槽位信息。*/ /* Slots info represented by string. */
    int numslots;   /* 该节点负责的槽位数量。*/ /* Number of slots handled by this node */
    int numslaves;  /* 如果是主节点，其从节点数量。*/ /* Number of slave nodes, if this is a master */
    struct clusterNode **slaves; /* 指向从节点的指针数组。*/ /* pointers to slave nodes */
    struct clusterNode *slaveof; /* 指向主节点的指针。注意：即使该节点是从节点，如果我们没有主节点信息，也可能为 NULL。*/ /* pointer to the master node. Note that it
                                    may be NULL even if the node is a slave
                                    if we don't have the master node in our
                                    tables. */
    mstime_t ping_sent;      /* 最近一次发送 ping 的时间（Unix 时间）。*/ /* Unix time we sent latest ping */
    mstime_t pong_received;  /* 最近一次收到 pong 的时间（Unix 时间）。*/ /* Unix time we received the pong */
    mstime_t data_received;  /* 最近一次收到任何数据的时间（Unix 时间）。*/ /* Unix time we received any data */
    mstime_t fail_time;      /* 设置 FAIL 标志的时间（Unix 时间）。*/ /* Unix time when FAIL flag was set */
    mstime_t voted_time;     /* 最近一次为该主节点的从节点投票的时间。*/ /* Last time we voted for a slave of this master */
    mstime_t repl_offset_time;  /* 最近一次收到该节点 offset 的时间（Unix 时间）。*/ /* Unix time we received offset for this node */
    mstime_t orphaned_time;     /* 主节点成为孤儿状态的起始时间。*/ /* Starting time of orphaned master condition */
    long long repl_offset;      /* 该节点最后一次已知的复制偏移量。*/ /* Last known repl offset for this node. */
    char ip[NET_IP_STR_LEN];    /* 该节点最近一次已知的 IP 地址。*/ /* Latest known IP address of this node */
    int port;                   /* 最近一次已知的客户端端口（TLS 或普通）。*/ /* Latest known clients port (TLS or plain). */
    int pport;                  /* 最近一次已知的客户端明文端口，仅在主端口为 TLS 时使用。*/ /* Latest known clients plaintext port. Only used
                                   if the main clients port is for TLS. */
    int cport;                  /* 最近一次已知的集群端口。*/ /* Latest known cluster port of this node. */
    clusterLink *link;          /* 与该节点的 TCP/IP 链接。*/ /* TCP/IP link with this node */
    list *fail_reports;         /* 标记该节点为故障的节点列表。*/ /* List of nodes signaling this as failing */
} clusterNode;

typedef struct clusterState {
    clusterNode *myself;  /* 当前节点。*/ /* This node */
    uint64_t currentEpoch;/* 当前纪元。*/
    int state;            /* 集群状态（CLUSTER_OK, CLUSTER_FAIL, ...）。*/ /* CLUSTER_OK, CLUSTER_FAIL, ... */
    int size;             /* 至少有一个槽的主节点数量。*/ /* Num of master nodes with at least one slot */
    dict *nodes;          /* 名字到 clusterNode 结构体的哈希表。*/ /* Hash table of name -> clusterNode structures */
    dict *nodes_black_list; /* 黑名单节点，几秒内不重新添加。*/ /* Nodes we don't re-add for a few seconds. */
    clusterNode *migrating_slots_to[CLUSTER_SLOTS];    /* 正在迁移槽的目标节点。*/
    clusterNode *importing_slots_from[CLUSTER_SLOTS];  /* 正在导入槽的来源节点。*/
    clusterNode *slots[CLUSTER_SLOTS];                 /* 每个槽对应的节点。*/
    uint64_t slots_keys_count[CLUSTER_SLOTS];          /* 每个槽的键数量。*/
    rax *slots_to_keys;                                /* 槽到键的映射。*/
    /* 以下字段用于选举时记录从节点状态。*/ /* The following fields are used to take the slave state on elections. */
    mstime_t failover_auth_time; /* 上一次或下一次选举的时间。*/ /* Time of previous or next election. */
    int failover_auth_count;     /* 到目前为止收到的选票数。*/ /* Number of votes received so far. */
    int failover_auth_sent;      /* 是否已经请求过投票。*/ /* True if we already asked for votes. */
    int failover_auth_rank;      /* 当前授权请求中该从节点的排名。*/ /* This slave rank for current auth request. */
    uint64_t failover_auth_epoch; /* 当前选举的纪元。*/ /* Epoch of the current election. */
    int cant_failover_reason;     /* 当前从节点无法故障转移的原因。见 CANT_FAILOVER_* 宏。*/ /* Why a slave is currently not able to
                                   failover. See the CANT_FAILOVER_* macros. */
    /* Manual failover state in common. */
    /* 手动故障转移的通用状态。*/
    mstime_t mf_end;            /* 手动故障转移的时间限制（毫秒 Unix 时间），为零表示没有正在进行的故障转移。*/ /* Manual failover time limit (ms unixtime).
                                   It is zero if there is no MF in progress. */
    /* 主节点的手动故障转移状态。*/ /* Manual failover state of master. */
    clusterNode *mf_slave;      /* 执行手动故障转移的从节点。*/ /* Slave performing the manual failover. */
    /* 从节点的手动故障转移状态。*/ /* Manual failover state of slave. */
    long long mf_master_offset; /* 从节点开始故障转移所需的主节点 offset，未收到则为 -1。*/ /* Master offset the slave needs to start MF
                                   or -1 if still not received. */
    int mf_can_start;           /* 非零表示可以开始请求主节点投票进行手动故障转移。*/ /* If non-zero signal that the manual failover
                                   can start requesting masters vote. */
    /* 以下字段用于主节点在选举时记录状态。*/ /* The following fields are used by masters to take state on elections. */
    uint64_t lastVoteEpoch;     /* 最近一次授权投票的纪元。*/ /* Epoch of the last vote granted. */
    int todo_before_sleep;      /* clusterBeforeSleep() 前需要做的事情。*/ /* Things to do in clusterBeforeSleep(). */
    /* 按类型统计发送的消息数量。*/ /* Messages received and sent by type. */
    long long stats_bus_messages_sent[CLUSTERMSG_TYPE_COUNT];
    long long stats_bus_messages_received[CLUSTERMSG_TYPE_COUNT]; /* 按类型统计接收的消息数量。*/
    long long stats_pfail_nodes;    /* 处于 PFAIL 状态的节点数量（不包括没有地址的节点）。*/ /* Number of nodes in PFAIL status,
                                       excluding nodes without address. */
} clusterState;

/* Redis 集群消息头 Redis cluster messages header */

/* Initially we don't know our "name", but we'll find it once we connect
 * to the first node, using the getsockname() function. Then we'll use this
 * address for all the next messages. */
// 起初我们并不知道自己的“名字”，但一旦连接到第一个节点后，会通过 getsockname() 函数获取。
// 然后我们会在后续所有消息中使用这个地址。
typedef struct {
    char nodename[CLUSTER_NAMELEN];
    uint32_t ping_sent;
    uint32_t pong_received;
    char ip[NET_IP_STR_LEN];    /* 上次看到时的IP地址 */ /* IP address last time it was seen */
    uint16_t port;              /* 上次看到时的基础端口 */ /* base port last time it was seen */
    uint16_t cport;             /* 上次看到时的集群端口 */ /* cluster port last time it was seen */
    uint16_t flags;             /* node->flags 的副本 */ /* node->flags copy */
    uint16_t pport;             /* 当基础端口为TLS时的明文端口 */ /* plaintext-port, when base port is TLS */
    uint16_t notused1;
} clusterMsgDataGossip;

typedef struct {
    char nodename[CLUSTER_NAMELEN];
} clusterMsgDataFail;

typedef struct {
    uint32_t channel_len;
    uint32_t message_len;
    unsigned char bulk_data[8]; /* 仅作为占位符的8字节 */ /* 8 bytes just as placeholder. */
} clusterMsgDataPublish;

typedef struct {
    uint64_t configEpoch;           /* 指定实例的配置纪元 */ /* Config epoch of the specified instance. */
    char nodename[CLUSTER_NAMELEN]; /* 槽位所有者的名称 */ /* Name of the slots owner. */
    unsigned char slots[CLUSTER_SLOTS/8]; /* 槽位位图 *//* Slots bitmap. */
} clusterMsgDataUpdate;

typedef struct {
    uint64_t module_id;     /* 发送模块的ID */ /* ID of the sender module. */
    uint32_t len;           /* 发送模块的ID长度 */ /* ID of the sender module. */
    uint8_t type;           /* 类型，范围0到255 *//* Type from 0 to 255. */
    unsigned char bulk_data[3]; /* 仅作为占位符的3字节 */ /* 3 bytes just as placeholder. */
} clusterMsgModule;

union clusterMsgData {
    /* PING, MEET 和 PONG */ /* PING, MEET and PONG */
    struct {
        /* N个clusterMsgDataGossip结构体数组 */ /* Array of N clusterMsgDataGossip structures */
        clusterMsgDataGossip gossip[1];
    } ping;

    /* FAIL */
    struct {
        clusterMsgDataFail about;
    } fail;

    /* PUBLISH */
    struct {
        clusterMsgDataPublish msg;
    } publish;

    /* UPDATE */
    struct {
        clusterMsgDataUpdate nodecfg;
    } update;

    /* MODULE */
    struct {
        clusterMsgModule msg;
    } module;
};

#define CLUSTER_PROTO_VER 1 /* 集群总线协议版本 */ /* Cluster bus protocol version. */

typedef struct {
    char sig[4];        /* 签名 "RCmb" (Redis Cluster message bus) */ /* Signature "RCmb" (Redis Cluster message bus). */
    uint32_t totlen;    /* 此消息的总长度 */ /* Total length of this message */
    uint16_t ver;       /* 协议版本，目前设为1 */ /* Protocol version, currently set to 1. */
    uint16_t port;      /* TCP基础端口号 */ /* TCP base port number. */
    uint16_t type;      /* 消息类型 */ /* Message type */
    uint16_t count;      /* 仅用于某些类型的消息 */ /* Only used for some kind of messages. */
    uint64_t currentEpoch;  /* 发送节点对应的纪元 */ /* The epoch accordingly to the sending node. */
    uint64_t configEpoch;   /* 如果是主节点则为配置纪元，若为从节点则为其主节点最后通告的纪元 */ /* The config epoch if it's a master, or the last
                               epoch advertised by its master if it is a
                               slave. */
    uint64_t offset;        /* 如果节点为主节点则为主节点复制偏移量，若为从节点则为已处理的复制偏移量 */ /* Master replication offset if node is a master or
                           processed replication offset if node is a slave. */
    char sender[CLUSTER_NAMELEN];           /* 发送节点名称 */ /* Name of the sender node */
    unsigned char myslots[CLUSTER_SLOTS/8];
    char slaveof[CLUSTER_NAMELEN];
    char myip[NET_IP_STR_LEN];    /* 发送者IP，如果不是全为零 */ /* Sender IP, if not all zeroed. */
    char notused1[32];   /* 预留32字节供将来使用 */ /* 32 bytes reserved for future usage. */
    uint16_t pport;      /* 如果基础端口为TLS，则为发送者TCP明文端口 */ /* Sender TCP plaintext port, if base port is TLS */
    uint16_t cport;      /* 发送者TCP集群总线端口 */ /* Sender TCP cluster bus port */
    uint16_t flags;      /* 发送节点标志 */ /* Sender node flags */
    unsigned char state; /* 发送者视角下的集群状态 */ /* Cluster state from the POV of the sender */
    unsigned char mflags[3]; /* 消息标志：CLUSTERMSG_FLAG[012]_... */ /* Message flags: CLUSTERMSG_FLAG[012]_... */
    union clusterMsgData data;
} clusterMsg;

#define CLUSTERMSG_MIN_LEN (sizeof(clusterMsg)-sizeof(union clusterMsgData))

/* Message flags better specify the packet content or are used to
 * provide some information about the node state. */
/* 消息标志用于更好地指定数据包内容或用于
 * 提供一些关于节点状态的信息。 */
#define CLUSTERMSG_FLAG0_PAUSED (1<<0)   /* 主节点因手动故障转移而暂停 */ /* Master paused for manual failover. */
#define CLUSTERMSG_FLAG0_FORCEACK (1<<1) /* 即使主节点正常也给AUTH_REQUEST回复ACK */ /* Give ACK to AUTH_REQUEST even if
                                            master is up. */

/* ---------------------- cluster.c外部导出的API - -------------------- */
clusterNode *getNodeByQuery(client *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *ask);
int clusterRedirectBlockedClientIfNeeded(client *c);
void clusterRedirectClient(client *c, clusterNode *n, int hashslot, int error_code);
unsigned long getClusterConnectionsCount(void);

#endif /* __CLUSTER_H */
