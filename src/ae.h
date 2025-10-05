/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef __AE_H__
#define __AE_H__

#include "monotonic.h"

#define AE_OK 0
#define AE_ERR -1

#define AE_NONE 0       /* 没有注册事件 */ /* No events registered. */
#define AE_READABLE 1   /* 当描述符可读时触发 */ /* Fire when descriptor is readable. */
#define AE_WRITABLE 2   /* 当描述符可写时触发 */ /* Fire when descriptor is writable. */
/**
 * AE_BARRIER 的作用
 * 默认行为：
 * Redis 通常会先处理 可读事件（AE_READABLE），然后处理 可写事件（AE_WRITABLE）。
 * 这种顺序的设计是为了提高性能，允许 Redis 在读取客户端请求后立即生成响应并发送。
 * 设置 AE_BARRIER 后：
 * Redis 会反转事件的处理顺序，先处理 可写事件，再处理 可读事件。
 * 这种反转顺序确保了某些关键操作（如数据持久化或状态同步）在处理客户端请求之前完成。
 */
#define AE_BARRIER 4    /* With WRITABLE, never fire the event if the
                           READABLE event already fired in the same event
                           loop iteration. Useful when you want to persist
                           things to disk before sending replies, and want
                           to do that in a group fashion. */

#define AE_FILE_EVENTS (1<<0) // 文件事件（如 socket 可读/可写）
#define AE_TIME_EVENTS (1<<1) // 时间事件（定时器）
#define AE_ALL_EVENTS (AE_FILE_EVENTS|AE_TIME_EVENTS) // 文件事件和定时事件的组合。
#define AE_DONT_WAIT (1<<2) // 非阻塞模式，不等待事件发生，立即返回。
#define AE_CALL_BEFORE_SLEEP (1<<3) // 在事件循环休眠前调用回调。
#define AE_CALL_AFTER_SLEEP (1<<4) // 在事件循环休眠后调用回调。

#define AE_NOMORE -1
#define AE_DELETED_EVENT_ID -1

/* Macros */
#define AE_NOTUSED(V) ((void) V)

struct aeEventLoop;

/* Types and data structures */
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);
typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);

/* 文件事件结构体 */ /* File event structure */
typedef struct aeFileEvent {
    int mask; /* 事件类型掩码，可能是 AE_READABLE、AE_WRITABLE 或 AE_BARRIER */ /* one of AE_(READABLE|WRITABLE|BARRIER) */
    aeFileProc *rfileProc;  /* 可读事件处理函数 */
    aeFileProc *wfileProc;  /* 可写事件处理函数 */
    void *clientData;  /* 用户自定义数据（如 client 或 connection 指针） */
} aeFileEvent;

/* Time event structure */
typedef struct aeTimeEvent {
    long long id; /* time event identifier. */
    monotime when;
    aeTimeProc *timeProc;
    aeEventFinalizerProc *finalizerProc;
    void *clientData;
    struct aeTimeEvent *prev;
    struct aeTimeEvent *next;
    int refcount; /* refcount to prevent timer events from being
  		   * freed in recursive time event calls. */
} aeTimeEvent;

/* A fired event */
typedef struct aeFiredEvent {
    int fd;
    int mask;
} aeFiredEvent;

/* 基于事件的程序的状态 */ /* State of an event based program */
typedef struct aeEventLoop {
    int maxfd;   /* 当前已注册的最高文件描述符 */ /* highest file descriptor currently registered */
    int setsize;  /* 跟踪的最大文件描述符数量 */ /* max number of file descriptors tracked */
    long long timeEventNextId;
    aeFileEvent *events; /* 已注册的事件 */ /* Registered events */
    aeFiredEvent *fired; /* 已触发的事件 */ /* Fired events */
    aeTimeEvent *timeEventHead; /* 定时事件链表头 */
    int stop;      /* 是否停止事件循环的标志 */
    void *apidata; /* 用于轮询 API 的特定数据，对应：aeApiState */ /* This is used for polling API specific data */
    aeBeforeSleepProc *beforesleep; /* 休眠前的回调函数 */
    aeBeforeSleepProc *aftersleep;  /* 休眠后的回调函数 */
    int flags;                      /* 事件循环的标志位 */
} aeEventLoop;

/* Prototypes */
aeEventLoop *aeCreateEventLoop(int setsize);
void aeDeleteEventLoop(aeEventLoop *eventLoop);
void aeStop(aeEventLoop *eventLoop);
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData);
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask);
int aeGetFileEvents(aeEventLoop *eventLoop, int fd);
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc);
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);
int aeProcessEvents(aeEventLoop *eventLoop, int flags);
int aeWait(int fd, int mask, long long milliseconds);
void aeMain(aeEventLoop *eventLoop);
char *aeGetApiName(void);
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep);
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep);
int aeGetSetSize(aeEventLoop *eventLoop);
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize);
void aeSetDontWait(aeEventLoop *eventLoop, int noWait);

#endif
