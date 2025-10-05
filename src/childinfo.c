/*
 * Copyright (c) 2016, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include <unistd.h>

typedef struct
{
    size_t keys;   // 子进程当前已处理或统计的键数量（用于生成进度等）。
    size_t cow;    // 当前写时复制 (Copy-On-Write) 占用的内存字节数，用于父进程监控内存影响。
    monotime cow_updated; // 记录上一次更新 cow 数值的时间戳（monotime，单调时间，便于计算间隔）。
    double progress;      // 子进程任务进度（0.0~1.0），例如 RDB/AOF 重写或其它子任务的完成比例。
    childInfoType information_type; /* 枚举 childInfoType，标识当前这条信息属于哪类子进程任务（如 RDB 保存、AOF 重写、模块子进程等）。 */ /* Type of information */
} child_info_data;

/* Open a child-parent channel used in order to move information about the
 * RDB / AOF saving process from the child to the parent (for instance
 * the amount of copy on write memory used) */
/* 
 * 打开一个父子进程之间的通道，
 * 用于将 RDB/AOF 保存过程中的信息从子进程传递给父进程（例如写时复制所占用的内存量）。
 * */
void openChildInfoPipe(void)
{
    if (pipe(server.child_info_pipe) == -1)
    {
        /* On error our two file descriptors should be still set to -1,
         * but we call anyway closeChildInfoPipe() since can't hurt. */
        /* 
         * 出错时这两个文件描述符本应仍为 -1，但我们还是调用 closeChildInfoPipe()，反正无妨。
         */
        closeChildInfoPipe();
    }
    else if (anetNonBlock(NULL, server.child_info_pipe[0]) != ANET_OK)
    {
        closeChildInfoPipe();
    }
    else
    {
        server.child_info_nread = 0;
    }
}

/* Close the pipes opened with openChildInfoPipe(). */
/**
 * 关闭先前通过 openChildInfoPipe() 打开的管道。
 **/
void closeChildInfoPipe(void)
{
    if (server.child_info_pipe[0] != -1 || server.child_info_pipe[1] != -1)
    {
        close(server.child_info_pipe[0]);
        close(server.child_info_pipe[1]);
        server.child_info_pipe[0] = -1;
        server.child_info_pipe[1] = -1;
        server.child_info_nread = 0;
    }
}

/* Send save data to parent. */
/* 
 * 将保存过程的数据发送给父进程。 
 */
void sendChildInfoGeneric(childInfoType info_type, size_t keys, double progress, char *pname)
{
    if (server.child_info_pipe[1] == -1)
        return;

    static monotime cow_updated = 0;
    static uint64_t cow_update_cost = 0;
    static size_t cow = 0;

    /**
     * 所有内容清零，包括填充字节，以满足 Valgrind 的检查。
     */
    child_info_data data = {0}; /* zero everything, including padding to satisfy valgrind */

    /* When called to report current info, we need to throttle down CoW updates as they
     * can be very expensive. To do that, we measure the time it takes to get a reading
     * and schedule the next reading to happen not before time*CHILD_COW_COST_FACTOR
     * passes. */
    /*
     * 当被调用用于上报当前信息时，需要对 CoW（写时复制）统计的更新进行节流，因为获取该数值可能开销很大。
     * 做法是：测量一次读取所花费的时间，然后安排下一次读取不得早于 本次耗时 * CHILD_COW_COST_FACTOR 之后再进行。
     **/

    monotime now = getMonotonicUs();
    if (info_type != CHILD_INFO_TYPE_CURRENT_INFO || !cow_updated ||
        now - cow_updated > cow_update_cost * CHILD_COW_DUTY_CYCLE)
    {
        cow = zmalloc_get_private_dirty(-1);
        cow_updated = getMonotonicUs();
        cow_update_cost = cow_updated - now;

        if (cow)
        {
            serverLog((info_type == CHILD_INFO_TYPE_CURRENT_INFO) ? LL_VERBOSE : LL_NOTICE,
                      "%s: %zu MB of memory used by copy-on-write", pname, cow / (1024 * 1024));
        }
    }

    data.information_type = info_type;
    data.keys = keys;
    data.cow = cow;
    data.cow_updated = cow_updated;
    data.progress = progress;

    ssize_t wlen = sizeof(data);

    if (write(server.child_info_pipe[1], &data, wlen) != wlen)
    {
        /*写入父进程失败，父进程可能已经被杀死，退出。*/ /* Failed writing to parent, it could have been killed, exit. */
        serverLog(LL_WARNING, "Child failed reporting info to parent, exiting. %s", strerror(errno));
        exitFromChild(1);
    }
}

/*更新进程信息*/ /* Update Child info. */
void updateChildInfo(childInfoType information_type, size_t cow, monotime cow_updated, size_t keys, double progress)
{
    if (information_type == CHILD_INFO_TYPE_CURRENT_INFO)
    {
        server.stat_current_cow_bytes = cow;
        server.stat_current_cow_updated = cow_updated;
        server.stat_current_save_keys_processed = keys;
        if (progress != -1)
            server.stat_module_progress = progress;
    }
    else if (information_type == CHILD_INFO_TYPE_AOF_COW_SIZE)
    {
        server.stat_aof_cow_bytes = cow;
    }
    else if (information_type == CHILD_INFO_TYPE_RDB_COW_SIZE)
    {
        server.stat_rdb_cow_bytes = cow;
    }
    else if (information_type == CHILD_INFO_TYPE_MODULE_COW_SIZE)
    {
        server.stat_module_cow_bytes = cow;
    }
}

/* Read child info data from the pipe.
 * if complete data read into the buffer,
 * data is stored into *buffer, and returns 1.
 * otherwise, the partial data is left in the buffer, waiting for the next read, and returns 0. */
/**
 * 从管道中读取子进程信息。
 * 如果完整数据读入缓冲区，
 * 则将数据存入 *buffer，并返回 1。
 * 否则，部分数据保留在缓冲区中等待下次读取，并返回 0。
 */
int readChildInfo(childInfoType *information_type, size_t *cow, monotime *cow_updated, size_t *keys, double *progress)
{
    /*
     * 使用静态缓冲区配合 server.child_info_nread 处理短读（未一次读满）的情况。
     **/ /* We are using here a static buffer in combination with the server.child_info_nread to handle short reads */
    static child_info_data buffer;
    ssize_t wlen = sizeof(buffer);

    
    /* 避免覆盖（避免越界或与未处理数据重叠）。*/ /* Do not overlap */
    if (server.child_info_nread == wlen)
        server.child_info_nread = 0;

    int nread =
        read(server.child_info_pipe[0], (char *)&buffer + server.child_info_nread, wlen - server.child_info_nread);
    if (nread > 0)
    {
        server.child_info_nread += nread;
    }

    /* We have complete child info */
    /* 已获取完整的子进程信息 */
    if (server.child_info_nread == wlen)
    {
        *information_type = buffer.information_type;
        *cow = buffer.cow;
        *cow_updated = buffer.cow_updated;
        *keys = buffer.keys;
        *progress = buffer.progress;
        return 1;
    }
    else
    {
        return 0;
    }
}

/* Receive info data from child. */
/**
 * 接收来自子进程的信息数据。
 */
void receiveChildInfo(void)
{
    if (server.child_info_pipe[0] == -1)
        return;

    size_t cow;
    monotime cow_updated;
    size_t keys;
    double progress;
    childInfoType information_type;

    /* Drain the pipe and update child info so that we get the final message. */
    /* 清空管道并更新子进程信息，以获取最终消息。 */
    while (readChildInfo(&information_type, &cow, &cow_updated, &keys, &progress))
    {
        updateChildInfo(information_type, cow, cow_updated, keys, progress);
    }
}
