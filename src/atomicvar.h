/**
 * 
 * 这段代码是一个 C 语言头文件的注释部分，描述了如何使用 C11 的 _Atomic、__atomic 或 __sync 宏来实现原子计数器（atomic counters）。如果这些功能不可用，编译时会抛出错误。
 * 导出的接口（宏）：
 * atomicIncr(var, count)：将原子计数器 var 增加 count。
 * atomicGetIncr(var, oldvalue_var, count)：获取原子计数器的当前值到 oldvalue_var，然后将计数器增加 count。
 * atomicDecr(var, count)：将原子计数器 var 减少 count。
 * atomicGet(var, dstvar)：获取原子计数器的值到 dstvar。
 * atomicSet(var, value)：将原子计数器设置为 value。
 * atomicGetWithSync(var, value)：带有线程同步的 atomicGet 操作。
 * atomicSetWithSync(var, value)：带有线程同步的 atomicSet 操作。
 */

/* This file implements atomic counters using c11 _Atomic, __atomic or __sync
 * macros if available, otherwise we will throw an error when compile.
 *
 * The exported interface is composed of three macros:
 *
 * atomicIncr(var,count) -- Increment the atomic counter
 * atomicGetIncr(var,oldvalue_var,count) -- Get and increment the atomic counter
 * atomicDecr(var,count) -- Decrement the atomic counter
 * atomicGet(var,dstvar) -- Fetch the atomic counter value
 * atomicSet(var,value)  -- Set the atomic counter value
 * atomicGetWithSync(var,value)  -- 'atomicGet' with inter-thread synchronization
 * atomicSetWithSync(var,value)  -- 'atomicSet' with inter-thread synchronization
 *
 * Never use return value from the macros, instead use the AtomicGetIncr()
 * if you need to get the current value and increment it atomically, like
 * in the following example:
 *
 *  long oldvalue;
 *  atomicGetIncr(myvar,oldvalue,1);
 *  doSomethingWith(oldvalue);
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2015, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include <pthread.h>
#include "config.h"

#ifndef __ATOMIC_VAR_H
#define __ATOMIC_VAR_H

/* 定义 redisAtomic 用于原子变量。 */ /* Define redisAtomic for atomic variable. */
#define redisAtomic

/* 为了使用 Helgrind（一个 Valgrind 工具）测试 Redis，定义以下宏是有用的，
 * 这样可以强制使用 __sync 宏：这些宏可以被 Helgrind 检测到
 * （尽管它们效率较低），从而避免报告误报。 */
// #define __ATOMIC_VAR_FORCE_SYNC_MACROS

/* To test Redis with Helgrind (a Valgrind tool) it is useful to define
 * the following macro, so that __sync macros are used: those can be detected
 * by Helgrind (even if they are less efficient) so that no false positive
 * is reported. */
// #define __ATOMIC_VAR_FORCE_SYNC_MACROS

/* There will be many false positives if we test Redis with Helgrind, since
 * Helgrind can't understand we have imposed ordering on the program, so
 * we use macros in helgrind.h to tell Helgrind inter-thread happens-before
 * relationship explicitly for avoiding false positives.
 *
 * For more details, please see: valgrind/helgrind.h and
 * https://www.valgrind.org/docs/manual/hg-manual.html#hg-manual.effective-use
 *
 * These macros take effect only when 'make helgrind', and you must first
 * install Valgrind in the default path configuration. */

/**
 * 这段注释解释了在使用 Helgrind（Valgrind 的一个工具）测试 Redis 时可能遇到的问题。由于 Helgrind 无法自动理解程序中已经定义的线程间执行顺序（happens-before 关系），会导致许多误报。
 * 为了解决这个问题，代码中引入了 helgrind.h 中的宏，用来显式地向 Helgrind 描述线程间的关系，从而避免误报。
 * 此外，这些宏只有在执行 make helgrind 时才会生效，并且需要确保 Valgrind 已正确安装。
 */
#ifdef __ATOMIC_VAR_FORCE_SYNC_MACROS
#include <valgrind/helgrind.h>
#else
#define ANNOTATE_HAPPENS_BEFORE(v) ((void) v)
#define ANNOTATE_HAPPENS_AFTER(v)  ((void) v)
#endif

#if !defined(__ATOMIC_VAR_FORCE_SYNC_MACROS) && defined(__STDC_VERSION__) && \
    (__STDC_VERSION__ >= 201112L) && !defined(__STDC_NO_ATOMICS__)
/* Use '_Atomic' keyword if the compiler supports. */
#undef  redisAtomic
#define redisAtomic _Atomic
/* Implementation using _Atomic in C11. */

#include <stdatomic.h>
#define atomicIncr(var,count) atomic_fetch_add_explicit(&var,(count),memory_order_relaxed) // 这是一个 C11 标准的原子操作函数，用于对变量执行加法操作。
#define atomicGetIncr(var,oldvalue_var,count) do { \
    oldvalue_var = atomic_fetch_add_explicit(&var,(count),memory_order_relaxed); \
} while(0)
#define atomicDecr(var,count) atomic_fetch_sub_explicit(&var,(count),memory_order_relaxed)
#define atomicGet(var,dstvar) do { \
    dstvar = atomic_load_explicit(&var,memory_order_relaxed); \
} while(0)
#define atomicSet(var,value) atomic_store_explicit(&var,value,memory_order_relaxed)
#define atomicGetWithSync(var,dstvar) do { \
    dstvar = atomic_load_explicit(&var,memory_order_seq_cst); \
} while(0)
#define atomicSetWithSync(var,value) \
    atomic_store_explicit(&var,value,memory_order_seq_cst)
#define REDIS_ATOMIC_API "c11-builtin"

#elif !defined(__ATOMIC_VAR_FORCE_SYNC_MACROS) && \
    (!defined(__clang__) || !defined(__APPLE__) || __apple_build_version__ > 4210057) && \
    defined(__ATOMIC_RELAXED) && defined(__ATOMIC_SEQ_CST)
/* Implementation using __atomic macros. */

#define atomicIncr(var,count) __atomic_add_fetch(&var,(count),__ATOMIC_RELAXED)
#define atomicGetIncr(var,oldvalue_var,count) do { \
    oldvalue_var = __atomic_fetch_add(&var,(count),__ATOMIC_RELAXED); \
} while(0)
#define atomicDecr(var,count) __atomic_sub_fetch(&var,(count),__ATOMIC_RELAXED)
#define atomicGet(var,dstvar) do { \
    dstvar = __atomic_load_n(&var,__ATOMIC_RELAXED); \
} while(0)
#define atomicSet(var,value) __atomic_store_n(&var,value,__ATOMIC_RELAXED)
#define atomicGetWithSync(var,dstvar) do { \
    dstvar = __atomic_load_n(&var,__ATOMIC_SEQ_CST); \
} while(0)
#define atomicSetWithSync(var,value) \
    __atomic_store_n(&var,value,__ATOMIC_SEQ_CST)
#define REDIS_ATOMIC_API "atomic-builtin"

#elif defined(HAVE_ATOMIC)
/* Implementation using __sync macros. */

#define atomicIncr(var,count) __sync_add_and_fetch(&var,(count))
#define atomicGetIncr(var,oldvalue_var,count) do { \
    oldvalue_var = __sync_fetch_and_add(&var,(count)); \
} while(0)
#define atomicDecr(var,count) __sync_sub_and_fetch(&var,(count))
#define atomicGet(var,dstvar) do { \
    dstvar = __sync_sub_and_fetch(&var,0); \
} while(0)
#define atomicSet(var,value) do { \
    while(!__sync_bool_compare_and_swap(&var,var,value)); \
} while(0)
/* Actually the builtin issues a full memory barrier by default. */
#define atomicGetWithSync(var,dstvar) { \
    dstvar = __sync_sub_and_fetch(&var,0,__sync_synchronize); \
    ANNOTATE_HAPPENS_AFTER(&var); \
} while(0)
#define atomicSetWithSync(var,value) do { \
    ANNOTATE_HAPPENS_BEFORE(&var);  \
    while(!__sync_bool_compare_and_swap(&var,var,value,__sync_synchronize)); \
} while(0)
#define REDIS_ATOMIC_API "sync-builtin"

#else
#error "Unable to determine atomic operations for your platform"

#endif
#endif /* __ATOMIC_VAR_H */
