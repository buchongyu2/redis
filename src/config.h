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
/**
 * 很多配置是为了跨平台的支持
 */
#ifndef __CONFIG_H
#define __CONFIG_H

#ifdef __APPLE__
#include <AvailabilityMacros.h>
#endif

#ifdef __linux__
#include <features.h>
#include <fcntl.h>
#endif

#if defined(__APPLE__) && defined(__MAC_OS_X_VERSION_MAX_ALLOWED) && __MAC_OS_X_VERSION_MAX_ALLOWED >= 1060
#define MAC_OS_10_6_DETECTED
#endif

/* 将 redis_fstat 定义为 fstat 或 fstat64()。用于实现跨平台的兼容性*/ /* Define redis_fstat to fstat or fstat64() */
#if defined(__APPLE__) && !defined(MAC_OS_10_6_DETECTED)
#define redis_fstat fstat64
#define redis_stat stat64
#else
#define redis_fstat fstat
#define redis_stat stat
#endif

/*测试是否支持 proc 文件系统。它提供了内核和进程的运行时信息，例如进程状态、内存使用、CPU 信息等。 */  /* Test for proc filesystem */
#ifdef __linux__
#define HAVE_PROC_STAT 1
#define HAVE_PROC_MAPS 1
#define HAVE_PROC_SMAPS 1
#define HAVE_PROC_SOMAXCONN 1
#define HAVE_PROC_OOM_SCORE_ADJ 1
#endif

/* 测试是否支持 task_info()。 */ /* Test for task_info() */
#if defined(__APPLE__)
#define HAVE_TASKINFO 1
#endif

/*backtrace() 是 GNU C 库中的一个函数，用于获取当前调用栈（call stack）的函数调用信息。它通常用于调试和错误报告，帮助开发者追踪程序运行时的调用路径。*/ /* Test for backtrace() */
#if defined(__APPLE__) || (defined(__linux__) && defined(__GLIBC__)) || \
    defined(__FreeBSD__) || ((defined(__OpenBSD__) || defined(__NetBSD__)) && defined(USE_BACKTRACE))\
 || defined(__DragonFly__) || (defined(__UCLIBC__) && defined(__UCLIBC_HAS_BACKTRACE__))
#define HAVE_BACKTRACE 1
#endif

/*
 * MSG_NOSIGNAL 是一个用于 send 或 sendto 系统调用的标志。它的作用是防止在向一个已关闭的管道或套接字发送数据时触发 SIGPIPE 信号。如果不使用 MSG_NOSIGNAL，
 * 默认情况下，向已关闭的连接发送数据会导致程序收到 SIGPIPE 信号，从而可能导致程序异常终*//* MSG_NOSIGNAL. 
 */
#ifdef __linux__
#define HAVE_MSG_NOSIGNAL 1
#endif

/* 测试 epoll 模型的 API */ /* Test for polling API */
#ifdef __linux__
#define HAVE_EPOLL 1
#endif

#if (defined(__APPLE__) && defined(MAC_OS_10_6_DETECTED)) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined (__NetBSD__)
#define HAVE_KQUEUE 1
#endif

#ifdef __sun
#include <sys/feature_tests.h>
#ifdef _DTRACE_VERSION
#define HAVE_EVPORT 1
#define HAVE_PSINFO 1
#endif
#endif

/*在 Linux 中将 redis_fsync 定义为 fdatasync()，在其他系统中定义为 fsync()。*//* Define redis_fsync to fdatasync() in Linux and fsync() for all the rest */
#ifdef __linux__
#define redis_fsync fdatasync
#else
#define redis_fsync fsync
#endif

#if __GNUC__ >= 4
#define redis_unreachable __builtin_unreachable
#else
#define redis_unreachable abort
#endif

#if __GNUC__ >= 3
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

/* Define rdb_fsync_range to sync_file_range() on Linux, otherwise we use
 * the plain fsync() call. */
/**
 * rdb_fsync_range 系统调用
 * linux 系统支持sync_file_range系统调用，它允许将指定范围内的文件数据异步地同步到存储设备上。
 * 在不支持sync_file_range系统调用的系统上，使用fsync函数将整个文件同步到存储设备上。
 */
#if (defined(__linux__) && defined(SYNC_FILE_RANGE_WAIT_BEFORE))
#define rdb_fsync_range(fd,off,size) sync_file_range(fd,off,size,SYNC_FILE_RANGE_WAIT_BEFORE|SYNC_FILE_RANGE_WRITE)
#else
#define rdb_fsync_range(fd,off,size) fsync(fd)
#endif

/* Check if we can use setproctitle().
 * BSD systems have support for it, we provide an implementation for
 * Linux and osx. */
/**
 * 检查是否可以使用 setproctitle()。
 * BSD 系统支持该功能，我们为 Linux 和 macOS 提供了一个实现。
 */
#if (defined __NetBSD__ || defined __FreeBSD__ || defined __OpenBSD__)
#define USE_SETPROCTITLE
#endif

#if defined(__HAIKU__)
#define ESOCKTNOSUPPORT 0
#endif

#if (defined __linux || defined __APPLE__)
#define USE_SETPROCTITLE
#define INIT_SETPROCTITLE_REPLACEMENT
void spt_init(int argc, char *argv[]);
void setproctitle(const char *fmt, ...);
#endif

/* 字节序检测 */ /* Byte ordering detection */
/* 二进制标识太繁琐，使用 16 进制表示。一个字节可以用两个 16 进制位标识 */
#include <sys/types.h> /*该头文件可能会定义 BYTE_ORDER。*/ /* This will likely define BYTE_ORDER */

#ifndef BYTE_ORDER
#if (BSD >= 199103)
# include <machine/endian.h>
#else
#if defined(linux) || defined(__linux__)
# include <endian.h>
#else
/**
 * 小端字节序（Little-Endian）：
 * 低位字节存储在低地址，高位字节存储在高地址。
 * 常见于 x86、x86_64 等架构。
 * 例如，0x12345678 在内存中的存储顺序是：78 56 34 12。

 * 大端字节序（Big-Endian）：
 * 高位字节存储在低地址，低位字节存储在高地址。
 * 常见于 IBM 系统、网络协议等。
 * 例如，0x12345678 在内存中的存储顺序是：12 34 56 78。
 */
#define	LITTLE_ENDIAN	1234	/* 78 56 34 12 */ /* least-significant byte first (vax, pc) */ 
#define	BIG_ENDIAN	4321	    /* 符合人类阅读习惯：12 34 56 78 */ /* most-significant byte first (IBM, net) */
#define	PDP_ENDIAN	3412	/* LSB first in word, MSW first in long (pdp)*/

#if defined(__i386__) || defined(__x86_64__) || defined(__amd64__) || \
   defined(vax) || defined(ns32000) || defined(sun386) || \
   defined(MIPSEL) || defined(_MIPSEL) || defined(BIT_ZERO_ON_RIGHT) || \
   defined(__alpha__) || defined(__alpha)
#define BYTE_ORDER    LITTLE_ENDIAN
#endif

#if defined(sel) || defined(pyr) || defined(mc68000) || defined(sparc) || \
    defined(is68k) || defined(tahoe) || defined(ibm032) || defined(ibm370) || \
    defined(MIPSEB) || defined(_MIPSEB) || defined(_IBMR2) || defined(DGUX) ||\
    defined(apollo) || defined(__convex__) || defined(_CRAY) || \
    defined(__hppa) || defined(__hp9000) || \
    defined(__hp9000s300) || defined(__hp9000s700) || \
    defined (BIT_ZERO_ON_LEFT) || defined(m68k) || defined(__sparc)
#define BYTE_ORDER	BIG_ENDIAN
#endif
#endif /* linux */
#endif /* BSD */
#endif /* BYTE_ORDER */ /* 字节序 */

/* Sometimes after including an OS-specific header that defines the
 * endianness we end with __BYTE_ORDER but not with BYTE_ORDER that is what
 * the Redis code uses. In this case let's define everything without the
 * underscores. */
/**
 * 有时在包含一个操作系统特定的头文件后，会定义 __BYTE_ORDER，但没有定义 BYTE_ORDER，而 Redis 代码使用的是 BYTE_ORDER。在这种情况下，我们将定义没有下划线的宏（BYTE_ORDER）。
 * 不同的操作系统或编译器可能会定义不同的字节序宏。
 * 例如，某些系统会定义 __BYTE_ORDER、__LITTLE_ENDIAN 和 __BIG_ENDIAN，而 Redis 代码中使用的是 BYTE_ORDER、LITTLE_ENDIAN 和 BIG_ENDIAN。
 */
#ifndef BYTE_ORDER
#ifdef __BYTE_ORDER
#if defined(__LITTLE_ENDIAN) && defined(__BIG_ENDIAN)
#ifndef LITTLE_ENDIAN
#define LITTLE_ENDIAN __LITTLE_ENDIAN
#endif
#ifndef BIG_ENDIAN
#define BIG_ENDIAN __BIG_ENDIAN
#endif
#if (__BYTE_ORDER == __LITTLE_ENDIAN)
#define BYTE_ORDER LITTLE_ENDIAN
#else
#define BYTE_ORDER BIG_ENDIAN
#endif
#endif
#endif
#endif

#if !defined(BYTE_ORDER) || \
    (BYTE_ORDER != BIG_ENDIAN && BYTE_ORDER != LITTLE_ENDIAN)
    /**
     * 你必须确定你的编译器使用的正确位顺序（bit order）。下一行是一个故意的错误，它会导致编译失败，直到你修复上面的宏定义为止。
     */
	/* you must determine what the correct bit order is for
	 * your compiler - the next line is an intentional error
	 * which will force your compiles to bomb until you fix
	 * the above macros.
	 */
#error "Undefined or invalid BYTE_ORDER"
#endif

#if (__i386 || __amd64 || __powerpc__) && __GNUC__
#define GNUC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)
#if defined(__clang__)
#define HAVE_ATOMIC
#endif
#if (defined(__GLIBC__) && defined(__GLIBC_PREREQ))
#if (GNUC_VERSION >= 40100 && __GLIBC_PREREQ(2, 6))
#define HAVE_ATOMIC
#endif
#endif
#endif

/* Make sure we can test for ARM just checking for __arm__, since sometimes
 * __arm is defined but __arm__ is not. */
/**
 * 确保我们可以通过检查 __arm__ 来测试是否为 ARM 架构，因为有时 __arm 被定义了，但 __arm__ 没有被定义。
 */
#if defined(__arm) && !defined(__arm__)
#define __arm__
#endif
#if defined (__aarch64__) && !defined(__arm64__)
#define __arm64__
#endif

/* 确保我们可以通过检查 __sparc__ 来测试是否为 SPARC 架构。 */ /* Make sure we can test for SPARC just checking for __sparc__. */
#if defined(__sparc) && !defined(__sparc__)
#define __sparc__
#endif

#if defined(__sparc__) || defined(__arm__)
#define USE_ALIGNED_ACCESS
#endif

/* 这是 Redis 中的一个函数，用于设置线程的标题（名称）。 */ /* Define for redis_set_thread_title */
#ifdef __linux__
#define redis_set_thread_title(name) pthread_setname_np(pthread_self(), name)
#else
#if (defined __FreeBSD__ || defined __OpenBSD__)
#include <pthread_np.h>
#define redis_set_thread_title(name) pthread_set_name_np(pthread_self(), name)
#elif defined __NetBSD__
#include <pthread.h>
#define redis_set_thread_title(name) pthread_setname_np(pthread_self(), "%s", name)
#elif defined __HAIKU__
#include <kernel/OS.h>
#define redis_set_thread_title(name) rename_thread(find_thread(0), name)
#else
#if (defined __APPLE__ && defined(__MAC_OS_X_VERSION_MAX_ALLOWED) && __MAC_OS_X_VERSION_MAX_ALLOWED >= 1070)
int pthread_setname_np(const char *name);
#include <pthread.h>
#define redis_set_thread_title(name) pthread_setname_np(name)
#else
#define redis_set_thread_title(name)
#endif
#endif
#endif

/*检查是否可以使用 setcpuaffinity()。*/ /* Check if we can use setcpuaffinity(). */
#if (defined __linux || defined __NetBSD__ || defined __FreeBSD__ || defined __DragonFly__)
#define USE_SETCPUAFFINITY
void setcpuaffinity(const char *cpulist);
#endif

#endif
