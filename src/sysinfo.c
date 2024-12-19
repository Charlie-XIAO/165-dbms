/**
 * @file sysinfo.c
 * @implements sysinfo.h
 */

#include <stdlib.h>
#include <sys/sysinfo.h>
#include <unistd.h>

#include "sysinfo.h"

int __n_processors__;

int __page_size__;

double __avg_load_1__;
double __avg_load_5__;
double __avg_load_15__;

/**
 * @implements init_sysinfo
 */
void init_sysinfo() {
  __n_processors__ = get_nprocs();
  __page_size__ = getpagesize();

  double loadavg[3];
  getloadavg(loadavg, 3);
  __avg_load_1__ = loadavg[0];
  __avg_load_5__ = loadavg[1];
  __avg_load_15__ = loadavg[2];
}
