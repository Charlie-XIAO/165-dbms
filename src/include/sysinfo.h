/**
 * @file sysinfo.h
 *
 * This header declares system informations that are meant to be used anywhere
 * in the system once initialized.
 */

#ifndef SYSINFO_H__
#define SYSINFO_H__

/**
 * The number of processors currently available in the system.
 */
extern int __n_processors__;

/**
 * The size of a page in bytes.
 */
extern int __page_size__;

/**
 * The average load of the system in the last 1 minute.
 */
extern double __avg_load_1__;

/**
 * The average load of the system in the last 5 minutes.
 */
extern double __avg_load_5__;

/**
 * The average load of the system in the last 15 minutes.
 */
extern double __avg_load_15__;

/**
 * Initialize system information.
 */
void init_sysinfo();

#endif /* SYSINFO_H__ */
