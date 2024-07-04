// SPDX-License-Identifier: GPL-2.0-or-later
/*
 * Author: Abby Cin
 * Mail: abbytsing@gmail.com
 * Create Time: 2024-06-26 22:42:07
 */

#ifndef META_TYPES_1719412927_H_
#define META_TYPES_1719412927_H_

#include "utils.h"
#include <cstdint>

// the layout of key and value ptr
// +----------+---------+-----------------------+
// |  length  | ck id   |  page offset in chunk |
// +----------+---------+-----------------------+
// | 24 bits  | 11 bits |         29 bits       |
// +----------+---------+-----------------------+

// we restrict max key and value length to ~ 16MB
isc_t k_length_bits = 24;
// limit to 2048 chunks
isc_t k_chunk_bits = 11;
// at most 512MB
isc_t k_data_bits = 29;

isc_t k_max_kv_sz = ((1UL << k_length_bits) - 1);
isc_t k_sys_page_sz = 4096; // assume system page size if 4096
isc_t k_index_page_sz = k_sys_page_sz;
isc_t k_data_page_sz = 64;
isc_t k_chunk_sz = (1UL << k_data_bits);

isc_t k_nr_index_chunk = (1UL << 10);
isc_t k_nr_data_chunk = (1UL << k_chunk_bits);

isc_t k_index_bitmap_bits = k_chunk_sz / k_index_page_sz;
isc_t k_index_page_reserved = k_index_bitmap_bits / 8 / k_index_page_sz;
static_assert(k_index_page_reserved > 0);
isc_t k_index_chunk_hdr_sz = k_index_page_reserved * k_index_page_sz;
static_assert(is_4k_aligned(k_index_chunk_hdr_sz));

isc_t k_data_bitmap_bits = k_chunk_sz / k_data_page_sz;
isc_t k_data_page_reserved = k_data_bitmap_bits / 8 / k_data_page_sz;
isc_t k_data_chunk_hdr_sz = k_data_page_reserved * k_data_page_sz;
static_assert(is_4k_aligned(k_data_chunk_hdr_sz));

struct disk_index_hdr {
	uint64_t magic;
	uint64_t nr_kv;
	uint64_t file_size;
	uint32_t last_chunk;
	ptr_t root;
	uint32_t chunk[k_nr_index_chunk];
};
isc_t k_index_hdr_sz = round_up(sizeof(disk_index_hdr), k_sys_page_sz);
isc_t k_index_page_per_chunk = k_index_bitmap_bits;

struct disk_data_hdr {
	uint64_t magic;
	uint64_t file_size;
	uint64_t last_chunk;
	uint32_t chunk[k_nr_data_chunk];
};
isc_t k_data_hdr_sz = round_up(sizeof(disk_data_hdr), k_sys_page_sz);
isc_t k_data_page_per_chunk = k_data_bitmap_bits;

struct node_t {
	node_type type;
	int count;
	ptr_t self;
	ptr_t parent;
	ptr_t prev;
	ptr_t next;
	uint8_t __pad[8];
};

struct kv_t {
	ptr_t key;
	ptr_t val;
};
static_assert(sizeof(kv_t) == 16);

struct kc_t {
	ptr_t key;
	ptr_t child;
};
static_assert(sizeof(kc_t) == sizeof(kv_t));

// reserve one extra kc_t for interior node
isc_t k_bpt_order = (k_index_page_sz - sizeof(node_t)) / sizeof(kv_t) - 1;
struct leaf_t : node_t {
	kv_t kv[k_bpt_order + 1];
};
static_assert(sizeof(leaf_t) == k_index_page_sz);

struct intl_t : node_t {
	kc_t kc[k_bpt_order + 1];
};
static_assert(sizeof(intl_t) == sizeof(leaf_t));

#endif // META_TYPES_1719412927_H_
