// SPDX-License-Identifier: GPL-2.0-or-later
/*
 * Author: Abby Cin
 * Mail: abbytsing@gmail.com
 * Create Time: 2024-06-27 15:40:06
 */

#ifndef META_1719474006_H_
#define META_1719474006_H_

#include "utils.h"
#include "meta_types.h"
#include "cache.h"
#include <cassert>
#include <cstddef>
#include <fcntl.h>
#include <filesystem>
#include <memory>
#include <sys/mman.h>
#include <unistd.h>

using path_t = std::filesystem::path;

class NodeFile;
class DataFile;

using NodePtr = std::unique_ptr<NodeFile>;
using DataPtr = std::unique_ptr<DataFile>;

namespace {

isc_t k_max_cache_chunks = 32;
isc_t k_max_cache_index = 256;
isc_t k_max_cache_data = 16384;

inline static constexpr ptr_t ptr_encode(uint32_t len, uint16_t ck, uint32_t id)
{
	uint64_t tmp = len;
	tmp <<= k_chunk_bits;
	tmp |= ck;
	tmp <<= k_data_bits;
	tmp |= id;
	return tmp;
}

inline static constexpr uint32_t ptr_chunk(ptr_t id)
{
	return (id >> k_data_bits) & ((1UL << k_chunk_bits) - 1);
}

inline static constexpr uint32_t ptr_length(ptr_t id)
{
	return (id >> (k_chunk_bits + k_data_bits)) &
		((1U << k_length_bits) - 1);
}

inline static constexpr uint32_t ptr_id(ptr_t id)
{
	return id & ((1UL << k_data_bits) - 1);
}

// NOTE: the `ptr_id` is including the `chunk header size` which is controlled
// by `Chunk::off_`, if you modify the `k_index_page_sz` to a value large than
// 8192, you should change this function and `Chunk::off_`
inline static constexpr ptr_t node_file_off(ptr_t id)
{
	return k_index_hdr_sz + ptr_chunk(id) * k_chunk_sz +
		ptr_id(id) * k_index_page_sz;
}

inline static constexpr ptr_t data_file_off(ptr_t id)
{
	return k_data_hdr_sz + ptr_chunk(id) * k_chunk_sz +
		ptr_id(id) * k_data_page_sz;
}

inline static constexpr uint32_t size_to_page(size_t n)
{
	// (1 << 6) == 64
	return (n >> 6) + (!!(n & (k_data_page_sz - 1)) * 1);
}

isc_t data_per_sys_page = k_sys_page_sz / k_data_page_sz;

inline static constexpr ptr_t build_cache_key(uint16_t ck, uint64_t id)
{
	ptr_t r = ck;
	r <<= 32;
	r |= (id / data_per_sys_page);
	return r;
}

inline static constexpr size_t in_sys_page_off(size_t data_page_off)
{
	return (data_page_off & (data_per_sys_page - 1)) * k_data_page_sz;
}

inline static void *map_file(int fd, size_t &size, size_t off, size_t len)
{
	bool alloc = false;
	if (size < off + len) {
		auto rc = ::posix_fallocate(fd, off, len);
		bassert(!rc, "fallocate fail errno %d", errno);
		alloc = true;
		size = off + len;
	}

	auto m = ::mmap(NULL, len, PROT_OP, MAP_OP, fd, off);
	bassert(m != MAP_FAILED, "mmap fail errno %d", errno);
	if (alloc)
		bzero(m, len);
	return m;
}

inline static void unmap_file(void *addr, size_t len)
{
	auto rc = ::munmap(addr, len);
	bassert(!rc, "unmap fail errno %d", errno);
}

inline static void sync_file(void *addr, size_t len, int flag = MS_SYNC)
{
	auto rc = ::msync(addr, len, flag);
	bassert(!rc, "msync fail errno %d", errno);
}
}

class Page {
public:
	Page(ptr_t id, void *m, uint32_t len, bool async = false)
		: flag_ { uint16_t(async ? MS_ASYNC : MS_SYNC) }
		, dirty_ { 0 }
		, len_ { len }
		, id_ { id }
		, data_ { m }
	{
	}

	ptr_t id() const
	{
		return id_;
	}

	void mark_dirty()
	{
		dirty_ = 1;
	}

	void sync(bool unmap = false)
	{
		if (unmap) {
			sync_file(data_, len_, flag_);
			return unmap_file(data_, len_);
		}
		if (dirty_) {
			sync_file(data_, len_, flag_);
			dirty_ = false;
		}
	}

	// the `off` is for data only
	template<typename T = void>
	T *into(size_t off = 0)
	{
		return (T *)((uint8_t *)data_ + off);
	}

private:
	uint16_t flag_;
	uint16_t dirty_;
	uint32_t len_;
	ptr_t id_; // page id for tree node, cache id for data
	void *data_;
};

class Chunk {
public:
	Chunk(ptr_t id, void *data, size_t off, size_t bits, size_t size)
		: id_ { id }
		, bits_ { (uint8_t *)data }
		, off_ { (uint32_t)off }
		, total_bits_ { (uint32_t)bits }
		, size_ { (uint32_t)size }
	{
		last_ = off_;
	}

	// allocate continuous n ptr
	ptr_t get(size_t n = 1)
	{
		auto r = last_;
		auto l = r;
		for (ptr_t i = off_; i < total_bits_; ++i, ++r) {
			if (r == total_bits_) {
				r = off_;
				l = r; // not cross chunk
			}
			if (test(r)) {
				l = r + 1;
				continue;
			}
			if (r - l + 1 == n) {
				last_ = r;
				return l;
			}
		}

		return ptr_null;
	}

	bool test(ptr_t id) const
	{
		return bits_[id >> 3] & (1U << (id & 7));
	}

	void mask(ptr_t p, int n = 1)
	{
		while (n > 0) {
			bits_[p >> 3] |= (1U << (p & 7));
			p += 1;
			n -= 1;
		}
	}

	void unmask(ptr_t p, int n = 1)
	{
		while (n > 0) {
			bits_[p >> 3] &= ~(1U << (p & 7));
			p += 1;
			n -= 1;
		}
	}

	void mark_dirty()
	{
		dirty_ = true;
	}

	ptr_t id() const
	{
		return id_;
	}

	void sync(bool unmap = false)
	{
		if (unmap) {
			sync_file(bits_, size_);
			return unmap_file(bits_, size_);
		}
		if (dirty_) {
			sync_file(bits_, size_);
			dirty_ = false;
		}
	}

private:
	bool dirty_;
	ptr_t id_;
	uint8_t *bits_;
	uint32_t off_;
	uint32_t total_bits_;
	uint32_t size_;
	uint32_t last_;
};

struct DataIter {
	int fd;
	uint32_t len; // at most k_max_kv_sz
	uint32_t ckid; // chunk id
	uint32_t used; // offset in page
	uint32_t off; // page offset in chunk
	ptr_t id;
	size_t *file_size;
	size_t file_off; // offset in file
	Cache<Page> *cache;

	Page *next()
	{
		if (len == 0)
			return nullptr;
		auto key = build_cache_key(ckid, off);
		auto page = cache->get(key);
		auto nbytes = std::min(len, (uint32_t)k_sys_page_sz - used);

		if (!page) {
			auto tmp = round_down(file_off, k_sys_page_sz);
			auto m = map_file(fd, *file_size, tmp, k_sys_page_sz);
			page = cache->put(
				{ key, m, (uint32_t)k_sys_page_sz, true });
		}

		used = 0;
		file_off += nbytes;
		len -= nbytes;
		off += size_to_page(nbytes);
		return page;
	}

	data_t collect()
	{
		// save a copy in case `this->len` changes in `next`
		size_t length = len;
		data_t d {};
		d.resize(length);
		size_t off = 0;
		auto used = in_sys_page_off(ptr_id(id));

		for (auto p = next(); p; p = next()) {
			auto addr = p->into(used);
			auto n = std::min(length, k_sys_page_sz - used);
			memcpy(d.data() + off, addr, n);
			bassert(length >= n, "len %zu nbytes %zu", length, n);
			length -= n;
			off += n;
			used = 0;
		}
		bassert(length == 0, "len %zu", length);
		return d;
	}
};

class NodeFile {
public:
	static void format(const std::string &name)
	{
		bassert(!name.empty(), "empty data name is not allowed");

		auto flag = O_CREAT | O_RDWR | O_DIRECT | O_TRUNC;
		int fd = ::open(name.c_str(), flag, 0644);
		bassert(fd > 0, "open %s fail errno %d", name.c_str(), errno);

		size_t size = 0;
		auto m = map_file(fd, size, 0, k_index_hdr_sz);

		bzero(m, k_index_hdr_sz);
		auto hdr = (disk_index_hdr *)m;

		hdr->magic = DB_MAGIC;
		hdr->file_size = k_index_hdr_sz;
		hdr->root = ptr_null; // necessary

		auto rc = ::msync(m, k_index_hdr_sz, MS_SYNC);
		bassert(rc == 0, "msync fail errno %d", errno);
		rc = ::munmap(m, k_index_hdr_sz);
		bassert(rc == 0, "umap fail errno %d", errno);
		::fsync(fd);
		close(fd);
	}

	static NodePtr load(const std::string &name)
	{
		int fd = ::open(name.c_str(), O_RDWR | O_DIRECT, 0644);
		bassert(fd > 0, "open %s fail errno %d", name.c_str(), errno);

		size_t size = k_index_hdr_sz;
		auto m = map_file(fd, size, 0, k_index_hdr_sz);

		auto hdr = (disk_index_hdr *)m;
		bassert(hdr->magic == DB_MAGIC, "invalid meta file");
		bassert(hdr->file_size >= k_index_hdr_sz, "invalid meta file");

		return NodePtr { new NodeFile { fd, hdr } };
	}

	~NodeFile()
	{
		meta_.clear();
		data_.clear();
		::fsync(fd_);
		::close(fd_);
	}

	NodeFile(const NodeFile &) = delete;
	NodeFile(NodeFile &&) = delete;
	NodeFile &operator=(const NodeFile &) = delete;
	NodeFile &operator=(NodeFile &&) = delete;

	// create a page and mask meta
	ptr_t get()
	{
		auto id = find_space();
		if (id == ptr_null)
			debug("no space available");
		return id;
	}

	Page *alloc(ptr_t id)
	{
		auto page = data_.get(id);
		if (page)
			return page;
		auto off = node_file_off(id);
		auto m = map_file(fd_, hdr_->file_size, off, k_index_page_sz);
		return data_.put({ id, m, k_index_page_sz });
	}

	void free(ptr_t id)
	{
		auto ckid = ptr_chunk(id);
		auto ck = get_chunk(ckid);
		ck->unmask(ptr_id(id));
		data_.evict(id);
		hdr_->chunk[ckid] -= 1;
	}

	void sync()
	{
		meta_.sync();
		data_.sync();
		::fsync(fd_);
	}

	disk_index_hdr *hdr()
	{
		return hdr_;
	}

private:
	int fd_;
	disk_index_hdr *hdr_;
	Cache<Chunk> meta_ { k_max_cache_chunks };
	Cache<Page> data_ { k_max_cache_index };

	NodeFile(int fd, disk_index_hdr *hdr) : fd_ { fd }, hdr_ { hdr }
	{
	}

	ptr_t find_space()
	{
		size_t ckid;
		ptr_t p;
		Chunk *c;

		for (size_t i = 0; i < k_nr_index_chunk; ++i) {
			ckid = (hdr_->last_chunk + 1) % k_nr_index_chunk;
			if (hdr_->chunk[ckid] == k_index_page_per_chunk)
				continue;
			c = get_chunk(ckid);
			p = c->get();
			if (p != ptr_null) {
				c->mask(p);
				c->mark_dirty();
				hdr_->chunk[ckid] += 1;
				return ptr_encode(k_index_page_sz, ckid, p);
			}
		}
		return ptr_null;
	}

	Chunk *get_chunk(size_t ckid)
	{
		auto ck = meta_.get(ckid);
		if (ck)
			return ck;
		auto offset = k_index_hdr_sz + ckid * k_chunk_sz;
		auto m = map_file(
			fd_, hdr_->file_size, offset, k_index_chunk_hdr_sz);
		return meta_.put({ ckid,
				   m,
				   k_index_bitmap_bits / k_index_page_sz,
				   k_index_bitmap_bits,
				   k_index_chunk_hdr_sz });
	}
};

class DataFile {
public:
	static void format(const std::string &name)
	{
		if (name.empty()) {
			debug("empty data name is not allowed");
			return;
		}

		auto flag = O_CREAT | O_RDWR | O_DIRECT | O_TRUNC;
		int fd = ::open(name.c_str(), flag, 0644);
		bassert(fd > 0, "open %s fail errno %d", name.c_str(), errno);

		size_t size = 0;
		auto m = map_file(fd, size, 0, k_data_hdr_sz);

		bzero(m, k_data_hdr_sz);
		auto hdr = (disk_data_hdr *)m;

		hdr->magic = DATA_MAGIC;
		hdr->file_size = k_data_hdr_sz;

		::msync(m, k_data_hdr_sz, MS_SYNC);
		::munmap(m, k_data_hdr_sz);
		::fsync(fd);
		close(fd);
	}

	static DataPtr load(const std::string &name)
	{
		int fd = ::open(name.c_str(), O_RDWR | O_DIRECT, 0644);
		bassert(fd > 0, "open %s fail errno %d", name.c_str(), errno);

		size_t size = k_data_hdr_sz;
		auto m = map_file(fd, size, 0, k_data_hdr_sz);

		auto hdr = (disk_data_hdr *)m;
		bassert(hdr->magic == DATA_MAGIC, "invalid data file");
		bassert(hdr->file_size >= k_data_hdr_sz, "invalid data file");

		return DataPtr { new DataFile { fd, hdr } };
	}

	~DataFile()
	{
		meta_.clear();
		data_.clear();
		::fsync(fd_);
		::close(fd_);
	}

	DataFile(const DataFile &) = delete;
	DataFile(DataFile &&) = delete;
	DataFile &operator=(const DataFile &) = delete;
	DataFile &operator=(DataFile &&) = delete;

	ptr_t store(View data)
	{
		ptr_t id = find_space(data.size());
		if (id == ptr_null) {
			debug("no space available");
			return id;
		}
		auto iter = new_iter(id, fd_, &hdr_->file_size, &data_);
		copy_to_file(iter, data);
		return id;
	}

	DataIter load(ptr_t id)
	{
		return new_iter(id, fd_, &hdr_->file_size, &data_);
	}

	// remove from cache and meta data
	void free(ptr_t id)
	{
		auto iter = new_iter(id, fd_, &hdr_->file_size, &data_);
		auto ckid = ptr_chunk(id);

		for (auto p = iter.next(); p; p = iter.next())
			data_.evict(p->id());

		auto ck = get_chunk(ckid);
		auto pages = size_to_page(ptr_length(id));
		ck->unmask(ptr_id(id), pages);
		hdr_->chunk[ckid] -= pages;
	}

	void sync()
	{
		data_.sync();
		meta_.sync();
		::msync(hdr_, k_data_hdr_sz, MS_SYNC);
		// NOTE: if file size not changed, it unnecessary
		::fsync(fd_);
	}

private:
	int fd_;
	disk_data_hdr *hdr_;
	Cache<Chunk> meta_ { k_max_cache_chunks };
	Cache<Page> data_ { k_max_cache_data };

	DataFile(int fd, disk_data_hdr *hdr) : fd_ { fd }, hdr_ { hdr }
	{
	}

	ptr_t find_space(size_t size)
	{
		auto n = size_to_page(size);
		size_t ckid;
		ptr_t p = ptr_null;
		Chunk *c;

		for (size_t i = 0; i < k_nr_data_chunk; ++i) {
			ckid = (hdr_->last_chunk + i) % k_nr_data_chunk;
			if (hdr_->chunk[ckid] + n >= k_data_page_per_chunk)
				continue;
			c = get_chunk(ckid);
			p = c->get(n);
			if (p != ptr_null) {
				c->mask(p, n);
				c->mark_dirty();
				hdr_->chunk[ckid] += n;
				return ptr_encode(size, ckid, p);
			}
		}
		return ptr_null;
	}

	Chunk *get_chunk(size_t ckid)
	{
		auto ck = meta_.get(ckid);
		if (ck)
			return ck;
		auto offset = k_data_hdr_sz + ckid * k_chunk_sz;
		auto m = map_file(
			fd_, hdr_->file_size, offset, k_data_chunk_hdr_sz);
		return meta_.put({ ckid,
				   m,
				   k_data_bitmap_bits / k_data_page_sz,
				   k_data_bitmap_bits,
				   k_data_chunk_hdr_sz });
	}

	static DataIter
	new_iter(ptr_t id, int fd, size_t *fsize, Cache<Page> *c)
	{
		uint32_t pid = ptr_id(id);
		return {
			.fd = fd,
			.len = ptr_length(id),
			.ckid = ptr_chunk(id),
			.used = (uint32_t)in_sys_page_off(pid),
			.off = pid,
			.id = id,
			.file_size = fsize,
			.file_off = data_file_off(id),
			.cache = c,
		};
	}

	static void copy_to_file(DataIter &iter, View v)
	{
		size_t len = v.size();
		size_t off = 0;
		size_t nbytes;
		auto used = iter.used;

		for (auto p = iter.next(); p; p = iter.next()) {
			auto addr = p->into(used);
			nbytes = std::min(len, k_sys_page_sz - used);
			memcpy(addr, v.data() + off, nbytes);
			assert(len >= nbytes);
			off += nbytes;
			len -= nbytes;
			used = 0;
			p->mark_dirty();
		}
		assert(len == 0);
	}
};

#endif // META_1719474006_H_
