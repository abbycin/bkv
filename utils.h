// SPDX-License-Identifier: GPL-2.0-or-later
/*
 * Author: Abby Cin
 * Mail: abbytsing@gmail.com
 * Create Time: 2024-06-26 23:20:44
 */

#ifndef UTILS_1719415244_H_
#define UTILS_1719415244_H_

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <concepts>
#include <compare>
#include <vector>

#ifndef _GNU_SOURCE
#error "_GNU_SOURCE must be enabled for direct IO"
#endif

#define container_of(ptr, type, member)                                        \
	({                                                                     \
		typeof(((type *)0)->member) *__mptr = (ptr);                   \
		(type *)((char *)__mptr - offsetof(type, member));             \
	})

#define debug(fmt, ...)                                                        \
	fprintf(stderr, "%s:%d " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__)

#define bassert(cond, ...)                                                     \
	do {                                                                   \
		if (!(cond)) {                                                 \
			debug("Assertion: `" #cond "` failed, " __VA_ARGS__);  \
			abort();                                               \
		}                                                              \
	}                                                                      \
	while (0)

#define PROT_OP (PROT_WRITE | PROT_READ)

#define MAP_OP (MAP_SHARED)

#define isc_t inline static constexpr size_t
#define DB_NAME "chaos"
#define DB_MAGIC 0x4348414F532D4442ULL
#define DATA_MAGIC 0x4348414F532D4441ULL

using ptr_t = uint64_t;
using data_t = std::vector<uint8_t>;
static_assert(sizeof(ptr_t) == sizeof(size_t));
inline static constexpr ptr_t ptr_null = -1;

enum node_type : int {
	e_node_leaf = 3,
	e_node_intl = 11,
};

inline constexpr ptr_t round_up(ptr_t size, ptr_t align)
{
	return (size + (align - 1)) & ~(align - 1);
}

inline constexpr ptr_t round_down(ptr_t size, ptr_t align)
{
	return size & ~(align - 1);
}

inline constexpr bool is_4k_aligned(ptr_t size)
{
	return (size & 4095) == 0;
}

template<typename T>
concept Container = requires(T x) {
	{ x.data() } -> std::convertible_to<const void *>;
	{ x.size() } -> std::convertible_to<size_t>;
};

template<typename T>
concept CacheType = requires(T x) {
	{ x.id() } -> std::same_as<ptr_t>;
	{ x.sync() };
};

template<typename T, typename U>
concept Comparable = requires(U x, U y) {
	{ T::compare(x, y) } -> std::same_as<int>;
};

class View {
public:
	View() : data_ { nullptr }, size_ { 0 }
	{
	}

	View(const void *p, size_t len)
		: data_ { reinterpret_cast<const uint8_t *>(p) }, size_ { len }
	{
	}

	View(const char *p)
		: data_ { reinterpret_cast<const uint8_t *>(p) }
		, size_ { strlen(p) }
	{
	}

	template<typename T, size_t N>
	View(T (&a)[N])
		: data_ { reinterpret_cast<const uint8_t *>(a) }, size_ { N }
	{
	}

	View(const Container auto &c)
		: data_ { reinterpret_cast<const uint8_t *>(c.data()) }
		, size_ { c.size() }
	{
	}

	explicit operator bool()
	{
		return data_ != nullptr && size_ != 0;
	}

	const uint8_t *data() const
	{
		return data_;
	}

	size_t size() const
	{
		return size_;
	}

	friend std::strong_ordering operator<=>(const View &l, const View &r)
	{
		auto len = std::min(l.size(), r.size());
		int rc = std::memcmp(l.data(), r.data(), len);

		if (rc == 0) {
			if (l.size() < r.size())
				return std::strong_ordering::less;
			if (l.size() == r.size())
				return std::strong_ordering::equal;
			return std::strong_ordering::greater;
		}
		if (rc < 0)
			return std::strong_ordering::less;
		return std::strong_ordering::greater;
	}

	friend bool operator==(const View &l, const View &r)
	{
		return (l <=> r == std::strong_ordering::equal);
	}

private:
	const uint8_t *data_;
	size_t size_;
};

struct BytewiseComparator {
	static int compare(const View &l, const View &r)
	{
		if (l < r)
			return -1;
		if (l == r)
			return 0;
		if (l > r)
			return 1;
		abort();
	}
};

#endif // UTILS_1719415244_H_
