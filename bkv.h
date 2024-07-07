// SPDX-License-Identifier: GPL-2.0-or-later
/*
 * Author: Abby Cin
 * Mail: abbytsing@gmail.com
 * Create Time: 2024-06-25 10:33:44
 */

#include "meta_types.h"
#ifndef BKV_1719282824_H_

#include "bptree.h"
#include <memory>
#include <filesystem>

template<typename Comparator>
	requires Comparable<Comparator, View>
class Db;

template<typename T>
using DbPtr = std::shared_ptr<Db<T>>;

template<typename Comparator = BytewiseComparator>
	requires Comparable<Comparator, View>
class Db {
public:
	using Iter = BpTree<Comparator>::Iter;

	static DbPtr<Comparator> open(const std::filesystem::path &root,
				      std::string name = DB_NAME)
	{
		auto tree = BpTree<Comparator>::open(root, name);
		if (!tree) {
			debug("meta initailize fail");
			return nullptr;
		}
		DbPtr<Comparator> db { new Db { std::move(tree) } };
		return db;
	}

	~Db() = default;

	bool put(View key, View val)
	{
		if (key.size() == 0 || key.size() > k_max_kv_sz) {
			debug("key size out of range [1, %lu]", k_max_kv_sz);
			return false;
		}
		if (val.size() == 0 || val.size() > k_max_kv_sz) {
			debug("val size out of range [1, %lu]", k_max_kv_sz);
			return false;
		}
		return tree_->put(key, val);
	}

	data_t get(View key)
	{
		return tree_->get(key);
	}

	bool contains(View key)
	{
		return tree_->contains(key);
	}

	void del(View key)
	{
		tree_->del(key);
	}

	Iter range(View from, View to)
	{
		return tree_->range(from, to);
	}

	void flush()
	{
		tree_->flush();
	}

	void close()
	{
		tree_.reset();
	}

	size_t item() const
	{
		return tree_->items();
	}

	size_t count() const
	{
		return tree_->count();
	}

private:
	BpTreePtr<Comparator> tree_;

	Db(BpTreePtr<Comparator> &&tree) : tree_ { std::move(tree) }
	{
	}
};

#endif // BKV_1719282824_H_
