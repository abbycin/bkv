// SPDX-License-Identifier: GPL-2.0-or-later
/*
 * Author: Abby Cin
 * Mail: abbytsing@gmail.com
 * Create Time: 2024-06-26 22:59:24
 */

#ifndef CACHE_1719413964_H_
#define CACHE_1719413964_H_

#include "utils.h"
#include <unordered_map>

template<CacheType T>
class Cache {
public:
	Cache(size_t limit) : head_ {}, map_ {}, limit_ { limit }
	{
		list_init(&head_);
	}

	T *put(T item)
	{
		auto it = map_.find(item.id());
		if (it != map_.end()) {
			bassert(false, "can't cache same item more then once");
			return nullptr;
		}
		auto node = new cache_node { item };
		list_append(&head_, &node->link);

		map_[item.id()] = node;

		if (map_.size() > limit_) {
			auto front = head_.prev;
			auto x = container_of(front, cache_node, link);
			evict_node(x);
		}
		return &node->item;
	}

	T *get(ptr_t id)
	{
		auto it = map_.find(id);
		if (it == map_.end())
			return nullptr;

		auto node = it->second;
		list_del(&node->link);
		list_append(&head_, &node->link);

		return &node->item;
	}

	void evict(ptr_t id)
	{
		auto it = map_.find(id);
		if (it != map_.end())
			evict_node(it->second);
	}

	void sync()
	{
		for (auto p = head_.next; p != &head_; p = p->next) {
			auto node = container_of(p, cache_node, link);
			node->item.sync();
		}
	}

	void clear()
	{
		while (!list_empty(&head_)) {
			auto next = head_.next;
			auto node = container_of(next, cache_node, link);
			evict_node(node);
		}
	}

private:
	struct list_node {
		list_node *prev, *next;
	};
	struct cache_node {
		T item;
		list_node link;

		cache_node(T p) : item { p }, link {}
		{
		}
	};

	list_node head_;
	std::unordered_map<ptr_t, cache_node *> map_;
	size_t limit_;

	static void list_init(list_node *head)
	{
		head->next = head;
		head->prev = head;
	}

	static bool list_empty(list_node *node)
	{
		return node->next == node;
	}

	static void list_append(list_node *head, list_node *node)
	{
		node->next = head->next;
		head->next->prev = node;
		node->prev = head;
		head->next = node;
	}

	static void list_del(list_node *node)
	{
		auto prev = node->prev;
		auto next = node->next;

		prev->next = next;
		next->prev = prev;
	}

	void evict_node(cache_node *node)
	{
		node->item.sync(true);
		list_del(&node->link);
		map_.erase(node->item.id());
		delete node;
	}
};

#endif // CACHE_1719413964_H_
