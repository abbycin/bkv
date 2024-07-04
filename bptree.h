// SPDX-License-Identifier: GPL-2.0-or-later
/*
 * Author: Abby Cin
 * Mail: abbytsing@gmail.com
 * Create Time: 2024-06-25 10:34:41
 */

#ifndef DISK_BPT_1719282881_H_
#define DISK_BPT_1719282881_H_

#include "meta.h"
#include "utils.h"
#include "meta_types.h"
#include <memory>
#include <cstring>

template<typename Cmp>
class BpTree;

template<typename Cmp>
using BpTreePtr = std::unique_ptr<BpTree<Cmp>>;

template<typename Cmp>
class BpTree {
	static constexpr int M = k_bpt_order;

public:
	class Iter {
	public:
		Iter(BpTree<Cmp> *t, ptr_t beg, ptr_t end, short b, short e)
			: tree_ { t }
			, off_ { b }
			, b_off_ { b }
			, e_off_ { e }
			, cursor_ { beg }
			, head_ { beg }
			, tail_ { end }
		{
		}

		Iter()
			: off_ {}
			, b_off_ {}
			, e_off_ {}
			, cursor_ { ptr_null }
			, head_ { ptr_null }
			, tail_ { ptr_null }
		{
		}

		data_t key()
		{
			auto p = tree_->load_node(cursor_)
					 ->template into<leaf_t>();
			bassert(p);
			return tree_->load_data(p->kv[off_].key);
		}

		data_t val()
		{
			auto p = tree_->load_node(cursor_)
					 ->template into<leaf_t>();
			bassert(p);
			return tree_->load_data(p->kv[off_].val);
		}

		explicit operator bool()
		{
			if (cursor_ == ptr_null)
				return false;

			if (cursor_ == head_ && off_ < b_off_)
				return false;
			if (cursor_ == tail_ && off_ > e_off_)
				return false;

			return true;
		}

		Iter &operator++()
		{
			off_ += 1;
			auto p = tree_->load_node(cursor_)
					 ->template into<leaf_t>();
			if (off_ >= p->count && cursor_ != tail_) {
				cursor_ = p->next;
				off_ = 0;
			}
			return *this;
		}

		Iter &operator--()
		{
			off_ -= 1;
			if (off_ <= 0 && cursor_ != head_) {
				auto p = tree_->load_node(cursor_);
				bassert(p);
				auto l = p->template into<leaf_t>();
				off_ = l->count - 1;
				cursor_ = l->prev;
			}
			return *this;
		}

		void seek_beg()
		{
			cursor_ = head_;
			off_ = b_off_;
		}

		void seek_end()
		{
			cursor_ = tail_;
			off_ = e_off_;
		}

	private:
		BpTree *tree_;
		int off_;
		short b_off_;
		short e_off_;
		ptr_t cursor_;
		ptr_t head_;
		ptr_t tail_;
	};

	~BpTree() = default;

	static BpTreePtr<Cmp> open(const path_t &root, const std::string &name)
	{
		std::filesystem::create_directories(root);

		if (name.empty()) {
			debug("empty database name is not allowed");
			return nullptr;
		}

		auto node_file = root / (name + ".db");
		auto data_file = root / (name + ".data");
		auto db_exists = std::filesystem::exists(node_file);
		auto data_exists = std::filesystem::exists(data_file);

		if (!db_exists || !data_exists) {
			NodeFile::format(node_file);
			DataFile::format(data_file);
		}

		auto n = NodeFile::load(node_file);
		auto d = DataFile::load(data_file);

		return BpTreePtr<Cmp> { new BpTree { std::move(n),
						     std::move(d) } };
	}

	bool put(View key, View val)
	{
		if (root_ == ptr_null) {
			auto [ok, pk, pv] = store_kv(key, val);
			bassert(ok, "can't set key/val");
			auto p = node_alloc(e_node_leaf);
			bassert(p, "can't alloc node");
			auto l = p->template into<leaf_t>();
			l->count = 1;
			l->kv[0] = kv_t { pk, pv };
			kv_inc();
			root_ = p->id();
			return true;
		} else {
			auto p = search(root_, key);
			bassert(p, "null root was handled in `if` branch");
			return leaf_put(p, key, val);
		}
	}

	data_t get(View key)
	{
		auto p = search(root_, key);
		if (p) {
			auto [ok, pos] = leaf_search(p, key);
			if (ok)
				return load_data(p->template into<leaf_t>()
							 ->kv[pos]
							 .val);
		}
		return {};
	}

	void del(View key)
	{
		auto p = search(root_, key);
		if (p)
			leaf_del(p, key);
	}

	Iter range(View from, View to)
	{
		if (root_ == ptr_null)
			return {};

		if (from > to)
			std::swap(from, to);
		auto pf = search(root_, from);
		auto pt = search(root_, to);

		bassert(pf);
		bassert(pt);
		auto l = pf->template into<leaf_t>();
		auto r = pt->template into<leaf_t>();

		auto [_b, beg] = leaf_search(pf, from);
		auto [_e, end] = leaf_search(pt, to);

		if (!_b && !_e && l == r) {
			if (beg == l->count && end == r->count)
				return {};
		}

		if (!_b) {
			if (beg == l->count) {
				pf = load_node(l->next);
				if (!pf)
					return {};

				beg = 0;
			}
		}

		if (!_e) {
			if (end == 0) {
				pt = load_node(r->prev);
				if (!pt)
					return {};
				end = pt->template into<leaf_t>()->count - 1;
			} else {
				end -= 1;
			}
		}
		l = pf->template into<leaf_t>();
		r = pt->template into<leaf_t>();
		return { this, l->self, r->self, (short)beg, (short)end };
	}

	bool contains(View key)
	{
		auto p = search(root_, key);
		if (p) {
			auto [ok, pos] = leaf_search(p, key);
			if (ok)
				return true;
		}
		return false;
	}

	size_t count()
	{
		size_t n = 0;
		auto cur = root_;

		if (cur != ptr_null) {
			auto page = load_node(cur);
			bassert(page);
			auto node = page->template into<intl_t>();

			while (node->type != e_node_leaf) {
				node = load_node(node->kc[0].child)
					       ->template into<intl_t>();
			}

			while (true) {
				n += node->count;
				if (node->next == ptr_null)
					break;
				node = load_node(node->next)
					       ->template into<intl_t>();
			}
		}
		return n;
	}

	size_t items()
	{
		return node_->hdr()->nr_kv;
	}

	void flush()
	{
		node_->sync();
		data_->sync();
	}

private:
	BpTree(NodePtr &&np, DataPtr &&dp)
		: node_ { std::move(np) }
		, data_ { std::move(dp) }
		, root_ { node_->hdr()->root }
	{
	}

	NodePtr node_;
	DataPtr data_;
	uint64_t &root_;

	Page *search(ptr_t cur, View key)
	{
		while (cur != ptr_null) {
			auto p = load_node(cur);
			bassert(p, "invalid id %zu", cur);
			auto node = p->template into<intl_t>();
			switch (node->type) {
			case e_node_leaf:
				return p;
			case e_node_intl: {
				auto [ok, pos] = intl_search(p, key);
				if (ok)
					pos += 1;
				cur = node->kc[pos].child;
			}
			}
		}
		bassert(root_ == ptr_null);
		return nullptr;
	}

	bool leaf_is_full(leaf_t *l)
	{
		return l->count == M - 1;
	}

	bool intl_is_full(intl_t *i)
	{
		return i->count == M;
	}

	bool leaf_overhalf(leaf_t *l)
	{
		return l->count > (M + 1) / 2;
	}

	bool intl_overhalf(intl_t *i)
	{
		return i->count > (M + 1) / 2;
	}

	template<typename T>
	static void rshift(T *arr, int size, int pos)
	{
		size -= pos;
		if (size > 0)
			memmove(arr + pos + 1, arr + pos, size * sizeof(T));
	}

	template<typename T>
	static void lshift(T *arr, int size, int pos)
	{
		size -= (pos + 1);
		if (size > 0)
			memmove(arr + pos, arr + pos + 1, size * sizeof(T));
	}

	void insert_fixup(Page *l, Page *r, ptr_t key)
	{
		auto lhs = l->template into<node_t>();
		auto rhs = r->template into<node_t>();
		if (lhs->parent == ptr_null && rhs->parent == ptr_null) {
			auto page = node_alloc(e_node_intl);
			auto parent = page->template into<intl_t>();

			bassert(lhs->self != ptr_null);
			bassert(rhs->self != ptr_null);
			parent->count = 2;
			parent->kc[0].key = key;
			parent->kc[0].child = lhs->self;
			parent->kc[1].child = rhs->self;

			lhs->parent = parent->self;
			rhs->parent = parent->self;

			root_ = parent->self;
			page->mark_dirty();
			l->mark_dirty();
			r->mark_dirty();
		} else {
			bassert(rhs->parent == ptr_null);
			rhs->parent = lhs->parent;
			r->mark_dirty();
			auto page = load_node(rhs->parent);
			bassert(page, "invalid id %zu", rhs->parent);
			intl_put(page, r, key);
		}
	}

	void intl_put(Page *page, Page *node, ptr_t key)
	{
		auto parent = page->template into<intl_t>();
		auto vec = load_data(key);
		auto [ok, pos] = intl_search(page, vec);
		bassert(!ok, "new key must not exists here, pos %d", pos);

		auto child = node->template into<node_t>();

		page->mark_dirty();
		if (!intl_is_full(parent)) {
			rshift(parent->kc, parent->count, pos);
			parent->kc[pos].key = key;
			parent->kc[pos + 1].child = child->self;
			parent->count += 1;
			return;
		}

		auto [k, r] = intl_split(page, child, pos, key);
		insert_fixup(page, r, k);
	}

	std::tuple<ptr_t, Page *>
	intl_split(Page *page, node_t *child, int pos, ptr_t key)
	{
		auto self = page->template into<intl_t>();
		int mid = (self->count + 1) / 2;
		auto node = node_alloc(e_node_intl);
		auto rhs = node->template into<intl_t>();

		node_append(self, rhs);

		rshift(self->kc, self->count, pos);
		self->kc[pos].key = key;
		self->kc[pos + 1].child = child->self;
		self->count += 1;

		auto rkey = self->kc[mid - 1].key;

		rhs->count = self->count - mid;
		for (int i = mid, j = 0; j < rhs->count; ++i, ++j) {
			rhs->kc[j] = self->kc[i];
			auto c = load_node(rhs->kc[j].child);
			if (c) {
				c->template into<intl_t>()->parent = rhs->self;
				c->mark_dirty();
			}
		}
		self->count -= rhs->count;
		return { rkey, node };
	}

	Page *leaf_split(leaf_t *leaf, int pos, kv_t kv)
	{
		auto mid = leaf->count / 2;
		auto page = node_alloc(e_node_leaf);
		auto node = page->template into<leaf_t>();

		node_append(leaf, node);

		rshift(leaf->kv, leaf->count, pos);
		leaf->kv[pos] = kv;
		leaf->count += 1;
		kv_inc();

		node->count = leaf->count - mid;
		copy(node->kv, leaf->kv + mid, node->count);
		leaf->count -= node->count;

		return page;
	}

	bool leaf_put(Page *page, View key, View val)
	{
		auto [ok, pos] = leaf_search(page, key);
		if (ok)
			return false;

		auto [done, pk, pv] = store_kv(key, val);
		if (!done) {
			debug("leaf put fail");
			return false;
		}

		auto leaf = page->template into<leaf_t>();

		if (!leaf_is_full(leaf)) {
			page->mark_dirty();
			rshift(leaf->kv, leaf->count, pos);
			leaf->kv[pos] = kv_t { pk, pv };
			leaf->count += 1;
			kv_inc();
			return true;
		}

		auto sibling = leaf_split(leaf, pos, kv_t { pk, pv });
		auto node = sibling->template into<leaf_t>();
		insert_fixup(page, sibling, node->kv[0].key);
		return true;
	}

	int key_index_in_parent(Page *parent, ptr_t key)
	{
		auto k = load_data(key);
		auto [ok, pos] = intl_search(parent, k);
		if (!ok)
			pos -= 1;
		return pos;
	}

	static int which_side(node_t *p, int idx, node_t *l, node_t *r)
	{
		if (idx == -1)
			return 1;
		if (idx == p->count - 2)
			return 0;
		return l->count >= r->count ? 0 : 1;
	}

	void leaf_simple_del(Page *page, int pos)
	{
		auto leaf = page->template into<leaf_t>();
		data_del(leaf->kv[pos]);
		lshift(leaf->kv, leaf->count, pos);
		leaf->count -= 1;
		kv_dec();
	}

	static void leaf_borrow_rhs(intl_t *p, leaf_t *leaf, leaf_t *r, int idx)
	{
		leaf->kv[leaf->count] = r->kv[0];
		leaf->count += 1;

		lshift(r->kv, r->count, 0);
		r->count -= 1;
		p->kc[idx].key = r->kv[0].key;
	}

	void leaf_merge_rhs(leaf_t *leaf, leaf_t *r)
	{
		copy(leaf->kv + leaf->count, r->kv, r->count);
		leaf->count += r->count;
		tree_del(r);
	}

	static void leaf_borrow_lhs(intl_t *p, leaf_t *leaf, leaf_t *l, int idx)
	{
		rshift(leaf->kv, leaf->count, 0);
		leaf->kv[0] = l->kv[l->count - 1];
		leaf->count += 1;
		l->count -= 1;
		p->kc[idx].key = leaf->kv[0].key;
	}

	void leaf_merge_lhs(leaf_t *leaf, leaf_t *l)
	{
		copy(l->kv + l->count, leaf->kv, leaf->count);
		l->count += leaf->count;
		tree_del(leaf);
	}

	void leaf_del(Page *page, View key)
	{
		auto [ok, pos] = leaf_search(page, key);
		if (!ok)
			return;

		page->mark_dirty();

		auto leaf = page->template into<leaf_t>();
		if (leaf_overhalf(leaf))
			return leaf_simple_del(page, pos);

		auto ppage = load_node(leaf->parent);
		if (!ppage) {
			if (leaf->count == 1) {
				data_del(leaf->kv[0]);
				kv_dec();
				tree_del(leaf);
				root_ = ptr_null;
				bassert(node_->hdr()->nr_kv == 0,
					"invalid kv count %zu expect 0",
					node_->hdr()->nr_kv);
			} else {
				leaf_simple_del(page, pos);
			}
			return;
		}

		auto parent = ppage->template into<intl_t>();

		auto idx = key_index_in_parent(ppage, leaf->kv[0].key);
		auto lhs = load_node(leaf->prev);
		auto rhs = load_node(leaf->next);
		auto l = lhs ? lhs->template into<leaf_t>() : nullptr;
		auto r = rhs ? rhs->template into<leaf_t>() : nullptr;
		auto right = which_side(parent, idx, l, r);

		leaf_simple_del(page, pos);

		if (right) {
			idx += 1;
			rhs->mark_dirty();
			if (leaf_overhalf(r)) {
				leaf_borrow_rhs(parent, leaf, r, idx);
			} else {
				leaf_merge_rhs(leaf, r);
				intl_del(ppage, idx);
			}
		} else {
			lhs->mark_dirty();
			if (leaf_overhalf(l)) {
				leaf_borrow_lhs(parent, leaf, l, idx);
			} else {
				leaf_merge_lhs(leaf, l);
				intl_del(ppage, idx);
			}
		}
	}

	void intl_borrow_rhs(intl_t *p, intl_t *node, intl_t *r, int idx)
	{
		node->kc[node->count - 1].key = p->kc[idx].key;
		p->kc[idx].key = r->kc[0].key;

		node->kc[node->count].child = r->kc[0].child;
		auto c = load_node(r->kc[0].child);
		bassert(c);
		c->template into<node_t>()->parent = node->self;
		node->count += 1;

		c->mark_dirty();

		for (int i = 0; i < r->count - 2; ++i)
			r->kc[i].key = r->kc[i + 1].key;
		for (int i = 0; i < r->count - 1; ++i)
			r->kc[i].child = r->kc[i + 1].child;

		r->count -= 1;
	}

	void intl_merge_rhs(intl_t *p, intl_t *node, intl_t *r, int idx)
	{
		node->kc[node->count - 1].key = p->kc[idx].key;

		for (int i = node->count, j = 0; j < r->count - 1; ++i, ++j)
			node->kc[i].key = r->kc[j].key;

		for (int i = node->count, j = 0; j < r->count; ++i, ++j) {
			node->kc[i].child = r->kc[j].child;
			auto c = load_node(r->kc[j].child);
			if (c) {
				c->template into<node_t>()->parent = node->self;
				c->mark_dirty();
			}
		}
		node->count += r->count;
		tree_del(node);
	}

	void
	intl_borrow_lhs(intl_t *p, intl_t *node, intl_t *l, int pos, int idx)
	{
		for (int i = pos; i > 0; --i)
			node->kc[i].key = node->kc[i - 1].key;

		for (int i = pos + 1; i > 0; --i)
			node->kc[i].child = node->kc[i - 1].child;

		node->kc[0].key = p->kc[idx].key;
		p->kc[idx].key = l->kc[l->count - 2].key;

		node->kc[0].child = l->kc[l->count - 1].child;
		auto c = load_node(node->kc[0].child);
		bassert(c);
		c->template into<node_t>()->parent = node->self;
		c->mark_dirty();
		l->count -= 1;
	}

	void
	intl_merge_lhs(intl_t *p, intl_t *node, intl_t *l, int pos, int idx)
	{
		l->kc[l->count - 1].key = p->kc[idx].key;

		for (int i = l->count, j = 0; j < node->count - 1; ++j) {
			if (j != pos) {
				l->kc[i].key = node->kc[j].key;
				i += 1;
			}
		}

		for (int i = l->count, j = 0; j < node->count; ++j) {
			// key's pos, thus child is pos + 1
			if (j == pos + 1)
				continue;
			l->kc[i].child = node->kc[j].child;
			auto c = load_node(l->kc[i].child);
			bassert(c);
			c->template into<node_t>()->parent = l->self;
			c->mark_dirty();
			i += 1;
		}

		l->count += node->count - 1;
		tree_del(node);
	}

	static void intl_simple_del(Page *page, int pos)
	{
		auto node = page->template into<intl_t>();
		bassert(node->count >= 2, "invalid node count %d", node->count);
		for (int i = pos; i < node->count - 2; ++i) {
			node->kc[i].key = node->kc[i + 1].key;
			node->kc[i + 1].child = node->kc[i + 2].child;
		}
		node->count -= 1;
	}

	void intl_del(Page *node, int pos)
	{
		node->mark_dirty();

		auto self = node->template into<intl_t>();
		if (intl_overhalf(self))
			return intl_simple_del(node, pos);

		auto ppage = load_node(self->parent);
		if (!ppage) {
			if (self->count == 2) {
				auto c = load_node(self->kc[0].child);
				bassert(c);
				c->template into<intl_t>()->parent = ptr_null;
				root_ = c->id();
				c->mark_dirty();
				tree_del(self);
			} else {
				intl_simple_del(node, pos);
			}
			return;
		}

		auto parent = ppage->template into<intl_t>();

		auto idx = key_index_in_parent(ppage, self->kc[0].key);
		auto lhs = load_node(self->prev);
		auto rhs = load_node(self->next);
		auto l = lhs ? lhs->template into<intl_t>() : nullptr;
		auto r = rhs ? rhs->template into<intl_t>() : nullptr;
		auto right = which_side(parent, idx, l, r);

		if (right) {
			idx += 1;
			rhs->mark_dirty();
			intl_simple_del(node, pos);
			if (intl_overhalf(r)) {
				intl_borrow_rhs(parent, self, r, idx);
			} else {
				intl_merge_rhs(parent, self, r, idx);
				intl_del(ppage, idx);
			}
		} else {
			lhs->mark_dirty();
			if (intl_overhalf(l)) {
				intl_borrow_lhs(parent, self, l, pos, idx);
			} else {
				intl_merge_lhs(parent, self, l, pos, idx);
				intl_del(ppage, idx);
			}
		}
	}

	template<typename T>
	static void copy(T *dst, T *src, int count)
	{
		memcpy(dst, src, count * sizeof(T));
	}

	template<typename T>
	int bsearch(T *arr, size_t n, View key)
	{
		int l = 0;
		int r = n - 1;

		while (l <= r) {
			int m = l + (r - l) / 2;
			// reply on implicit convert to View
			auto tmp = load_data(arr[m].key);
			if (Cmp::compare(tmp, key) >= 0)
				r = m - 1;
			else
				l = m + 1;
		}
		return l;
	}

	void node_append(node_t *head, node_t *node)
	{
		node->prev = head->self;
		node->next = head->next;
		head->next = node->self;
		auto next = load_node(node->next);

		if (next) {
			auto n = next->template into<node_t>();
			n->prev = node->self;
			next->mark_dirty();
		}
	}

	void node_del(node_t *node)
	{
		auto pprev = load_node(node->prev);
		auto pnext = load_node(node->next);

		if (pprev) {
			auto prev = pprev->template into<node_t>();
			prev->next = pnext ? pnext->id() : ptr_null;
			pprev->mark_dirty();
		}
		if (pnext) {
			auto next = pnext->template into<node_t>();
			next->prev = pprev ? pprev->id() : ptr_null;
			pnext->mark_dirty();
		}
	}

	std::tuple<bool, int> leaf_search(Page *page, View key)
	{
		auto l = page->template into<leaf_t>();
		auto pos = bsearch(l->kv, l->count, key);
		if (pos < l->count &&
		    Cmp::compare(load_data(l->kv[pos].key), key) == 0)
			return { true, pos };
		return { false, pos };
	}

	std::tuple<bool, int> intl_search(Page *page, View key)
	{
		auto it = page->template into<intl_t>();
		auto pos = bsearch(it->kc, it->count - 1, key);

		if (pos < it->count - 1 &&
		    Cmp::compare(load_data(it->kc[pos].key), key) == 0)
			return { true, pos };
		return { false, pos };
	}

	Page *node_alloc(enum node_type t)
	{
		auto id = node_->get();
		if (id == ptr_null)
			return nullptr;
		auto p = load_node(id);
		if (p) {
			auto node = p->template into<node_t>();
			node->parent = ptr_null;
			node->next = ptr_null;
			node->prev = ptr_null;
			node->type = t;
			node->self = p->id();
		}
		return p;
	}

	auto store_kv(View key, View val) -> std::tuple<bool, ptr_t, ptr_t>
	{
		auto pk = data_->store(key);
		if (pk == ptr_null)
			return { false, ptr_null, ptr_null };
		auto pv = data_->store(val);

		return { pv != ptr_null, pk, pv };
	}

	data_t load_data(ptr_t key)
	{
		return data_->load(key).collect();
	}

	Page *load_node(ptr_t key)
	{
		if (key == ptr_null)
			return nullptr;
		return node_->alloc(key);
	}

	void tree_del(node_t *node)
	{
		node_del(node);
		node_->free(node->self);
	}

	void data_del(kv_t kv)
	{
		data_->free(kv.key);
		data_->free(kv.val);
	}

	void kv_inc()
	{
		node_->hdr()->nr_kv += 1;
	}

	void kv_dec()
	{
		node_->hdr()->nr_kv -= 1;
	}
};

#endif // DISK_BPT_1719282881_H_
