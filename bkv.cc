// SPDX-License-Identifier: GPL-2.0-or-later
/*
 * Author: Abby Cin
 * Mail: abbytsing@gmail.com
 * Create Time: 2024-06-25 10:35:13
 */

#include "bkv.h"
#include <iostream>

void print_kv(View k, View v)
{
	std::cout.write((char *)k.data(), k.size());
	std::cout << " => ";
	std::cout.write((char *)v.data(), v.size());
	std::cout << '\n';
}

int main(int argc, char *argv[])
{
	if (argc != 2) {
		debug("%s db_dir", argv[0]);
		return 1;
	}

	auto db = Db<>::open(argv[1]);
	data_t motto;

	db->put("alpha", "alpah");
	db->put("beta", "beta");
	db->put("garma", "garma");
	db->put("delta", "delta");

	auto it = db->range("garma", "zeta");

	while (it) {
		auto k = it.key();
		auto v = it.val();

		print_kv(k, v);
		++it;
	}

	debug("before items %zu count %zu", db->item(), db->count());
	std::string s {};

	size_t n = 20000;
	s.reserve(n);
	for (size_t i = 0; i < n; ++i)
		s.push_back('a');

	for (size_t i = 0; i < n; ++i) {
		View v { s.data(), i + 1 };
		db->put(v, v);
		if (i % 1000 == 0)
			db->flush();
		auto r = db->get(v);
		bassert(r == v);
	}

	debug("insert items %zu count %zu", db->item(), db->count());

	for (size_t i = 0; i < n; ++i) {
		View v { s.data(), i + 1 };
		db->del(v);
		if (i % 1000 == 0)
			db->flush();
		bassert(!db->contains(v));
	}
	debug("after items %zu count %zu", db->item(), db->count());
}
