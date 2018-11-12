
#include <unistd.h>
#include <uuid/uuid.h>

#include "Generator.hpp"
#include "Time.hpp"
#include "Logger.hpp"
#include "Util.hpp"
#include "Container.hpp"

using namespace Hyper;

bool Hyper::g_char_ignore = false;

GenGlobal::Segment GenGlobal::segment_char;
GenGlobal::Segment GenGlobal::segment_data;
GenGlobal Hyper::g_generator;

void
GenGlobal::Init(int seed)
{
	static bool s_initial = false;
	if (s_initial) return;
	s_initial = true;

	::srandom(seed);

	segment_char.Init(true);
	segment_data.Init(false);
}

void
UUIDGen::Data(char*& data, int& size)
{
	get_UUID(mData);
	data = mData;
	size = 32;
}

void
generator_keydatagen_test()
{
	g_generator.Init(100);

	TimeRecord time;
	char* key;
	char* data;
	int klen, dlen;

	KeyDataGen gen;

	gen.SetKey(false, true);
	gen.SetData(true, true);

	gen.DataRange(1024 * 40, 1024 * 10);

	time.begin();
	for (int i = 0; i < 100; i++) {
		gen.Key(key, klen);
		gen.Data(data, dlen);
		
		if (i % 100000000 == 0) {
			log_info("len : " << klen << " - " << dlen << ", key: " << string(key, klen)
					<< ", data : " << string(data, dlen));
		}
	}
	time.check();
	log_info("using " << string_time(time.elapse()));
}

void
generator_rand_test()
{
	g_generator.Init(100);

	int length = 20;
	bool char_data = true;
	int loop = 1000000;

	RandomGen rand;
	rand.Range(length);
	rand.Init(char_data);

	int count = 0;
	int total = 0;
	char* data;
	int size;

	TypeSet<std::string> set;
	for (int i = 0; i < loop; i++) {
		rand.Data(data, size);
		std::string v(data, size);
		if (set.get(&v)) {
			count++;
		} else {
			set.add(&v);
			total++;
		}

		if (total % 1000000 == 0) {
			log_info("conflict count " << count << ", total " << total);
		}
	}
	log_info("conflict count " << count << ", total " << total);
}

void
Hyper::generator_test(int type)
{
	switch(type) {
	case 0: generator_keydatagen_test(); break;
	case 1: generator_rand_test(); break;
	default: assert(0); break;
	}
	exit(0);
}

