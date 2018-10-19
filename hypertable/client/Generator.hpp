
#ifndef _HYPER_GENERATOR_H
#define _HYPER_GENERATOR_H

#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <string>

using namespace std;

namespace Hyper {

	/** check data as readable char */
	#define DATA_CHECK_CHAR		0

	enum CHAR_RANGE {
		char_min = 32,
		/** should never equal max */
		char_max = 127,
	};

	/** ignore some char */
	extern bool g_char_ignore;

	inline void	SetCharIgnore(bool set) {
		g_char_ignore = set;
	}

	/** ignore some char */
	inline void	CharIgnore(char& c) {
		if (g_char_ignore) {
			if (c == '\\') c++;
		}
	}

	/**
	 * get random data in range
	 **/
	inline int Random(int max, int min = 0) {
		if (max == min) return max;
		assert(max > min);

		return ::random() % (max - min) + min;
	}

	/**
	 * get random char in range
	 **/
	inline char RandomChar() {
		char c = ::random() % (char_max - char_min) + char_min;
		CharIgnore(c);
		return c;
	}

	inline void FillData(const char* buffer, int length) {
		int* data = (int*)buffer;

		for (; data < (int*)(buffer + length); data++) {
			*data = ::random();
		}
	}

	inline void UpdateData(const char* buffer, int length, int count = 0) {
		do {
			int offset = Random(length - sizeof(int));
			int *data = (int*)(buffer + offset);
			*data = ::random();
		} while (count-- > 0);
	}

	inline void FillChar(const char* buffer, int length) {
		char* data = (char*)buffer;

		for (; data < (buffer + length); data++) {
			*data = RandomChar();
		}
	}

	inline void UpdateChar(const char* buffer, int length, int count = 0) {
		do {
			int offset = Random(length - sizeof(char));
			char *data = (char*)(buffer + offset);
			*data = RandomChar();
		} while (count-- > 0);
	}

	inline void	CheckChar(bool check, char* buffer, int length) {
		#if DATA_CHECK_CHAR
		if (!check) return;

		for (char* data = buffer; data < buffer + length; data++) {
			if ((*data) < char_min || (*data) >= char_max) {
				assert(0);
			}
		}
		#endif
	}

	class GenGlobal
	{
	public:
		enum {
			table_unit_count = 256,
			table_unit_size = 4096,
			gen_data_size 	= 64 * 1024,
		};
	public:
		void	Init(int seed);

	public:
		/**
		 * global segment supplier
		 **/
		class Segment {

		public:
			Segment() {}

			/**
			 * default operation unit
			 **/
			struct Unit
			{
			public:
				void Init(bool rand_char = false) {
					if (rand_char) {
						FillChar(mData, GenGlobal::GenGlobal::table_unit_size);

						CheckChar(rand_char, mData, table_unit_size);

					} else {
						FillData(mData, GenGlobal::GenGlobal::table_unit_size);
					}
				}

			public:
				char mData[table_unit_size];
			};

		public:
			void	Init(bool rand_char = false) {

				for (size_t i = 0; i < GenGlobal::table_unit_count; i++) {
					mUnits[i].Init(rand_char);
				}
			}

			const char* Data() { return mUnits[Next()].mData; }

			int		Size() { return table_unit_size; }

		protected:
			int		Next() { return Random(GenGlobal::table_unit_count); }

		public:
			Unit	mUnits[GenGlobal::table_unit_count];
		};

	public:
		const char* Data(bool rand_char = false) {
			return rand_char ? segment_char.Data() : segment_data.Data();
		}

		int		Size(bool rand_char = false) {
			return rand_char ? segment_char.Size() : segment_data.Size();
		}

	public:
		static Segment segment_char;
		static Segment segment_data;
	};
	extern GenGlobal g_generator;

	class Generator {
	public:
		Generator() : mRandChar(false) { memset(mData, 0, GenGlobal::gen_data_size); };
		virtual ~Generator() {}

	public:
		void	Init(bool rand_char = false) {
			mRandChar = rand_char;
			for (int off = 0; off + g_generator.Size(rand_char) < GenGlobal::gen_data_size;
				off += g_generator.Size(rand_char))
			{
				memcpy(mData + off, g_generator.Data(rand_char), g_generator.Size());
			}
		}

		int		Size() { return GenGlobal::gen_data_size; }

		virtual void Data(char*& data, int& size) = 0;

	protected:
		char	mData[GenGlobal::gen_data_size];
		bool	mRandChar;

		friend class KeyDataGen;
	};


	class SequenGen : public Generator {
	public:
		SequenGen() : mSize(20) {}

	public:

		void	Size(int size) {
			mSize = size;
			mData[mSize] = 0;
		}

		virtual void Data(char*& data, int& size) {
			Next();

			data = mData;
			size = mSize;
		}

	protected:

		virtual void Next() {
			int* data = (int*)mData;

			while (1) {
				*data = (*data) + 1;
				if ((*data) != 0) {
					break;
				}

				data++;
				if (data == (int*)(mData + mSize)) {
					data = (int*)mData;
				}
			};
		}

	protected:
		int		mSize;
	};

	class SeqCharGen : public SequenGen {
	public:
		SeqCharGen() {}

		void	Init(bool rand_char = false) {
			SequenGen::Init(true);
		}
	protected:

		virtual void Next() {
			char* data = mData;

			while (1) {
				*data = (*data) + 1;
				CharIgnore(*data);

				if ((*data) != (char)char_max) {
					break;
				}
				*data = (char)char_min;

				data++;
				if (data == (mData + mSize)) {
					data = mData;
				}
			}
		}
	};

	class RandomGen : public Generator {
	public:
		RandomGen() : mMin(0), mMax(0) {}

	public:
		void	Range(int max, int min = -1) {
			mMin = std::min(max, min);
			mMax = std::max(max, min);
			{
				if (mMin == -1) {
					mMin = mMax;
					assert(mMin != -1);
				} else {
					mMin = std::max(1, mMin);
				}
			}
			assert(mMax <= Size());
		}

		virtual void Data(char*& data, int& size) {
			/** Todo: using loop buffer; use one more byte for string \0 */
			static int s_ending = Size() - 1 - g_generator.Size(mRandChar);
			int offset, length;

			/** get random size */
			size = Random(mMax, mMin);
			/** get random offset */
			if (s_ending < size) {
				offset = Size() - size;

			} else {
				offset = Random(s_ending - size);
			}
			data = mData + offset;

			/** copy head data form global */
			length = size < g_generator.Size(mRandChar) ?
					size : g_generator.Size(mRandChar);
			memcpy(mData + offset, g_generator.Data(mRandChar), length);

			/** set new data random */
			if (mRandChar) {
				UpdateChar(data, size, std::min(4, size));
			} else {
				UpdateData(data, size);
			}

			#if DATA_CHECK_CHAR
			CheckChar(mRandChar, data, size);
			#endif
		}

	protected:
		int		mMin;
		int		mMax;
	};

	class UUIDGen : public Generator {
	public:
		virtual void Data(char*& data, int& size);
	};

	class KeyDataGen {

	public:
		KeyDataGen() : mRandKey(false), mRandData(false), mKey(NULL), mData(NULL), mLast(NULL), mKeep(RandomChar()) {}

	public:
		void	SetKey(bool rand_key, bool use_char, bool uuid_key = false) {
			mRandKey = rand_key;
			if (uuid_key) {
				mKey = &mKeyUUID;

			} else if (mRandKey) {
				mKey = &mKeyRand;
				mKeyRand.Init(use_char);

			} else {
				mKey = &mKeySeqn;
				mKeySeqn.Init(use_char);
			}
			/** force key using char */
			use_char = true;
			mLast = mKey->mData;
		}

		void	SetData(bool rand_data, bool use_char, bool uuid_data = false) {
			mRandData = rand_data;

			if (uuid_data) {
				mData = &mDataUUID;

			} else if(mRandData) {
				mData = &mDataRand;
				mDataRand.Init(use_char);

			} else {
				mData = &mDataSeqn;
				mDataSeqn.Init(use_char);
			}
		}

		void	KeyRange(int max, int min = 0) {
			if (mRandKey) {
				mKeyRand.Range(max, min);
			} else {
				mKeySeqn.Size(max);
			}
		}

		void	DataRange(int max, int min = 0) {
			if (mRandData) {
				mDataRand.Range(max, min);
			} else {
				mDataSeqn.Size(max);
			}
		}

	public:
		void	Key(char*& data, int& size) {
			if (mRandKey) {
				*mLast = mKeep;
			}

			mKey->Data(data, size);

			if (mRandKey) {
				mLast = &data[size];
				mKeep = data[size];
				data[size] = 0;

				#if CHECK_GENERATE_DATA
				assert(mLast < mKey->mData +
					GenGlobal::gen_data_size);
				#endif
			}
		}

		void	Data(char*& data, int& size) {
			mData->Data(data, size);
		}

	protected:
		bool		mRandKey;
		bool		mRandData;
		Generator*	mKey;
		Generator* 	mData;

		SeqCharGen	mKeySeqn;
		RandomGen	mKeyRand;
		UUIDGen		mKeyUUID;

		SequenGen	mDataSeqn;
		RandomGen	mDataRand;
		UUIDGen		mDataUUID;

		char*		mLast;
		char		mKeep;
	};

	void	generator_test(int type);
}

#endif //_HYPER_GENERATOR_H
