

#pragma once

#include <string>
#include <vector>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace common {

typedef int file_handle_t;

static const int c_invalid_handle = -1;
static const int c_dir_permit 	= (S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP | S_IXGRP);
static const int c_file_permit	= (S_IRWXU | S_IROTH | S_IXOTH | S_IRGRP | S_IXGRP);
static const uint64_t c_invalid_offset = (uint64_t)-1LL;

/**
 * @brief get current app path
 */
std::string	module_path();

/**
 * @brief get current path
 */
std::string	curr_path();

/**
 * @brief get path and file
 * @param path total path, absolute or relative
 * @return absolute path in current enviorment
 * @note if path is a absolute path, just split;
 * or get current path and make it as absolute
 */
std::string	smart_path(const std::string& path);

/**
 * @brief check path is a dir
 * @param path the src path
 */
bool	is_path(const std::string& path);

/**
 * @brief check path is a absolute path
 * @param path the src path
 */
bool	s_absolute_path(const std::string& path);

/**
 * @brief split dir and file from path
 * @param path the src path
 * @param dir dir ptr
 * @param file file ptr
 * @param slash must have slash
 */
void	split_path_file(const std::string& path, std::string* dir = NULL, std::string* file = NULL, bool slash = false);

/**
 * get last part as file name
 **/
const char* last_part(const char* name);

/**
 * @brief split dir from path
 * @param path the src path
 * @param slash must have slash
 */
std::string	split_path(const std::string& path, bool slash = false);

/**
 * @brief split file from path
 * @param path the src path
 */
std::string	split_file(const std::string& path);

/**
 * @brief split app from file name
 * @param file the src file name
 */
std::string	split_app(const std::string& file);

/**
 * @brief insert sub dir to file
 * @param path the src path
 */
std::string	ins_sub_dir(const std::string& path, const std::string& dir);

/**
 * @brief erase end slash to keep dir as the same style
 * @param path the src path
 */
std::string	cut_end_slash(const std::string& path);

/**
 * @brief prune duplicate black slash
 * @param path the src path
 */
std::string	prune_dup_slash(const std::string& path);

/**
 * @brief fix path to unix style
 * @param path the orgin path to fix
 * @note convert '\\' to '/'
 */
std::string	fix_path_unix(const std::string& path);

/**
 * @brief make sure dir exist
 * @param path the src path exluding file name
 * @param mode dir mode if need create
 * @return result
 */
bool	make_path(const std::string& path, int mode = c_dir_permit);

/**
 * @brief make sure file parent dir exist
 * @param path the src path including file name
 * @param mode dir mode if need create
 */
bool	make_file_path(const std::string& path, int mode = c_dir_permit);

//////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////
/**
 * @brief file operate api for flexible operation system
 */

/**
 * @brief open file and get file handle
 * @param path file complete path including file name
 * @param create create file if not exist
 * @param trunc open with trunc flag
 * @param write open with write flag
 * @param mode create mode
 * @return file handle
 */
file_handle_t file_open(const std::string& path, bool create = false,
				bool trunc = false, bool write = true, int mode = c_file_permit);

/**
 * @brief close file handle
 * @param fd file handle
 */
bool		file_close(file_handle_t fd);

/**
 * @brief get file len
 * @param fd file handle
 */
uint64_t	file_len(file_handle_t fd);

/**
 * @brief get current file handle pos
 * @param fd file handle
 */
uint64_t	file_pos(file_handle_t fd);

/**
 * @brief seek file to a new pos
 * @param fd file handle
 * @param off new position, if default, seek to file end
 * @return suc or fail
 */
bool		file_seek(file_handle_t fd, uint64_t off = c_invalid_offset);

/**
 * @brief truncate file to a new len
 * @param fd file handle
 * @param len new file len
 * @return suc or failed
 */
bool		file_trunc(file_handle_t fd, uint64_t len);

/**
 * @brief read file to buffer
 * @param fd file handle
 * @param buffer the src buffer
 * @param len buffer len
 * @param off read off, if default, read at current pos
 */
int			file_read (file_handle_t fd, char* buffer, uint32_t len, uint64_t off = c_invalid_offset);

/**
 * @brief write buffer to file
 * @param fd file handle
 * @param buffer the src buffer
 * @param len buffer len
 * @param off write off, if default, write at current pos
 */
int			file_write(file_handle_t fd, const char* buffer, uint32_t len, uint64_t off = c_invalid_offset);

#ifndef _WIN32
uint32_t		file_write_all(int fd, char* buffer, uint32_t len);
uint32_t		file_read_all(int fd, char* buffer, uint32_t len);
#endif

//////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////
/**
 * @brief file operate utility for flexible operation system
 */

/**
 * @brief get file len
 * @param path the src file path
 * @return file len
 */
uint64_t	file_len(const std::string& path);

/**
 * @brief load file to buffer
 * @param path the src file path
 * @param buffer the dst buffer
 * @param len buffer len and the the len try to read out
 * @param off read off, if default, read at 0
 * @return acctually loaded len
 */
uint32_t	file_load(const std::string& path, char* buffer, uint32_t len, uint64_t off = c_invalid_offset);

/**
 * @brief rush buffer to a new file
 * @param path the dst file path
 * @param buffer the src buffer
 * @param len buffer len
 * @param mode file mode if need create
 * @return if writted len not match buffer len, return false
 */
bool		file_rush(const std::string& path, const char* buffer, uint32_t len, int mode = c_file_permit);

/**
 * @brief check if file is exist
 * @param path the src file path
 */
bool		file_exist(const std::string& path);

/**
 * @brief check if given path is a dir
 * @param path the src file path
 * @return result
 */
bool		file_dir(const std::string& path);

/**
 * @brief check if given path is a regular file
 * @param path the src file path
 */
bool		file_reg(const std::string& path);

/**
 * @brief rush buffer to a new file
 * @param path the src file path
 * @param path the dst file path
 * @param force if dst exist,with param -f
 * @return result
 */
bool		file_copy(std::string src, std::string dst, bool force = false);

/**
 * @brief delete file or dir
 * @param path the src file path
 * @param dir delete type
 * @return result
 */
bool		file_rm(const std::string& path, bool dir = false);

/**
 * @brief truncate file to a new len
 * @param path the src file path
 * @param newlen file new len
 * @param create create file if not exist
 * @return file old len
 */
uint64_t	file_trunc(const std::string& path, uint64_t newlen, bool create = true);

/**
 * @brief move origin file to a new place
 * @param pathold the src file path
 * @param pathnew the dst file path
 */
bool		file_move(const std::string& pathold, const std::string& pathnew);

/**
 *@brief base file handler
 */
class FileBase
{
public:
	FileBase();
	virtual ~FileBase();

	/**
	 * @brief define open type
	 */
	enum {
		op_null		= 0x00,			///< none
		op_create	= 0x01,			///< create flag
		op_trunc	= 0x02,			///< trunc flag
		op_read		= 0x04,			///< open for read
		op_write	= 0x08,			///< open for write
	};
public:
	/**
	 * @brief get file path
	 */
	const std::string& path() { return m_path; }

	/**
	 * @brief set file path
	 */
	void		path(const std::string& p) { m_path = p; }

	/**
	 * @brief get file name
	 */
	std::string	name() { return m_name; }

	/**
	 * @brief open file
	 * @param filename open name
	 * @param flag open flag
	 * @param mode open mode
	 * @return suc or fail
	 */
	bool		open(const std::string& path, int flag = op_create | op_read | op_write, int mode = c_file_permit);

	/**
	 * @brief close file
	 * @note flush buffer and close file
	 * @note not force flush system buffer
	 */
	bool		close();

	/**
	 * @brief truncate file to a new len
	 * @param len file new len
	 * @return suc or fail
	 */
	bool		trunc(uint64_t len);

	/**
	 * @brief seek file to new off
	 * @param off new off, -1 means move to file end
	 */
	bool		seek(uint64_t off = c_invalid_offset){
		return file_seek(m_fd, off);
	}

	/**
	 * @brief write file use no buffer
	 * @param buf the src buffer
	 * @param len the buffer len
	 * @param off write off, -1 means use current off
	 * @return suc >= 0
	 */
	int			write(const void* buf, uint32_t len, uint64_t off = c_invalid_offset){
		return file_write(m_fd, (char*)buf, len, off);
	}

	/**
	 * @brief read file use no buffer
	 * @param buf the src buffer
	 * @param len the buffer len
	 * @param off read off, -1 means use current off
	 * @return suc >= 0
	 */
	int			read(void* buf, uint32_t len, uint64_t off = c_invalid_offset){
		return file_read(m_fd, (char*)buf, len, off);
	}

	/**
	 * @param buf the src buffer
	 * @param len the buffer len
	 * @param off read off, -1 means use current off
	 * @return suc >= 0
	 */
	int			pwrite(const void* buf, uint32_t len, uint64_t off){
		if( !is_open() ) return -1;
		return ::pwrite(m_fd, buf, len, off);
	}

	/**
	 * @brief read file use no buffer
	 * @param buf the src buffer
	 * @param len the buffer len
	 * @param off read off, -1 means use current off
	 * @return suc >= 0
	 */
	int			pread(void* buf, uint32_t len, uint64_t off){
		if( !is_open() ) return -1;
		len = ::pread(m_fd, buf, len, off);
		return len;
	}

	/**
	 * @brief flush all right now
	 */
	bool		flush();

	/**
	 * @brief check file open stat
	 */
	bool		is_open() { return m_fd != c_invalid_handle; }

	/**
	 * @brief get file len
	 */
	uint64_t	file_len();

	/**
	 * @brief get file handle
	 */
	file_handle_t	fd() { return m_fd; }

	/**
	 * @brief reset state
	 */
	void		clear();

protected:
	/** file handle */
	file_handle_t	m_fd;
	/** file path */
	std::string		m_path;
	/** file name */
	std::string		m_name;
};

}

using common::FileBase;

