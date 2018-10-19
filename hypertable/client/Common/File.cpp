
/////////////////////////////////////////////////////////////
/// Copyright(c) 2010 rivonet, All rights reserved.
/// @file:		FileUt.cpp
/// @brief:		base file utility
///
/// @author:	liudong.8284@gmail.com
/// @date:		2010-09-01
/////////////////////////////////////////////////////////////

#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/statvfs.h>
#include <string.h>
#include "File.hpp"

namespace common {

std::string
module_path()
{
	char data[4096];
	int count = readlink("/proc/self/exe", data, 4096);
	if (count < 0 || count >= 4096) return "";
	data[count] = 0;
	return data;
}

std::string
curr_path()
{
	char data[4096];
	return getcwd(data, 4096);
}

bool
is_absolute_path(const std::string& path)
{
	return path.find_first_of("/\\") == 0;
}

    std::string
smart_path(const std::string& path)
{
	if( is_absolute_path(path) ) return path;
	std::string curr = cut_end_slash(curr_path());
	return curr + (path.length() == 0
		? "" : "/") + path;
}

bool
is_path(const std::string& path)
{
	return path.find_first_of("/\\") != std::string::npos;
}

std::string
split_path(const std::string& path, bool slash)
{
	std::string dir;
	split_path_file(path, &dir, NULL, slash);
	return dir;
}

std::string
split_file(const std::string& path)
{
	std::string file;
	split_path_file(path, NULL, &file);
	return file;
}

void
split_path_file(const std::string& path, std::string* dir, std::string* file, bool slash)
{
	std::string::size_type pos = path.find_last_of("/\\");

	///< we hope that there should not have any dup '/' or '\'
	///< like /file
	if( pos == 0 ){
		if( dir )	*dir = path.at(0);
		if( file )	*file = path.substr(1);
		return;
	}

	///< when path not including any '/' or '\', then
	///<	if you set slash true, dir is empty and path is also file
	///<	if you set slash false,dir and file both be path
	if( dir ){
		if( pos == std::string::npos ){
			*dir = !slash ? path : "";
		}else{
			*dir = path.substr(0, pos);
		}
	}

	if( file ){
		if( pos == std::string::npos )
			*file = path;
		else
			*file = path.substr(pos + 1);
	}
	return;
}

const char*
last_part(const char* name)
{
    const char* last = strrchr(name, '/');
    return last ? last + 1 : name;
}

std::string
split_app(const std::string& file)
{
	std::string::size_type pos = file.find_last_of(".");
	if( pos == std::string::npos ) return file;
	return file.substr(0, pos);
}

std::string
ins_sub_dir(const std::string& path, const std::string& dir)
{
	std::string spath = split_path(path, true);
	if(spath.length() != 0 || path.find_first_of("/\\") == 0)
		spath += "/";
	return spath + dir + "/" + split_file(path);
}

bool
make_path(const std::string& path, int mode)
{
	if( file_exist(path) ) return true;

	for (std::string::size_type i = 0; i < path.size(); ){
		std::string::size_type a = path.find_first_of("/\\", i);
		if (a == std::string::npos)	a = path.size();
		if( !mkdir(path.substr(0, a).c_str(), mode) ){
			///< TEST_DEBUG
			//_dbg(path.substr(0, a) << " mkdir err " << errno );
			if(errno != EEXIST) return false;
		}
		i = a + 1;
	}
	return true;
}

bool
make_file_path(const std::string& path, int mode)
{
	return make_path(split_path(path, true), mode);
}

std::string
cut_end_slash(const std::string& path)
{
	std::string::size_type pos = path.find_last_not_of("/\\");
	if( pos != path.length() - 1 ) return path.substr(0, pos + 1);
	return path;
}

std::string
prune_dup_slash(const std::string& path)
{
	///< no need prune
	if( path.find("//") == std::string::npos ) return path;

	std::string s;
	for (std::string::size_type i = 0; i < path.size(); ){
		std::string::size_type a = path.find_first_of("/", i);
		if ( a == std::string::npos ) a = path.size() - 1;
		s += path.substr( i, a - i + 1 );
		std::string::size_type b = path.find_first_not_of( "/", a + 1 );
		if ( b == std::string::npos ) break;
		i = b;
	}
	return s;
}

std::string
fix_path_unix(const std::string& path)
{
	std::string s = path;
	for( std::string::size_type i = 0; i < s.size(); i++ ){
		if( s[ i ] == '\\' ) s[ i ] = '/';
	}
	return s;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

file_handle_t
file_open(const std::string& path, bool create, bool trunc, bool write, int mode)
{
	return ::open(path.c_str(), O_RDWR | ( create ? O_CREAT : 0) | ( trunc ? O_TRUNC : 0 ), mode);

}

bool
file_close(file_handle_t fd)
{
	if( fd == c_invalid_handle ) return true;
	return close(fd) == 0;
}

uint64_t
file_len(file_handle_t fd)
{
	struct stat st;
	if( fstat(fd, &st) < 0 ) return -1;
	return st.st_size;
}

uint64_t
file_pos(file_handle_t fd)
{
	return lseek64(fd, 0, SEEK_CUR);
}

bool
file_seek(file_handle_t fd, uint64_t off)
{
	if( off == c_invalid_offset ){
		return lseek64(fd, 0, SEEK_END) != (off_t)-1;
	}else{
		return lseek64(fd, off, SEEK_SET) != (off_t)-1;
	}
}

bool
file_trunc(file_handle_t fd, uint64_t len)
{
	return ftruncate(fd, len) == 0;
}

int
file_write(file_handle_t fd, const char* data, uint32_t len, uint64_t off)
{
	if( off != c_invalid_offset && !file_seek(fd, off) ) return -1;
	return write(fd, data, len);
}

int
file_read(file_handle_t fd, char* data, uint32_t len, uint64_t off)
{
	if( off != c_invalid_offset && !file_seek(fd, off) ) return -1;
	return read(fd, data, len);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////

uint32_t
file_load(const std::string& path, char* data, uint32_t len, uint64_t off)
{
	file_handle_t fd = file_open(path, false, false, false);
	if( fd == c_invalid_handle ) return -1;
	len = file_read(fd, (char*)data, len, off);
	file_close(fd);
	return len;
}

bool
file_rush(const std::string& path, const char* data, uint32_t len, int mode)
{
	file_handle_t fd = file_open(path.c_str(), true, true, true, mode);
	if( fd == c_invalid_handle ) return false;
	uint32_t wlen = file_write(fd, (char*)data, len);
	file_close(fd);
	return len == wlen;
}

uint64_t
file_len(const std::string& path)
{
	file_handle_t hd = file_open(path);
	if( hd == c_invalid_handle ) return -1;
	uint64_t len = lseek64(hd, 0, SEEK_END);
	file_close(hd);
	return len;
}

bool
file_exist(const std::string& path)
{
	return access(path.c_str(), F_OK) == 0;
}

bool
file_dir(const std::string& path)
{
	struct stat filestat;
	return !(lstat(path.c_str(), &filestat) < 0 || S_ISDIR(filestat.st_mode) == 0);
}

bool
file_reg(const std::string& path)
{
	struct stat filestat;
	return !(lstat(path.c_str(), &filestat) < 0 || S_ISREG(filestat.st_mode) == 0);
}

bool
file_rm(const std::string& path, bool dir)
{
	return dir ? rmdir(path.c_str()) == 0 : unlink(path.c_str()) == 0;
}

uint64_t
file_trunc(const std::string& path, uint64_t newlen, bool create)
{
	file_handle_t hd = file_open(path, create);
	if( hd == c_invalid_handle ) return -1;

	uint64_t len = lseek64(hd, 0, SEEK_END);
	if( len != newlen ){
		if( ftruncate(hd, newlen) != 0 ){
			file_close(hd);
			return -1;
		}
	}
	file_close(hd);
	return len;
}

bool
file_move(const std::string& pathold, const std::string& pathnew)
{
	file_rm(pathnew, false);
	file_rm(pathnew, true);
	return rename(pathold.c_str(), pathnew.c_str()) == 0;
}


FileBase::FileBase()
{
	clear();
}

FileBase::~FileBase()
{
	close();
}

void
FileBase::clear()
{
	m_fd = c_invalid_handle;
}

bool
FileBase::open(const std::string& filepath, int flag, int mode)
{
	/** if already opend, close first */
	if( is_open() ) close();

	m_path = filepath;
	m_name = split_file(m_path);

	/** when create, try to make path */
	if( (flag & op_create) &&
		!make_file_path(path()) ) return false;

	m_fd = file_open(path(),
		(flag & op_create)!= 0,
		(flag & op_trunc) != 0,
		(flag & op_write) != 0,
		mode);

	return is_open();
}

bool
FileBase::close()
{
	if( !is_open() ) return true;

	if( m_fd != c_invalid_handle ){
		bool ret = file_close(m_fd);
		m_fd = c_invalid_handle;
		return ret;
	}
	return true;
}

uint64_t
FileBase::file_len()
{
	if( !is_open() ) return -1;
	return common::file_len(m_fd);
}

bool
FileBase::trunc(uint64_t len)
{
	if( !is_open() ) return false;
	return file_trunc(m_fd, len);
}

bool
FileBase::flush()
{
	if( !is_open() ) return false;
	///< should flush_buf firset
	return 0 == fsync(m_fd);		 //fdatasync
}
}

