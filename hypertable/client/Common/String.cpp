
#include "String.hpp"

namespace common {

const char* c_newline		= "\n";
const char* c_seperators	= " \t\n\r,";
const char* c_remark_string = "\"#";

std::string&
toupper(std::string& str)
{
	for( std::string::size_type i = 0; i < str.length(); i++ )
		str[i] = ::toupper( str[i] );
	return str;
}

std::string&
tolower(std::string& str)
{
	for( std::string::size_type i = 0; i < str.length(); i++ )
		str[i] = ::tolower( str[i] );
	return str;
}

bool
stnicmp(const std::string& s1, const std::string& s2, bool nonempty)
{
	if(nonempty && s1.length() == 0 && s2.length() == 0) return false;
	return s1.length() == s2.length() &&
		strncasecmp(s1.c_str(), s2.c_str(), s1.length()) == 0;
}

std::string&
trim_left(std::string& str, const char* tick)
{
	///< find string valid head
	std::string::size_type pos = str.find_first_not_of(tick);
	///< nothing left
	if( pos == std::string::npos ){
		str.clear();
	}else if( pos != 0 ){
		str = str.substr(pos);
	}
	return str;
}

std::string&
trim_right(std::string& str, const char* tick)
{
	///< find string valid tail
	std::string::size_type pos = str.find_last_not_of(tick);
	if( pos == std::string::npos ){
		str.clear();
	}else if( pos != str.length() - 1 ){
		str = str.substr(0, pos + 1);
	}
	return str;
}

std::string&
trim_str(std::string& str, const std::string& match, bool left)
{
	std::string::size_type pos;
	if( left ){
		if( (pos = str.find(match)) == 0 )
			str = str.substr(match.length());
	}else{
		if( (pos = str.rfind(match)) != std::string::npos &&
			pos + match.length() == str.length() )
			str = str.substr(0, pos);
	}
	return str;
}

std::string&
parse(std::string& str, std::string& part, const char* sp)
{
	///< trim left by sp
	trim_left(str, sp);
	///< find first char in sp, find part end
	std::string::size_type pos = str.find_first_of(sp);
	if( pos == std::string::npos ){
	///< only left one param
		part = str;
		str.clear();
	}else{
	///< distill next param
		part = str.substr(0, pos);
		str = str.substr(pos + 1);
	}
	return part;
}

std::string
column(const std::string& str, int num, const char* sp)
{
	if( num < 0 ) return "";

	std::string tmp = str;
	std::string::size_type pos;
	while(1){
		///< find first char in sp, find part end
		if( (pos = tmp.find_first_not_of(sp)) ==
			std::string::npos )	break;
		if( pos != 0 ) tmp = tmp.substr(pos);
		pos = tmp.find_first_of(sp);
		if( num-- == 0 ){
			return pos == std::string::npos ?
				tmp : tmp.substr(0, pos);
		}
		if( pos == std::string::npos ) break;
		tmp = tmp.substr(pos + 1);
	}
	return "";
}

bool
check_head(std::string& src, const std::string& cp, bool trunc, const char* sp)
{
	std::string str = src;
	///< trim left by sp
	trim_left(str, sp);
	if( !stnicmp(parse(str, sp), cp) ){
		return false;

	}else if( trunc ){
		src = str;
	}
	return true;
}

bool
check_rmk(const std::string& src, const char* rmk, const char* sp)
{
	std::string str = src;
	///< trim left by sp
	trim_left(str, sp);
	return str.find_first_of(rmk) == 0;
}

bool
split(std::string str, std::string& v1, std::string& v2, const char* sp, bool reverse)
{
	std::string::size_type pos = !reverse ? str.find_first_of(sp) : str.find_last_of(sp);
	///< not find sp
	if( pos == std::string::npos ){
		v1 = str;
		v2 = "";
		return false;
	}
	///< trim two string
	trim((v1 = str.substr(0, pos)));
	trim((v2 = str.substr(pos + 1)));
	return true;
}

bool
split_str(std::string str, std::string& v1, std::string& v2, const char* sp)
{
	std::string::size_type pos = str.find(sp);
	///< not find sp
	if( pos == std::string::npos ){
		v1 = str;
		v2 = "";
		return false;
	}
	///< trim two string
	trim((v1 = str.substr(0, pos)));
	trim((v2 = str.substr(pos + strlen(sp))));
	return true;
}

bool
distill(std::string str, std::string& v, const char* sp1, const char* sp2, bool reverse)
{
	std::string::size_type pos1 = str.find_first_of(sp1);
	std::string::size_type pos2 = !reverse ? str.find_first_of(sp2) : str.find_last_of(sp2);

	///< not find value
	if( pos1 == std::string::npos || pos2 == std::string::npos || pos1 >= pos2) return false;
	///< trim value
	trim((v = str.substr(pos1 + 1, pos2 - pos1 - 1)));
	return true;
}

bool
find(const std::string& str, const char* sp, const char* nsp, bool bstr)
{
	if( !bstr ){
		return str.find_first_of(sp) != std::string::npos
			&& (nsp == NULL || str.find_first_of(nsp) == std::string::npos);
	}else{
		return str.find(sp) != std::string::npos
			&& (nsp == NULL || strlen(nsp) == 0 || str.find(nsp) == std::string::npos);
	}
}

bool
find_exact(const std::string& str, const std::string& substr)
{
	std::string::size_type pos = 0;

	while( (pos = str.find(substr, pos))
		!= std::string::npos )
	{
		pos += substr.length();

		/** till the and or not a num or alpha */
		if( str.length() == pos ||
			::isalnum(str.at(pos)) == 0 )
			return true;
	}
	return false;
}

std::string
before(const std::string& str, const char* sp, bool bstr)
{
	std::string::size_type pos = bstr ? str.find(sp) : str.find_first_of(sp);
	if( pos == std::string::npos ) return str;
	return str.substr(0, pos);
}

std::string
after(const std::string& str, const char* sp, bool bstr)
{
	std::string::size_type pos = bstr ? str.find(sp) : str.find_first_of(sp);
	if( pos == std::string::npos ) return "";
	size_t len = (bstr ? strlen(sp) : 1);
	return str.substr(pos + len);
}

void
append(std::string& str, size_t count, char c)
{
	std::string tmp(count, c);
	str += tmp;
	return;
}

}

