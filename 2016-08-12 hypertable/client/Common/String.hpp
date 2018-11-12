

#pragma once

#include <string>
#include <string.h>

namespace common {

extern const char* c_newline;
extern const char* c_seperators;
extern const char* c_remark_string;

/**
 *@brief check if string is made of digit
 */
inline bool	isdigit(const std::string& str) {
	for(std::string::size_type i = 0; i < str.length(); i++)
		if(::isdigit(str[i]) == 0) return false;
	return true;
}

/**
 *@brief convert string to upper
 *@note return value is the input string
 */
std::string&	toupper(std::string& str);

/**
 *@brief convert string to lower
 */
std::string&	tolower(std::string& str);

/**
 *@brief convert string to upper
 */
inline std::string	toupper_cp(const std::string& str){
	std::string scp = str;
	return toupper(scp);
}

/**
 *@brief convert string to lower
 */
inline std::string	tolower_cp(const std::string& str){
	std::string scp = str;
	return tolower(scp);
}

/**
 *@brief compare two string careless
 *@param s1 left string
 *@param s2 right string
 *@param nonempty make two string equal even if they are
 *all empty
 */
bool		stnicmp(const std::string& s1, const std::string& s2, bool nonempty = false);

/**
 *@brief trim tick chars from string left
 */
std::string&	trim_left(std::string& str, const char* tick = c_seperators);

/**
 *@brief trim tick chars from string right
 */
std::string&	trim_right(std::string& str, const char* tick = c_seperators);

/**
 * @brief trim string with match
 * @param str source string
 * @param match match string
 * @param left trim left or right
 */
std::string&	trim_str(std::string& str, const std::string& match, bool left = true);

/**
 *@brief trim tick chars from string two ended
 */
inline std::string&	trim(std::string& str, const char* tick = c_seperators){
	trim_left(str);
	trim_right(str);
	return str;
}

/**
 *@brief parse next param seperated by sp chars
 *@param str src string
 *@param part [out]
 *@note trim left
 */
std::string&	parse(std::string& str, std::string& part, const char* sp = c_seperators);

/**
 *@brief parse next param seperated by sp chars
 *@note trim left
 */
inline std::string	parse(std::string& str, const char* sp = c_seperators){
	std::string part;
	return parse(str, part, sp);
}

/**
 *@brief get the specifi column
 *@param str origin std::string
 *@param num column number
 *@param sp chars of seperator
 */
std::string		column(const std::string& str, int num, const char* sp = c_seperators);

/**
 *@brief check if head match the given word
 *@param src source string
 *@param cp str to compare
 *@param trunc trunc match string from origin string
 *@param sp chars of seperator
 */
bool		check_head(std::string& src, const std::string& cp, bool trunc = false, const char* sp = c_seperators);

/**
 *@brief check if head remark match the given set
 *@param rmk chars of remark
 *@param sp chars of seperator
 */
bool		check_rmk(const std::string& src, const char* rmk = c_remark_string, const char* sp = c_seperators);

/**
 *@brief parse next param seperated by blank
 */
inline std::string string_next(std::string& str){
	std::string part;
	return parse(str, part, " ");
}

/**
 *@brief str to v1 and v2 by sp chars
 *@param str src string
 *@param v1 left value
 *@param v2 right value
 *@param reverse find direction
 */
bool		split(std::string str, std::string& v1, std::string& v2, const char* sp = "=", bool reverse = false);

/**
 *@brief str to v1 and v2 by sp string
 *@param str src string
 *@param v1 left value
 *@param v2 right value
 *@param reverse find direction
 */
bool		split_str(std::string str, std::string& v1, std::string& v2, const char* sp);

/**
 *@brief distill value from string between sp1 chars and sp2 chars
 *@param str src string
 *@param v inner value
 *@param sp1 left  sp
 *@param sp2 right sp
 *@param reverse find direction
 */
bool		distill(std::string str, std::string& v, const char* sp1 = "[", const char* sp2 = "]", bool reverse = false);

/**
 *@brief find if given string include sp and exclude nsp
 *@param str src string
 *@param sp  including string
 *@param nsp exluding string
 *@param bstr process sp and nsp as string or chars
 */
bool		find(const std::string& str, const char* sp = "[]=", const char* nsp = NULL, bool bstr = false);

/**
 *@brief find if given string total including just sub string and no more other alpha or num
 *@param str source string
 *@param sub sub string
 *@return true if str just include sub string
 *@note we just check the first appearance for sub string, if want check all sub string,
 *you should check by while {} yourself
 */
bool		find_exact(const std::string& str, const std::string& sub);

/**
 *@brief get string before sp in given string
 *@param str src string
 *@param sp seperator
 *@param bstr process sp as string or chars
 */
std::string		before(const std::string& str, const char* sp, bool bstr = false);

/**
 *@brief get string after sp in given string
 *@param str src string
 *@param sp seperator
 *@param bstr process sp as string or chars
 */
std::string		after(const std::string& str, const char* sp, bool bstr = false);

/**
 *@brief append string with chars
 */
void		append(std::string& str, size_t count, char c);

}
