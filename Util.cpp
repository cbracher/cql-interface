#include "log4cxx/logger.h"
#include <algorithm>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <boost/functional/hash.hpp>
#include <boost/algorithm/string/regex.hpp>
#include <boost/algorithm/string.hpp>
#include "Util.h"

using namespace std;
using namespace log4cxx;
using namespace boost::filesystem;

namespace cb {
namespace util {

  static log4cxx::LoggerPtr logger(Logger::getLogger("cb.util"));

  std::string random_string(const std::string& regex_mask, 
                            unsigned max_len,
                            EmptyOpt eopt) {
    string mask;
    string input;
    for (char t = -127; t < 127; ++t) {
      input.append(1, t);
    }
    boost::basic_regex<char> ex(regex_mask);
    boost::algorithm::erase_all_regex_copy(back_inserter(mask),
                                           input,
                                           ex);
    LOG4CXX_DEBUG(logger, "regex_mask: \"" << regex_mask 
                          << "\" mask: \"" << mask << "\"");
    string out;
    if (mask.length()) {
      unsigned len = 0;
      if (eopt == MUST_BE_FULL) {
        len = max_len;
      } else {
        len = random() % max_len + (eopt == NO_CAN_BE_EMPTY ? 1 : 0);
      }
      for (unsigned n = 0; n < len; ++n) {
        out.append( 1, mask.at(random() % mask.size()));
      }
    } else {
      LOG4CXX_ERROR(logger, "no mask for regex_mask: " << regex_mask);
    }
    return out;
  }

    string make_rand_ip()
    {
        ostringstream os;
        os << (rand() % 256)
            << "." << (rand() % 256)
            << "." << (rand() % 256)
            << "." << (rand() % 256);
        return os.str();
    }

}  // util
}  // cb
