#ifndef CB_CASS_UTIL_H
#define CB_CASS_UTIL_H

#include "log4cxx/logger.h"

#include <string>
#include <iostream>
#include <sstream>

namespace cb {

namespace cass_util {

  const std::string BaseTermNS = "/";

  template<typename T> void seq_print(std::ostream& os, const T& t) {
    for (typename T::const_iterator it = t.begin();
         it != t.end();
         ++it) {
      if (it != t.begin()) {
        os << ", ";
      }
      os << *it;
    }
  }

  template<typename T> void map_print(std::ostream& os, const T& t) {
    for (typename T::const_iterator it = t.begin();
         it != t.end();
         ++it) {
      if (it != t.begin()) {
        os << ", ";
      }
      os << "(" << it->first << ", " << it->second << ")";
    }
  }

  template<typename T> std::string seq_to_string(const T& t) {
    std::ostringstream os;
    cb::cass_util::seq_print(os, t);
    return os.str();
  }

  template<typename T> std::string to_string(const T& t) {
    std::ostringstream os;
    t.print(os);
    return os.str();
  }

  template <typename T>
          void reset(T& t)
          {
              t.clear();
              t.str("");
          }

  template <typename T>
  inline void reset_val(T& val)
  {
    static const T def_val;
    val = def_val;
  }

  enum EmptyOpt {NO_CAN_BE_EMPTY = 0, CAN_BE_EMPTY = 1, MUST_BE_FULL = 2};
  const std::string ALNUM = "[^a-zA-Z0-9]";
  const std::string ALPHA = "[^a-zA-Z]";
  const std::string DIGIT = "[^0-9]";
  std::string random_string(const std::string& regex_mask = ALNUM, 
                            unsigned max_len = 10,
                            EmptyOpt eopt = NO_CAN_BE_EMPTY);

  // random string of the form [0-9]{3}.[0-9]{3}.[0-9]{3}.[0-9]{3}
  std::string make_rand_ip();

  // random string of the form [a-zA-Z0-9]{n}@[a-zA-Z0-9]{m}.[a-zA-Z0-9]{3}
  std::string make_rand_email();

}
}

#endif

