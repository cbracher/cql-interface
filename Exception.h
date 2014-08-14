#ifndef CB_EXCEPTION_H
#define CB_EXCEPTION_H

#include <iostream>
#include <string>
#include <exception>

namespace cb {
  enum ExceptionLogLevel{ E_TRACE, E_DEBUG, E_INFO, E_WARN, E_ERROR, E_FATAL};
  class Exception : public std::exception  {
  public:

    Exception(const std::string& what,
              const std::string& file,
              unsigned num,
              ExceptionLogLevel e_level = E_ERROR);

    virtual ~Exception() throw () {}

    virtual const char* what() const throw () {
      return what_str.c_str();
    }

    const char* file() const {
      return file_str.c_str();
    }

    unsigned file_num() const {
      return num;
    }

    void print(std::ostream& os) const;

  private:
    std::string what_str;
    std::string file_str;
    unsigned num;
  };
}

std::ostream& operator<<(std::ostream& os, const cb::Exception& ex);

#endif // CB_EXCEPTION_H

