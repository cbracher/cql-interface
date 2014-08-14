#include "log4cxx/logger.h"
#include "Exception.h"
#include "Util.h"

using namespace std;
using namespace cb;
using namespace log4cxx;

static LoggerPtr logger(Logger::getLogger("cb.except"));

Exception::Exception(const std::string& what_in,
                     const std::string& file_in,
                     unsigned num_in,
                     ExceptionLogLevel e_level) 
: what_str(what_in),
  file_str(file_in),
  num(num_in)
{
  switch (e_level) {
    case E_ERROR:
       LOG4CXX_ERROR(logger, cb::util::to_string(*this));
       break;
    case E_FATAL:
       LOG4CXX_FATAL(logger, cb::util::to_string(*this));
       break;
    case E_WARN:
       LOG4CXX_WARN(logger, cb::util::to_string(*this));
       break;
    case E_INFO:
       LOG4CXX_INFO(logger, cb::util::to_string(*this));
       break;
    case E_DEBUG:
       LOG4CXX_DEBUG(logger, cb::util::to_string(*this));
       break;
    case E_TRACE:
       LOG4CXX_ERROR(logger, cb::util::to_string(*this));
       break;
  }
}

void Exception::print(std::ostream& os) const {
  os << "File: " << file_str << " Line: " << num << " what: " << what_str;
}

std::ostream& operator<<(std::ostream& os, const cb::Exception& ex) {
  ex.print(os);
  return os;
}
