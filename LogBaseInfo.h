#ifndef CB_LOG_BASE_INFO_H
#define CB_LOG_BASE_INFO_H

#include <boost/program_options.hpp>
#include <string>

namespace cb {
  class LogBaseInfo {
  public:
    LogBaseInfo(boost::program_options::options_description& desc);

    void evaluate(boost::program_options::variables_map& vm,
                  const boost::program_options::options_description& desc);

    const std::string& get_logger_config() const {return logger_config;}

    // clean up (say for valgrind) on program exit
    static void clean_up();
  private:

    std::string logger_config;
  };
}

#endif // CB_LOG_BASE_INFO_H

