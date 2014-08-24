#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/propertyconfigurator.h"
#include "log4cxx/helpers/exception.h"
#include "log4cxx/logmanager.h"

#include <iostream>

#include "cql-interface/LogBaseInfo.h"

using namespace cb;
using namespace std;
namespace po = boost::program_options;

using namespace log4cxx;
using namespace log4cxx::helpers;

LogBaseInfo::LogBaseInfo(po::options_description& desc) {
  po::options_description general_opts("general options");
  general_opts.add_options()
    ("help", "produce help message")
    ("my_help", "produce help message")
    ("logger_config", po::value<string>(&logger_config)->default_value("test_log.cfg"), 
      "config file for logging");
  desc.add(general_opts);
}

void LogBaseInfo::evaluate(boost::program_options::variables_map& vm,
                           const boost::program_options::options_description& desc) {
  if (logger_config.size()) {
    PropertyConfigurator::configure(logger_config);
  } else {
    BasicConfigurator::configure();
  }
  log4cxx::LoggerPtr logger(log4cxx::Logger::getRootLogger());
  LOG4CXX_INFO(logger, "logging initialized");
  
  if (vm.count("help") || vm.count("my_help")) {
    cout << desc << "\n";
    exit(0);
  }
}

void LogBaseInfo::clean_up()
{
    log4cxx::LogManager::shutdown();
}
