#include <boost/test/unit_test.hpp>
#include <boost/test/included/unit_test.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include "log4cxx/logger.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/propertyconfigurator.h"
#include "log4cxx/helpers/exception.h"

#include "cql-interface/cql-interface.h"

using namespace std;
using namespace cb;
namespace po = boost::program_options;

using namespace boost::unit_test;

using namespace log4cxx;
using namespace log4cxx::helpers;

unsigned nruns = 100;
unsigned nsize = 100;
unsigned npost = 8;
string src_dir;

namespace {

    class Cleaner
    {
        public:
            Cleaner() {}

            ~Cleaner()
            {
                sleep(1);
                LogBaseInfo::clean_up();
            }
    };
}


test_suite*
init_unit_test_suite( int argc, char* argv[] ) {

    framework::master_test_suite().p_name.value = "CQL Interface Tests";

    vector<string> cassandra_ips;
    cass_duration_t cassandra_timeout = 0;
    po::options_description desc("Allowed options");
    desc.add_options()
      ("nruns", po::value<unsigned>(&nruns)->default_value(10), "number of runs to make")
      ("nsize", po::value<unsigned>(&nsize)->default_value(100), "number of entries per posting list")
      ("src_dir", 
            po::value<string>(&src_dir)->default_value("."), 
            "source directory with supporting file for tests")
      ("cassandra_ip", 
            po::value<vector<string>>(&cassandra_ips), 
            "host ips for cassandra, defaults to localhost if not set")
      ("cassandra_timeout", 
            po::value<cass_duration_t>(&cassandra_timeout)->default_value(20000000), 
            "cassandra timeout in ms")
      ("new_help", "produce help message")
      ;
    LogBaseInfo log_info(desc);

    po::variables_map vm;
    // po::store(po::parse_command_line(argc, argv, desc), vm);
    po::store(po::command_line_parser(argc, argv).options(desc).allow_unregistered().run(), vm);
    po::notify(vm);    

    // use --my_help to see help message for this
    log_info.evaluate(vm, desc);

    if (!cassandra_ips.size())
    {
        cassandra_ips.push_back("127.0.0.1");
    }
    std::set<string> use_cass_ips(cassandra_ips.begin(), cassandra_ips.end());
    CassConn::static_init(use_cass_ips, "cql_interface_test", cassandra_timeout);

    BOOST_GLOBAL_FIXTURE( Cleaner );

    return 0;
}


