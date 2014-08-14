#include <boost/program_options.hpp>
#include <boost/test/unit_test.hpp>
#include "Util.h"
#include "RefId.h"

#include "log4cxx/logger.h"

using namespace log4cxx;
using namespace log4cxx::helpers;

using namespace std;
using namespace cb::util;
using namespace cb;

extern unsigned nruns;
extern unsigned nsize;
extern unsigned npost;

namespace 
{
    static log4cxx::LoggerPtr logger(Logger::getLogger("cb.refid_test"));
}


BOOST_AUTO_TEST_SUITE( RefIdTests )

BOOST_AUTO_TEST_CASE(test_refid) 
{
    RefId refid; 
    BOOST_REQUIRE_MESSAGE(refid.empty(), "default constructed refid is empty");

    string refid_str = refid.to_string();
    BOOST_REQUIRE_MESSAGE(refid.to_string() == "00000000-0000-0000-0000-000000000000", 
                            "refid.to_string()[" << refid.to_string() 
                            << "] == \"00000000-0000-0000-0000-000000000000\"");

    stringstream os;
    os << refid;
    BOOST_REQUIRE_MESSAGE(os.str() == "00000000-0000-0000-0000-000000000000", 
                            "os.str()[" << os.str() 
                            << "] == \"00000000-0000-0000-0000-000000000000\"");

    RefIdImp some_time(time(0));
    BOOST_REQUIRE_MESSAGE(!some_time.empty(), "some_time not empty: " << some_time.to_string());
    BOOST_REQUIRE_MESSAGE(some_time, "some_time not false: " << some_time.to_string());

    some_time.reset();
    BOOST_REQUIRE_MESSAGE(some_time.empty(), "some_time now empty after reset: " << some_time.to_string());
    BOOST_REQUIRE_MESSAGE(!some_time, "some_time now false after reset: " << some_time.to_string());
}

BOOST_AUTO_TEST_CASE(test_refid_assign) 
{
    for (unsigned i=0; i<nruns; ++i)
    {
        RefId refid1; 
        refid1.randomize();
        BOOST_REQUIRE_MESSAGE(!refid1.empty(), "refid1 not empty: " << refid1.to_string());

        RefId refid2;
        refid2 = refid1; 
        BOOST_REQUIRE_MESSAGE(!refid2.empty(), "refid2 not empty: " << refid2.to_string());
        BOOST_REQUIRE_MESSAGE(refid2.to_string() == refid1.to_string(), 
                                "refid2.to_string()[" << refid2.to_string() 
                                << "] == refid1.to_string()[" << refid1.to_string() << "]");
        BOOST_REQUIRE_MESSAGE(refid2 == refid1, "refid2 == refid1"); 
    }
}

BOOST_AUTO_TEST_CASE(test_refid_constructor) 
{
    for (unsigned i=0; i<nruns; ++i)
    {
        RefId refid1; 
        refid1.randomize();
        BOOST_REQUIRE_MESSAGE(!refid1.empty(), "refid1 not empty: " << refid1.to_string());

        RefId refid2 = refid1; 
        BOOST_REQUIRE_MESSAGE(!refid2.empty(), "refid2 not empty: " << refid2.to_string());
        BOOST_REQUIRE_MESSAGE(refid2.to_string() == refid1.to_string(), 
                                "refid2.to_string()[" << refid2.to_string() 
                                << "] == refid1.to_string()[" << refid1.to_string() << "]");
        BOOST_REQUIRE_MESSAGE(refid2 == refid1, "refid2 == refid1"); 
    }
}

BOOST_AUTO_TEST_CASE(test_refid_assign_is) 
{
    for (unsigned i=0; i<nruns; ++i)
    {
        RefId refid1; 
        refid1.randomize();
        BOOST_REQUIRE_MESSAGE(!refid1.empty(), "refid1 not empty: " << refid1.to_string());

        stringstream ss;
        ss << refid1;

        RefId refid2;
        refid2.assign(ss);
        BOOST_REQUIRE_MESSAGE(!refid2.empty(), "refid2 not empty: " << refid2.to_string());
        BOOST_REQUIRE_MESSAGE(refid2.to_string() == refid1.to_string(), 
                                "refid2.to_string()[" << refid2.to_string() 
                                << "] == refid1.to_string()[" << refid1.to_string() << "]");
        BOOST_REQUIRE_MESSAGE(refid2 == refid1, "refid2 == refid1"); 
    }
}

BOOST_AUTO_TEST_CASE(test_refid_cass_constructor) 
{
    for (unsigned i=0; i<nruns; ++i)
    {
        CassUuid uuid; 
        cass_uuid_generate_random(uuid);

        RefId refid2 = uuid; 
        BOOST_REQUIRE_MESSAGE(!refid2.empty(), "refid2 not empty: " << refid2.to_string());
        BOOST_REQUIRE_MESSAGE(refid2.to_string() == CassandraConn::uuid_to_string(uuid),
                                "refid2.to_string()[" << refid2.to_string() 
                                << "] == CassandraConn::uuid_to_string(uuid)[" 
                                << CassandraConn::uuid_to_string(uuid) << "]");
    }
}

BOOST_AUTO_TEST_CASE(test_refid_cass_assign) 
{
    for (unsigned i=0; i<nruns; ++i)
    {
        CassUuid uuid; 
        cass_uuid_generate_random(uuid);

        RefId refid2;
        refid2 = uuid; 
        BOOST_REQUIRE_MESSAGE(!refid2.empty(), "refid2 not empty: " << refid2.to_string());
        BOOST_REQUIRE_MESSAGE(refid2.to_string() == CassandraConn::uuid_to_string(uuid),
                                "refid2.to_string()[" << refid2.to_string() 
                                << "] == CassandraConn::uuid_to_string(uuid)[" 
                                << CassandraConn::uuid_to_string(uuid) << "]");
    }
}



BOOST_AUTO_TEST_SUITE_END()


