#include <boost/program_options.hpp>
#include <boost/test/unit_test.hpp>
#include "Util.h"
#include "CassFetchHelper.h"
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
    static log4cxx::LoggerPtr logger(Logger::getLogger("cb.cassandra_test"));

    struct TestDocPair
    {
        RefId docid;
        std::string value;
    };
    typedef vector<TestDocPair> TestDocPairs;

    class TestFetcher : public CassFetcher
    {
    public:

        virtual bool fetch(const CassRow& row)
        {
            TestDocPair tmp;
            bool retVal = CassFetchHelper::get_nth(0, tmp.docid, row) 
                            && CassFetchHelper::get_nth(1, tmp.value, row);
            if (retVal)
            {
                doc_pairs.push_back(tmp);
            }
            return retVal;
        }

        void reset()
        {
            doc_pairs.clear();
        }

        TestDocPairs doc_pairs;
    };

    class TestIfAppliedFetcher : public CassFetcher
    {
    public:

        virtual bool fetch(const CassRow& row)
        {
            bool was_applied;
            bool retVal = CassFetchHelper::get_nth(0, was_applied, row);
            return retVal && was_applied;
        }
    };

    CassConsistency consist = CASS_CONSISTENCY_ONE;
}


BOOST_AUTO_TEST_SUITE( CassandralTests )

BOOST_AUTO_TEST_CASE(test_cassandra_store) 
{
    bool ok = CassandraConn::truncate("test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared test_data");
    RefId auto_refid;
    auto_refid.reset();
    ok = CassandraConn::store("insert into test_data (docid, value) values(AUTO_UUID, 'test data1')", 
                              UUID_ENUM,
                              auto_refid,
                              consist);
    BOOST_REQUIRE_MESSAGE(!ok, "unable to insert non timebased uuid");
    ok = CassandraConn::store("insert into test_data (docid, value) values(AUTO_UUID, 'test data1')", 
                              TIMEUUID_ENUM,
                              auto_refid,
                              consist);
    BOOST_REQUIRE_MESSAGE(ok && auto_refid, "found auto increment non-zero with text: " 
                                                << auto_refid.to_string());
    auto_refid.reset();
    ok = CassandraConn::store("insert into test_data (docid, value) values(AUTO_UUID, 'test data2')", 
                              TIMEUUID_ENUM,
                              auto_refid,
                              consist);
    BOOST_REQUIRE_MESSAGE(ok && auto_refid, "found auto increment non-zero with text: " 
                                                << auto_refid.to_string());


    TestFetcher fetcher;
    ostringstream cmd;
    cmd << "select * from test_data where docid = " << auto_refid;
    ok = CassandraConn::fetch(cmd.str(), fetcher, consist);
    BOOST_REQUIRE_MESSAGE(ok, "did select");
    BOOST_REQUIRE_MESSAGE(fetcher.doc_pairs.size() == 1, 
                          "fetcher.doc_pairs.size() ("
                          <<  fetcher.doc_pairs.size()
                          << ") == 1");
    if (fetcher.doc_pairs.size())
    {
        BOOST_REQUIRE_MESSAGE(fetcher.doc_pairs.front().docid.to_string() == auto_refid.to_string(), 
                            "docid matched expected: "
                            << fetcher.doc_pairs.front().docid.to_string()
                            << " == "
                            << auto_refid.to_string());
        BOOST_REQUIRE_MESSAGE(fetcher.doc_pairs.front().value == "test data2", 
                            "value matched expected: "
                            + fetcher.doc_pairs.front().value
                            + " == test data2");
    }

    fetcher.reset();
    ok = CassandraConn::fetch("select * from test_data", fetcher, consist);
    BOOST_REQUIRE_MESSAGE(ok, "did select");
    BOOST_REQUIRE_MESSAGE(fetcher.doc_pairs.size() == 2, 
                          "fetcher.doc_pairs.size() ("
                          <<  fetcher.doc_pairs.size()
                          << ") == 2");
}

BOOST_AUTO_TEST_CASE(test_cassandra_store_if_exists) 
{
    bool ok = CassandraConn::truncate("test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared test_data");
    for (unsigned i=0; i<nruns; ++i)
    {
        RefId auto_refid;
        auto_refid.reset();
        ok = CassandraConn::store("insert into test_data (docid, value) values(AUTO_UUID, 'test data1')", 
                                TIMEUUID_ENUM,
                                auto_refid,
                                consist);
        BOOST_REQUIRE_MESSAGE(ok, "inserted into test data: " << auto_refid);

        ostringstream query;
        query << "insert into test_data (docid, value) values("
                                << auto_refid << ", 'test data update') if not exists";
        TestIfAppliedFetcher fetcher_if;
        ok = CassandraConn::fetch(query.str(), fetcher_if, consist);
        BOOST_REQUIRE_MESSAGE(!ok, "cannot insert over pre-existing: " << auto_refid);

        TestFetcher fetcher;
        ok = CassandraConn::fetch("select * from test_data", fetcher, consist);
        BOOST_REQUIRE_MESSAGE(ok, "did select");
        BOOST_REQUIRE_MESSAGE(fetcher.doc_pairs.size() == i+1, 
                            "fetcher.doc_pairs.size() ("
                            <<  fetcher.doc_pairs.size()
                            << ") == " << i+1);
    }
}

BOOST_AUTO_TEST_CASE(test_cassandra_store_if_exists_2) 
{
    bool ok = CassandraConn::truncate("test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared test_data");
    for (unsigned i=0; i<nruns; ++i)
    {
        RefId refid;
        CassUuid time_uuid;
        CassandraConn::set_uuid_from_time(time_uuid);
        refid = time_uuid;
        ostringstream query;
        query << "insert into test_data (docid, value) values("
                                << refid << ", 'test data update')";
        ok = CassandraConn::store_if_not_exists(query.str()); 
        BOOST_REQUIRE_MESSAGE(ok, "inserted into test data: " << refid
                                    << " on first store_if_not_exists");

        ok = CassandraConn::store_if_not_exists(query.str()); 
        BOOST_REQUIRE_MESSAGE(!ok, "failed insert into test data: " << refid
                                    << " on second store_if_not_exists");

        TestFetcher fetcher;
        ok = CassandraConn::fetch("select * from test_data", fetcher, consist);
        BOOST_REQUIRE_MESSAGE(fetcher.doc_pairs.size() == i+1, 
                            "fetcher.doc_pairs.size() ("
                            <<  fetcher.doc_pairs.size()
                            << ") == " << i+1);
    }
}

BOOST_AUTO_TEST_SUITE_END()


