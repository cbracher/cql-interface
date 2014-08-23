#include "cql-interface.h"

#include <boost/program_options.hpp>
#include <boost/test/unit_test.hpp>
#include <string>
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

    // use this consistency for the tests
    CassConsistency consist = CASS_CONSISTENCY_ONE;

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
            bool retVal = FetchHelper::get_nth(0, tmp.docid, row) 
                            && FetchHelper::get_nth(1, tmp.value, row);
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
            bool retVal = FetchHelper::get_nth(0, was_applied, row);
            return retVal && was_applied;
        }
    };

    template <typename T, typename Con> void test_container()
    {
        bool ok = CassandraConn::truncate("other_test_data", consist);
        BOOST_REQUIRE_MESSAGE(ok, "cleared other_test_data");
        BOOST_REQUIRE_MESSAGE(ok, "adding some other_test_data");
        BOOST_REQUIRE(CassandraConn::store("insert into other_test_data (docid, value) values(1, 'test data1')"));
        BOOST_REQUIRE(CassandraConn::store("insert into other_test_data (docid, value) values(2, 'test data2')"));
        BOOST_REQUIRE(CassandraConn::store("insert into other_test_data (docid, value) values(3, 'test data3')"));
        BOOST_REQUIRE(CassandraConn::store("insert into other_test_data (docid, value) values(4, 'test data4')"));

        ConFetcher<T, Con> fetcher;
        Con val;
        BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid in (1)", val));
        BOOST_REQUIRE(val.size()==1);
        BOOST_REQUIRE(val.front()=="test data1");

        // can repeat
        BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid in (1)", val));
        BOOST_REQUIRE(val.size()==1);
        BOOST_REQUIRE(val.front()=="test data1");

        // select a few more
        BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid in (2,3,5,6)", val));
        BOOST_REQUIRE(val.size()==2);
        BOOST_REQUIRE(val.front()=="test data2" || val.front()=="test data3");
        BOOST_REQUIRE(val.back()=="test data2" || val.back()=="test data3");
        BOOST_REQUIRE(val.back()!=val.front());

        // select none
        BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid in (5,6)", val));
        BOOST_REQUIRE(val.size()==0);

        // select bad
        BOOST_REQUIRE(!fetcher.do_fetch("select value from bad_table where docid in (5,6)", val));
        BOOST_REQUIRE(val.size()==0);
    }
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

BOOST_AUTO_TEST_CASE(test_single_fetcher) 
{
    bool ok = CassandraConn::truncate("other_test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared other_test_data");
    BOOST_REQUIRE_MESSAGE(ok, "adding some other_test_data");
    BOOST_REQUIRE(CassandraConn::store("insert into other_test_data (docid, value) values(1, 'test data1')"));
    BOOST_REQUIRE(CassandraConn::store("insert into other_test_data (docid, value) values(2, 'test data2')"));
    BOOST_REQUIRE(CassandraConn::store("insert into other_test_data (docid, value) values(3, 'test data3')"));
    BOOST_REQUIRE(CassandraConn::store("insert into other_test_data (docid, value) values(4, 'test data4')"));

    Fetcher<string> fetcher;
    string val;
    BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid=1", val));
    BOOST_REQUIRE(val=="test data1");

    BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid=2", val));
    BOOST_REQUIRE(val=="test data2");

    BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid=3", val));
    BOOST_REQUIRE(val=="test data3");

    BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid=4", val));
    BOOST_REQUIRE(val=="test data4");

    BOOST_REQUIRE(!fetcher.do_fetch("select value from other_test_data where docid=5", val));
    BOOST_REQUIRE(!fetcher.do_fetch("select value from no_table where docid=4", val));

}

BOOST_AUTO_TEST_CASE(test_container_vec_fetcher) 
{
    test_container<string, vector<string> >();
}

BOOST_AUTO_TEST_CASE(test_container_list_fetcher) 
{
    test_container<string, list<string> >();
}

BOOST_AUTO_TEST_CASE(test_fetcher_coll) 
{
    bool ok = CassandraConn::truncate("coll_test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared coll_test_data");
    BOOST_REQUIRE_MESSAGE(ok, "adding some coll_test_data");
    BOOST_REQUIRE(CassandraConn::store("insert into coll_test_data (docid, value_list, value_set, value_map) values(1, [1,2,4,8], {1,2,4,8}, {1:2, 2:4, 4:8, 8:16})"));

    {
        Fetcher<vector<int>> fetcher;
        vector<int> val;
        BOOST_REQUIRE(fetcher.do_fetch("select value_list from coll_test_data where docid=1", val));
        BOOST_REQUIRE(val.size() == 4);
        std::sort(val.begin(), val.end());
        BOOST_REQUIRE(val == vector<int>({1,2,4,8}) );

        // empty result
        BOOST_REQUIRE(!fetcher.do_fetch("select value_list from coll_test_data where docid=2", val));
    }
    {
        Fetcher<list<int>> fetcher;
        list<int> val;
        BOOST_REQUIRE(fetcher.do_fetch("select value_list from coll_test_data where docid=1", val));
        BOOST_REQUIRE(val.size() == 4);
        val.sort();
        BOOST_REQUIRE(val == list<int>({1,2,4,8}) );

        // repeat with no growth
        BOOST_REQUIRE(fetcher.do_fetch("select value_list from coll_test_data where docid=1", val));
        BOOST_REQUIRE(val.size() == 4);
        val.sort();
        BOOST_REQUIRE(val == list<int>({1,2,4,8}) );

        // empty result
        BOOST_REQUIRE(!fetcher.do_fetch("select value_list from coll_test_data where docid=2", val));
    }
    {
        Fetcher<set<int>> fetcher;
        set<int> val;
        BOOST_REQUIRE(fetcher.do_fetch("select value_set from coll_test_data where docid=1", val));
        BOOST_REQUIRE(val.size() == 4);
        BOOST_REQUIRE(val == set<int>({1,2,4,8}) );

        // empty result
        BOOST_REQUIRE(!fetcher.do_fetch("select value_set from coll_test_data where docid=2", val));
    }
    {
        Fetcher<map<int,int>> fetcher;
        map<int,int> val;
        BOOST_REQUIRE(fetcher.do_fetch("select value_map from coll_test_data where docid=1", val));
        BOOST_REQUIRE(val.size() == 4);
        map<int,int> check_val({ {1,2}, {2,4}, {4,8}, {8,16} });
        BOOST_REQUIRE(val ==  check_val);

        // empty result
        BOOST_REQUIRE(!fetcher.do_fetch("select value_map from coll_test_data where docid=2", val));
    }

}

BOOST_AUTO_TEST_CASE(test_fetcher_multi_coll) 
{
    bool ok = CassandraConn::truncate("coll_test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared coll_test_data");
    BOOST_REQUIRE_MESSAGE(ok, "adding some coll_test_data");
    BOOST_REQUIRE(CassandraConn::store("insert into coll_test_data (docid, value_list) values(1, [1,2,4,8])"));
    BOOST_REQUIRE(CassandraConn::store("insert into coll_test_data (docid, value_list) values(2, [11,12,14,18])"));

    {
        ConFetcher<vector<int>, vector<vector<int>>> fetcher;
        vector<vector<int>> val;
        BOOST_REQUIRE(fetcher.do_fetch("select value_list from coll_test_data", val));
        BOOST_REQUIRE(val.size() == 2);
        BOOST_REQUIRE(val[0] == vector<int>({1,2,4,8})  || val[0] == vector<int>({11,12,14,18}));
        BOOST_REQUIRE(val[1] == vector<int>({1,2,4,8})  || val[1] == vector<int>({11,12,14,18}));
        BOOST_REQUIRE(val[0] != val[1]);

        // empty result
        BOOST_REQUIRE(fetcher.do_fetch("select value_list from coll_test_data where docid=3", val));
        BOOST_REQUIRE(val.size() == 0);
    }
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


