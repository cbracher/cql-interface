#include "cql-interface/cql-interface.h"

#include <boost/program_options.hpp>
#include <boost/test/unit_test.hpp>
#include <string>
#include "log4cxx/logger.h"

using namespace log4cxx;
using namespace log4cxx::helpers;

using namespace std;
using namespace cb::cass_util;
using namespace cb;

extern unsigned nruns;
extern unsigned nsize;
extern unsigned npost;

namespace 
{
    log4cxx::LoggerPtr logger(Logger::getLogger("cb.cassandra_test"));

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
        bool ok = CassConn::truncate("other_test_data", consist);
        BOOST_REQUIRE_MESSAGE(ok, "cleared other_test_data");
        BOOST_REQUIRE_MESSAGE(ok, "adding some other_test_data");
        BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(1, 'test data1')"));
        BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(2, 'test data2')"));
        BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(3, 'test data3')"));
        BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(4, 'test data4')"));

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

    template <typename MyCon> void test_async_container()
    {
        bool ok = CassConn::truncate("other_test_data", consist);
        BOOST_REQUIRE_MESSAGE(ok, "cleared other_test_data");
        BOOST_REQUIRE_MESSAGE(ok, "adding some other_test_data");
        BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(1, 'test data1')"));
        BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(2, 'test data2')"));
        BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(3, 'test data3')"));
        BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(4, 'test data4')"));

        MyCon val1;
        CassFetcherHolderPtr holder1 = async_fetch<string,MyCon>("select value from other_test_data where docid in (1,2,10)", val1);
        MyCon val2;
        CassFetcherHolderPtr holder2 = async_fetch<string,MyCon>("select value from other_test_data where docid in (4,3)", val2);

        BOOST_REQUIRE(holder2 && holder2->get_fetcher());
        BOOST_REQUIRE(val2.size() == 2);
        BOOST_REQUIRE(val2.front() == "test data3"
                        || val2.front() == "test data4");
        BOOST_REQUIRE(val2.back() == "test data3"
                        || val2.back() == "test data4");
        BOOST_REQUIRE(val2.back() != val2.front());

        // additional checks not an issue
        BOOST_REQUIRE(holder2 && holder2->get_fetcher());
        BOOST_REQUIRE(holder2 && holder2->get_fetcher());

        BOOST_REQUIRE(holder1 && holder1->get_fetcher());
        BOOST_REQUIRE(val1.size() == 2);
        BOOST_REQUIRE(val1.front() == "test data1"
                        || val1.front() == "test data2");
        BOOST_REQUIRE(val1.back() == "test data1"
                        || val1.back() == "test data2");
        BOOST_REQUIRE(val1.back() != val1.front());

    }

    int int_key = 0;

    // checks that a prepared statement can store a value as well as update it. Also verifies
    // that null binding works
    template<typename T>
    void test_prep_store_call(const T& val_in, const string& field_name)
    {
        ostringstream prep_query;
        prep_query << "insert into prep_store (int_key, " << field_name << ") values(?, ?)";
        PreparedStorePtr prep_store = CassConn::prepare_store(prep_query.str(), 2);
        BOOST_REQUIRE(prep_store);

        NullBinder null_bind;

        for (unsigned i=0; i<3; ++i)
        {
            BOOST_REQUIRE(prep_store->store(++int_key,val_in));
            Fetcher<T> fetcher;
            T val;
            ostringstream query;
            query << "select " << field_name << " from prep_store where int_key=" << int_key;
            BOOST_REQUIRE(fetcher.do_fetch(query.str(), val));
            BOOST_REQUIRE_MESSAGE(val == val_in, 
                                  "val[" << val << "] == val_in[" << val_in << "] for field: "
                                    << field_name);

            if (i == 0)
            {
                ostringstream query2;
                query2 << "update prep_store set " << field_name << " = ? where int_key=?";
                PreparedStorePtr prep_store_2 = CassConn::prepare_store(query2.str(), 2);
                BOOST_REQUIRE(prep_store_2->store(null_bind, int_key));
                BOOST_REQUIRE(!fetcher.do_fetch(query.str(), val));     // since it is gone
            }
        }
    }


}
namespace std
{
    ostream& operator<<(ostream& os, const vector<int>& container)
    {
        for (auto it = container.begin(); it != container.end(); ++it)
        {
            if (it != container.begin()) os << ",";
            os << *it;
        }
        return os;
    }
    ostream& operator<<(ostream& os, const list<int>& container)
    {
        for (auto it = container.begin(); it != container.end(); ++it)
        {
            if (it != container.begin()) os << ",";
            os << *it;
        }
        return os;
    }
    ostream& operator<<(ostream& os, const set<int>& container)
    {
        for (auto it = container.begin(); it != container.end(); ++it)
        {
            if (it != container.begin()) os << ",";
            os << *it;
        }
        return os;
    }
    ostream& operator<<(ostream& os, const map<int,int>& container)
    {
        for (auto it = container.begin(); it != container.end(); ++it)
        {
            if (it != container.begin()) os << ",";
            os << it->first << "->" << it->second;
        }
        return os;
    }
}

BOOST_AUTO_TEST_SUITE( CassandralTests )

BOOST_AUTO_TEST_CASE(test_cassandra_store) 
{
    bool ok = CassConn::truncate("test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared test_data");
    RefId auto_refid;
    auto_refid.reset();
    ok = CassConn::store("insert into test_data (docid, value) values(AUTO_UUID, 'test data1')", 
                              UUID_ENUM,
                              auto_refid,
                              consist);
    BOOST_REQUIRE_MESSAGE(!ok, "unable to insert non timebased uuid");
    ok = CassConn::store("insert into test_data (docid, value) values(AUTO_UUID, 'test data1')", 
                              TIMEUUID_ENUM,
                              auto_refid,
                              consist);
    BOOST_REQUIRE_MESSAGE(ok && auto_refid, "found auto increment non-zero with text: " 
                                                << auto_refid.to_string());
    auto_refid.reset();
    ok = CassConn::store("insert into test_data (docid, value) values(AUTO_UUID, 'test data2')", 
                              TIMEUUID_ENUM,
                              auto_refid,
                              consist);
    BOOST_REQUIRE_MESSAGE(ok && auto_refid, "found auto increment non-zero with text: " 
                                                << auto_refid.to_string());


    TestFetcher fetcher;
    ostringstream cmd;
    cmd << "select * from test_data where docid = " << auto_refid;
    ok = CassConn::fetch(cmd.str(), fetcher, consist);
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
    ok = CassConn::fetch("select * from test_data", fetcher, consist);
    BOOST_REQUIRE_MESSAGE(ok, "did select");
    BOOST_REQUIRE_MESSAGE(fetcher.doc_pairs.size() == 2, 
                          "fetcher.doc_pairs.size() ("
                          <<  fetcher.doc_pairs.size()
                          << ") == 2");
}

BOOST_AUTO_TEST_CASE(test_change) 
{
    bool ok = CassConn::truncate("other_test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared other_test_data");
    BOOST_REQUIRE_MESSAGE(ok, "adding some other_test_data");
    BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(1, 'test data1')"));

    Fetcher<string> fetcher;
    string val;
    BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid=1", val));
    BOOST_REQUIRE(val=="test data1");

    BOOST_REQUIRE(CassConn::change("update other_test_data set value = 'changed' where docid=1"));
    BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid=1", val));
    BOOST_REQUIRE(val=="changed");

    BOOST_REQUIRE(!CassConn::change("update non_table set value = 'changed' where docid=1"));

}

BOOST_AUTO_TEST_CASE(test_escape) 
{
    bool ok = CassConn::truncate("other_test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared other_test_data");
    BOOST_REQUIRE_MESSAGE(ok, "adding some other_test_data");

    string value = "line1\nline2\n'intenal quote'";
    ostringstream cmd;
    cmd << "insert into other_test_data (docid, value) values(1, '";
    CassConn::escape(cmd, value);
    cmd << "')";
    BOOST_REQUIRE(CassConn::store(cmd.str()));

    Fetcher<string> fetcher;
    string val;
    BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid=1", val));
    BOOST_REQUIRE_MESSAGE(val==value, "val[" << val << "] ==value["
                                        << value << "]");
}

BOOST_AUTO_TEST_CASE(test_single_fetcher) 
{
    bool ok = CassConn::truncate("other_test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared other_test_data");
    BOOST_REQUIRE_MESSAGE(ok, "adding some other_test_data");
    BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(1, 'test data1')"));
    BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(2, 'test data2')"));
    BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(3, 'test data3')"));
    BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(4, 'test data4')"));

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

BOOST_AUTO_TEST_CASE(test_blob) 
{
    bool ok = CassConn::truncate("blob_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared coll_test_data");

    {
        BOOST_REQUIRE(CassConn::store("insert into blob_data (docid, value) values (1, textAsBlob('hello world'));"));
        Fetcher<CassBytesMgr> fetcher;
        CassBytesMgr val;
        BOOST_REQUIRE(fetcher.do_fetch("select value from blob_data where docid=1", val));
        BOOST_REQUIRE(val.size());
        string val_out;
        for (auto it = val.data().begin(); it != val.data().end(); ++it)
        {
            val_out += char(*it);
        }
        BOOST_REQUIRE_MESSAGE(val_out == "hello world",
                                "val_out[" << val_out << "] == 'hello world'"
                                << " where val.size() = " << val.size());
    }
    for (unsigned n=0; n<64; ++n)
    {
        ostringstream cmd;
        cmd << "insert into blob_data (docid, value) values (3, bigintAsBlob(" << n << "));";
        BOOST_REQUIRE_MESSAGE(CassConn::store(cmd.str()), cmd.str());
        Fetcher<int64_t> fetcher;
        int64_t val;
        BOOST_REQUIRE(fetcher.do_fetch("select blobAsBigint(value) from blob_data where docid=3", val));
        BOOST_REQUIRE_MESSAGE(val == n, "val[" << val << "] == n[" << n << "]");
    }
}

BOOST_AUTO_TEST_CASE(test_fetcher_coll) 
{
    bool ok = CassConn::truncate("coll_test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared coll_test_data");
    BOOST_REQUIRE_MESSAGE(ok, "adding some coll_test_data");
    BOOST_REQUIRE(CassConn::store("insert into coll_test_data (docid, value_list, value_set, value_map) values(1, [1,2,4,8], {1,2,4,8}, {1:2, 2:4, 4:8, 8:16})"));

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
    {
        Fetcher< vector<pair<int,int>> > fetcher;
        vector<pair<int,int>> val;
        BOOST_REQUIRE(fetcher.do_fetch("select value_map from coll_test_data where docid=1", val));
        BOOST_REQUIRE(val.size() == 4);
        vector<pair<int,int>> check_val({ {1,2}, {2,4}, {4,8}, {8,16} });
        BOOST_REQUIRE(val ==  check_val);

        // empty result
        BOOST_REQUIRE(!fetcher.do_fetch("select value_map from coll_test_data where docid=2", val));
    }

}

BOOST_AUTO_TEST_CASE(test_fetcher_multi_coll) 
{
    bool ok = CassConn::truncate("coll_test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared coll_test_data");
    BOOST_REQUIRE_MESSAGE(ok, "adding some coll_test_data");
    BOOST_REQUIRE(CassConn::store("insert into coll_test_data (docid, value_list) values(1, [1,2,4,8])"));
    BOOST_REQUIRE(CassConn::store("insert into coll_test_data (docid, value_list) values(2, [11,12,14,18])"));

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

BOOST_AUTO_TEST_CASE(test_async_fetcher) 
{
    bool ok = CassConn::truncate("other_test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared other_test_data");
    BOOST_REQUIRE_MESSAGE(ok, "adding some other_test_data");
    BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(1, 'test data1')"));
    BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(2, 'test data2')"));
    BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(3, 'test data3')"));
    BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(4, 'test data4')"));

    string val1;
    CassFetcherHolderPtr holder1 = async_fetch("select value from other_test_data where docid=1", val1);
    string val2;
    CassFetcherHolderPtr holder2 = async_fetch("select value from other_test_data where docid=2", val2);
    string val3;
    CassFetcherHolderPtr holder3 = async_fetch("select value from other_test_data where docid=3", val3);
    string val4;
    CassFetcherHolderPtr holder4 = async_fetch("select value from other_test_data where docid=4", val4);

    BOOST_REQUIRE(holder2 && holder2->was_set());
    BOOST_REQUIRE(val2=="test data2");

    // additional checks not an issue
    BOOST_REQUIRE(holder2 && holder2->was_set());
    BOOST_REQUIRE(holder2 && holder2->was_set());

    BOOST_REQUIRE(holder4 && holder4->was_set());
    BOOST_REQUIRE(val4=="test data4");

    BOOST_REQUIRE(holder3 && holder3->was_set());
    BOOST_REQUIRE(val3=="test data3");

    BOOST_REQUIRE(holder1 && holder1->was_set());
    BOOST_REQUIRE(val1=="test data1");
}

BOOST_AUTO_TEST_CASE(test_async_con_vec_fetcher) 
{
    test_async_container<vector<string>>();
}

BOOST_AUTO_TEST_CASE(test_async_con_list_fetcher) 
{
    test_async_container<list<string>>();
}

BOOST_AUTO_TEST_CASE(test_cassandra_store_if_exists) 
{
    bool ok = CassConn::truncate("test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared test_data");
    for (unsigned i=0; i<nruns; ++i)
    {
        RefId auto_refid;
        auto_refid.reset();
        ok = CassConn::store("insert into test_data (docid, value) values(AUTO_UUID, 'test data1')", 
                                TIMEUUID_ENUM,
                                auto_refid,
                                consist);
        BOOST_REQUIRE_MESSAGE(ok, "inserted into test data: " << auto_refid);

        ostringstream query;
        query << "insert into test_data (docid, value) values("
                                << auto_refid << ", 'test data update') if not exists";
        TestIfAppliedFetcher fetcher_if;
        ok = CassConn::fetch(query.str(), fetcher_if, consist);
        BOOST_REQUIRE_MESSAGE(!ok, "cannot insert over pre-existing: " << auto_refid);

        TestFetcher fetcher;
        ok = CassConn::fetch("select * from test_data", fetcher, consist);
        BOOST_REQUIRE_MESSAGE(ok, "did select");
        BOOST_REQUIRE_MESSAGE(fetcher.doc_pairs.size() == i+1, 
                            "fetcher.doc_pairs.size() ("
                            <<  fetcher.doc_pairs.size()
                            << ") == " << i+1);
    }
}

BOOST_AUTO_TEST_CASE(test_cassandra_store_if_exists_2) 
{
    bool ok = CassConn::truncate("test_data", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared test_data");
    for (unsigned i=0; i<nruns; ++i)
    {
        RefId refid;
        CassUuid time_uuid;
        CassConn::set_uuid_from_time(time_uuid);
        refid = time_uuid;
        ostringstream query;
        query << "insert into test_data (docid, value) values("
                                << refid << ", 'test data update')";
        ok = CassConn::store_if_not_exists(query.str()); 
        BOOST_REQUIRE_MESSAGE(ok, "inserted into test data: " << refid
                                    << " on first store_if_not_exists");

        ok = CassConn::store_if_not_exists(query.str()); 
        BOOST_REQUIRE_MESSAGE(!ok, "failed insert into test data: " << refid
                                    << " on second store_if_not_exists");

        TestFetcher fetcher;
        ok = CassConn::fetch("select * from test_data", fetcher, consist);
        BOOST_REQUIRE_MESSAGE(fetcher.doc_pairs.size() == i+1, 
                            "fetcher.doc_pairs.size() ("
                            <<  fetcher.doc_pairs.size()
                            << ") == " << i+1);
    }
}

BOOST_AUTO_TEST_CASE(test_stats_truncate) 
{
    CassConn::FullStats stats;
    CassConn::get_stats(stats);     // clear current stats

    // good case
    for (unsigned i=0; i<nruns; ++i)
    {
        bool ok = CassConn::truncate("test_data", consist);
        BOOST_REQUIRE_MESSAGE(ok, i << " cleared test_data");
    }
    CassConn::get_stats(stats);     // clear current stats
    BOOST_REQUIRE(stats.m_truncated.m_call == nruns);
    BOOST_REQUIRE(stats.m_truncated.m_timeout == 0);
    BOOST_REQUIRE(stats.m_truncated.m_bad == 0);

    // bad case
    for (unsigned i=0; i<3; ++i)
    {
        bool ok = CassConn::truncate("test_data_not_exists", consist);
        BOOST_REQUIRE_MESSAGE(!ok, i << " failed truncate test_data_not_exists");
    }
    CassConn::get_stats(stats);     // clear current stats
    BOOST_REQUIRE(stats.m_truncated.m_call == 0);
    BOOST_REQUIRE(stats.m_truncated.m_timeout == 0);
    BOOST_REQUIRE(stats.m_truncated.m_bad == 3);

    // timeout case
    for (unsigned i=0; i<3; ++i)
    {
        bool ok = CassConn::truncate("test_data", consist, 1);
        BOOST_MESSAGE(i << " truncate succeeded: " << ok);
    }
    CassConn::get_stats(stats);     // clear current stats
    BOOST_REQUIRE(stats.m_truncated.m_call >= 0);
    BOOST_REQUIRE(stats.m_truncated.m_timeout >= 0);
    BOOST_REQUIRE(stats.m_truncated.m_timeout + stats.m_truncated.m_call == 3);
    BOOST_REQUIRE(stats.m_truncated.m_bad == 0);
    BOOST_MESSAGE("did see " << stats.m_truncated.m_timeout << " timeouts");

}

BOOST_AUTO_TEST_CASE(test_stats_store) 
{
    CassConn::FullStats stats;

    BOOST_REQUIRE(CassConn::truncate("other_test_data", consist));

    CassConn::get_stats(stats);     // clear current stats

    // good case
    for (unsigned i=0; i<nruns; ++i)
    {
        // can just overwrite
        BOOST_REQUIRE(CassConn::store("insert into other_test_data (docid, value) values(1, 'test data1')"));
    }
    CassConn::get_stats(stats);     // clear current stats
    BOOST_REQUIRE_MESSAGE(stats.m_stored.m_call == nruns,
                          "stats.m_stored.m_call[" << stats.m_stored.m_call
                          << "] == nruns[" << nruns << "]");
    BOOST_REQUIRE(stats.m_stored.m_timeout == 0);
    BOOST_REQUIRE(stats.m_stored.m_bad == 0);

    // bad case
    for (unsigned i=0; i<3; ++i)
    {
        BOOST_REQUIRE(!CassConn::store("insert into non_table (docid, value) values(1, 'test data1')"));
    }
    CassConn::get_stats(stats);     // clear current stats
    BOOST_REQUIRE(stats.m_stored.m_call == 0);
    BOOST_REQUIRE(stats.m_stored.m_timeout == 0);
    BOOST_REQUIRE(stats.m_stored.m_bad == 3);

    // timeout case
    for (unsigned i=0; i<nruns; ++i)
    {
        bool ok = CassConn::store("insert into other_test_data (docid, value) values(1, 'test data1')",
                                  CASS_CONSISTENCY_LOCAL_QUORUM, 
                                  1);
        if (!ok)
        {
            BOOST_MESSAGE(i << " had a timeout");
        }
    }
    CassConn::get_stats(stats);     // clear current stats
    BOOST_REQUIRE(stats.m_stored.m_call == 0);
    BOOST_REQUIRE(stats.m_stored.m_timeout == nruns);
    BOOST_REQUIRE(stats.m_stored.m_bad == 0);
    BOOST_MESSAGE("did see " << stats.m_stored.m_timeout << " timeouts");
}

BOOST_AUTO_TEST_CASE(test_stats_fetched) 
{
    CassConn::FullStats stats;

    BOOST_REQUIRE(CassConn::truncate("other_test_data", consist));

    for (unsigned i=0; i<nruns; ++i)
    {
        ostringstream cmd;
        cmd << "insert into other_test_data (docid, value) values(" << i << ", 'test data" << i << "')";
        BOOST_REQUIRE(CassConn::store(cmd.str()));
    }
    CassConn::get_stats(stats);     // clear current stats

    // good case
    Fetcher<string> fetcher;
    string val;
    for (unsigned i=0; i<nruns; ++i)
    {
        BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid in (1)", val));
        BOOST_REQUIRE(val=="test data1");
    }
    CassConn::get_stats(stats);     // clear current stats
    BOOST_REQUIRE_MESSAGE(stats.m_fetched.m_call == nruns,
                          "stats.m_fetched.m_call[" << stats.m_fetched.m_call
                          << "] == nruns[" << nruns << "]");
    BOOST_REQUIRE(stats.m_fetched.m_timeout == 0);
    BOOST_REQUIRE(stats.m_fetched.m_bad == 0);

    // bad case
    for (unsigned i=0; i<3; ++i)
    {
        BOOST_REQUIRE(!fetcher.do_fetch("select value from non_table where docid in (1)", val));
    }
    CassConn::get_stats(stats);     // clear current stats
    BOOST_REQUIRE(stats.m_fetched.m_call == 0);
    BOOST_REQUIRE(stats.m_fetched.m_timeout == 0);
    BOOST_REQUIRE(stats.m_fetched.m_bad == 3);

    // timeout case
    for (unsigned i=0; i<nruns; ++i)
    {
        BOOST_REQUIRE(!fetcher.do_fetch("select value from other_test_data where docid in (1)", 
                                        val,
                                        CASS_CONSISTENCY_LOCAL_QUORUM, 
                                        1));  // 1 microsec
    }
    CassConn::get_stats(stats);     // clear current stats
    BOOST_REQUIRE(stats.m_fetched.m_call == 0);
    BOOST_REQUIRE(stats.m_fetched.m_timeout == nruns);
    BOOST_REQUIRE(stats.m_fetched.m_bad == 0);
    BOOST_MESSAGE("did see " << stats.m_fetched.m_timeout << " timeouts");
}

BOOST_AUTO_TEST_CASE(test_prep_store) 
{
    bool ok = CassConn::truncate("prep_store", consist);
    BOOST_REQUIRE_MESSAGE(ok, "cleared prep_store table");

    {
        string val = "hi there";
        test_prep_store_call(val, "ascii_value");
    }
    {
        string val = "hi there";
        test_prep_store_call(val, "text_value");
    }
    {
        cass_int64_t val = 1233424332344;
        test_prep_store_call(val, "bigint_value");
    }
    {
        CassBytesMgr val;
        for (uint8_t i=0; i<128; ++i)
        {
            val.push_back(i);
        }
        test_prep_store_call(val, "blob_value");
    }
    {
        RefId val;
        val.randomize();
        test_prep_store_call(val, "uuid_value");
    }
    {
        bool val = cass_false;
        test_prep_store_call(val, "boolean_value");
        val = cass_true;
        test_prep_store_call(val, "boolean_value");
    }
    {
        float val = 3.14159;
        test_prep_store_call(val, "float_value");
    }
    {
        double val = 10000003.14159;
        test_prep_store_call(val, "double_value");
    }
    {
        int val = 123456;
        test_prep_store_call(val, "int_value");
    }
    {
        cass_uint8_t address[CASS_INET_V4_LENGTH] = {127,0,0,1};
        CassInet val = cass_inet_init_v4(address);
        test_prep_store_call(val, "inet_value");
    }
    {
        cass_uint8_t address[CASS_INET_V6_LENGTH] = {127,0,0,1,6,7};
        CassInet val = cass_inet_init_v6(address);
        test_prep_store_call(val, "inet_value");
    }
    {
        vector<int> val = {1,2,3,4,11,9,8};
        test_prep_store_call(val, "list_value");
    }
    {
        list<int> val = {9,3,1,3,9};
        test_prep_store_call(val, "list_value");
    }
    {
        set<int> val = {1,2,3,5,7,9,11};
        test_prep_store_call(val, "set_value");
    }
    {
        map<int,int> val = { {1,2}, {2,4}, {4,8}};
        test_prep_store_call(val, "map_value");
    }
}

BOOST_AUTO_TEST_SUITE_END()


