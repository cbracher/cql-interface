cql-interface
=============

provides an interface on top of the latest cassandra c++ cpp-driver from datastax.

https://github.com/datastax/cpp-driver


Main utility is simplifying calls to store, update, truncate or select data. Also wraps the CassUuid in RefIdImp, which makes it easier to manage uuid values. 

Also add Fetcher and ConFetcher which work with FetchHelper to ease pulling data out of select statements.


examples from tests (in test/TestCassandra.cpp):

store some data:

    CassandraConn::store("insert into other_test_data (docid, value) values(1, 'test data1')");

change some data

    BOOST_REQUIRE(CassandraConn::store("insert into other_test_data (docid, value) values(1, 'test data1')"));


    Fetcher<string> fetcher;

    string val;

    BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid=1", val));

    BOOST_REQUIRE(val=="test data1");

    BOOST_REQUIRE(CassandraConn::change("update other_test_data set value = 'changed' where docid=1"));

    BOOST_REQUIRE(fetcher.do_fetch("select value from other_test_data where docid=1", val));

    BOOST_REQUIRE(val=="changed");


single fetch:

    CassandraConn::store("insert into other_test_data (docid, value) values(1, 'test data1')");

    Fetcher<string> fetcher;

    string val;

    fetcher.do_fetch("select value from other_test_data where docid=1", val);

    val=="test data1";


async fetch:

    CassandraConn::store("insert into other_test_data (docid, value) values(1, 'test data1')");

    string val1;

    CassFetcherHolderPtr holder1 = async_fetch("select value from other_test_data where docid=1", val1);

    BOOST_REQUIRE(holder1 && holder1->was_set());

    BOOST_REQUIRE(val1=="test data1");


pulling collections:

    CassandraConn::store("insert into coll_test_data (docid, value_list) values(1, [1,2,4,8])");

    Fetcher<vector<int>> fetcher;

    vector<int> val;

    fetcher.do_fetch("select value_list from coll_test_data where docid=1", val);

    val.size() == 4;

    std::sort(val.begin(), val.end());

    val == vector<int>({1,2,4,8});


Added calls to truncate tables since have seen some odd timeouts on truncation. This will truncate and then wait as long as it takes for the table to finish truncation.

        bool ok = CassandraConn::truncate("other_test_data", consist);

        BOOST_REQUIRE_MESSAGE(ok, "cleared other_test_data");


Added explicit calls to support paxos transaction if not exists calls

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


Added CassandraConn::escape call to use for escaping string data.

    string value = "line1\nline2\n'intenal quote'";

    ostringstream cmd;

    cmd << "insert into other_test_data (docid, value) values(1, '";

    CassandraConn::escape(cmd, value);

    cmd << "')";

    BOOST_REQUIRE(CassandraConn::store(cmd.str()));


