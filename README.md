cql-interface
=============

provides an interface on top of the latest cassandra c++ cpp-driver from datastax.
https://github.com/datastax/cpp-driver

Main utility is wrapping the CassUuid in RefIdImp, which makes it easier to manage uuid values. 

Also add Fetcher and ConFetcher which work with FetchHelper to ease pulling data out of select statements.


examples from tests (in test/TestCassandra.cpp):

store some data:
    CassandraConn::store("insert into other_test_data (docid, value) values(1, 'test data1')");

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

Added calls to truncate tables [CassandraConn::truncate], since have seen some odd timeouts on truncation. This will truncate and then wait as long as it takes for the table to finish truncation.

Added explicit calls to support storage if not exists, CassandraConn::store_if_not_exists.

Added CassandraConn::escape call to use for escaping string data.

