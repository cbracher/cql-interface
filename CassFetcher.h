#ifndef CB_CASS_FETCHER_H
#define CB_CASS_FETCHER_H

#include <boost/make_shared.hpp>
#include <cassandra.h>

namespace cb {

    // abstract base class for fetchers.
    // used in CassandraConn::fetch and CassandraConn::async_fetch calls.
    // Encapsulate pulling the data out of select results.
    class CassFetcher
    {
    public:

        virtual ~CassFetcher() {}

        virtual bool fetch(const CassRow& result_row) = 0;

        // virtual call filled in by derived classes. Indicates that a value was set.
        // up to derived classes to implement.
        virtual bool was_set() const
        {
            return false;
        }
    };
    typedef boost::shared_ptr<CassFetcher> CassFetcherPtr;
}

#endif 

