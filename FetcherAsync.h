#ifndef CB_FETCHER_ASYNC_H
#define CB_FETCHER_ASYNC_H

#include "FetchHelper.h"
#include "CassandraConn.h"

namespace cb {

    // fetch a single value result, must be known to FetchHelper
    // returns true if value obtained, otherwise false
    template <typename T>
    class FetcherAsync : public CassFetcher
    {
    public:

        FetcherAsync(const std::string& query, T& obj) 
        {
            m_was_set = false;
            m_ptr = &obj;
        }
        virtual ~FetcherAsync() {}

        virtual bool was_set() const
        {
            return m_was_set;
        }

    protected:

        virtual bool fetch(const CassRow& result)
        {
            m_was_set = true;
            return FetchHelper::get_first(*m_ptr, result);
        }

        T* m_ptr;
        bool m_was_set;
    };

    template <typename T> 
    CassFetcherHolderPtr async_fetch(const std::string& query, 
                                    T& obj,
                                    CassConsistency consist = CASS_CONSISTENCY_LOCAL_QUORUM)
    {
        CassFetcherPtr fetcher = boost::make_shared<FetcherAsync<T>>(query, obj);
        CassFetcherHolderPtr holder = boost::make_shared<CassFetcherHolder>();

        CassandraConn::async_fetch(query, fetcher, holder, consist);
        return holder;
    }

}

#endif 

