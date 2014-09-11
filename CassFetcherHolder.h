#ifndef CB_CASS_FETCHER_HOLDER_H
#define CB_CASS_FETCHER_HOLDER_H

#include "cql-interface/CassFetcher.h"

namespace cb {

    // holds CassFetcher in a asyn fetch context.
    // Can be used to set aside a query and pick it up asyncronously.
    class CassFetcherHolder
    {
    public:

        CassFetcherHolder()
        {
            clear();
        }

        ~CassFetcherHolder() 
        {
        }

        // set this Holder with some contents
        bool assign(CassFuture* future, 
                    CassFetcherPtr fetcher,
                    const std::string& query,
                    cass_duration_t timeout_in_micro);

        // will wait for the future to complete (if necessary) and 
        // process the fetcher against it.
        CassFetcherPtr get_fetcher();

        // this will call get_fetch and check the was_set call.
        // Can do this with a FetcherAsync or ConFetcherAsync or any
        // fetcher where the fetcher does not hold values you need.
        bool was_set();

        // clears out current contents
        void clear();
    private:

        // copy semantics are almost ok, but would prefer to avoid 
        // 2 calls on CassConn::process_future
        CassFetcherHolder(const CassFetcherHolder&) = delete;
        CassFetcherHolder& operator=(const CassFetcherHolder&) = delete;

        CassFetcherPtr m_fetcher;
        CassFuture* m_future;
        std::string m_query;
        cass_duration_t m_timeout_in_micro;

        bool m_was_called;
    };
    typedef boost::shared_ptr<CassFetcherHolder> CassFetcherHolderPtr;
}

#endif 

