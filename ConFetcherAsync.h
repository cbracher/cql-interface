#ifndef CB_CON_FETCHER_ASYNC_H
#define CB_CON_FETCHER_ASYNC_H

#include "FetchHelper.h"
#include "CassandraConn.h"

namespace cb {

    // fetch multiple value result, must be known to FetchHelper
    // note this container must work with push_back. If using a set, need to make a new template.
    template <typename T, typename Con>
    class ConFetcherAsync : public CassFetcher
    {
    public:

        ConFetcherAsync(const std::string& query, Con& con)
        {
            m_was_set = false;
            con.clear();
            m_conPtr = &con;
        }

        virtual ~ConFetcherAsync() {}

        virtual bool was_set() const
        {
            return m_was_set;
        }

    protected:

        virtual bool fetch(const CassRow& result)
        {
            if (FetchHelper::get_first(m_tmp, result))
            {
                m_was_set = true;
                m_conPtr->push_back(m_tmp);
                return true;
            }
            return false;
        }
        Con* m_conPtr;
        T m_tmp;
        bool m_was_set;
    };

    template <typename T, typename Con> 
    CassFetcherHolderPtr async_fetch(const std::string& query, 
                                    Con& obj,
                                    CassConsistency consist = CASS_CONSISTENCY_LOCAL_QUORUM)
    {
        CassFetcherPtr fetcher 
               = boost::make_shared<ConFetcherAsync<T,Con>>(query, obj);
        CassFetcherHolderPtr holder = boost::make_shared<CassFetcherHolder>();

        CassandraConn::async_fetch(query, fetcher, holder, consist);
        return holder;
    }
}

#endif 

