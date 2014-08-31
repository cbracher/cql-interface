#ifndef CB_FETCHER_H
#define CB_FETCHER_H

#include "cql-interface/FetchHelper.h"
#include "cql-interface/CassConn.h"

namespace cb {

    // fetch a single value result, must be known to FetchHelper
    // returns true if value obtained, otherwise false
    template <typename T>
    class Fetcher : public CassFetcher
    {
    public:

        virtual ~Fetcher() {}

        bool do_fetch(const std::string& query, T& obj)
        {
            m_was_set = false;
            m_ptr = &obj;
            return CassConn::fetch(query, *this) && m_was_set;
        }

        // if timeout_in_micro == 0, will use global timeout default set in CassConn
        bool do_fetch(const std::string& query, 
                      T& obj, 
                      CassConsistency consist,
                      cass_duration_t timeout_in_micro = 0)
        {
            m_was_set = false;
            m_ptr = &obj;
            return CassConn::fetch(query, *this, consist, timeout_in_micro) && m_was_set;
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
}

#endif 

