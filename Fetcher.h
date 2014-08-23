#ifndef CB_FETCHER_H
#define CB_FETCHER_H

#include "FetchHelper.h"
#include "CassandraConn.h"

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
            return CassandraConn::fetch(query, *this) && m_was_set;
        }

        bool do_fetch(const std::string& query, const std::string& param, T& obj)
        {
            m_ptr = &obj;
            return CassandraConn::fetch(query + "'" + param + "'", *this) && m_was_set;
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

