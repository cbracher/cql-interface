#ifndef CB_GEN_CASS_FETCHER_H
#define CB_GEN_CASS_FETCHER_H

#include "CassFetchHelper.h"
#include "CassandraConn.h"

namespace cb {

    template <typename T, typename Con>
    class GenCassFetcher : public CassFetcher
    {
    public:

        virtual ~GenCassFetcher() {}

        bool do_fetch(const std::string& query, Con& con)
        {
            m_conPtr = &con;
            return CassandraConn::fetch(query, *this);
        }

        bool do_fetch(const std::string& query, const std::string& param, Con& con)
        {
            m_conPtr = &con;
            return CassandraConn::fetch(query + "'" + param + "'", *this);
        }
    protected:

        virtual bool fetch(const CassRow& result)
        {
            if (CassFetchHelper::get_first(m_tmp, result))
            {
                m_conPtr->push_back(m_tmp);
                return true;
            }
            return false;
        }
        Con* m_conPtr;
        T m_tmp;
    };

    template <typename T>
    class SingleGenCassFetcher : public CassFetcher
    {
    public:

        virtual ~SingleGenCassFetcher() {}

        bool do_fetch(const std::string& query, T& obj)
        {
            m_ptr = &obj;
            return CassandraConn::fetch(query, *this);
        }

        bool do_fetch(const std::string& query, const std::string& param, T& obj)
        {
            m_ptr = &obj;
            return CassandraConn::fetch(query + "'" + param + "'", *this);
        }

    protected:

        virtual bool fetch(const CassRow& result)
        {
            return CassFetchHelper::get_first(*m_ptr, result);
        }
        T* m_ptr;
    };

}

#endif 

