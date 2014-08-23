#ifndef CB_CON_FETCHER_H
#define CB_CON_FETCHER_H

#include "FetchHelper.h"
#include "CassandraConn.h"

namespace cb {

    // fetch multiple value result, must be known to FetchHelper
    // note this container must work with push_back. If using a set, need to make a new template.
    // returns true if value obtained, otherwise false
    template <typename T, typename Con>
    class ConFetcher : public CassFetcher
    {
    public:

        virtual ~ConFetcher() {}

        bool do_fetch(const std::string& query, Con& con)
        {
            con.clear();
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
            if (FetchHelper::get_first(m_tmp, result))
            {
                m_conPtr->push_back(m_tmp);
                return true;
            }
            return false;
        }
        Con* m_conPtr;
        T m_tmp;
    };

}

#endif 

