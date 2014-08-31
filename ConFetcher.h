#ifndef CB_CON_FETCHER_H
#define CB_CON_FETCHER_H

#include "cql-interface/FetchHelper.h"
#include "cql-interface/CassConn.h"

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
            return CassConn::fetch(query, *this);
        }

        // if timeout_in_micro == 0, will use global timeout default set in CassConn
        bool do_fetch(const std::string& query, 
                      Con& con, 
                      CassConsistency consist,
                      cass_duration_t timeout_in_micro = 0)
        {
            con.clear();
            m_conPtr = &con;
            return CassConn::fetch(query, *this, consist, timeout_in_micro);
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

