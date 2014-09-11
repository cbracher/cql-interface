#include "cql-interface/CassFetcherHolder.h"
#include "cql-interface/CassConn.h"

using namespace cb;

void CassFetcherHolder::clear()
{
    m_was_called = true;    // no need to call again
    m_fetcher.reset();
    m_future = 0;
    m_query.clear();
    m_timeout_in_micro = 0;
}

bool CassFetcherHolder::assign(CassFuture* future, 
                               CassFetcherPtr fetcher,
                               const std::string& query,
                               cass_duration_t timeout_in_micro)
{
    if (fetcher && future)
    {
        m_was_called = false;
        m_fetcher = fetcher;
        m_future = future;
        m_query = query;
        m_timeout_in_micro = timeout_in_micro;
        return true;
    } else
    {
        clear();
    }
    return false;
}

CassFetcherPtr CassFetcherHolder::get_fetcher()
{
    if (m_future && m_fetcher && !m_was_called)
    {
        m_was_called = true;
        CassConn::process_future(m_future, *m_fetcher, m_query, m_timeout_in_micro);
    }
    return m_fetcher;
}

bool CassFetcherHolder::was_set()
{
    if (m_future && m_fetcher && !m_was_called)
    {
        m_was_called = true;
        CassConn::process_future(m_future, *m_fetcher, m_query, m_timeout_in_micro);
    }
    return (m_fetcher ? m_fetcher->was_set() : false);
}
