#include "cql-interface/CassFetcherHolder.h"
#include "cql-interface/CassConn.h"

using namespace cb;

bool CassFetcherHolder::assign(CassFuture* future, 
                               CassFetcherPtr fetcher,
                               const std::string& query)
{
    if (fetcher && future)
    {
        m_was_called = false;
        m_fetcher = fetcher;
        m_future = future;
        m_query = query;
        return true;
    } else
    {
        m_was_called = true;    // no need to call again
        m_fetcher.reset();
        m_future = 0;
        m_query.clear();
    }
    return false;
}

CassFetcherPtr CassFetcherHolder::get_fetcher()
{
    if (m_future && m_fetcher && !m_was_called)
    {
        m_was_called = true;
        CassConn::process_future(m_future, *m_fetcher, m_query);
    }
    return m_fetcher;
}

bool CassFetcherHolder::was_set()
{
    if (m_future && m_fetcher && !m_was_called)
    {
        m_was_called = true;
        CassConn::process_future(m_future, *m_fetcher, m_query);
    }
    return (m_fetcher ? m_fetcher->was_set() : false);
}
