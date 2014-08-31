#include <string.h>
#include <boost/make_shared.hpp>
#include <boost/algorithm/string.hpp>
#include "log4cxx/logger.h"

#include <atomic>

#include "cql-interface/CassUtil.h"
#include "cql-interface/RefId.h"
#include "cql-interface/Exception.h"
#include "cql-interface/CassConn.h"
#include "cql-interface/Fetcher.h"

using namespace cb;
using namespace std;

namespace {
    log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("cb.cassandra"));

    // use this for timeouts
    cass_duration_t g_timeout_in_micro = 5000000;
    CassConsistency g_consist = CASS_CONSISTENCY_LOCAL_QUORUM;

    class CassBase
    {
        public:

            CassBase(const std::set<std::string>& ip_list, 
                     const std::string& keyspace, 
                     const std::string& login,
                     const std::string& passwd)
            : m_cluster(0),
              m_session_future(0),
              m_session(0)
            {
                try
                {
                    if (!ip_list.size())
                    {
                        throw Exception("can't initialize cassandra cluster with empty ip_list",
                                         __FILE__, __LINE__);
                    }

                    m_cluster = cass_cluster_new();
                    if (login.size() && passwd.size())
                    {
                        CassError ok = cass_cluster_set_credentials(m_cluster, 
                                                                    login.c_str(),
                                                                    passwd.c_str());
                        if (ok != CASS_OK)
                        {
                            throw Exception(string("failed cassandra credentials for: ") 
                                                    + login + " not working!",
                                                    __FILE__, __LINE__);
                        }
                    } else if (login.size() || passwd.size())
                    {
                        throw Exception(string("must pass both login and passwd for cassandra credentials: "), 
                                                __FILE__, __LINE__);
                    }
                    for (auto it = ip_list.begin(); it != ip_list.end(); ++it)
                    {
                        cass_cluster_set_contact_points(m_cluster, it->c_str());
                    }
                    m_session_future = cass_cluster_connect_keyspace(m_cluster, 
                                                                     keyspace.c_str());
                    cass_future_wait_timed(m_session_future, g_timeout_in_micro);
                    CassError rc = cass_future_error_code(m_session_future);

                    if (rc == CASS_OK) 
                    {
                        m_session = cass_future_get_session(m_session_future);
                    } else {
                        CassString message = cass_future_error_message(m_session_future);
                        throw Exception(string("failed making cassandra session: ") 
                                                + string(message.data,message.length),
                                                __FILE__, __LINE__);
                    }


                    if (!m_session) {
                        throw Exception("failed making cassandra session", __FILE__, __LINE__);
                    }
                } catch(std::exception& e)
                {
                    LOG4CXX_FATAL(logger, "Exception: " << e.what());
                    exit(-1);
                }
            }

            // get the session so we can make queries
            CassSession* session()
            {
                return m_session;
            }

            ~CassBase()
            {
                CassFuture* close_future = cass_session_close(m_session);
                cass_future_wait(close_future);
                cass_future_free(m_session_future);
                cass_cluster_free(m_cluster);
            }

        protected:
            CassCluster* m_cluster;
            CassFuture* m_session_future;
            CassSession* m_session;
    };

    boost::shared_ptr<CassBase> cass_base;
    CassSession* empty_session = 0;

    class TestIfAppliedFetcher : public CassFetcher
    {
    public:

        // on an store if exists, noting that we see:
        // insert into test_data (docid, value) 
        //     values (c4cb3000-20d9-11e4-a064-a105ab7859ae, 'add one') if not exists;
        // [applied] | docid                                | value
        // -----------+--------------------------------------+------------
        //     False | c4cb3000-20d9-11e4-a064-a105ab7859ae | test data1
        // So we do this insert as a fetch and then look for '[applied]' == true
        // in the first column of the result.
        virtual bool fetch(const CassRow& row)
        {
            bool was_applied;
            bool retVal = FetchHelper::get_nth(0, was_applied, row);
            return retVal && was_applied;
        }
    };
}

void CassConn::static_init(const std::set<std::string>& ip_list, 
                                const std::string& keyspace,
                                cass_duration_t use_timeout_in_micro,
                                const std::string& login,
                                const std::string& passwd,
                                CassConsistency consist)
{
    LOG4CXX_INFO(logger, "connecting to Cassandra with hosts: " 
                            << cass_util::seq_to_string(ip_list)
                            << " and keyspace: \"" << keyspace << "\""
                            << " and login: \"" << login << "\""
                            << " and passwd: <not shown>"
                            << " and timeout_in_micro: " << use_timeout_in_micro
                            << " and consist : " << consist);
    g_timeout_in_micro = use_timeout_in_micro;
    g_consist = consist;
    cass_base.reset(new CassBase(ip_list, keyspace, login, passwd));
    if (keyspace.size())
    {
        if (change(string("Use " + keyspace)))
        {
            LOG4CXX_INFO(logger, "using keyspace: " << keyspace);
        } else
        {
            LOG4CXX_ERROR(logger, "FAILED using keyspace: " << keyspace);
        }
    }
}

bool CassConn::store(const std::string& query)
{
    return store(query, g_consist, g_timeout_in_micro);
}

bool CassConn::store(const std::string& query, 
                     CassConsistency consist, 
                     cass_duration_t timeout_in_micro_in)
{
    bool retVal = false;

    cass_duration_t timeout_in_micro = (timeout_in_micro_in 
                                         ? timeout_in_micro_in 
                                         : g_timeout_in_micro);

    CassSession* use_session = cass_base ? cass_base->session() : empty_session;

    if (use_session)
    {
        CassFuture* future = NULL;
        CassStatement* statement = cass_statement_new(cass_string_init(query.c_str()), 0);
        cass_statement_set_consistency(statement, consist);
        
        future = cass_session_execute(use_session, statement);
        cass_statement_free(statement);

        cass_future_wait_timed(future, timeout_in_micro);
        
        CassError rc = cass_future_error_code(future);
        if(rc == CASS_OK) 
        {
            retVal = true;
            LOG4CXX_TRACE(logger, "executed: \"" << query << "\"");
        } else
        {
            CassString message = cass_future_error_message(future);
            LOG4CXX_ERROR(logger, "calling store: \"" << query 
                                    << "\" has error: " << string(message.data, message.length));
        }
        
        cass_future_free(future);
    } else
    {
        LOG4CXX_ERROR(logger, "calling store: \"" << query << "\" before cassandra is initialized");
    }
    return retVal;
}

bool CassConn::store(const std::string& query_in, 
                          UUID_TYPE_ENUM uuid_opt,
                          RefIdImp& auto_increment_id)
{
    return store(query_in, uuid_opt, auto_increment_id, g_consist, g_timeout_in_micro);
}
bool CassConn::store(const std::string& query_in, 
                          UUID_TYPE_ENUM uuid_opt,
                          RefIdImp& auto_increment_id,
                          CassConsistency consist, 
                          cass_duration_t timeout_in_micro_in)
{
    static const string auto_token = "AUTO_UUID";

    string query = query_in;

    cass_duration_t timeout_in_micro = (timeout_in_micro_in 
                                         ? timeout_in_micro_in 
                                         : g_timeout_in_micro);

    if (query.find(auto_token) != string::npos)
    {
        if (uuid_opt == TIMEUUID_ENUM)
        {
            CassUuid uuid;
            set_uuid_from_time(uuid);
            auto_increment_id = uuid;
        } else
        {
            CassUuid uuid;
            set_uuid_rand(uuid);
            auto_increment_id = uuid;
        }
        boost::replace_first(query, auto_token, auto_increment_id.to_string());
    }
    return store(query, consist, timeout_in_micro);
}

bool CassConn::change(const std::string& query)
{
    return change(query, g_consist, g_timeout_in_micro);
}
bool CassConn::change(const std::string& query, 
                      CassConsistency consist, 
                      cass_duration_t timeout_in_micro)
{
    return store(query, consist, timeout_in_micro);
}

bool CassConn::truncate(const std::string& table_name, unsigned timeout_in_sec)
{
    return truncate(table_name, g_consist, timeout_in_sec);
}
bool CassConn::truncate(const std::string& table_name, 
                        CassConsistency consist, 
                        unsigned timeout_in_sec)
{
    string cmd = string("truncate ") + table_name;
    // note that this change might fail
    bool truncated = change(cmd, consist);
    if (!truncated)
    {
        unsigned num_retries = (timeout_in_sec ? timeout_in_sec : 5);
        for (unsigned i=0; i<num_retries; ++i)
        {
            sleep(1);
            int64_t count = 0;
            Fetcher<int64_t> fetcher;
            fetcher.do_fetch(string("select count(*) from ") + table_name, count);
            truncated = !count;
            if (truncated)
            {
                break;
            }
        }
        if (!truncated)
        {
            LOG4CXX_ERROR(logger, "failed truncate on table: " << table_name);
        } else
        {
            LOG4CXX_DEBUG(logger, "did truncate on table: " << table_name
                                    << " despite apparent issue");
        }
    }
    return truncated;
}

bool CassConn::store_if_not_exists(const std::string& query)
{
    return store_if_not_exists(query, g_consist, g_timeout_in_micro);
}
bool CassConn::store_if_not_exists(const std::string& query, 
                                   CassConsistency consist,
                                   cass_duration_t timeout_in_micro_in)
{
    string use_query = query + " if not exists";

    cass_duration_t timeout_in_micro = (timeout_in_micro_in 
                                         ? timeout_in_micro_in 
                                         : g_timeout_in_micro);

    TestIfAppliedFetcher fetcher_if;
    bool retVal = CassConn::fetch(use_query, fetcher_if, consist, timeout_in_micro);
    if (!retVal)
    {
        LOG4CXX_DEBUG(logger, "failed: \"" << use_query 
                              << "\" either due to query issue or due to record already being present");
    }
    return retVal;
}

bool CassConn::async_fetch(const std::string& query, 
                                CassFetcherPtr fetcher,
                                CassFetcherHolderPtr fetch_holder)
{
    return async_fetch(query, fetcher, fetch_holder, g_consist, g_timeout_in_micro);
}

bool CassConn::async_fetch(const std::string& query, 
                                CassFetcherPtr fetcher,
                                CassFetcherHolderPtr fetch_holder,
                                CassConsistency consist, 
                                cass_duration_t timeout_in_micro_in)
{
    bool retVal = false;
    CassSession* use_session = cass_base ? cass_base->session() : empty_session;

    cass_duration_t timeout_in_micro = (timeout_in_micro_in 
                                         ? timeout_in_micro_in 
                                         : g_timeout_in_micro);

    if (use_session && fetcher && fetch_holder)
    {
        CassStatement* statement = cass_statement_new(cass_string_init(query.c_str()), 0);
        cass_statement_set_consistency(statement, consist);
        
        CassFuture* future = cass_session_execute(use_session, statement);
        cass_statement_free(statement);

        retVal = fetch_holder->assign(future, fetcher, query, timeout_in_micro);
    } else if (!fetcher)
    {
        LOG4CXX_ERROR(logger, "calling fetch: \"" << query << "\" with null CassFetcherPtr");
    } else if (!fetch_holder)
    {
        LOG4CXX_ERROR(logger, "calling fetch: \"" << query << "\" with null CassFetcherHolderPtr");
    } else
    {
        LOG4CXX_ERROR(logger, "calling fetch: \"" << query << "\" before cassandra is initialized");
    }
    return retVal;
}


bool CassConn::fetch(const std::string& query, 
                     CassFetcher& fetcher)
{
    return fetch(query, fetcher, g_consist, g_timeout_in_micro);
}

bool CassConn::fetch(const std::string& query, 
                     CassFetcher& fetcher,
                     CassConsistency consist, 
                     cass_duration_t timeout_in_micro_in)
{
    bool retVal = false;
    CassSession* use_session = cass_base ? cass_base->session() : empty_session;

    cass_duration_t timeout_in_micro = (timeout_in_micro_in 
                                         ? timeout_in_micro_in 
                                         : g_timeout_in_micro);

    if (use_session)
    {
        CassStatement* statement = cass_statement_new(cass_string_init(query.c_str()), 0);
        cass_statement_set_consistency(statement, consist);
        
        CassFuture* future = cass_session_execute(use_session, statement);
        cass_statement_free(statement);

        retVal = process_future(future, fetcher, query, timeout_in_micro);
    } else
    {
        LOG4CXX_ERROR(logger, "calling fetch: \"" << query << "\" before cassandra is initialized");
    }
    return retVal;
}

bool CassConn::process_future(CassFuture* future, 
                              CassFetcher& fetcher, 
                              const std::string& query,
                              cass_duration_t timeout_in_micro)
{
    bool retVal = false;
    if (!future)
    {
        LOG4CXX_ERROR(logger, "getting null future in CassConn::process_future");
        return retVal;
    }
    cass_future_wait_timed(future, timeout_in_micro);
    
    CassError rc = cass_future_error_code(future);
    if(rc == CASS_OK) 
    {
        retVal = true;
        const CassResult* result = cass_future_get_result(future);
        CassIterator* iterator = cass_iterator_from_result(result);

        unsigned nrows = 0;
        while(retVal && cass_iterator_next(iterator)) {
            const CassRow* row = cass_iterator_get_row(iterator);
            if (row)
            {
                ++nrows;
                try
                {
                    retVal = fetcher.fetch(*row);
                } catch(std::exception& e)
                {
                    retVal = false;
                    LOG4CXX_ERROR(logger, "Exception in fetcher.fetch for query: " << query
                                            << " error: " << e.what());
                }
            } else
            {
                LOG4CXX_ERROR(logger, "fetch: \"" << query << "\" getting null row");
                retVal = false;
            }
        }
        LOG4CXX_TRACE(logger, "fetch: \"" << query << "\" returned " << nrows << " rows");
        cass_result_free(result);
        cass_iterator_free(iterator);
    } else
    {
        CassString message = cass_future_error_message(future);
        LOG4CXX_ERROR(logger, "calling fetch: \"" << query 
                                << "\" has error: " << string(message.data, message.length));
    }
    cass_future_free(future);
    return retVal;
}

void CassConn::set_uuid_rand(CassUuid uuid)
{
    cass_uuid_generate_random(uuid);
}

void CassConn::set_uuid_from_time(CassUuid uuid)
{
    cass_uuid_generate_time(uuid);
}

void CassConn::reset(CassUuid uuid)
{
    for (size_t i=0; i<CASS_UUID_NUM_BYTES; ++i)
    {
        uuid[i] = 0;
    }
}

std::string CassConn::uuid_to_string(CassUuid uuid)
{
    string retVal;
    uuid_to_string(uuid, retVal);
    return retVal;
}

void CassConn::uuid_to_string(CassUuid uuid, std::string& retVal)
{
    char tmp[CASS_UUID_STRING_LENGTH];
    cass_uuid_string(uuid, tmp);
    retVal = tmp;
}

void CassConn::reset(CassDecimal& val)
{
    // BUGBUG - not clear what to do here
    LOG4CXX_ERROR(logger, "CassConn::reset(CassDecimal& val) called but not valid");
    /*
    if (val.varint.data)
    {
        for (size_t i=0; i<val.varint.size; ++i)
        {
            val.varint.data[i] = 0;
        }
    }
    */
}

void CassConn::reset(CassInet& val)
{
    for (size_t i=0; i<CASS_INET_V6_LENGTH; ++i)
    {
        val.address[i] = 0;
    }
}

void CassConn::escape(std::ostream& os, const std::string& text)
{
    for (auto it = text.begin(); it != text.end(); ++it)
    {
        os << *it;
        if (*it == '\'')
        {
            os << *it;
        }
    }
}

