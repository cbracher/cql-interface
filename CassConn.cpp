#include <atomic>
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
#include "cql-interface/PreparedStore.h"

using namespace cb;
using namespace std;

namespace {
    log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("cb.cassandra"));

    // use this for timeouts
    cass_duration_t g_timeout_in_micro = 5000000;
    CassConsistency g_consist = CASS_CONSISTENCY_LOCAL_QUORUM;

    struct CallStats
    {
        CallStats()
        {
            m_call = 0;
            m_timeout = 0;
            m_bad = 0;
        }

        void set_value_of(CassConn::Stats& stats)
        {
            stats.m_call = m_call.exchange(0);
            stats.m_timeout = m_timeout.exchange(0);
            stats.m_bad = m_bad.exchange(0);
        }

        atomic<uint64_t> m_call;
        atomic<uint64_t> m_timeout;
        atomic<uint64_t> m_bad;
    };
    CallStats fetched;
    CallStats stored;
    CallStats truncated;

    atomic<bool> active_logger(true);
    void CassLogger(cass_uint64_t time,
                    CassLogLevel severity,
                    CassString message,
                    void* data)
    {
        if (!active_logger)
        {
            return;
        }
        switch (severity)
        {
            case CASS_LOG_INFO:
                LOG4CXX_INFO(logger, "raw_cassandra: " << string(message.data,message.length));
                break;
            case CASS_LOG_DEBUG:
                LOG4CXX_DEBUG(logger, "raw_cassandra: " << string(message.data,message.length));
                break;
            case CASS_LOG_CRITICAL:
            case CASS_LOG_ERROR:
                LOG4CXX_ERROR(logger, "raw_cassandra: " << string(message.data,message.length));
                break;
            case CASS_LOG_WARN:
                LOG4CXX_WARN(logger, "raw_cassandra: " << string(message.data,message.length));
                break;
            case CASS_LOG_LAST_ENTRY:
                break;
            case CASS_LOG_DISABLED:
                break;
        }
    }

    class CassBase
    {
        public:

            CassBase(const std::set<std::string>& ip_list, 
                     const std::string& keyspace, 
                     const std::string& login,
                     const std::string& passwd,
                     const std::string& local_dc,
                     unsigned num_threads_io,
                     unsigned max_connections_per_host,
                     unsigned queue_size_io,
                     CassLogLevel log_level)
            : m_cluster(0),
              m_session_future(0),
              m_session(0),
              m_local_dc(local_dc)
            {
                try
                {
                    if (!ip_list.size())
                    {
                        throw Exception("can't initialize cassandra cluster with empty ip_list",
                                         __FILE__, __LINE__);
                    }

                    m_cluster = cass_cluster_new();
                    if (!m_cluster)
                    {
                        throw Exception("failed creating cassandra cluster",
                                                __FILE__, __LINE__);
                    }

                    if (cass_cluster_set_log_callback(m_cluster, CassLogger, 0)
                            != CASS_OK)
                    {
                        throw Exception("failed attaching cass logger", __FILE__, __LINE__);
                    }

                    if (cass_cluster_set_log_level(m_cluster, log_level) != CASS_OK)
                    {
                        ostringstream err;
                        err << "failed cass_cluster_set_log_level: " 
                            << cass_log_level_string(log_level);
                        throw Exception(err.str(), __FILE__, __LINE__);
                    }

                    if (cass_cluster_set_num_threads_io(m_cluster, num_threads_io)
                            != CASS_OK)
                    {
                        ostringstream err;
                        err << "failed cass_cluster_set_num_threads_io: " 
                            << num_threads_io;
                        throw Exception(err.str(), __FILE__, __LINE__);
                    }

                    if (cass_cluster_set_core_connections_per_host(m_cluster, max_connections_per_host)
                            != CASS_OK)
                    {
                        ostringstream err;
                        err << "failed cass_cluster_set_core_connections_per_host: " 
                            << max_connections_per_host;
                        throw Exception(err.str(), __FILE__, __LINE__);
                    }

                    if (cass_cluster_set_max_connections_per_host(m_cluster, max_connections_per_host)
                            != CASS_OK)
                    {
                        ostringstream err;
                        err << "failed cass_cluster_set_max_connections_per_host: " 
                            << max_connections_per_host;
                        throw Exception(err.str(), __FILE__, __LINE__);
                    }

                    if (cass_cluster_set_max_pending_requests(m_cluster, queue_size_io)
                            != CASS_OK)
                    {
                        ostringstream err;
                        err << "failed cass_cluster_set_max_pending_requests: " 
                            << queue_size_io;
                        throw Exception(err.str(), __FILE__, __LINE__);
                    }

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
                    if (m_local_dc.size())
                    {
                        CassError ok = cass_cluster_set_load_balance_dc_aware(m_cluster, m_local_dc.c_str());
                        if (ok != CASS_OK)
                        {
                            throw Exception(string("failed setting dc aware policy for: \"") 
                                                    + m_local_dc + "\"",
                                                    __FILE__, __LINE__);
                        }
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
                active_logger = false;
                sleep(1);
                CassFuture* close_future = cass_session_close(m_session);
                cass_future_wait(close_future);
                cass_future_free(m_session_future);
                cass_cluster_free(m_cluster);
            }

        protected:
            CassCluster* m_cluster;
            CassFuture* m_session_future;
            CassSession* m_session;
            std::string m_local_dc;
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
                                CassConsistency consist,
                                const std::string& local_dc,
                                unsigned num_threads_io,
                                unsigned max_connections_per_host,
                                unsigned queue_size_io,
                                CassLogLevel log_level)
{
    LOG4CXX_INFO(logger, "connecting to Cassandra with hosts: " 
                            << cass_util::seq_to_string(ip_list)
                            << " and keyspace: \"" << keyspace << "\""
                            << " and login: \"" << login << "\""
                            << " and passwd: <not shown>"
                            << " and timeout_in_micro: " << use_timeout_in_micro
                            << " and consist : " << consist
                            << " and local_dc : \"" << local_dc << "\""
                            << " and num_threads_io : " << num_threads_io
                            << " and max_connections_per_host : " << max_connections_per_host
                            << " and queue_size_io : " << queue_size_io
                            << " and log_level : " << cass_log_level_string(log_level)
                            );
    g_timeout_in_micro = use_timeout_in_micro;
    g_consist = consist;
    cass_base.reset(new CassBase(ip_list, 
                                 keyspace, 
                                 login, 
                                 passwd, 
                                 local_dc,
                                 num_threads_io,
                                 max_connections_per_host,
                                 queue_size_io,
                                 log_level));
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

        if (!cass_future_wait_timed(future, timeout_in_micro))
        {
            stored.m_timeout.fetch_add(1);
            LOG4CXX_ERROR(logger, "calling store: \"" << query 
                                    << "\" had local timeout");
        } else
        {
            CassError rc = cass_future_error_code(future);
            if(rc == CASS_OK) 
            {
                retVal = true;
                stored.m_call.fetch_add(1);
                LOG4CXX_TRACE(logger, "executed: \"" << query << "\"");
            } else if(rc == CASS_ERROR_SERVER_WRITE_TIMEOUT) 
            {
                stored.m_timeout.fetch_add(1);
                LOG4CXX_ERROR(logger, "calling store: \"" << query 
                                        << "\" had server side timeout");
            } else
            {
                stored.m_bad.fetch_add(1);
                CassString message = cass_future_error_message(future);
                LOG4CXX_ERROR(logger, "calling store: \"" << query 
                                        << "\" has error: " << string(message.data, message.length));
            }
        }
        cass_future_free(future);
    } else
    {
        LOG4CXX_ERROR(logger, "calling store: \"" << query << "\" before cassandra is initialized");
    }
    return retVal;
}

PreparedStorePtr CassConn::prepare_store(const std::string& query, unsigned num_args)
{
    return prepare_store(query, num_args, g_consist, g_timeout_in_micro);
}

PreparedStorePtr CassConn::prepare_store(const std::string& query, 
                                         unsigned num_args, 
                                         CassConsistency consist, 
                                         cass_duration_t timeout_in_micro_in)
{
    PreparedStorePtr retVal;

    cass_duration_t timeout_in_micro = (timeout_in_micro_in 
                                         ? timeout_in_micro_in 
                                         : g_timeout_in_micro);

    CassSession* use_session = cass_base ? cass_base->session() : empty_session;

    if (use_session)
    {
        CassStatement* statement = cass_statement_new(cass_string_init(query.c_str()), num_args);

        if (statement)
        {
            cass_statement_set_consistency(statement, consist);
            // can't use make_shared since constructor is protected
            retVal.reset(new PreparedStore(query, statement, num_args, timeout_in_micro));
        }
    }
    return retVal;
}

bool CassConn::store(PreparedStore& prep_store)
{
    bool retVal = false;
    CassSession* use_session = cass_base ? cass_base->session() : empty_session;

    if (use_session)
    {
        CassFuture* future = cass_session_execute(use_session, prep_store.m_statement);

        if (!cass_future_wait_timed(future, prep_store.m_timeout_in_micro))
        {
            stored.m_timeout.fetch_add(1);
            LOG4CXX_ERROR(logger, "calling store: \"" << prep_store.m_query 
                                    << "\" had local timeout");
        } else
        {
            CassError rc = cass_future_error_code(future);
            if(rc == CASS_OK) 
            {
                retVal = true;
                stored.m_call.fetch_add(1);
                LOG4CXX_TRACE(logger, "executed: \"" << prep_store.m_query << "\"");
            } else if(rc == CASS_ERROR_SERVER_WRITE_TIMEOUT) 
            {
                stored.m_timeout.fetch_add(1);
                LOG4CXX_ERROR(logger, "calling store: \"" << prep_store.m_query 
                                        << "\" had server side timeout");
            } else
            {
                stored.m_bad.fetch_add(1);
                CassString message = cass_future_error_message(future);
                LOG4CXX_ERROR(logger, "calling store: \"" << prep_store.m_query 
                                        << "\" has error: " << string(message.data, message.length));
            }
        }
        cass_future_free(future);
    } else
    {
        LOG4CXX_ERROR(logger, "calling store: \"" << prep_store.m_query << "\" before cassandra is initialized");
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
    bool did_truncate = change(cmd, consist);
    if (!did_truncate)
    {
        unsigned num_retries = (timeout_in_sec ? timeout_in_sec : 5);
        for (unsigned i=0; i<num_retries; ++i)
        {
            sleep(1);
            int64_t count = 0;
            Fetcher<int64_t> fetcher;
            did_truncate = fetcher.do_fetch(string("select count(*) from ") + table_name, count)
                            && !count;
            if (did_truncate)
            {
                break;
            }
        }
        if (!did_truncate)
        {
            LOG4CXX_ERROR(logger, "failed truncate on table: " << table_name);
            truncated.m_bad.fetch_add(1);
        } else
        {
            truncated.m_call.fetch_add(1);
            LOG4CXX_DEBUG(logger, "did truncate on table: " << table_name
                                    << " despite apparent issue");
        }
    } else
    {
        truncated.m_call.fetch_add(1);
    }
    return did_truncate;
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
    if (!retVal && fetch_holder)
    {
        // make sure there is no residual fetch holder content
        fetch_holder->clear();
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
        LOG4CXX_DEBUG(logger, "calling fetch: \"" << query << "\" "
                                << (retVal ? "success" : "FAILED"));
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
    if (!cass_future_wait_timed(future, timeout_in_micro))
    {
        fetched.m_timeout.fetch_add(1);
        LOG4CXX_ERROR(logger, "calling fetch: \"" << query 
                                << "\" had local timeout");
    } else
    {
        CassError rc = cass_future_error_code(future);
        if(rc == CASS_OK) 
        {
            retVal = true;
            fetched.m_call.fetch_add(1);
            const CassResult* result = cass_future_get_result(future);
            if (result)
            {
                CassIterator* iterator = cass_iterator_from_result(result);
                if (iterator)
                {
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
                    LOG4CXX_TRACE(logger, "fetch: \"" << query 
                                            << "\" returned " << nrows 
                                            << " rows");
                    cass_iterator_free(iterator);
                } else
                {
                    LOG4CXX_ERROR(logger, "fetcher.fetch getting null iterator for query: " << query);
                }
                cass_result_free(result);
            } else
            {
                LOG4CXX_ERROR(logger, "fetcher.fetch getting null result for query: " << query);
            }
        } else if(rc == CASS_ERROR_SERVER_READ_TIMEOUT) 
        {
            fetched.m_timeout.fetch_add(1);
            LOG4CXX_ERROR(logger, "calling fetch: \"" << query 
                                    << "\" had server side timeout");
        } else
        {
            fetched.m_bad.fetch_add(1);
            CassString message = cass_future_error_message(future);
            LOG4CXX_ERROR(logger, "calling fetch: \"" << query 
                                    << "\" has error: " << string(message.data, message.length));
        }
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

void CassConn::get_stats(CassConn::FullStats& stats)
{
    fetched.set_value_of(stats.m_fetched);
    stored.set_value_of(stats.m_stored);
    truncated.set_value_of(stats.m_truncated);
}
