#ifndef CB_CASS_CONN_H
#define CB_CASS_CONN_H

#include <string>
#include <set>
#include <cassandra.h>
#include "cql-interface/CassFetcherHolder.h"

#define CASS_UUID_NUM_BYTES  16

namespace cb {

    class RefIdImp;

    enum UUID_TYPE_ENUM { TIMEUUID_ENUM, UUID_ENUM};
    class CassConn {
    public:

        // make process_future available to the CassFetcherHolder
        friend class CassFetcherHolder;

        static const unsigned def_timeout_in_micro_arg = 5000000; // 5 seconds
  
        // list of ips where we can find cassandra
        // the keyspace to use for queries
        // a default timeout, which can be overridden per query.
        // login + password as needed
        // a default consistency which can be overridden per query
        static void static_init(const std::set<std::string>& ip_list, 
                                const std::string& keyspace,
                                cass_duration_t timeout_in_micro = def_timeout_in_micro_arg,
                                const std::string& login = "",
                                const std::string& passwd = "",
                                CassConsistency consist = CASS_CONSISTENCY_LOCAL_QUORUM);

        // All calls go with the default timeout and consistency unless overridden
        // in the specific call. If you leave timeout_in_micro == 0, we use the
        // default timeout for the calls below

        static bool store(const std::string& query);
        static bool store(const std::string& query, 
                          CassConsistency consist,
                          cass_duration_t timeout_in_micro = 0);

        // note that " if not exists " is added by the call.
        static bool store_if_not_exists(const std::string& query);
        static bool store_if_not_exists(const std::string& query, 
                                        CassConsistency consist,
                                        cass_duration_t timeout_in_micro = 0);

        static bool truncate(const std::string& table_name, unsigned timeout_in_sec = 5);
        static bool truncate(const std::string& table_name, 
                             CassConsistency consist, 
                             unsigned timeout_in_sec = 5);

        // replaces first token "AUTO_UUID" with auto_increment_id
        // and makes the store. The values stored is captured in auto_increment_id.
        // Select the type of uuid with uuid_opt. 
        // Corresponds to cassandra field types: timeuuid, uuid
        static bool store(const std::string& query,    
                          UUID_TYPE_ENUM uuid_opt,
                          RefIdImp& auto_increment_id);
        static bool store(const std::string& query,    
                          UUID_TYPE_ENUM uuid_opt,
                          RefIdImp& auto_increment_id,
                          CassConsistency consist,
                          cass_duration_t timeout_in_micro = 0);

        static bool change(const std::string& query);
        static bool change(const std::string& query, 
                           CassConsistency consist,
                           cass_duration_t timeout_in_micro = 0);

        static bool fetch(const std::string& query, 
                          CassFetcher& fetcher);
        static bool fetch(const std::string& query, 
                          CassFetcher& fetcher,
                          CassConsistency consist,
                          cass_duration_t timeout_in_micro = 0);

        // the CassFetcherHolder holds the query processing asyncronously. 
        // will have to wait when the fetcher within is accessed.
        static bool async_fetch(const std::string& query, 
                                CassFetcherPtr fetcher,
                                CassFetcherHolderPtr fetch_holder);
        static bool async_fetch(const std::string& query, 
                                CassFetcherPtr fetcher,
                                CassFetcherHolderPtr fetch_holder,
                                CassConsistency consist,
                                cass_duration_t timeout_in_micro = 0);

        // uuid management
        static void set_uuid_rand(CassUuid uuid);
        static void set_uuid_from_time(CassUuid uuid);
        static void reset(CassUuid uuid);
        static std::string uuid_to_string(CassUuid uuid);
        static void uuid_to_string(CassUuid uuid, std::string& retVal);
        static void reset(CassDecimal& val);
        static void reset(CassInet& val);

        // escape management
        // text fields must escape a "'" with another "'" for "''"
        static void escape(std::ostream& os, const std::string& text);

    protected:

        // utility call used in CassConn::fetch as well as by CassFetcherHolder
        // for asyncronous processing
        static bool process_future(CassFuture* future, 
                                   CassFetcher& fetcher, 
                                   const std::string& query,
                                   cass_duration_t timeout_in_micro);

        // default consistency
        CassConsistency m_consist;

        CassConn() = delete;
        ~CassConn() = delete;
        CassConn& operator=(const CassConn&) = delete;
		CassConn(const CassConn&) = delete;
    };

    inline std::ostream& operator<<(std::ostream& os, const CassUuid& ref_id)
    {
        char* tmp = 0;
        cass_uuid_string(const_cast<CassUuid&>(ref_id), tmp);
        if (tmp)
        {
            os << tmp;
            delete tmp;
        }
        return os;
    }

}

#endif 

