#ifndef CB_CASS_CONN_H
#define CB_CASS_CONN_H

#include <memory>
#include <string>
#include <set>
#include <cassandra.h>
#include "cql-interface/CassFetcherHolder.h"

#define CASS_UUID_NUM_BYTES  16

namespace cb {

    class RefIdImp;
    class PreparedStore;

    enum UUID_TYPE_ENUM { TIMEUUID_ENUM, UUID_ENUM};
    class CassConn {
    public:

        static const unsigned def_timeout_in_micro_arg = 5000000; // 5 seconds
  
        // list of ips where we can find cassandra
        // the keyspace to use for queries
        // a default timeout, which can be overridden per query.
        // login + password as needed
        // a default consistency which can be overridden per query
        // local_dc - sets the cluster to use for a local dc aware policy
        static void static_init(const std::set<std::string>& ip_list, 
                                const std::string& keyspace,
                                cass_duration_t timeout_in_micro = def_timeout_in_micro_arg,
                                const std::string& login = "",
                                const std::string& passwd = "",
                                CassConsistency consist = CASS_CONSISTENCY_LOCAL_QUORUM,
                                const std::string& local_dc = "");

        // All calls go with the default timeout and consistency unless overridden
        // in the specific call. If you leave timeout_in_micro == 0, we use the
        // default timeout for the calls below

        static bool store(const std::string& query);
        static bool store(const std::string& query, 
                          CassConsistency consist,
                          cass_duration_t timeout_in_micro = 0);

        // now same for prepared statements which will store later with bound values
        // returns null PreparedStorePtr in most failure cases.
        // could throw if there is an unexpected error.
        // Should return a PreparedStorePtr which you can use to make store commands with
        // bound variables.
        static std::shared_ptr<PreparedStore> prepare_store(const std::string& query, unsigned num_args);
        static std::shared_ptr<PreparedStore> prepare_store(const std::string& query, 
                                                            unsigned num_args, 
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

        // same as store, used to indicate purpose
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
        static void reset(CassInet& val);

        // escape management
        // text fields must escape a "'" with another "'" for "''"
        static void escape(std::ostream& os, const std::string& text);

        struct Stats
        {
            uint64_t m_call = 0;     // number of successful calls
            uint64_t m_timeout = 0;  // number of local or server side timeouts
            uint64_t m_bad = 0;      // number of bad calls
        };
        struct FullStats
        {
            Stats m_fetched;
            Stats m_stored;
            Stats m_truncated;
        };
        // call will clear the current stats
        static void get_stats(FullStats& stats);

    protected:

        // utility call used in CassConn::fetch as well as by CassFetcherHolder
        // for asyncronous processing
        friend class CassFetcherHolder;
        static bool process_future(CassFuture* future, 
                                   CassFetcher& fetcher, 
                                   const std::string& query,
                                   cass_duration_t timeout_in_micro);

        // used by PreparedStore for storing data
        friend class PreparedStore;
        static bool store(PreparedStore& prep_store);


        // default consistency
        CassConsistency m_consist;

        CassConn() = delete;
        ~CassConn() = delete;
        CassConn& operator=(const CassConn&) = delete;
		CassConn(const CassConn&) = delete;
    };

    // supplemental helper
    inline bool operator==(const CassInet& obj1, const CassInet& obj2)
    {
        bool retVal = obj1.address_length == obj2.address_length;
        for (uint32_t i = 0; i < obj1.address_length && retVal; ++i)
        {
            retVal = obj1.address[i] == obj2.address[i];
        }
        return retVal;
    }
}

// supplemental helpers for the base C* objects
namespace std
{
    inline ostream& operator<<(ostream& os, const CassUuid& ref_id)
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
    inline ostream& operator<<(ostream& os, const CassInet& inet)
    {
        for (unsigned i=0; i<inet.address_length; ++i)
        {
            if (i) os << ".";
            os << int16_t(inet.address[i]);
        }
        return os;
    }
}

#endif 

