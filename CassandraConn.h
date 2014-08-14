#ifndef CB_CASS_CONN_H
#define CB_CASS_CONN_H

#include <string>
#include <set>
#include <cassandra.h>

#define CASS_UUID_NUM_BYTES  16

namespace cb {

    class RefIdImp;

    class CassFetcher
    {
    public:

        virtual ~CassFetcher() {}

        virtual bool fetch(const CassRow& result_row) = 0;
    };

    enum UUID_TYPE_ENUM { TIMEUUID_ENUM, UUID_ENUM};
    class CassandraConn {
    public:
  
        // list of ips where we can find cassandra
        // can select to use ssl. Note that this does not seem to work with the 
        // current Cassandra driver for C++
        static void static_init(const std::set<std::string>& ip_list, 
                                const std::string& keyspace,
                                cass_duration_t timeout_in_micro = 5000000,
                                bool use_ssl = false);

        static bool store(const std::string& query, 
                          CassConsistency_ = CASS_CONSISTENCY_LOCAL_QUORUM);

        // note that " if not exists " is added by the call.
        static bool store_if_not_exists(const std::string& query, 
                          CassConsistency_ = CASS_CONSISTENCY_LOCAL_QUORUM);

        static bool truncate(const std::string& table_name, 
                             CassConsistency_ = CASS_CONSISTENCY_LOCAL_QUORUM);

        // replaces first token "AUTO_UUID" with auto_increment_id
        // and makes the store. The values stored is captured in auto_increment_id.
        // Select the type of uuid with uuid_opt. 
        // Corresponds to cassandra field types: timeuuid, uuid
        static bool store(const std::string& query,    
                          UUID_TYPE_ENUM uuid_opt,
                          RefIdImp& auto_increment_id,
                          CassConsistency_ = CASS_CONSISTENCY_LOCAL_QUORUM);
        static bool change(const std::string& query, 
                           CassConsistency_ = CASS_CONSISTENCY_LOCAL_QUORUM);

        static bool fetch(const std::string& query, 
                          CassFetcher& fetcher,
                          CassConsistency_ = CASS_CONSISTENCY_LOCAL_QUORUM);

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

        CassandraConn() = delete;
        ~CassandraConn() = delete;
        CassandraConn& operator=(const CassandraConn&) = delete;
		CassandraConn(const CassandraConn&) = delete;
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

