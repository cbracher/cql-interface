#ifndef CB_PREPARED_STORE_H
#define CB_PREPARED_STORE_H

#include <memory>
#include <mutex>
#include <vector>
#include <sstream>
#include "cql-interface/CassConn.h"
#include "cql-interface/RefId.h"
#include "cql-interface/Exception.h"

namespace cb {

    // use this for binding null values
    struct NullBinder
    {
    };

    // use this to execute a prepared store/change statement.
    class PreparedStore 
    {
    public:

        ~PreparedStore()
        {
            if (m_statement)
            {
                cass_statement_free(m_statement);
            }
        }

        // see http://en.cppreference.com/w/cpp/language/parameter_pack
        // will throw if there is an issue binding values.
        template<typename... Targs>
        bool store(Targs... Fargs) 
        {
            if (m_num_args != sizeof...(Fargs))
            {
                std::ostringstream err;
                err << "must call store with " << m_num_args << " for PreparedStore: " 
                    << m_query.c_str();
                throw Exception(err.str(), __FILE__, __LINE__);
            }

            // will be changing the bound values of this statement, must lock
            std::lock_guard<std::mutex> guard(m_mutex);

            // will throw if bind fails, since this is not expected. Only have to bind if we 
            // have m_num_args > 0
            if (m_num_args)
            {
                bind(0, Fargs...);
            }
            return CassConn::store(*this);
        }

    protected:

        bool do_bind(unsigned index, const cass_int32_t& val)
        {
            return cass_statement_bind_int32(m_statement, index, val) == CASS_OK;
        }

        bool do_bind(unsigned index, const cass_int64_t& val)
        {
            return cass_statement_bind_int64(m_statement, index, val) == CASS_OK;
        }

        bool do_bind(unsigned index, const cass_float_t& val)
        {
            return cass_statement_bind_float(m_statement, index, val) == CASS_OK;
        }

        bool do_bind(unsigned index, const cass_double_t& val)
        {
            return cass_statement_bind_double(m_statement, index, val) == CASS_OK;
        }

        bool do_bind(unsigned index, const bool& val_in)
        {
            cass_bool_t val = val_in ? cass_true : cass_false;
            return cass_statement_bind_bool(m_statement, index, val) == CASS_OK;
        }

        bool do_bind(unsigned index, const CassString& val)
        {
            return cass_statement_bind_string(m_statement, index, val) == CASS_OK;
        }

        bool do_bind(unsigned index, const std::string& val_in)
        {
            CassString val;
            val.length = val_in.size();
            if (val.length)
            {
                val.data = val_in.c_str();
            } else
            {
                val.data = 0;
            }
            return cass_statement_bind_string(m_statement, index, val) == CASS_OK;
        }

        bool do_bind(unsigned index, const CassBytes& val)
        {
            return cass_statement_bind_bytes(m_statement, index, val) == CASS_OK;
        }

        bool do_bind(unsigned index, const std::vector<cass_byte_t>& val_in)
        {
            CassBytes val;
            val.size = val_in.size();
            if (val.size)
            {
                val.data = &val_in.front();
            } else
            {
                val.data = 0;
            }
            return cass_statement_bind_bytes(m_statement, index, val) == CASS_OK;
        }

        bool do_bind(unsigned index, const CassBytesMgr& val_in)
        {
            return do_bind(index, val_in.data());
        }


        bool do_bind(unsigned index, const CassUuid& val)
        {
            return cass_statement_bind_uuid(m_statement, index, val) == CASS_OK;
        }

        bool do_bind(unsigned index, const cb::RefId& val)
        {
            return cass_statement_bind_uuid(m_statement, index, val.get_uuid()) == CASS_OK;
        }

        bool do_bind(unsigned index, const CassInet& val)
        {
            return cass_statement_bind_inet(m_statement, index, val) == CASS_OK;
        }

        bool do_bind(unsigned index, const CassDecimal& val)
        {
            return cass_statement_bind_decimal(m_statement, index, val) == CASS_OK;
        }

        bool do_bind(unsigned index, const CassCollection*& val)
        {
            return cass_statement_bind_collection(m_statement, index, val) == CASS_OK;
        }

        // all these do_append there to support standard std containers
        bool do_append(CassCollection* coll_ptr, const cass_int32_t& val)
        {
            return cass_collection_append_int32(coll_ptr, val);
        }
        bool do_append(CassCollection* coll_ptr, const cass_int64_t& val)
        {
            return cass_collection_append_int64(coll_ptr, val);
        }
        bool do_append(CassCollection* coll_ptr, const cass_float_t& val)
        {
            return cass_collection_append_float(coll_ptr, val);
        }
        bool do_append(CassCollection* coll_ptr, const cass_double_t& val)
        {
            return cass_collection_append_double(coll_ptr, val);
        }
        bool do_append(CassCollection* coll_ptr, const cass_bool_t& val)
        {
            return cass_collection_append_bool(coll_ptr, val);
        }
        bool do_append(CassCollection* coll_ptr, const bool& val_in)
        {
            cass_bool_t val = val_in ? cass_true : cass_false;
            return cass_collection_append_bool(coll_ptr, val);
        }
        bool do_append(CassCollection* coll_ptr, const CassString& val)
        {
            return cass_collection_append_string(coll_ptr, val);
        }
        bool do_append(CassCollection* coll_ptr, const std::string& val_in)
        {
            CassString val;
            val.length = val_in.size();
            if (val.length)
            {
                val.data = val_in.c_str();
            } else
            {
                val.data = 0;
            }
            return cass_collection_append_string(coll_ptr, val);
        }
        bool do_append(CassCollection* coll_ptr, const CassBytes& val)
        {
            return cass_collection_append_bytes(coll_ptr, val);
        }
        bool do_append(CassCollection* coll_ptr, const std::vector<cass_byte_t>& val_in)
        {
            CassBytes val;
            val.size = val_in.size();
            if (val.size)
            {
                val.data = &val_in.front();
            } else
            {
                val.data = 0;
            }
            return cass_collection_append_bytes(coll_ptr, val);
        }
        bool do_append(CassCollection* coll_ptr, const CassBytesMgr& val)
        {
            return do_append(coll_ptr, val.data());
        }
        bool do_append(CassCollection* coll_ptr, const CassUuid& val)
        {
            // could be a dangerous const cast
            return cass_collection_append_uuid(coll_ptr, const_cast<CassUuid&>(val));
        }
        bool do_append(CassCollection* coll_ptr, const cb::RefId& val)
        {
            return do_append(coll_ptr, val.get_uuid());
        }
        bool do_append(CassCollection* coll_ptr, const CassInet& val)
        {
            return cass_collection_append_inet(coll_ptr, val);
        }
        bool do_append(CassCollection* coll_ptr, const CassDecimal& val)
        {
            return cass_collection_append_decimal(coll_ptr, val);
        }

        template<typename T>
        bool do_bind(unsigned index, const std::vector<T>& val)
        {
            CassCollection* coll_ptr = cass_collection_new(CASS_COLLECTION_TYPE_LIST, val.size());
            bool retVal = true;
            if (coll_ptr)
            {
                for (auto it = val.begin(); it != val.end() && retVal; ++it)
                {
                    retVal = do_append(coll_ptr, *it) == CASS_OK;
                }
                if (retVal)
                {
                    retVal = cass_statement_bind_collection(m_statement, index, coll_ptr) == CASS_OK;
                }
                cass_collection_free(coll_ptr);
            }
            return retVal;
        }

        template<typename T>
        bool do_bind(unsigned index, const std::list<T>& val)
        {
            CassCollection* coll_ptr = cass_collection_new(CASS_COLLECTION_TYPE_LIST, val.size());
            bool retVal = true;
            if (coll_ptr)
            {
                for (auto it = val.begin(); it != val.end() && retVal; ++it)
                {
                    retVal = do_append(coll_ptr, *it) == CASS_OK;
                }
                if (retVal)
                {
                    retVal = cass_statement_bind_collection(m_statement, index, coll_ptr) == CASS_OK;
                }
                cass_collection_free(coll_ptr);
            }
            return retVal;
        }

        template<typename T>
        bool do_bind(unsigned index, const std::set<T>& val)
        {
            CassCollection* coll_ptr = cass_collection_new(CASS_COLLECTION_TYPE_SET, val.size());
            bool retVal = true;
            if (coll_ptr)
            {
                for (auto it = val.begin(); it != val.end() && retVal; ++it)
                {
                    retVal = do_append(coll_ptr, *it) == CASS_OK;
                }
                if (retVal)
                {
                    retVal = cass_statement_bind_collection(m_statement, index, coll_ptr) == CASS_OK;
                }
                cass_collection_free(coll_ptr);
            }
            return retVal;
        }

        template<typename T, typename S>
        bool do_bind(unsigned index, const std::map<S,T>& val)
        {
            CassCollection* coll_ptr = cass_collection_new(CASS_COLLECTION_TYPE_MAP, val.size());
            bool retVal = true;
            if (coll_ptr)
            {
                for (auto it = val.begin(); it != val.end() && retVal; ++it)
                {
                    retVal = (do_append(coll_ptr, it->first) == CASS_OK)
                             && (do_append(coll_ptr, it->second) == CASS_OK);
                }
                if (retVal)
                {
                    retVal = cass_statement_bind_collection(m_statement, index, coll_ptr) == CASS_OK;
                }
                cass_collection_free(coll_ptr);
            }
            return retVal;
        }

        bool do_bind(unsigned index, const NullBinder& val)
        {
            return cass_statement_bind_null(m_statement, index) == CASS_OK;
        }

        template<typename T, typename... Targs>
        void bind(unsigned index, const T& value, Targs... Fargs)
        {
            if (!do_bind(index, value))
            {
                std::ostringstream err;
                err << "failed binding index: " << index << "/" << m_num_args << " for PreparedStore: " 
                    << m_query.c_str();
                throw Exception(err.str(), __FILE__, __LINE__);
            }
            if (sizeof...(Fargs) > 0)
            {
                // bind the next arg, if there is one
                bind(++index, Fargs...);
            }
        }

        void bind(unsigned index)
        {
            // no - op
        }

        friend class cb::CassConn;
        // only created from CassConn
        PreparedStore(const std::string& query,
                      CassStatement* statement, 
                      unsigned num_args, 
                      cass_duration_t timeout_in_micro)
        : m_query(query.c_str()),                       // might be used in different threads.
          m_statement(statement),
          m_num_args(num_args),
          m_timeout_in_micro(timeout_in_micro)
        {
            if (!m_statement)
            {
                throw Exception("can't make PreparedStore with null statement",
                                 __FILE__, __LINE__);
            }
        }

        PreparedStore() = delete;
        PreparedStore(const PreparedStore&) = delete;

        std::string m_query;
        CassStatement* m_statement;
        unsigned m_num_args;
        cass_duration_t m_timeout_in_micro;

        std::mutex m_mutex;
    };
    typedef std::shared_ptr<PreparedStore> PreparedStorePtr;

}

#endif 

