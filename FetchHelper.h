#ifndef CB_FETCHER_HELPER_H
#define CB_FETCHER_HELPER_H

#include <vector>
#include <set>
#include <map>
#include <list>
#include "log4cxx/logger.h"
#include "cql-interface/CassConn.h"
#include "cql-interface/RefId.h"
#include "cql-interface/CassUtil.h"

namespace cb {


    // helper functions used in the CassFetchers below
    enum cass_fld_required_enum_t { CASS_FLD_IS_REQUIRED_ENUM, CASS_FLD_NOT_REQUIRED_ENUM};
    class FetchHelper
    {
    public:
        template<typename T> static bool get_first(T& val, 
                                                   const CassRow& row,
                                                   cass_fld_required_enum_t req_opt 
                                                        = CASS_FLD_IS_REQUIRED_ENUM)
        {
            return get_nth(0, val, row, req_opt);
        }

        template <typename T>
        static bool get_nth(int field, 
                            T& val, 
                            const CassRow& row,
                            cass_fld_required_enum_t req_opt 
                                  = CASS_FLD_IS_REQUIRED_ENUM)
        {
            const CassValue* cass_value = cass_row_get_column(&row, field);
            if (cass_value_is_null(cass_value))
            {
                my_reset(val);
                return req_opt != CASS_FLD_IS_REQUIRED_ENUM;
            }
            return did_extract(val, cass_value);
        }

        template <typename T>
        static bool get_nth(int field, 
                            std::vector<T>& val, 
                            const CassRow& row,
                            cass_fld_required_enum_t req_opt 
                                  = CASS_FLD_IS_REQUIRED_ENUM)
        {
            val.clear();
            const CassValue* cass_value = cass_row_get_column(&row, field);
            if (cass_value_is_null(cass_value))
            {
                return req_opt != CASS_FLD_IS_REQUIRED_ENUM;
            }
            CassIterator* items_iterator = cass_iterator_from_collection(cass_value);
            T tmp;
            while(cass_iterator_next(items_iterator))
            {
                if (did_extract(tmp, cass_iterator_get_value(items_iterator)))
                {
                    val.push_back(tmp);
                } else
                {
                    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("cb.cass_fetch_helper"));
                    LOG4CXX_ERROR(logger, "failed reading a vector collection value");
                    return false;
                }
            }
            cass_iterator_free(items_iterator);
            return true;
        }

        template <typename T>
        static bool get_nth(int field, 
                            std::list<T>& val, 
                            const CassRow& row,
                            cass_fld_required_enum_t req_opt 
                                  = CASS_FLD_IS_REQUIRED_ENUM)
        {
            val.clear();
            const CassValue* cass_value = cass_row_get_column(&row, field);
            if (cass_value_is_null(cass_value))
            {
                return req_opt != CASS_FLD_IS_REQUIRED_ENUM;
            }
            CassIterator* items_iterator = cass_iterator_from_collection(cass_value);
            T tmp;
            while(cass_iterator_next(items_iterator))
            {
                if (did_extract(tmp, cass_iterator_get_value(items_iterator)))
                {
                    val.push_back(tmp);
                } else
                {
                    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("cb.cass_fetch_helper"));
                    LOG4CXX_ERROR(logger, "failed reading a list collection value");
                    return false;
                }
            }
            cass_iterator_free(items_iterator);
            return true;
        }

        template <typename T>
        static bool get_nth(int field, 
                            std::set<T>& val, 
                            const CassRow& row,
                            cass_fld_required_enum_t req_opt 
                                  = CASS_FLD_IS_REQUIRED_ENUM)
        {
            val.clear();
            const CassValue* cass_value = cass_row_get_column(&row, field);
            if (cass_value_is_null(cass_value))
            {
                return req_opt != CASS_FLD_IS_REQUIRED_ENUM;
            }
            CassIterator* items_iterator = cass_iterator_from_collection(cass_value);
            T tmp;
            while(cass_iterator_next(items_iterator))
            {
                if (did_extract(tmp, cass_iterator_get_value(items_iterator)))
                {
                    val.insert(tmp);
                } else
                {
                    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("cb.cass_fetch_helper"));
                    LOG4CXX_ERROR(logger, "failed reading a set collection value");
                    return false;
                }
            }
            cass_iterator_free(items_iterator);
            return true;
        }

        template <typename S, typename T>
        static bool get_nth(int field, 
                            std::map<S,T>& val, 
                            const CassRow& row,
                            cass_fld_required_enum_t req_opt 
                                  = CASS_FLD_IS_REQUIRED_ENUM)
        {
            static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("cb.cass_fetch_helper"));
            val.clear();
            const CassValue* cass_value = cass_row_get_column(&row, field);
            if (cass_value_is_null(cass_value))
            {
                return req_opt != CASS_FLD_IS_REQUIRED_ENUM;
            }
            CassIterator* items_iterator = cass_iterator_from_collection(cass_value);
            S tmp_key;
            T tmp_val;
            while(cass_iterator_next(items_iterator))
            {
                if (did_extract(tmp_key, cass_iterator_get_value(items_iterator))
                    && cass_iterator_next(items_iterator)
                    && did_extract(tmp_val, cass_iterator_get_value(items_iterator)))
                {
                    LOG4CXX_TRACE(logger, "extracted key: " << tmp_key
                                            << " and value: " << tmp_val);
                    val[tmp_key] = tmp_val;
                } else
                {
                    LOG4CXX_ERROR(logger, "failed reading a map collection value");
                    return false;
                }
            }
            cass_iterator_free(items_iterator);
            return true;
        }

    protected:

        static bool did_extract(bool& val_in, const CassValue* cass_value)
        {
            cass_bool_t val;
            CassError rc = cass_value_get_bool(cass_value, &val);
            if (rc == CASS_OK)
            {
                val_in = (val == cass_true ? true : false);
            } else
            {
                val_in = false;
            }
            return rc == CASS_OK;
        }

        static bool did_extract(int32_t& val_in, const CassValue* cass_value)
        {
            cass_int32_t val = 0;
            CassError rc = cass_value_get_int32(cass_value, &val);
            if (rc == CASS_OK)
            {
                val_in = val;
            } else
            {
                val_in = 0;
            }
            return rc == CASS_OK;
        }

        static bool did_extract(cass_float_t& val, const CassValue* cass_value)
        {
            CassError rc = cass_value_get_float(cass_value, &val);
            return rc == CASS_OK;
        }

        static bool did_extract(cass_double_t& val, const CassValue* cass_value)
        {
            CassError rc = cass_value_get_double(cass_value, &val);
            return rc == CASS_OK;
        }

        static bool did_extract(cass_int64_t& val, const CassValue* cass_value)
        {
            CassError rc = cass_value_get_int64(cass_value, &val);
            return rc == CASS_OK;
        }

        static bool did_extract(std::string& val_in, const CassValue* cass_value)
        {
            CassString val;
            CassError rc = cass_value_get_string(cass_value, &val);
            if (rc == CASS_OK)
            {
                val_in.assign(val.data, val.length);
            }
            return rc == CASS_OK;
        }

        static bool did_extract(RefId& val, const CassValue* cass_value)
        {
            return val.did_extract(cass_value);
        }

        static bool did_extract(CassDecimal& val, const CassValue* cass_value)
        {
            CassError rc = cass_value_get_decimal(cass_value, &val);
            return rc == CASS_OK;
        }

        static bool did_extract(CassBytes& val, const CassValue* cass_value)
        {
            CassError rc = cass_value_get_bytes(cass_value, &val);
            return rc == CASS_OK;
        }

        static bool did_extract(CassInet& val, const CassValue* cass_value)
        {
            CassError rc = cass_value_get_inet(cass_value, &val);
            return rc == CASS_OK;
        }

        // resets
        static void my_reset(bool& val)
        {
            val = false;
        }

        static void my_reset(cass_int32_t& val)
        {
            val = 0;
        }

        static void my_reset(cass_float_t& val)
        {
            val = 0;
        }

        static void my_reset(cass_double_t& val)
        {
            val = 0;
        }

        static void my_reset(cass_int64_t& val)
        {
            val = 0;
        }

        static void my_reset(std::string& val_in)
        {
            val_in.clear();
        }

        static void my_reset(RefId& val)
        {
            val.reset();
        }

        static void my_reset(CassDecimal& val)
        {
            CassConn::reset(val);
        }

        static void my_reset(CassBytes& val)
        {
            val.data = 0;
            val.size = 0;
        }

        static void my_reset(CassInet& val)
        {
            CassConn::reset(val);
        }

        private:

        FetchHelper() = delete;
        ~FetchHelper() = delete;
    };
}

#endif 

