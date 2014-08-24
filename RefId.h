#ifndef CB_REFID_H
#define CB_REFID_H

#include <iostream>
#include <vector>
#include "cql-interface/CassConn.h"

namespace cb {

    class RefIdImp
    {
        public:
            // makes a reset ref_id, which is empty
            RefIdImp();

            RefIdImp(int64_t some_time);
            RefIdImp(const CassUuid& uuid);
            RefIdImp(const RefIdImp& other);
            RefIdImp(const RefIdImp&& other);

            RefIdImp& operator=(const RefIdImp& other);
            RefIdImp& operator=(const RefIdImp&& other);
            RefIdImp& operator=(const CassUuid& uuid);

            // looking for input of the form:
            // "550e8400-e29b-41d4-a716-446655440000"
            void assign(std::istream& is);

            // randomize this RefId
            void randomize();

            void reset();

            bool empty() const;

            // is this set (not empty)?
            operator bool() const {return !empty();}

            // compare
            bool operator==(const RefIdImp& other) const;
            bool operator<(const RefIdImp& other) const;

            // efficient write in standard uuid format
            void write(std::ostream& os) const;

            // a bit costly
            std::string to_string() const;
            
            bool did_extract(const CassValue* value);

        protected:

            CassUuid m_uuid;
    };

    typedef RefIdImp RefId;
}

namespace std
{
    // moved inside std namespace due to compile error that was showing
    // up with it outside.
    inline ostream& operator<<(ostream& os, const cb::RefIdImp& refid)
    {
        refid.write(os);
        return os;
    }
}


#endif // CB_REFID_H

