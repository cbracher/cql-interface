#ifndef CB_CASS_BYTES_MGR_H
#define CB_CASS_BYTES_MGR_H

#include <boost/make_shared.hpp>
#include <cassandra.h>
#include <algorithm>
#include <vector>

namespace cb {

    // We are often working with CassBytes after the result which hold the data is deleted.
    // Using this class for managing the CassBytes storage. 
    class CassBytesMgr
    {
    public:

        CassBytesMgr()
        {
        }

        typedef std::vector<cass_byte_t> Bytes;

        const Bytes& data() const
        {
            return m_buffer;
        }

        cass_size_t size() const
        {
            return m_buffer.size();
        }

        void clear()
        {
            m_buffer.clear();
        }

        void assign(const CassBytes& bytes)
        {
            m_buffer.clear();
            m_buffer.resize(bytes.size);
            std::copy(bytes.data, bytes.data + bytes.size, m_buffer.begin());
        }

    private:

        std::vector<cass_byte_t> m_buffer;
    };
}

#endif 

