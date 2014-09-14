#ifndef CB_CASS_BYTES_MGR_H
#define CB_CASS_BYTES_MGR_H

#include <algorithm>
#include <iostream>
#include <vector>
#include <boost/make_shared.hpp>
#include <cassandra.h>

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

        void assign(const std::vector<cass_byte_t>& other)
        {
            m_buffer.clear();
            m_buffer.resize(other.size());
            if (other.size())
            {
                std::copy(other.begin(), other.end(), m_buffer.begin());
            }
        }

        void push_back(uint8_t byte)
        {
            m_buffer.push_back(byte);
        }

        bool operator==(const CassBytesMgr& other) const
        {
            return m_buffer == other.m_buffer;
        }

    private:

        std::vector<cass_byte_t> m_buffer;
    };

}

namespace std
{
    inline ostream& operator<<(ostream& os, const cb::CassBytesMgr& obj)
    {
        for (auto it = obj.data().begin(); it != obj.data().end(); ++it)
        {
            os << "|" << uint16_t(*it);
        }
        return os;
    }
}

#endif 

