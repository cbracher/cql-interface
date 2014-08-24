#include <iomanip>
#include <iostream>
#include <sstream>
#include "cql-interface/RefId.h"

using namespace cb;
using namespace std;

RefIdImp::RefIdImp()
{
    reset();
}

RefIdImp::RefIdImp(int64_t some_time)
{
    cass_uuid_from_time(some_time, m_uuid);
}

RefIdImp::RefIdImp(const CassUuid& uuid)
{
    operator=(uuid);
}

RefIdImp::RefIdImp(const RefIdImp& other)
{
    operator=(other);
}

RefIdImp::RefIdImp(const RefIdImp&& other)
{
    operator=(other);
}


RefIdImp& RefIdImp::operator=(const RefIdImp& other)
{
    return operator=(other.m_uuid);
}

RefIdImp& RefIdImp::operator=(const RefIdImp&& other)
{
    return operator=(other.m_uuid);
}

RefIdImp& RefIdImp::operator=(const CassUuid& uuid)
{
    for (size_t i=0; i<CASS_UUID_NUM_BYTES; ++i)
    {
        m_uuid[i] = uuid[i];
    }
    return *this;
}

void RefIdImp::randomize()
{
    cass_uuid_generate_random(m_uuid);
}


bool RefIdImp::operator==(const RefIdImp& other) const
{
    bool retVal = true;
    for (size_t i=0; i<CASS_UUID_NUM_BYTES; ++i)
    {
        if (m_uuid[i] != other.m_uuid[i])
        {
            retVal = false;
            break;
        }
    }
    return retVal;
}

bool RefIdImp::operator<(const RefIdImp& other) const
{
    bool retVal = false;
    for (size_t i=0; i<CASS_UUID_NUM_BYTES; ++i)
    {
        if (m_uuid[i] < other.m_uuid[i])
        {
            retVal = true;
            break;
        } else if (m_uuid[i] > other.m_uuid[i])
        {
            retVal = false;
            break;
        }
    }
    return retVal;
}

bool RefIdImp::empty() const
{
    bool retVal = true;
    for (size_t i=0; i<CASS_UUID_NUM_BYTES; ++i)
    {
        if (m_uuid[i])
        {
            retVal = false;
            break;
        }
    }
    return retVal;
}

void RefIdImp::reset()
{
    CassConn::reset(m_uuid);
}

std::string RefIdImp::to_string() const
{
    ostringstream os;
    write(os);
    return os.str();
}

void RefIdImp::assign(std::istream& is)
{
    unsigned index = 0;
    uint64_t tmp1 = 0;
    char ctmp = 0;
    if (is) is >> std::hex >> tmp1;
    m_uuid[index++] = tmp1 >> (4*2*3);
    m_uuid[index++] = (tmp1 >> (4*2*2)) & 255;
    m_uuid[index++] = (tmp1 >> (4*2*1)) & 255;
    m_uuid[index++] = tmp1 & 255;

    if (is) is >> ctmp;
    if (is) is >> std::hex >> tmp1;
    m_uuid[index++] = tmp1 >> (4*2);
    m_uuid[index++] = tmp1 & 255;

    if (is) is >> ctmp;
    if (is) is >> std::hex >> tmp1;
    m_uuid[index++] = tmp1 >> (4*2);
    m_uuid[index++] = tmp1 & 255;

    if (is) is >> ctmp;
    if (is) is >> std::hex >> tmp1;
    m_uuid[index++] = tmp1 >> (4*2);
    m_uuid[index++] = tmp1 & 255;

    if (is) is >> ctmp;
    if (is) is >> std::hex >> tmp1;
    m_uuid[index++] = tmp1 >> (4*2*5);
    m_uuid[index++] = (tmp1 >> (4*2*4)) & 255;
    m_uuid[index++] = (tmp1 >> (4*2*3)) & 255;
    m_uuid[index++] = (tmp1 >> (4*2*2)) & 255;
    m_uuid[index++] = (tmp1 >> (4*2*1)) & 255;
    m_uuid[index++] = tmp1 & 255;
}

void RefIdImp::write(std::ostream& os) const
{
    os << hex << setfill('0');
    for(size_t i = 0; i < CASS_UUID_NUM_BYTES; ++i) {
        os << setw(2) << hex << uint16_t(m_uuid[i]);
        if (i == 3 || i == 5 || i == 7 || i == 9) {
            os << '-';
        }
    }
    os << dec;
}

bool RefIdImp::did_extract(const CassValue* value)
{
    CassError rc = cass_value_get_uuid(value, m_uuid);
    return rc == CASS_OK;
}

