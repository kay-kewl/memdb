#include "memdb/core/QueryResultIterator.h"

namespace memdb {
namespace core {

ResultRow QueryResultIterator::operator*() const {
    return ResultRow(*data_iter_, *columns_);
}

QueryResultIterator& QueryResultIterator::operator++() {
    ++data_iter_;
    return *this;
}

bool QueryResultIterator::operator!=(const QueryResultIterator& other) const {
    return data_iter_ != other.data_iter_;
}

}
}