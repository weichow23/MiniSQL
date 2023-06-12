#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/*Colunm 序列化*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t offset = 0, len= name_.size() * sizeof(char);
  MACH_WRITE_TO(uint32_t, buf, COLUMN_MAGIC_NUM);
  offset += sizeof(uint32_t);
  memcpy(buf + offset, &len, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  memcpy(buf + offset, name_.c_str(), len);
  offset += len;
  MACH_WRITE_TO(TypeId, buf + offset, type_);
  offset += sizeof(TypeId);
  MACH_WRITE_TO(uint32_t, buf + offset, len_);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t, buf + offset, table_ind_);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(bool, buf + offset, nullable_);
  offset += sizeof(bool);
  MACH_WRITE_TO(bool, buf + offset, unique_);
  offset += sizeof(bool);
  return offset;
}

/* 返回序列化的Column的长度*/
uint32_t Column::GetSerializedSize() const {
  return 4 * sizeof(uint32_t) + name_.size() * sizeof(char) + sizeof(TypeId) + 2 * sizeof(bool);
}

/*反序列化*/
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." 									 << std::endl;
  }
  auto magic_num = MACH_READ_FROM(uint32_t, buf);
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Column magic num error.");

  uint32_t offset = sizeof(uint32_t);
  auto len = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  char tmp[len / sizeof(char) + 1];
  memset(tmp, '\0', sizeof(tmp));
  memcpy(tmp, buf + offset, len);
  auto name_(tmp);
  offset += len;
  auto type_ = MACH_READ_FROM(TypeId, buf + offset);
  offset += sizeof(TypeId);
  auto len_ = MACH_READ_FROM(TypeId, buf + offset);
  offset += sizeof(uint32_t);
  auto table_ind_ = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);
  auto nullable_ = MACH_READ_FROM(bool, buf + offset);
  offset += sizeof(bool);
  auto unique_ = MACH_READ_FROM(bool, buf + offset);
  offset += sizeof(bool);
  // not char type
  if (type_ == kTypeChar) {
    column = new Column(name_, type_, len_, table_ind_, nullable_, unique_);
  } else {
    column = new Column(name_, type_, table_ind_, nullable_, unique_);
  }
  return offset;
}
