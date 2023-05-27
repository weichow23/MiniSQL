#include "record/schema.h"

// Schema 序列化
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t offset = 0;

  MACH_WRITE_TO(uint32_t, buf, SCHEMA_MAGIC_NUM);
  offset += sizeof(uint32_t);

  MACH_WRITE_UINT32(buf + offset, columns_.size());
  offset += sizeof(uint32_t);

  for (auto &itr : columns_) {
    offset += itr->SerializeTo(buf + offset);
  }

  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = 0;
  for (auto &itr : columns_) {
    size += itr->GetSerializedSize();
  }
  return size + 2 * sizeof(uint32_t);
}
// Schema 反序列化
uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  auto num = MACH_READ_FROM(uint32_t, buf);
  ASSERT(num == Schema::SCHEMA_MAGIC_NUM, "Schema magic num error.");
  uint32_t offset = sizeof(uint32_t);
  auto col_size = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  std::vector<Column *> columns;
  for (auto i = 0u; i < col_size; i++) {
    Column *col;
    offset += Column::DeserializeFrom(buf + offset, col);
    columns.push_back(col);
  }
  schema = new Schema(columns);
  return offset;
}