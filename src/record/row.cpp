#include "record/row.h"

/*Row 序列化*/
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
    uint32_t offset = 0;
  ASSERT(schema != nullptr, "Schema should be null.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size don't match schema's column size.");
  MACH_WRITE_TO(RowId, buf+offset, rid_);
  offset += sizeof(RowId);
  // write fields
  uint32_t column_count = schema->GetColumnCount();
  uint32_t i;
  for(i=0;i<column_count;i++){
    MACH_WRITE_TO(bool,buf+offset,fields_[i]->IsNull());
    offset += sizeof(bool);
    offset += fields_[i]->SerializeTo(buf+offset);
  }
  return offset;
}
/*Row 反序列化*/
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
    uint32_t offset = 0;
  ASSERT(schema != nullptr, "Schema should be null.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  this->rid_ = MACH_READ_FROM(RowId, buf+offset);
  offset += sizeof(RowId);
  uint32_t column_count = schema->GetColumnCount();
  if(column_count>fields_.capacity()) fields_.resize(column_count);
  for (uint32_t i = 0; i < column_count; ++i) {
    bool is_null = MACH_READ_FROM(bool,buf+offset);
    offset += sizeof(bool);
    const Column *temp = schema->GetColumn(i);
    offset += this->fields_[i]->DeserializeFrom(buf + offset, temp->GetType(), &fields_[i],is_null);
  }
  return offset;
}
/*Row 序列化大小*/
uint32_t Row::GetSerializedSize(Schema *schema) const {
    uint32_t offset = 0;
    uint32_t colomn_count = schema->GetColumnCount();
  ASSERT(schema != nullptr, "Schema should be null.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size don't match schema's column size.");
  for (uint32_t i=0;i<colomn_count;++i) {
    offset += sizeof(bool);
    offset += this->fields_[i]->GetSerializedSize();
  }
  offset += sizeof(RowId);
  return offset;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
    std::vector<Field> fields;
    uint32_t idx;
  auto columns = key_schema->GetColumns();
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}