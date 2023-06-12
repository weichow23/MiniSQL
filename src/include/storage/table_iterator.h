#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
 public:
  // you may define your own constructor based on your member variables
  //  explicit TableIterator();
  explicit TableIterator(){};
  explicit TableIterator(TableHeap * t,RowId rid);  //修改了构造函数

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  //  inline bool operator==(const TableIterator &itr) const;
  bool operator==(const TableIterator &itr) const;

  //  inline bool operator!=(const TableIterator &itr) const;
  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

 private:
  // add your own private member variables here
  TableHeap* table_heap_;// 新加
  Row* row_;// 新加
};

#endif  // MINISQL_TABLE_ITERATOR_H
