#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>
#include <map> //用于存储id和iterator的映射关系

#include "buffer/replacer.h"
#include "common/config.h"
#include "glog/logging.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
 /*增加的成员变量*/
 std::list<frame_id_t> victims_;  // 记录最近使用的页面, 最近使用的页面在前面
 std::map<frame_id_t, std::list<frame_id_t>::iterator> frame_map;  // id -> iterator
 size_t max_pages;  // 最大页面数
};

#endif  // MINISQL_LRU_REPLACER_H
