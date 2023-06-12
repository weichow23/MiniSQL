#ifndef MINISQL_LRUK_REPLACER_H
#define MINISQL_LRUK_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUKReplacer : public Replacer {
 public:

  explicit LRUKReplacer(size_t num_pages);

  ~LRUKReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
  // add your own private member variables here
  std::unordered_map<frame_id_t,unsigned int> access_time;
  std::list<frame_id_t> history_list_;
  std::list<frame_id_t> cache_list_;
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> history_map_;
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> cache_map_;

  std::unordered_map<frame_id_t, bool> is_evictable_;  
  int64_t k_;
};

#endif  // MINISQL_LRUK_REPLACER_H