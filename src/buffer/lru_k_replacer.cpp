#include "buffer/lru_k_replacer.h"

LRUKReplacer::LRUKReplacer(size_t k){
    k_=k;
}

LRUKReplacer::~LRUKReplacer(){ }

bool LRUKReplacer::Victim(frame_id_t *frame_id) {
    if(cache_list_.empty()&&history_list_.empty())
        return false;
    for(auto& it:history_list_){
        auto frame=it;
        if(is_evictable_[frame]){
            access_time[frame]=0;
            history_list_.erase(history_map_[frame]);
            history_map_.erase(frame);
            *frame_id=frame;
            is_evictable_[frame]=false;
            return true;
        }
    }
    for(auto& it:cache_list_){
        auto frame=it;
        if(is_evictable_[frame]){
            access_time[frame]=0;
            cache_list_.erase(history_map_[frame]);
            cache_map_.erase(frame);
            *frame_id=frame;
            is_evictable_[frame]=false;
            return true;
        }        
    }
    return false;
}

void LRUKReplacer::Pin(frame_id_t frame_id){
   if(access_time[frame_id]==0)
        return;
    is_evictable_[frame_id]=false;
}

void LRUKReplacer::Unpin(frame_id_t frame_id){
   access_time[frame_id]++;
   is_evictable_[frame_id]=true;
  if (access_time[frame_id] == k_) {
    auto it = history_map_[frame_id];
    history_list_.erase(it);
    history_map_.erase(frame_id);
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } 
  else if (access_time[frame_id] > k_) {
    if (cache_map_.count(frame_id) != 0U) {
      auto it = cache_map_[frame_id];
      cache_list_.erase(it);
    }
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } 
  else {
    if (history_map_.count(frame_id) == 0U) {
      history_list_.push_front(frame_id);
      history_map_[frame_id] = history_list_.begin();
    }
  }
}

size_t LRUKReplacer::Size() { 
  return cache_list_.size()+history_list_.size(); 
}