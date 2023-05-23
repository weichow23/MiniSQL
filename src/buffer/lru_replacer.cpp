#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/*替换（即删除）与所有被跟踪的页相比最近最少被访问的页，
 * 将其页帧号（即数据页在Buffer Pool的Page数组中的下标）
 * 存储在输出参数frame_id中输出并返回true，如果当前没有可以替换的元素则返回false*/
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // Check if the list is empty, if not assign the least recently used frame to frame_id
  if (!victims_.empty()) {
    *frame_id = victims_.back();
    frame_map.erase(*frame_id);  //从map中删除
    victims_.pop_back();  // 从list中删除
    return true;
  }
  // throw std:: runtime_error("No victim found");
  return false;
}
/*将数据页固定使之不能被Replacer替换，即从lru_list_中移除该数据页对应的页帧
 * Pin函数应当在一个数据页被Buffer Pool Manager固定时被调用；*/
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto iter = frame_map.find(frame_id); // 检查frame_id是否在map中
  if (iter != frame_map.end()) { // 如果在map中,则从map和list中删除
    victims_.erase(iter->second);
    frame_map.erase(iter);
  }
}
/*将数据页解除固定，放入lru_list_中，使之可以在必要时被Replacer替换掉
 * Unpin函数应当在一个数据页的引用计数变为0时被Buffer Pool Manager调用，
 * 使页帧对应的数据页能够在必要时被替换*/
void LRUReplacer::Unpin(frame_id_t frame_id) {
  // 检查frame_id是否在map中
  if (frame_map.find(frame_id) == frame_map.end() && victims_.size() < max_pages) {
    victims_.push_front(frame_id);  // 添加到list的头部
    frame_map[frame_id] = victims_.begin();  // 添加到map
  }
}
/*返回当前LRUReplacer中能够被替换的数据页的数量*/
size_t LRUReplacer::Size() {
  return victims_.size();
}