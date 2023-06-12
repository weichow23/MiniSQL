#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages)
    : capacity(num_pages) {}

CLOCKReplacer::~CLOCKReplacer() { }

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  if (clock_list.empty()) {
    frame_id = nullptr;
    return false;
  }
  // 遍历，状态替换
  while (true) {
    frame_id_t current_frame_id = clock_list.front();
    clock_list.pop_front();
    if (clock_status[current_frame_id] == State::USED) {
      clock_status[current_frame_id] = State::UNUSED; // second chance reset
      clock_list.push_back(current_frame_id);
    } else {
      clock_status.erase(current_frame_id);
      *frame_id = current_frame_id;
      return true;
    }
  }
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
  if (clock_status.count(frame_id) == 1) {
    clock_status.erase(frame_id);
    clock_list.remove(frame_id);
  }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  if (clock_status.count(frame_id) == 0) {
    clock_status[frame_id] = State::USED;
    clock_list.push_back(frame_id);
  }
}

size_t CLOCKReplacer::Size() {
  return clock_list.size();
}

bool CLOCKReplacer::IsEmpty(CLOCKReplacer::State& item) {
  return item != State::EMPTY;
}
