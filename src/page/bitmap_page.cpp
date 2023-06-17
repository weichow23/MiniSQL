#include "page/bitmap_page.h"

#include "glog/logging.h"

/*  分配一个空闲页，并通过page_offset返回所分配的空闲页位于该段中的下标
 * 尤其需要注意，下标从0开始
 * */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ < MAX_CHARS * 8) {
    bytes[next_free_page_ / 8] |= 1 << (next_free_page_ % 8);
    page_offset = next_free_page_;
    page_allocated_++;
    //执行查找下一个空闲的page：如果index达到了MAX_CHARS，并且在下次分配之前没有释放，那么下次分配就没有空闲page，allocate失败
    while (!IsPageFreeLow(next_free_page_ / 8, next_free_page_ % 8) && next_free_page_ < MAX_CHARS * 8) {
      next_free_page_++;
    }
    return true;
  }
  return false;
}

/*回收已经被分配的页*/
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (!IsPageFreeLow(page_offset / 8, page_offset % 8)) {
    bytes[page_offset / 8] &= ~(1 << (page_offset % 8));
    page_allocated_--;
    if (page_offset < next_free_page_) {
      next_free_page_ = page_offset;
    }
    return true;
  }
  return false; //没有被使用过，不能回收
}

/*判断给定的页是否是空闲（未分配）的*/
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return !(bytes[byte_index] & (1 << bit_index));
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;