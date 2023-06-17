#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_==MAX_CHARS*8) return false;
  else
  {
    page_allocated_++;
    for(uint32_t i=0;i<MAX_CHARS*8;i++)
      if(IsPageFree(i))
      {
        next_free_page_=i;
        break;
      }
  }
  page_offset=next_free_page_;
  uint32_t byte_index = page_offset / 8;
  uint32_t bit_index  = page_offset % 8;
  if(bytes[byte_index]>>bit_index&1)
    return false;
  bytes[byte_index]+=1<<bit_index;
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  uint32_t byte_index = page_offset / 8;
  uint32_t bit_index  = page_offset % 8;
  if(!(bytes[byte_index]>>bit_index&1))
    return false;
  bytes[byte_index]-=1<<bit_index;
  page_allocated_--;
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_index = page_offset / 8;
  uint32_t bit_index  = page_offset % 8;
  return !(bytes[byte_index]>>bit_index&1);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;