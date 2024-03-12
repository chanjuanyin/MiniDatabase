#include "block_handle.h"

BlockHandle::~BlockHandle() {
  BlockInfo *p = first_block_;
  while (bcount_ > 0) {
    BlockInfo *pn = p->next();
    delete p;
    p = pn;
    bcount_--;
  }
}

// Initiate 301 new BlockInfo(0) and return the last new BlockInfo(0) block
// first_block_ new BlockInfo(0) will never be used
// Note: new BlockInfo(0) will have block number 0
BlockInfo *BlockHandle::Add(BlockInfo *block) {
  BlockInfo *adder = new BlockInfo(0);
  adder->set_next(block->next());
  block->set_next(adder);
  bcount_++;
  if (bcount_ == bsize_) {
    return adder;
  } else {
    return Add(adder);
  }
}

// Pop and get the empty block which lines up immediately after first_block_
BlockInfo *BlockHandle::GetUsableBlock() {
  if (bcount_ == 0) {
    return NULL;
  }

  BlockInfo *p = first_block_->next();
  first_block_->set_next(first_block_->next()->next());
  bcount_--;
  p->ResetAge();
  p->set_next(NULL);
  return p;
}

// Put back an empty block after first_block_
void BlockHandle::FreeBlock(BlockInfo *block) {
  if (bcount_ == 0) {
    first_block_ = block;
    block->set_next(block);
  } else {
    block->set_next(first_block_->next());
    first_block_->set_next(block);
  }
  bcount_++;
}