#ifndef MINIDB_BLOCK_HANDLE_H_
#define MINIDB_BLOCK_HANDLE_H_

#include "block_info.h"

// The purpose of block_handle is to provide a total of 300 empty blocks for the purpose of buffer. 
// Each time when the file_handle needs to use a block
// if there are still empty blocks remaining, the file_handle will take an empty block from block_handle
// if there are no empty blocks remaining, then file_handle will recycle its own oldest block
// once a block is free, file_handle will then put back an empty block into block_handle
class BlockHandle {
private:
  BlockInfo *first_block_;
  int bsize_;  // total #
  int bcount_; // usable #
  std::string path_;

  // Inits BlockHandle
  BlockInfo *Add(BlockInfo *block);
  // Initiate 301 new BlockInfo(0) and return the last new BlockInfo(0) block
  // first_block_ new BlockInfo(0) will never be used
  // Note: new BlockInfo(0) will have block number 0

public:
  BlockHandle(std::string p)
      : first_block_(new BlockInfo(0)), bsize_(300), bcount_(0), path_(p) {
    Add(first_block_);
  }

  BlockHandle(std::string p, int bsize)
      : first_block_(new BlockInfo(0)), bsize_(bsize), bcount_(0), path_(p) {
    Add(first_block_);
  }

  ~BlockHandle();

  int bcount() { return bcount_; }

  BlockInfo *GetUsableBlock(); // Pop and get the empty block which lines up immediately after first_block_

  void FreeBlock(BlockInfo *block); // Put back an empty block after first_block_

  // Stack data structure
};

#endif /* defined(MINIDB_BLOCK_HANDLE_H_) */
