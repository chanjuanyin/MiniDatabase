#ifndef MINIDB_BUFFER_MANAGER_H_
#define MINIDB_BUFFER_MANAGER_H_

#include <string>

#include "block_handle.h"
#include "file_handle.h"
#include "block_info.h"

class BufferManager {
private:
  BlockHandle *bhandle_; // container of initial 300 empty blocks
  FileHandle *fhandle_;  // container of all blocks that are currently in use
  std::string path_;

  BlockInfo *GetUsableBlock(); // if bhandle_ has empty block, use it; else recycle the oldest block from fhandle_

public:
  BufferManager(std::string p)
      : bhandle_(new BlockHandle(p, 300)), fhandle_(new FileHandle(p)), path_(p) {}
  ~BufferManager() {
    delete bhandle_;
    delete fhandle_;
  }

  BlockInfo *GetFileBlock(std::string db_name, std::string tb_name,
                          int file_type, int block_num);
  void WriteBlock(BlockInfo *block);
  void WriteToDisk();
};

#endif /* defined(MINIDB_HANDLE_H_) */