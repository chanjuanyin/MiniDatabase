#ifndef MINIDB_FILE_HANDLE_H_
#define MINIDB_FILE_HANDLE_H_

#include <string>

#include "block_info.h"
#include "file_info.h"

// file_handle is the handle that really uses a block.
// Such blocks are initially taken from block_handle as empty blocks
// once all empty blocks are used up in block_handle, if file_handle needs to use more blocks it will recycle the oldest block
// once a block has finished its usage, file_handle will then put back the block (as empty block) into block_handle
class FileHandle {
private:
  FileInfo *first_file_;
  std::string path_;

public:
  FileHandle(std::string p) : first_file_(new FileInfo()), path_(p) {}
  ~FileHandle();
  FileInfo* GetFileInfo(std::string db_name, std::string tb_name,
                        int file_type);
  BlockInfo* GetBlockInfo(FileInfo *file, int block_pos);
  void AddBlockInfo(BlockInfo *block); // Add block to the last
  void IncreaseAge(); // Increase age for all blocks inside all files
  BlockInfo *RecycleBlock(); // Pop and get the oldest block
  void AddFileInfo(FileInfo *file); // Add fileinfo to the last
  void WriteToDisk();
};

#endif /* defined(MINIDB_FILE_HANDLE_H_) */
