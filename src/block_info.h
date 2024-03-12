#ifndef MINIDB_BLOCK_INFO_H_
#define MINIDB_BLOCK_INFO_H_

#include <sys/types.h>

#include "commons.h"
#include "file_info.h"

class BlockInfo {
private:
  FileInfo *file_;
  int block_num_;
  char *data_;
  bool dirty_;
  long age_;
  BlockInfo *next_;

public:
  BlockInfo(int num) // input block number
      : dirty_(false), next_(NULL), file_(NULL), age_(0), block_num_(num) {
    data_ = new char[4 * 1024];
  }

  // byte index 0-3 record previous block number, 
  // byte index 4-7 record next block number
  // byte index 8-11 record record count, which means number of rows of the table contained in this block
  // byte index 12 onwards, record content

  virtual ~BlockInfo() { delete[] data_; }
  FileInfo *file() { return file_; }
  void set_file(FileInfo *f) { file_ = f; }

  int block_num() { return block_num_; }
  void set_block_num(int num) { block_num_ = num; }

  char *data() { return data_; }

  long age() { return age_; }

  bool dirty() { return dirty_; }
  void set_dirty(bool dt) { dirty_ = true; }

  BlockInfo *next() { return next_; }
  void set_next(BlockInfo *block) { next_ = block; }

  void IncreaseAge() { ++age_; }
  void ResetAge() { age_ = 0; }

  void SetPrevBlockNum(int num) { *(int *)(data_) = num; }

  int GetPrevBlockNum() { return *(int *)(data_); }

  void SetNextBlockNum(int num) { *(int *)(data_ + 4) = num; }

  int GetNextBlockNum() { return *(int *)(data_ + 4); }

  void SetRecordCount(int count) { *(int *)(data_ + 8) = count; } // byte index 8-11 record record count, which means number of rows of the table contained in this block

  void DecreaseRecordCount() { *(int *)(data_ + 8) = *(int *)(data_ + 8) - 1; } // byte index 8-11 record record count, which means number of rows of the table contained in this block

  int GetRecordCount() { return *(int *)(data_ + 8); } // byte index 8-11 record record count, which means number of rows of the table contained in this block

  char *GetContentAddress() { return data_ + 12; } // byte index 12 onwards, record content

  void ReadInfo(std::string path);
  void WriteInfo(std::string path);
};

#endif /* MINIDB_BLOCK_INFO_H_ */
