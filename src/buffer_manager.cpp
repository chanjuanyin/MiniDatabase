#include "buffer_manager.h"

#include <fstream>

#include "commons.h"

#include "file_info.h"

using namespace std;

BlockInfo *BufferManager::GetFileBlock(string db_name, string tb_name,
                                       int file_type, int block_num) {

  fhandle_->IncreaseAge();

  FileInfo *file = fhandle_->GetFileInfo(db_name, tb_name, file_type);

  // remember fhandle is the container of all blocks that are currently in use

  if (file) { // fhandle_ contains blocks whose file_info matches with the file_info you are looking for
    BlockInfo *block = fhandle_->GetBlockInfo(file, block_num);
    // if fhandle contains the block of which the file info and block_num matches with what you need
    if (block) {
      return block;
    } 
    // else, get one block either from bhandle_ (empty block) or from fhandle_ (recycled block)
    // then set the block to what you need
    // and add it back to fhandle
    else {
      BlockInfo *bp = GetUsableBlock();
      bp->set_block_num(block_num);
      bp->set_file(file);
      bp->ReadInfo(path_);
      fhandle_->AddBlockInfo(bp);
      return bp;
    }
  } else { // fhandle_ does not contain blocks whose file_info matches with the file_info you are looking for
    BlockInfo *bp = GetUsableBlock(); // get one block either from bhandle_ (empty block) or from fhandle_ (recycled block)
    bp->set_block_num(block_num); // set the block to what you need
    FileInfo *fp = new FileInfo(db_name, file_type, tb_name, 0, 0, NULL, NULL); // add new file_info into fhandle_
    fhandle_->AddFileInfo(fp);
    bp->set_file(fp);
    bp->ReadInfo(path_);
    fhandle_->AddBlockInfo(bp);
    return bp;
  }
  return 0;
}


BlockInfo *BufferManager::GetUsableBlock() {
  if (bhandle_->bcount() > 0) { // if bhandle_ still has empty blocks
    return bhandle_->GetUsableBlock(); // remember that handle_->GetUsableBlock() will write the data from the block to the memory
  } else { // b_handle has no empty blocks, therefore need to recycle the oldest block from fhandle_
    return fhandle_->RecycleBlock(); // remember that handle_->GetUsableBlock() will write the data from the block to the memory
  }
}

void BufferManager::WriteBlock(BlockInfo *block) { block->set_dirty(true); }

void BufferManager::WriteToDisk() { fhandle_->WriteToDisk(); } // write every blocks in fhandle_ to disk