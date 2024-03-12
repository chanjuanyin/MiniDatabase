#include "record_manager.h"

#include <iomanip>
#include <iostream>

#include "index_manager.h"

using namespace std;

// Get the block_info which matches your db_name_, tbl->tb_name() and block_num
// Get the block_info from hdl (buffer) fhandle_
// if fhandle_ doesn't have one, then either get an empty block from bhandle_, or recycle the oldest block from fhandle_
// and read from the memory of the corresponding db_name_, tbl->tb_name() and block_num
BlockInfo* RecordManager::GetBlockInfo(Table *tbl, int block_num) {
  if (block_num == -1) {
    return NULL;
  }
  BlockInfo *block = hdl_->GetFileBlock(db_name_, tbl->tb_name(), 0, block_num); // 0 here refers to the file type (0 means record, 1 means index)
  return block;
}

void RecordManager::Insert(SQLInsert &st) {
  string tb_name = st.tb_name();
  unsigned long values_size = st.values().size();

  Table *tbl = cm_->GetDB(db_name_)->GetTable(tb_name);

  if (tbl == NULL) {
    throw TableNotExistException();
  }

  int max_count = (4096 - 12) / (tbl->record_length()); // tbl->record_length() is total number of bytes for all attributes
  // max_count means the maximum number of rows that one block can fit

  vector<TKey> tkey_values;
  int pk_index = -1;

  for (int i = 0; i < values_size; ++i) {
    int value_type = st.values()[i].data_type;
    string value = st.values()[i].value;
    int length = tbl->ats()[i].length(); // tbl->ats() means table's attribute vector
    // The length here is the number of bytes of the data
    // if data_type_==0 or ==1 then length_==4, otherwise if it is a string then length depends on how it is defined

    TKey tmp(value_type, length);
    tmp.ReadValue(value.c_str());
    tkey_values.push_back(tmp);

    if (tbl->ats()[i].attr_type() == 1) {
      pk_index = i;
    }
  }

  // if there is a primary key
  // then of course need to check against PrimaryKeyConflictException
  if (pk_index != -1) {

    if (tbl->GetIndexNum() != 0) { // Get the number of indices associated with this table

      BPlusTree tree(tbl->GetIndex(0), hdl_, cm_, db_name_);

      int value = tree.GetVal(tkey_values[pk_index]);
      if (value != -1) {
        throw PrimaryKeyConflictException();
      }
    } else { // There is a primary key but doesn't have an index for this table
      int block_num = tbl->first_block_num();
      for (int i = 0; i < tbl->block_count(); ++i) {
        BlockInfo* bp = GetBlockInfo(tbl, block_num);

        for (int j = 0; j < bp->GetRecordCount(); ++j) {
          vector<TKey> tkey_value = GetRecord(tbl, block_num, j);

          if (tkey_value[pk_index] == tkey_values[pk_index]) {
            throw PrimaryKeyConflictException();
          }
        }

        block_num = bp->GetNextBlockNum();
      }
    }
  }

  char *content;
  int ub = tbl->first_block_num();    // used block
  int frb = tbl->first_rubbish_num(); // first rubbish block
  int lastub;
  int blocknum, offset;

  // I want you to first imagine a linkedlist of useful blocks which belong to the same file (double-way linkedlist)
  // Arranging in block number, they are, e.g. 5 <-> 4 <-> 1 <-> 0. Not all useful blocks are full
  // There is also another linkedlist of rubbish blocks, e.g. 2 -> 3 (single-way linkedlist)
  // Now what the function is doing is that:
  // First, it traverse across the useful linkedlist and see if there is any block that is not full yet. If so, then it fills the row into that block
  // If all blocks in the useful linkedlist are filled then it does the following:
  // If there exists rubbish block, then take the first rubbish block, in our case is block number 2, and append it to the end of the useful linkedlist
  // So now our useful linkedlist (double-way) becomes 5 <-> 4 <-> 1 <-> 0 <-> 2 and our rubbish linkedlist (single-way) becomes 3
  // The new inserted row will be inserted into block number 2. 
  // If on the other hand there are no rubbish blocks, but since you still need to insert a new row
  // So what you will do is add a new useful block to the front of the useful linkedlist
  // So now the useful linkedlist (double-way) becomes 6 <-> 5 <-> 4 <-> 1 <-> 0 <-> 2 with the new row added into block number 6.

  // First, it traverse across the useful linkedlist and see if there is any block that is not full yet. If so, then it fills the row into that block
  while (ub != -1) { // keep traversing through the linkedlist of useful blocks
    lastub = ub;     // lastup is used to record the last element of the useful linkedlist
    BlockInfo *bp = GetBlockInfo(tbl, ub);
    // if this block is filled, move on to the next block in the useful linkedlist
    if (bp->GetRecordCount() == max_count) { // bp->GetRecordCount() means number of rows contained in this block
      ub = bp->GetNextBlockNum();
      continue;
    }
    content =
        bp->GetContentAddress() + bp->GetRecordCount() * tbl->record_length();  
        // bp->GetRecordCount() means number of rows contained in this block
        // tbl->record_length() is total number of bytes for all attributes
    for (vector<TKey>::iterator iter = tkey_values.begin();
         iter != tkey_values.end(); ++iter) {
      memcpy(content, iter->key(), iter->length());
      content += iter->length();
    }
    bp->SetRecordCount(1 + bp->GetRecordCount()); // set the number of rows to +1

    blocknum = ub;
    offset = bp->GetRecordCount() - 1;

    hdl_->WriteBlock(bp); // only setting bp to dirty

    // add record to index
    if (tbl->GetIndexNum() != 0) {
      BPlusTree tree(tbl->GetIndex(0), hdl_, cm_, db_name_);
      for (int i = 0; i < tbl->ats().size(); ++i) {
        if (tbl->GetIndex(0)->attr_name() == tbl->GetIndex(i)->attr_name()) {
          tree.Add(tkey_values[i], blocknum, offset);
          break;
        }
      }
    }

    hdl_->WriteToDisk();
    cm_->WriteArchiveFile();

    return;
  }

  // Suppose our original useful linkedlist (double-way) is 5 <-> 4 <-> 1 <-> 0 and our original rubbish linkedlist (single-way) is 2 -> 3
  // If all blocks in the useful linkedlist are filled then it does the following:

  // If there exists rubbish block, then take the first rubbish block, in our case is block number 2, and append it to the end of the useful linkedlist
  // So now our useful linkedlist (double-way) becomes 5 <-> 4 <-> 1 <-> 0 <-> 2 and our rubbish linkedlist (single-way) becomes 3
  // The new inserted row will be inserted into block number 2. 

  // Remember lastup is the last element of the original useful linkedlist, in our case, it is block number 0

  if (frb != -1) { // if there is rubbish block in the table
    BlockInfo *bp = GetBlockInfo(tbl, frb);
    content = bp->GetContentAddress();
    for (vector<TKey>::iterator iter = tkey_values.begin();
         iter != tkey_values.end(); ++iter) {
      memcpy(content, iter->key(), iter->length());
      content += iter->length();
    }
    bp->SetRecordCount(1);

    BlockInfo *lastubp = GetBlockInfo(tbl, lastub); // Remember lastup is the last element of the original useful linkedlist, in our case, it is block number 0
    lastubp->SetNextBlockNum(frb);

    tbl->set_first_rubbish_num(bp->GetNextBlockNum());

    bp->SetPrevBlockNum(lastub);
    bp->SetNextBlockNum(-1);

    blocknum = frb;
    offset = 0;

    hdl_->WriteBlock(bp); // set bp as dirty
    hdl_->WriteBlock(lastubp); // set lastubp as dirty

  } 

  // Suppose our original useful linkedlist (double-way) is 5 <-> 4 <-> 1 <-> 0 <-> 2 and our original rubbish linkedlist (single-way) is 3

  // If on the other hand there are no rubbish blocks, but since you still need to insert a new row
  // So what you will do is add a new useful block to the front of the useful linkedlist
  // So now the useful linkedlist (double-way) becomes 6 <-> 5 <-> 4 <-> 1 <-> 0 <-> 2 with the new row added into block number 6.

  else { // there is no rubbish block in the table
    int next_block = tbl->first_block_num(); // get the head of the original useful linkedlist, in our case is block number 5
    // If the useful linkedlist is not empty, i.e. if the head of the useful linkedlist is not -1
    if (tbl->first_block_num() != -1) {
      BlockInfo *upbp = GetBlockInfo(tbl, tbl->first_block_num());
      upbp->SetPrevBlockNum(tbl->block_count()); // preparing to add a new block (block number 6) at the front of the useful linkedlist
      hdl_->WriteBlock(upbp); // set upbp to dirty, since you have changed the value of byte index 0-3 in this block
    }
    tbl->set_first_block_num(tbl->block_count()); // setting the head of the useful linkedlist (double-way) to be block number 6
    BlockInfo* bp = GetBlockInfo(tbl, tbl->first_block_num());

    bp->SetPrevBlockNum(-1);
    bp->SetNextBlockNum(next_block);
    bp->SetRecordCount(1);

    content = bp->GetContentAddress();
    for (vector<TKey>::iterator iter = tkey_values.begin();
         iter != tkey_values.end(); ++iter) {
      memcpy(content, iter->key(), iter->length());
      content += iter->length();
    }

    blocknum = tbl->block_count();
    offset = 0;

    hdl_->WriteBlock(bp); // set bp to dirty

    tbl->IncreaseBlockCount();
  }

  // add record to index
  if (tbl->GetIndexNum() != 0) {
    BPlusTree tree(tbl->GetIndex(0), hdl_, cm_, db_name_);
    for (int i = 0; i < tbl->ats().size(); ++i) {
      if (tbl->GetIndex(0)->attr_name() == tbl->GetIndex(i)->name()) {
        tree.Add(tkey_values[i], blocknum, offset);
        break;
      }
    }
  }
  cm_->WriteArchiveFile();
  hdl_->WriteToDisk();
}

void RecordManager::Select(SQLSelect &st) {

  Table *tbl = cm_->GetDB(db_name_)->GetTable(st.tb_name());

  for (int i = 0; i < tbl->GetAttributeNum(); ++i) {
    cout << setw(9) << left << tbl->ats()[i].attr_name();
  }
  cout << endl;

  vector<vector<TKey> > tkey_values;

  bool has_index = false;
  int index_idx;
  int where_idx;

  if (tbl->GetIndexNum() != 0) {
    for (int i = 0; i < tbl->GetIndexNum(); ++i) {
      Index *idx = tbl->GetIndex(i);
      for (int j = 0; j < st.wheres().size(); ++j) {
        if (idx->attr_name() == st.wheres()[j].key) {
          if (st.wheres()[j].sign_type == SIGN_EQ) {
            has_index = true;
            index_idx = i;
            where_idx = j;
          }
        }
      }
    }
  }

  // if no index
  if (!has_index) {
    int block_num = tbl->first_block_num();
    for (int i = 0; i < tbl->block_count(); ++i) {
      BlockInfo *bp = GetBlockInfo(tbl, block_num);

      for (int j = 0; j < bp->GetRecordCount(); ++j) {
        vector<TKey> tkey_value = GetRecord(tbl, block_num, j);

        bool sats = true;

        for (int k = 0; k < st.wheres().size(); ++k) {
          SQLWhere where = st.wheres()[k];
          if (!SatisfyWhere(tbl, tkey_value, where)) {
            sats = false;
          }
        }
        if (sats) {
          tkey_values.push_back(tkey_value);
        }
      }

      block_num = bp->GetNextBlockNum();
    }
  } 
  else { // if has index
    BPlusTree tree(tbl->GetIndex(index_idx), hdl_, cm_, db_name_);

    // build TKey for search
    int type = tbl->GetIndex(index_idx)->key_type();
    int length = tbl->GetIndex(index_idx)->key_len();
    std::string value = st.wheres()[where_idx].value;
    TKey dest_key(type, length);
    dest_key.ReadValue(value);

    int blocknum = tree.GetVal(dest_key);

    if (blocknum != -1) {
      int blockoffset = blocknum;
      blocknum = blocknum >> 16;
      blocknum = blocknum & 0xffff;
      blockoffset = blockoffset & 0xffff;
      vector<TKey> tkey_value = GetRecord(tbl, blocknum, blockoffset);
      bool sats = true;

      for (int k = 0; k < st.wheres().size(); ++k) {
        SQLWhere where = st.wheres()[k];
        if (!SatisfyWhere(tbl, tkey_value, where)) {
          sats = false;
        }
      }
      if (sats) {
        tkey_values.push_back(tkey_value);
      }
    }
  }

  for (int i = 0; i < tkey_values.size(); ++i) {
    for (int j = 0; j < tkey_values[i].size(); ++j) {
      cout << setw(9) << left << tkey_values[i][j];
    }
    cout << endl;
  }
  if (tbl->GetIndexNum() != 0) {
    BPlusTree tree(tbl->GetIndex(0), hdl_, cm_, db_name_);
    tree.Print();
  }
}

void RecordManager::Delete(SQLDelete &st) {

  Table *tbl = cm_->GetDB(db_name_)->GetTable(st.tb_name());

  bool has_index = false;
  int index_idx;
  int where_idx;

  if (tbl->GetIndexNum() != 0) {
    for (int i = 0; i < tbl->GetIndexNum(); ++i) {
      Index *idx = tbl->GetIndex(i);
      for (int j = 0; j < st.wheres().size(); ++j) {
        if (idx->attr_name() == st.wheres()[j].key) {
          index_idx = i;
          if (st.wheres()[j].sign_type == SIGN_EQ) {
            has_index = true;
            where_idx = j;
          }
        }
      }
    }
  }

  // if no index
  if (!has_index) {
    int block_num = tbl->first_block_num();
    for (int i = 0; i < tbl->block_count(); ++i) {
      BlockInfo *bp = GetBlockInfo(tbl, block_num);
      int count = bp->GetRecordCount();
      for (int j = 0; j < count; ++j) {
        vector<TKey> tkey_value = GetRecord(tbl, block_num, j);

        bool sats = true;

        for (int k = 0; k < st.wheres().size(); ++k) {
          SQLWhere where = st.wheres()[k];
          if (!SatisfyWhere(tbl, tkey_value, where)) {
            sats = false;
          }
        }
        if (sats) {
          DeleteRecord(tbl, block_num, j);
          if (tbl->GetIndexNum() != 0) {
            BPlusTree tree(tbl->GetIndex(index_idx), hdl_, cm_, db_name_);

            int idx = -1;
            for (int i = 0; i < tbl->GetAttributeNum(); ++i) {
              if (tbl->ats()[i].attr_name() ==
                  tbl->GetIndex(index_idx)->attr_name()) {
                idx = i;
              }
            }

            tree.Remove(tkey_value[idx]);
          }
        }
      }

      block_num = bp->GetNextBlockNum();
    }
  } 
  else { // if has index
    BPlusTree tree(tbl->GetIndex(index_idx), hdl_, cm_, db_name_);

    // build TKey for search
    int type = tbl->GetIndex(index_idx)->key_type();
    int length = tbl->GetIndex(index_idx)->key_len();
    std::string value = st.wheres()[where_idx].value;
    TKey dest_key(type, length);
    dest_key.ReadValue(value);

    int blocknum = tree.GetVal(dest_key);

    if (blocknum != -1) {
      int blockoffset = blocknum;
      blocknum = blocknum >> 16;
      blocknum = blocknum & 0xffff;
      blockoffset = blockoffset & 0xffff;
      vector<TKey> tkey_value = GetRecord(tbl, blocknum, blockoffset);
      bool sats = true;

      for (int k = 0; k < st.wheres().size(); ++k) {
        SQLWhere where = st.wheres()[k];
        if (!SatisfyWhere(tbl, tkey_value, where)) {
          sats = false;
        }
      }
      if (sats) {
        DeleteRecord(tbl, blocknum, blockoffset);
        tree.Remove(dest_key);
      }
    }
  }

  hdl_->WriteToDisk();
}

void RecordManager::Update(SQLUpdate &st) {
  Table *tbl = cm_->GetDB(db_name_)->GetTable(st.tb_name());

  vector<int> indices;
  vector<TKey> values;
  int pk_index = -1;
  int affect_index = -1;

  for (int i = 0; i < tbl->ats().size(); ++i) {
    if (tbl->ats()[i].attr_type() == 1) {
      pk_index = i;
    }
  }

  for (int i = 0; i < st.keyvalues().size(); ++i) {
    int index = tbl->GetAttributeIndex(st.keyvalues()[i].key);
    indices.push_back(index);
    TKey value(tbl->ats()[index].data_type(), tbl->ats()[index].length());
    value.ReadValue(st.keyvalues()[i].value);
    values.push_back(value);

    if (index == pk_index) {
      affect_index = i;
    }
  }

  if (affect_index != -1) {
    if (tbl->GetIndexNum() != 0) {

      BPlusTree tree(tbl->GetIndex(0), hdl_, cm_, db_name_);

      int value = tree.GetVal(values[affect_index]);
      if (value != -1) {
        throw PrimaryKeyConflictException();
      }
    } else {
      int block_num = tbl->first_block_num();
      for (int i = 0; i < tbl->block_count(); ++i) {
        BlockInfo *bp = GetBlockInfo(tbl, block_num);

        for (int j = 0; j < bp->GetRecordCount(); ++j) {
          vector<TKey> tkey_value = GetRecord(tbl, block_num, j);

          if (tkey_value[pk_index] == values[affect_index]) {
            throw PrimaryKeyConflictException();
          }
        }

        block_num = bp->GetNextBlockNum();
      }
    }
  }

  int block_num = tbl->first_block_num();
  for (int i = 0; i < tbl->block_count(); ++i) {
    BlockInfo *bp = GetBlockInfo(tbl, block_num);

    for (int j = 0; j < bp->GetRecordCount(); ++j) {
      vector<TKey> tkey_value = GetRecord(tbl, block_num, j);

      bool sats = true;

      for (int k = 0; k < st.wheres().size(); ++k) {
        SQLWhere where = st.wheres()[k];
        if (!SatisfyWhere(tbl, tkey_value, where)) {
          sats = false;
        }
      }
      if (sats) {
        if (tbl->GetIndexNum() != 0) {
          BPlusTree tree(tbl->GetIndex(0), hdl_, cm_, db_name_);

          int idx = -1;
          for (int i = 0; i < tbl->GetAttributeNum(); ++i) {
            if (tbl->ats()[i].attr_name() == tbl->GetIndex(0)->attr_name()) {
              idx = i;
            }
          }

          tree.Remove(tkey_value[idx]);
        }

        UpdateRecord(tbl, block_num, j, indices, values);

        tkey_value = GetRecord(tbl, block_num, j);

        if (tbl->GetIndexNum() != 0) {
          BPlusTree tree(tbl->GetIndex(0), hdl_, cm_, db_name_);

          int idx = -1;
          for (int i = 0; i < tbl->GetAttributeNum(); ++i) {
            if (tbl->ats()[i].attr_name() == tbl->GetIndex(0)->attr_name()) {
              idx = i;
            }
          }

          tree.Add(tkey_value[idx], block_num, j);
        }
      }
    }

    block_num = bp->GetNextBlockNum();
  }

  hdl_->WriteToDisk();
}

std::vector<TKey> RecordManager::GetRecord(Table *tbl, int block_num,
                                           int offset) {
  vector<TKey> keys;
  BlockInfo *bp = GetBlockInfo(tbl, block_num);

  char *content = bp->data() + offset * tbl->record_length() + 12;

  for (int i = 0; i < tbl->GetAttributeNum(); ++i) {
    int value_type = tbl->ats()[i].data_type();
    int length = tbl->ats()[i].length();

    TKey tmp(value_type, length);

    memcpy(tmp.key(), content, length);

    keys.push_back(tmp);

    content += length;
  }

  return keys;
}

void RecordManager::DeleteRecord(Table *tbl, int block_num, int offset) {
  BlockInfo *bp = GetBlockInfo(tbl, block_num);

  char *content = bp->data() + offset * tbl->record_length() + 12;
  char *replace =
      bp->data() + (bp->GetRecordCount() - 1) * tbl->record_length() + 12;
  memcpy(content, replace, tbl->record_length());

  bp->DecreaseRecordCount();

  if (bp->GetRecordCount() == 0) { // add the block to rubbish block chain

    int prevnum = bp->GetPrevBlockNum();
    int nextnum = bp->GetNextBlockNum();

    if (prevnum != -1) {
      BlockInfo *pbp = GetBlockInfo(tbl, prevnum);
      pbp->SetNextBlockNum(nextnum);
      hdl_->WriteBlock(pbp);
    }

    if (nextnum != -1) {
      BlockInfo *nbp = GetBlockInfo(tbl, nextnum);
      nbp->SetPrevBlockNum(prevnum);
      hdl_->WriteBlock(nbp);
    }

    BlockInfo *firstrubbish = GetBlockInfo(tbl, tbl->first_rubbish_num());
    bp->SetNextBlockNum(-1);
    bp->SetPrevBlockNum(-1);
    if (firstrubbish != NULL) {
      firstrubbish->SetPrevBlockNum(block_num);
      bp->SetNextBlockNum(firstrubbish->block_num());
    }
    tbl->set_first_rubbish_num(block_num);
  }

  hdl_->WriteBlock(bp);
}

void RecordManager::UpdateRecord(Table *tbl, int block_num, int offset,
                                 std::vector<int> &indices,
                                 std::vector<TKey> &values) {

  BlockInfo *bp = GetBlockInfo(tbl, block_num);

  char *content = bp->data() + offset * tbl->record_length() + 12;

  for (int i = 0; i < tbl->GetAttributeNum(); ++i) {
    vector<int>::iterator iter = find(indices.begin(), indices.end(), i);
    if (iter != indices.end()) {
      memcpy(content, values[iter - indices.begin()].key(),
             values[iter - indices.begin()].length());
    }

    content += tbl->ats()[i].length();
  }

  hdl_->WriteBlock(bp);
}

bool RecordManager::SatisfyWhere(Table *tbl, std::vector<TKey> keys,
                                 SQLWhere where) {
  int idx = -1;
  for (int i = 0; i < tbl->GetAttributeNum(); ++i) {
    if (tbl->ats()[i].attr_name() == where.key) {
      idx = i;
    }
  }
  // cout << idx;

  TKey tmp(tbl->ats()[idx].data_type(), tbl->ats()[idx].length());
  tmp.ReadValue(where.value.c_str());
  switch (where.sign_type) {
  case SIGN_EQ:
    return keys[idx] == tmp;
    break;
  case SIGN_NE:
    return keys[idx] != tmp;
    break;
  case SIGN_LT:
    return keys[idx] < tmp;
    break;
  case SIGN_GT:
    return keys[idx] > tmp;
    break;
  case SIGN_LE:
    return keys[idx] <= tmp;
    break;
  case SIGN_GE:
    return keys[idx] >= tmp;
    break;
  default:
    return false;
    break;
  }
}
