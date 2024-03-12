#ifndef MINIDB_MINIDB_API_H_
#define MINIDB_MINIDB_API_H_

#include <string>

#include "buffer_manager.h"
#include "catalog_manager.h"
#include "sql_statement.h"

class MiniDBAPI {
private:
  std::string path_;
  CatalogManager *cm_;
  BufferManager *hdl_;
  std::string curr_db_;

public:
  MiniDBAPI(std::string p);
  ~MiniDBAPI();
  void Quit();  // Case 10
  void Help();  // Case 20
  void CreateDatabase(SQLCreateDatabase &st);  // Case 30
  void CreateTable(SQLCreateTable &st);  // Case 31
  void CreateIndex(SQLCreateIndex &st);  // Case 32
  void ShowDatabases();  // Case 40
  void ShowTables();  // Case 41
  void DropDatabase(SQLDropDatabase &st);  // Case 50
  void DropTable(SQLDropTable &st);  // Case 51
  void DropIndex(SQLDropIndex &st);  // Case 52
  void Use(SQLUse &st);  // Case 60
  void Insert(SQLInsert &st);  // Case 70
  void Select(SQLSelect &st);  // Case 90
  void Delete(SQLDelete &st);  // Case 100
  void Update(SQLUpdate &st);  // Case 110
};

#endif /* MINIDB_MINIDB_API_H_ */
