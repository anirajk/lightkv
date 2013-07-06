#include <time.h>
#include <stdio.h>
#include "sqlite3.h"
#include <stdlib.h>
static int callback(void *NotUsed, int argc, char **argv, char **azColName){
  int i;
  for(i=0; i<argc; i++){
    printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("\n");
  return 0;
}

int main(){
  sqlite3 *db;
  char *zErrMsg = 0;
  int rc, i;
  clock_t t;

  rc = sqlite3_open("/tmp/db_test", &db);
  if( rc ){
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    exit(1);
  }

  #define RUN(cmd) \
    { \
      rc = sqlite3_exec(db, cmd, callback, 0, &zErrMsg); \
      if( rc!=SQLITE_OK ){ \
        fprintf(stderr, "SQL error on %d: %s\n", i, zErrMsg); \
        sqlite3_free(zErrMsg); \
        exit(1); \
      } \
    }

  #define TIME(msg) \
    { \
      printf(msg " : took %d ms\n", (1000*(clock()-t))/CLOCKS_PER_SEC); \
      t = clock(); \
    }

  t = clock();
  TIME("'startup'");

  RUN("CREATE TABLE kv(k INTEGER,  v VARCHAR(100));");
  TIME("create table");

  RUN("BEGIN;");

  // 25000 INSERTs in a transaction
  for (i = 0; i < 5000; i++) {
    RUN("INSERT INTO kv VALUES(1, 'one 1 one 1 one 1');");
    RUN("INSERT INTO kv VALUES(2, 'two two two two');");
    RUN("INSERT INTO kv VALUES(3, 'three three 33333333333 three');");
    RUN("INSERT INTO kv VALUES(4, 'FOUR four 4 phor FOUR 44444');");
    RUN("INSERT INTO kv VALUES(5, 'five 5 FIVE Five phayve 55 5 5 5 5 55  5');");
  }
  TIME("25,000 inserts");

  RUN("COMMIT;");
  TIME("commit");

  // Counts
  RUN("SELECT count(*) FROM kv;");
  RUN("SELECT count(*) FROM kv WHERE k == 4");
  RUN("SELECT count(*) FROM kv WHERE v like '%three%';");
  TIME("selects");

  sqlite3_close(db);

//  return test();
}

