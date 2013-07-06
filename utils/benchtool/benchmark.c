#include <time.h>
#include <stdio.h>
#include "sqlite3.h"
#include <stdlib.h>
#include <ctype.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
// Standard callback

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
  int i;
  for(i=0; i<argc; i++){
    printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("\n");
  return 0;
}
uint64_t GetTimeStamp() {
  struct timeval tv;
  gettimeofday(&tv,NULL);
  return tv.tv_sec*(uint64_t)1000000+tv.tv_usec;
}


int main(int argc, char **argv ){
  sqlite3 *db;
  char *zErrMsg = 0, *num_ops  = NULL;
  char cmd[1024], value[] = "value", new_value[] = "new_value";
  int rc, i, c, insert = 0, update = 0, select = 0, ops=0;
  uint64_t tcurr, tprev;


// Handling options
  while ((c = getopt (argc, argv, "iusn:")) != -1) {
      switch (c) {
          case 'i':
            insert = 1;
            break;
          case 'u':
            update = 1;
            break;
          case 's':
            select = 1;
            break;
          case 'n':
            num_ops = optarg;
            ops = atoi(num_ops);
            break;
          case '?':
            if (optopt == 'n') {
                fprintf (stderr, "Option -%c requires an argument.\n", optopt);
                printf ("Usage: %c -i<inserts/optional> -u<updates optional> -s<selects optional> -n <number_of_ops/required>",argv[0]);
            }
            else if (isprint (optopt)) {
                fprintf (stderr, "Unknown option `-%c'.\n", optopt);
                printf ("Usage: %c -i<inserts/optional> -u<updates optional> -s<selects optional> -n <number_of_ops/required>",argv[0]);
            }
            else {
                fprintf (stderr, "Unknown option character `\\x%x'.\n", optopt);
                printf ("Usage: %c -i<inserts/optional> -u<updates optional> -s<selects optional> -n <number_of_ops/required>",argv[0]);
           }
            return 1;
          default:
            abort();
      }
  }

 for (i = optind; i < argc; i++) {
     printf ("Non-option argument %s\n", argv[i]);
     printf ("Usage: %c -i<inserts/optional> -u<updates optional> -s<selects optional> -n <number_of_ops/required>",argv[0]);
     return 1;
 }


 rc = sqlite3_open("/tmp/db_test", &db);
  if( rc ){
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    exit(1);
  }

//defining sql execute

  #define RUN(cmd) \
    { \
      rc = sqlite3_exec(db, cmd, callback, 0, &zErrMsg); \
      if( rc!=SQLITE_OK ){ \
        fprintf(stderr, "SQL error on %d: %s\n", i, zErrMsg); \
        sqlite3_free(zErrMsg); \
        exit(1); \
      } \
    }

// Counter
  #define TIME(msg) \
    { \
      tcurr = GetTimeStamp(); \
      printf(msg " : took microsecs %"PRIu64" actual \n", (tcurr - tprev)); \
      tprev = tcurr; \
    }


  tprev = GetTimeStamp();
  TIME("'startup'");
  RUN("CREATE TABLE kv(k INTEGER,  v VARCHAR(100));");
  TIME("table creation");

//Insertions

  if(insert) {
  RUN("BEGIN;");
  for (i = 0; i < ops; i++) {
    sprintf(cmd, "INSERT INTO kv VALUES(%d,'%s');",i,value);
    RUN(cmd);
  }
  printf("%d",ops);
  TIME(" inserts");

  RUN("COMMIT;");
  TIME("commit");

  }

// Updates


  if(update) {
  RUN("BEGIN;");
  for (i = 0; i < ops; i++) {
    sprintf(cmd, "UPDATE kv SET v = '%s' where k=%d;",new_value, i);
    RUN(cmd);
  }
  printf("%d",ops);
  TIME(" updates");

  RUN("COMMIT;");
  TIME("commit");

  }


//Selects

  if(select) {
  for (i = 0; i < ops; i++) {
    sprintf(cmd, "SELECT count(*) FROM kv WHERE k == %d", i);
    RUN(cmd);
  }
  printf("%d",ops);
  TIME(" reads");

  }

// Counts

  sqlite3_close(db);
  return 0;

}

