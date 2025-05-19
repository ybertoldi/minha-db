#include "buffer.h"


#ifndef PARSING_H
  #define PARSING_H
  //WARN: remover esta struct 
  typedef struct {
    unsigned int id;
    char username[32];
    char email[50];
  } Row ;

  typedef enum {STATEMENT_INSERT, STATEMENT_SELECT} StatementType ;
  typedef struct {
    StatementType type;
    Row row_to_insert;
    char *error_message;
  } Statement;

  typedef enum {
    META_COMMAND_SUCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
  } MetaCommandResult;


  typedef enum {
    PREPARE_SUCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR
  } PrepareResult;


  MetaCommandResult do_meta_command(InputBuffer* );
  PrepareResult prepare_statement(InputBuffer* , Statement*);
#endif
