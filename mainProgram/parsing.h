#include "buffer.h"

#ifndef PARSING_H
#define PARSING_H

// WARN: tabela dummy temporaria. remover no futuro
#define USERNAME_MAX_CHARACTERS 32
#define EMAIL_MAX_CHARACTERS 100
typedef struct {
  unsigned int id;
  char username[USERNAME_MAX_CHARACTERS + 1];
  char email[EMAIL_MAX_CHARACTERS + 1];
} Row;



typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;
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
  PREPARE_SYNTAX_ERROR,
  PREPARE_STRING_TOO_LONG,
  PREPARE_INVALID_ID
} PrepareResult;

PrepareResult prepare_statement(InputBuffer *, Statement *);
#endif
