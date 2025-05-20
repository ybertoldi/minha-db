#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer.h"
#include "parsing.h"

MetaCommandResult do_meta_command(InputBuffer *input_buffer) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement){
  statement->type = STATEMENT_INSERT;

  char *keyword = strtok(input_buffer->buffer, " ");
  char *id_string = strtok(NULL, " ");
  char *username = strtok(NULL, " ");
  char *email = strtok(NULL, " ");

  int id = atoi(id_string);
  if (id < 0) {
    return PREPARE_INVALID_ID;
  }
  if (id_string == NULL || username == NULL || email == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }
  if (strlen(username) > USERNAME_MAX_CHARACTERS) {
    return PREPARE_STRING_TOO_LONG;
  }
  if (strlen(email) > USERNAME_MAX_CHARACTERS) {
    return PREPARE_STRING_TOO_LONG;
  }

  statement->row_to_insert.id = id;
  strcpy(statement->row_to_insert.username, username);
  strcpy(statement->row_to_insert.email, email);

  return PREPARE_SUCESS;
}

PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement);
  }
  if (strncmp(input_buffer->buffer, "select", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

