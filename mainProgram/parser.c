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

PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    statement->type = STATEMENT_INSERT;
    int args_assigned = sscanf(
        input_buffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id),
        statement->row_to_insert.username, statement->row_to_insert.email);
    if (args_assigned < 3) {
      char *err_msg = malloc(input_buffer->input_length + 50);
      strcpy(err_msg, "invalid syntax for insertion at '");
      strcat(err_msg, input_buffer->buffer);
      strcat(err_msg, "'. 3 arguments are required.\n");
      statement->error_message = err_msg;
      return PREPARE_SYNTAX_ERROR;
    }
    return PREPARE_SUCESS;
  }
  if (strncmp(input_buffer->buffer, "select", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement *statement) {
  switch (statement->type) {
  case STATEMENT_INSERT:
    printf("TODO: inserting...\n");
    break;

  case STATEMENT_SELECT:
    printf("TODO: selecting...\n");
    break;
  }
  free(statement->error_message);
}
