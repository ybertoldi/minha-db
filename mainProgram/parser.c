#include <string.h>
#include <stdio.h>

#include "buffer.h"
#include "parsing.h"

MetaCommandResult do_meta_command(InputBuffer* input_buffer ){
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_statement(InputBuffer* input_buffer , Statement* statement){
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    statement->type = STATEMENT_INSERT;
    return PREPARE_SUCESS;
  }
  if (strcmp(input_buffer->buffer, "select") == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}


void execute_statement(Statement* statement){
  switch (statement->type) {
    case STATEMENT_INSERT:
      printf("TODO: inserting...\n");
      break;

    case STATEMENT_SELECT:
      printf("TODO: selecting...\n");
      break;
  }
}

