#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer.h"

typedef enum {STATEMENT_INSERT, STATEMENT_SELECT} StatementType ;
typedef struct {
  StatementType type;
} Statement;

typedef enum {
  META_COMMAND_SUCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;


typedef enum {
  PREPARE_SUCESS,
  PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

void print_prompt() { printf("db > "); }

MetaCommandResult do_meta_command(InputBuffer* );
PrepareResult prepare_statement(InputBuffer* , Statement*);
void execute_statement(Statement*);

int main() {
  InputBuffer *input_buffer = new_input_buffer();
  while (true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer)) {
        case (META_COMMAND_SUCESS):
          continue;
        case (META_COMMAND_UNRECOGNIZED_COMMAND):
          printf("Unrecognized command '%s' \n", input_buffer->buffer);
          continue;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
      case (PREPARE_SUCESS):
        break;
      case (PREPARE_UNRECOGNIZED_STATEMENT):
        printf("unrecognized keyword at start of '%s' \n", input_buffer->buffer);
        continue;
    }

    execute_statement(&statement);
    printf("Executed. \n");

  }
}


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

