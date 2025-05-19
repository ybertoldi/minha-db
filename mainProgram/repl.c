#include <stdbool.h>
#include <stdio.h>

#include "buffer.h"
#include "parsing.h"

void print_prompt() { printf("db > "); }

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

