#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "buffer.h"
#include "parsing.h"

#define size_of_attribute(Struct, Attribute) sizeof( ((Struct*)0)->Attribute )
const u_int32_t ID_SIZE = size_of_attribute(Row, id);
const u_int32_t USERNAME_SIZE = size_of_attribute(Row, username);
const u_int32_t EMAIL_SIZE = size_of_attribute(Row, email);
const u_int32_t ID_OFFSET = 0;
const u_int32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const u_int32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const u_int32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;


const u_int32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const u_int32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const u_int32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
  u_int32_t num_rows;
  void *pages[TABLE_MAX_PAGES];
} Table;

void print_prompt() { printf("db > "); }
void deserialize_row(void *source, Row *destination);
void *row_slot(Table *table, u_int32_t row_num);

void serialize_row(Row *source, void *destination);

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
    case (PREPARE_SYNTAX_ERROR):
      printf("%s", statement.error_message);
      continue;
    }

    execute_statement(&statement);
    printf("Executed. \n");
  }
}

void serialize_row(Row *source, void *destination) {
  memcpy(destination + ID_OFFSET, &source->id, ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void *row_slot(Table *table, u_int32_t row_num) {
  u_int32_t page_num = row_num / ROWS_PER_PAGE;
  void *page = table->pages[page_num];

  if (page == NULL) {
    // Allocate memory only when we try to acess page
    page = table->pages[page_num] = malloc(PAGE_SIZE);
  }

  u_int32_t row_offset = row_num % ROWS_PER_PAGE;
  u_int32_t byte_offset = row_offset * ROW_SIZE;
  return page + byte_offset;
}



