#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "buffer.h"
#include "parsing.h"

#define DB_FILENAME "teste.db"

typedef enum { EXECUTE_TABLE_FULL, EXECUTE_SUCESS } ExecuteResult;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
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
  int file_descriptor;
  size_t file_lenght;
  void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
  u_int32_t num_rows;
  Pager *pager;
} Table;

typedef struct {
  Table *table;
  u_int32_t row_num;
  bool end_of_table;
} Cursor;

void print_prompt() { printf("db > "); }
Table *db_open(const char *);
ExecuteResult execute_statement(Statement *statement, Table *table);
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table);

int main() {
  InputBuffer *input_buffer = new_input_buffer();
  Table *table = db_open(DB_FILENAME);

  while (true) {
    print_prompt();
    read_input(input_buffer);

    // META_COMMAND INTERPRETATION AND EXECUTION
    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer, table)) {
      case (META_COMMAND_SUCESS):
        continue;
      case (META_COMMAND_UNRECOGNIZED_COMMAND):
        printf("Unrecognized command '%s' \n", input_buffer->buffer);
        continue;
      }
    }

    // STATEMENT INTERPRETATION
    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
    case (PREPARE_SUCESS):
      break;
    case (PREPARE_UNRECOGNIZED_STATEMENT):
      printf("unrecognized keyword at start of '%s' \n", input_buffer->buffer);
      continue;
    case (PREPARE_SYNTAX_ERROR):
      continue;
    case (PREPARE_STRING_TOO_LONG):
      printf("String is too long, value not accepted\n");
      continue;
    case (PREPARE_INVALID_ID):
      printf("Invalid id, the value should be positive and belo INT_MAX\n");
      continue;
    }

    // STATEMENT EXECUTION
    switch (execute_statement(&statement, table)) {
    case EXECUTE_TABLE_FULL:
      printf("could not execute command, page is full");
      break;
    case EXECUTE_SUCESS:
      printf("Executed. \n");
      break;
    default:
      printf("unknown error at execution");
    }
  }
}

// MEMORY AND FILE MANAGMENT
Pager *pager_open(const char *filename) {
  int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  if (fd == -1) {
    printf("pager_open: could not open file '%s'.", filename);
    exit(EXIT_FAILURE);
  }

  Pager *pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_lenght = lseek(fd, 0, SEEK_END);
  for (int i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }

  return pager;
}

void *get_page(Pager *pager, u_int32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("pager_get_page: page out of bounds.\n");
    exit(EXIT_FAILURE);
  }
  if (pager->pages[page_num] == NULL) { // if page not in cache
    void *page = malloc(PAGE_SIZE);
    u_int32_t num_pages = pager->file_lenght / PAGE_SIZE;

    // if there's a partial page at the end of the file
    if (pager->file_lenght % PAGE_SIZE != 0) {
      num_pages += 1;
    }

    if (page_num < num_pages) { // if page exists in file
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("pager_get_page: could not read page %d from file\n", page_num);
        exit(EXIT_FAILURE);
      }
    }
    pager->pages[page_num] = page;
  }

  return pager->pages[page_num];
}

void pager_flush(Pager *pager, u_int32_t page_num, u_int32_t size) {
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written =
      write(pager->file_descriptor, pager->pages[page_num], size);

  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

Table *db_open(const char *filename) {
  Table *table = malloc(sizeof(Table));
  Pager *pager = pager_open(filename);

  table->pager = pager;
  table->num_rows = pager->file_lenght / ROW_SIZE;
  return table;
}

void db_close(Table *table) {
  Pager *pager = table->pager;

  // flush the pages
  int num_full_pages = table->num_rows / ROWS_PER_PAGE;
  for (int i = 0; i < num_full_pages; i++) {
    if (pager->pages[i] == NULL)
      continue;
    pager_flush(pager, i, PAGE_SIZE);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  // flush incomplete page, if exists
  if (table->num_rows % ROWS_PER_PAGE != 0) {
    int last_page = num_full_pages;
    int last_page_size = (table->num_rows % ROWS_PER_PAGE) * ROW_SIZE;
    pager_flush(pager, last_page, last_page_size);
    free(pager->pages[last_page]);
    pager->pages[last_page] = NULL;
  }

  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("db_close: could not close file\n");
    exit(EXIT_FAILURE);
  }

  for (int i = 0; i < TABLE_MAX_PAGES; i++) {
    void *page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }

  free(pager);
  free(table);
}

void serialize_row(Row *source, void *destination) {
  memcpy(destination + ID_OFFSET, &source->id, ID_SIZE);
  strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
  strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

// CURSOR
Cursor *cursor_start_of_table(Table *table) {
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->end_of_table = false;
  cursor->table = table;
  cursor->row_num = 0;
  return cursor;
}

Cursor *cursor_end_of_table(Table *table) {
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->end_of_table = true;
  cursor->table = table;
  cursor->row_num = table->num_rows;
  return cursor;
}

void cursor_move(Cursor *cursor, int delta) {
  int new_row = cursor->row_num + delta;
  if (new_row < 0) {
    printf("move_cursor: tried to move cursor to negative position\n");
    exit(EXIT_FAILURE);
  }

  if (new_row > cursor->table->num_rows) {
    printf("move_cursor: tried to move cursor past table size\n");
    exit(EXIT_FAILURE);
  }

  if (new_row > TABLE_MAX_ROWS) {
    printf("move_cursor: tried to move cursor beyond table_max_rows\n");
    exit(EXIT_FAILURE);
  }

  if (new_row == cursor->table->num_rows)
    cursor->end_of_table = true;
  cursor->row_num = new_row;
}

void cursor_advance(Cursor *cursor) { cursor_move(cursor, 1); }

void cursor_go_back(Cursor *cursor) { cursor_move(cursor, -1); }

void *cursor_value(Cursor *cursor) {
  u_int32_t row_num = cursor->row_num;
  u_int32_t page_num = row_num / ROWS_PER_PAGE;
  void *page = get_page(cursor->table->pager, page_num);

  u_int32_t relative_row = row_num % ROWS_PER_PAGE;
  u_int32_t byte_offset = relative_row * ROW_SIZE;
  return page + byte_offset;
}

// META-COMMANDS
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    db_close(table);
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

// STATEMENT EXECUTIONS
ExecuteResult execute_insert(Statement *statement, Table *table) {
  if (table->num_rows > TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }
  Row *row_to_insert = &(statement->row_to_insert);
  Cursor *cursor = cursor_end_of_table(table);

  serialize_row(row_to_insert, cursor_value(cursor));
  table->num_rows += 1;

  free(cursor);
  return EXECUTE_SUCESS;
}
void print_row(Row *row) {
  printf("id: %d | username: %s | email: %s\n", row->id, row->username,
         row->email);
}

// TODO: ADD SELECTION OPTIONS
ExecuteResult execute_select(Table *table) {
  Row row;
  Cursor *cursor = cursor_start_of_table(table);
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);
    cursor_advance(cursor);
    print_row(&row);
  }

  free(cursor);
  return EXECUTE_SUCESS;
}

// TODO: ADD ARGUMENT TYPE VALIDATION
ExecuteResult execute_statement(Statement *statement, Table *table) {
  switch (statement->type) {
  case STATEMENT_INSERT:
    return execute_insert(statement, table);
  case STATEMENT_SELECT:
    return execute_select(table);
  }
  printf("Execute_Statement: unkown error\n");
}
