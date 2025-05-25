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

#define DB_FILENAME "teste_Btree.db"

typedef enum { EXECUTE_TABLE_FULL, EXECUTE_SUCESS } ExecuteResult;
typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

// ROW CONSTANTS
// TODO: change dummy row to generic row
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
const u_int32_t ID_SIZE = size_of_attribute(Row, id);
const u_int32_t USERNAME_SIZE = size_of_attribute(Row, username);
const u_int32_t EMAIL_SIZE = size_of_attribute(Row, email);
const u_int32_t ID_OFFSET = 0;
const u_int32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const u_int32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const u_int32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

// PAGE AND TABLE CONSTANTS
const u_int32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100

//// NODE CONSTANTS FOR SERIALITAION ////
// common node header
const u_int32_t NODE_TYPE_SIZE = sizeof(u_int8_t);
const u_int32_t NODE_TYPE_OFFSET = 0;
const u_int32_t IS_ROOT_SIZE = sizeof(u_int8_t);
const u_int32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const u_int32_t PARENT_POINTER_SIZE = sizeof(u_int32_t);
const u_int32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const u_int32_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// leaf node header (at start of each page)
const u_int32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(u_int32_t);
const u_int32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const u_int32_t LEAF_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// leaf node body
const u_int32_t LEAF_NODE_KEY_SIZE = sizeof(u_int32_t);
const u_int32_t LEAF_NODE_KEY_OFFSET = 0;
const u_int32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const u_int32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_OFFSET;

// leaf node cells
const u_int32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const u_int32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_CELL_SIZE;
const u_int32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

typedef struct {
  int file_descriptor;
  size_t file_lenght;
  void *pages[TABLE_MAX_PAGES];
  u_int32_t num_pages;
} Pager;

typedef struct {
  u_int32_t num_rows;
  Pager *pager;
  u_int32_t root_page_num;
} Table;

typedef struct {
  Table *table;
  u_int32_t page_num;
  u_int32_t cell_num;
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
  pager->num_pages = pager->file_lenght / PAGE_SIZE;

  if (pager->file_lenght % PAGE_SIZE != 0) {
    printf("pager_open: DB has not a whole number of pages, file %s is "
           "corrupted\n",
           filename);
    exit(EXIT_FAILURE);
  }

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

    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }
  }

  return pager->pages[page_num];
}

void pager_flush(Pager *pager, u_int32_t page_num) {
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
      write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

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

// TODO: check for errors due to new file format
void db_close(Table *table) {
  Pager *pager = table->pager;

  // flush the pages
  for (int i = 0; i < pager->num_pages; i++) {
    if (pager->pages[i] == NULL)
      continue;
    pager_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
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

/* SERIALIZATION
 * take a pointer to the values of a row and turn its data into a row,
 * or vice-versa
 */
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

/*  NODES
 *  each page inside the pager struct is considered a node.
 *  each node has the following data:
 *  | Header (18 bytes): u8 node_type, u8 is_root, u32 parent_pointer,
 *  | u32 num_cells.
 *
 *  | Cells (142 bytes each): u32 key, Row value.
 *
 *  WARN: TO CHANGE
 *  the current dummy row consists of u32 id, char[33] username, char[101] email.
 */

u_int32_t *leaf_node_num_cells(void *node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void *leaf_node_cell(void *node, u_int32_t n_cell) {
  return node + LEAF_NODE_HEADER_SIZE + n_cell * LEAF_NODE_CELL_SIZE;
}

u_int32_t *leaf_node_key(void *node, u_int32_t n_cell) {
  return leaf_node_cell(node, n_cell);
}

void *leaf_node_value(void *node, u_int32_t n_cell) {
  return leaf_node_cell(node, n_cell) + LEAF_NODE_KEY_SIZE;
}

void initalize_leaf_node(void *node) { *(leaf_node_num_cells(node)) = 0; }

// CURSOR
// TODO: ADAPT CURSOR TO BTREE NAVIGATION
Cursor *cursor_start_of_table(Table *table) {
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = table->root_page_num;
  cursor->cell_num = 0;

  void *root_page = get_page(table->pager, table->root_page_num);
  u_int32_t num_cells = *(leaf_node_num_cells(root_page));
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

Cursor *cursor_end_of_table(Table *table) {
  Cursor *cursor = malloc(sizeof(Cursor));

  cursor->table = table;
  cursor->page_num = table->pager->num_pages - 1;
  cursor->end_of_table = true;

  void *last_page = get_page(table->pager, cursor->page_num);
  int last_cell_num = *(leaf_node_num_cells(last_page)) - 1;
  cursor->cell_num = last_cell_num;

  return cursor;
}

// TODO: deal with negative delta
// WARN: make sure this function deals with all the cases
void cursor_move(Cursor *cursor, int d_cells) {
  void *curp = get_page(cursor->table->pager, cursor->page_num);
  u_int32_t pcells = *(leaf_node_num_cells(curp));

  // while the wanted cell ins't in the current page;
  while (pcells - (cursor->cell_num + 1) < d_cells) {
    d_cells = d_cells % (pcells - (cursor->cell_num + 1));
    cursor->page_num += 1;
    cursor->cell_num = 0;

    // TODO: find a better way to deal with this case
    if (cursor->page_num >= cursor->table->pager->num_pages) {
      printf("cursor_move: reached unexisting page %d\n", cursor->page_num);
    }
    curp = get_page(cursor->table->pager, cursor->page_num);
    pcells = *(leaf_node_num_cells(curp));
  }

  cursor->cell_num += d_cells;
}

void cursor_advance(Cursor *cursor) {
  cursor->cell_num += 1;

  u_int32_t pcells =
      *leaf_node_num_cells(get_page(cursor->table->pager, cursor->page_num));
  if (cursor->cell_num >= pcells) {
    cursor->cell_num = 0;

    cursor->page_num += 1;
    if (cursor->page_num >= cursor->table->pager->num_pages) {
      printf("cursor_advance: reached unexisting page\n");
      cursor->end_of_table = true;
    }
  }
}

void *cursor_value(Cursor *cursor) {
  void *page = get_page(cursor->table->pager, cursor->page_num);
  return leaf_node_value(page, cursor->cell_num);
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
// TODO: change to new cursor
ExecuteResult execute_insert(Statement *statement, Table *table) {
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

// TODO: SALVAR ARQUIVO COM BTREE
// TODO: LER ARQUIVO COM BTREE
// TODO: MIGRAR OS DADOS EM TESTE.DB PARA BTREE
// TODO:
