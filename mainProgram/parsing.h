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


MetaCommandResult do_meta_command(InputBuffer* );
PrepareResult prepare_statement(InputBuffer* , Statement*);
void execute_statement(Statement*);

