#include <stdlib.h>

#ifndef BUFFER_H

#define BUFFER_H
typedef struct{
	char* buffer;
	size_t buffer_length;
	ssize_t input_length;
}InputBuffer;

InputBuffer* new_input_buffer(void);
void read_input(InputBuffer*);
void close_input_buffer(InputBuffer*);
#endif
