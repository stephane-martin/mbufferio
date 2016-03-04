#ifndef MURMUR_INC_H
#define MURMUR_INC_H

#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

uint32_t qhashmurmur3_32(const void *data, size_t nbytes);
int qhashmurmur3_128(const void *data, size_t nbytes, void *retbuf);

#endif
