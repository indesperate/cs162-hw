/*
 * mm_alloc.c
 */

#include "mm_alloc.h"

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

struct chunked_block {
  size_t size;
  struct chunked_block* next;
  struct chunked_block* prev;
  int free;
  char data[];
};

struct chunked_block* first_block = NULL;
struct chunked_block* end_block = NULL;

#define offsetof(TYPE, MEMBER) ((size_t) & ((TYPE*)0)->MEMBER)
#define get_block_entry(ptr)                                                                       \
  ((struct chunked_block*)((char*)ptr - offsetof(struct chunked_block, data)))

struct chunked_block* create_block(size_t size) {
  struct chunked_block* block = sbrk(size + sizeof(struct chunked_block));
  if (block == (void*)-1) {
    return NULL;
  }
  memset(block, 0, size + sizeof(struct chunked_block));
  block->size = size;
  block->free = 1;
  return block;
}

void* mm_malloc(size_t size) {
  if (size == 0) {
    return NULL;
  }
  if (!first_block) {
    first_block = create_block(size);
    if (!first_block) {
      return NULL;
    }
    first_block->free = 0;
    end_block = first_block;
    return first_block->data;
  } else {
    struct chunked_block* current = first_block;
    while (current) {
      if (current->free && current->size >= size) {
        current->free = 0;
        return current->data;
      }
      current = current->next;
    }
    struct chunked_block* new_block = create_block(size);
    if (!new_block) {
      return NULL;
    }
    new_block->prev = end_block;
    end_block->next = new_block;
    new_block->free = 0;
    end_block = new_block;
    return new_block->data;
  }
}

void* mm_realloc(void* ptr, size_t size) {
  if (size == 0) {
    mm_free(ptr);
    return NULL;
  }
  if (!ptr) {
    return mm_malloc(size);
  }
  struct chunked_block* block = get_block_entry(ptr);
  if (block->size >= size) {
    return ptr;
  } else {
    block->free = 1;
    return mm_malloc(size);
  }
}

void mm_free(void* ptr) {
  if (!ptr) {
    return;
  }
  struct chunked_block* block = get_block_entry(ptr);
  block->free = 1;
  while (block->next && block->next->free) {
    block->size += block->next->size + sizeof(struct chunked_block);
    block->next = block->next->next;
    if (block->next) {
      block->next->prev = block;
    } else {
      end_block = block;
    }
    memset(block->data, 0, block->size);
  }
}
