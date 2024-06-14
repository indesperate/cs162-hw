#include <assert.h>
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>

/* Function pointers to hw3 functions */
void* (*mm_malloc)(size_t);
void* (*mm_realloc)(void*, size_t);
void (*mm_free)(void*);

static void* try_dlsym(void* handle, const char* symbol) {
  char* error;
  void* function = dlsym(handle, symbol);
  if ((error = dlerror())) {
    fprintf(stderr, "%s\n", error);
    exit(EXIT_FAILURE);
  }
  return function;
}

static void load_alloc_functions() {
  void* handle = dlopen("hw3lib.so", RTLD_NOW);
  if (!handle) {
    fprintf(stderr, "%s\n", dlerror());
    exit(EXIT_FAILURE);
  }

  mm_malloc = try_dlsym(handle, "mm_malloc");
  mm_realloc = try_dlsym(handle, "mm_realloc");
  mm_free = try_dlsym(handle, "mm_free");
}

int main() {
  load_alloc_functions();
  static const int num = 100;
  int* arr[num];
  for (int i = 0; i < num; i++) {
    arr[i] = mm_malloc(sizeof(int));
    arr[i][0] = 0x162;
    arr[i] = mm_realloc(arr[i], sizeof(int) * 10);
    arr[i][0] = 0x162;
    arr[i][9] = 0x162;
  }
  printf("allocated %d ints\n", num);
  for (int i = num - 1; i > 0; i--) {
    mm_free(arr[i]);
  }
  puts("malloc test successful!");
}
