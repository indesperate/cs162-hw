#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>
#include <termios.h>
#include <unistd.h>

#include "tokenizer.h"

/* Convenience macro to silence compiler warnings about unused function parameters. */
#define unused __attribute__((unused))

/* max path length */
#define MAX_PATH_LENGTH 4096
#define MAX_NAME_LENGTH 256

/* Whether the shell is connected to an actual terminal or not. */
bool shell_is_interactive;

/* File descriptor for the shell input */
int shell_terminal;

/* Terminal mode settings for the shell */
struct termios shell_tmodes;

/* Process group id for the shell */
pid_t shell_pgid;

int cmd_exit(struct tokens* tokens);
int cmd_help(struct tokens* tokens);
int cmd_pwd(struct tokens* tokens);
int cmd_cd(struct tokens* tokens);
int cmd_wait(struct tokens* tokens);

/* handler child exit signal */
void sigchild_handler(int sig);

/* Built-in command functions take token array (see parse.h) and return int */
typedef int cmd_fun_t(struct tokens* tokens);

/* Built-in command struct and lookup table */
typedef struct fun_desc {
  cmd_fun_t* fun;
  char* cmd;
  char* doc;
} fun_desc_t;

fun_desc_t cmd_table[] = {
    {cmd_help, "?", "show this help menu"},
    {cmd_exit, "exit", "exit the command shell"},
    {cmd_pwd, "pwd", "print the current working directory"},
    {cmd_cd, "cd", "change the current direcotry to another directory"},
    {cmd_wait, "wait", "wait for the processes to end"},
};

typedef struct fd_pair {
  int fd[2];
} fd_pair;

/* create pipes to n processes */
int create_pipe_process(pid_t pids[], int n_pipes, bool is_background) {
  int n_processes = n_pipes + 1;
  fd_pair fd_pair_vec[n_pipes];
  /* create n pipes */
  for (int i = 0; i < n_pipes; i++) {
    if (pipe(fd_pair_vec[i].fd)) {
      printf("Create pipe filed\n");
      exit(-1);
    };
  }
  /* fork n processes and get index */
  int index = -1;
  for (int i = 0; i < n_processes; i++) {
    pid_t id = fork();
    if (id == 0) {
      if (i == 0) {
        if (setpgid(getpid(), getpid()) == -1) {
          printf("set process group id failed\n");
          exit(-1);
        }
      } else {
        if (setpgid(getpid(), pids[0]) == -1) {
          printf("set process group id failed\n");
          exit(-1);
        }
      }
      signal(SIGCHLD, SIG_DFL);
      index = i;
      break;
    } else {
      pids[i] = id;
    }
  }
  /* duplicate the pipes to stdin and stdout, only duplicate in child process */
  if (index >= 0) {
    if (index > 0) {
      /* fd[0] for read, fisrt process read from stdin */
      dup2(fd_pair_vec[index - 1].fd[0], STDIN_FILENO);
    }
    if (index < n_pipes) {
      /* fd[1] for write, last process write to stdout */
      dup2(fd_pair_vec[index].fd[1], STDOUT_FILENO);
    }
  }
  /* close all pipes, left the duplicated pipes above */
  for (int i = 0; i < n_pipes; i++) {
    close(fd_pair_vec[i].fd[0]);
    close(fd_pair_vec[i].fd[1]);
  }
  return index;
}

/* Prints a helpful description for the given command */
int cmd_help(unused struct tokens* tokens) {
  for (unsigned int i = 0; i < sizeof(cmd_table) / sizeof(fun_desc_t); i++)
    printf("%s - %s\n", cmd_table[i].cmd, cmd_table[i].doc);
  return 1;
}

/* Exits this shell */
int cmd_exit(unused struct tokens* tokens) { exit(0); }

/* print directory */
int cmd_pwd(unused struct tokens* tokens) {
  char buf[MAX_PATH_LENGTH];
  if (getcwd(buf, MAX_PATH_LENGTH) == NULL) {
    printf("Can't print current directory");
    return 0;
  }
  printf("%s\n", buf);
  return 1;
}

int cmd_cd(struct tokens* tokens) {
  char* directory = tokens_get_token(tokens, 1);
  if (chdir(directory) != 0) {
    printf("Change direcotry failed\n");
    return 0;
  };
  return 1;
}

int cmd_wait(struct tokens* tokens) {
  while (waitpid(-1, NULL, 0) > 0)
    ;
  return 1;
};

/* Looks up the built-in command, if it exists. */
int lookup(char cmd[]) {
  for (unsigned int i = 0; i < sizeof(cmd_table) / sizeof(fun_desc_t); i++)
    if (cmd && (strcmp(cmd_table[i].cmd, cmd) == 0))
      return i;
  return -1;
}

/* Intialization procedures for this shell */
void init_shell() {
  /* Our shell is connected to standard input. */
  shell_terminal = STDIN_FILENO;

  /* Check if we are running interactively */
  shell_is_interactive = isatty(shell_terminal);

  /* automatic */
  signal(SIGCHLD, sigchild_handler);

  if (shell_is_interactive) {
    /* If the shell is not currently in the foreground, we must pause the shell until it becomes a
     * foreground process. We use SIGTTIN to pause the shell. When the shell gets moved to the
     * foreground, we'll receive a SIGCONT. */
    while (tcgetpgrp(shell_terminal) != (shell_pgid = getpgrp()))
      kill(-shell_pgid, SIGTTIN);

    /* Saves the shell's process id */
    shell_pgid = getpid();

    /* Take control of the terminal */
    tcsetpgrp(shell_terminal, shell_pgid);

    /* Save the current termios to a variable, so it can be restored later. */
    tcgetattr(shell_terminal, &shell_tmodes);
  }
}

bool check_if_background(struct tokens* tokens) {
  return tokens_get_token(tokens, tokens_get_length(tokens) - 1)[0] == '&';
}

/* handle child process */
void sigchild_handler(int sig) {
  while (waitpid(-1, NULL, WNOHANG) > 0)
    ;
}

void get_program_args(struct tokens* tokens, int program_index, char* args[], bool is_background) {
  int args_index = 0;
  int num_tokens = tokens_get_length(tokens);
  if (is_background) {
    num_tokens--;
  }
  for (int i = 0; i < num_tokens; i++) {
    if (program_index > 0) {
      if (tokens_get_token(tokens, i)[0] == '|') {
        program_index--;
      }
      continue;
    } else {
      if (tokens_get_token(tokens, i)[0] == '|') {
        break;
      }
    }
    args[args_index] = tokens_get_token(tokens, i);
    args_index++;
  }
  // null terminated
  args[args_index] = NULL;
}

void exec_program_in_path(char* args[]) {
  char* program_name = args[0];
  if (program_name[0] != '/') {
    // envrionemnt varibale is not modefiable
    char* path = strndup(getenv("PATH"), MAX_PATH_LENGTH);
    char* path_token;
    char program_full_name[MAX_PATH_LENGTH];
    char* save_ptr;
    // split the string
    path_token = strtok_r(path, ":", &save_ptr);
    while (path_token) {
      // complete the full path string
      strncpy(program_full_name, path_token, MAX_PATH_LENGTH);
      strncat(program_full_name, "/", 2);
      strncat(program_full_name, program_name, MAX_NAME_LENGTH);
      execv(program_full_name, args);
      path_token = strtok_r(NULL, ":", &save_ptr);
    }
  } else {
    execv(program_name, args);
  }
  printf("Program %s is not in path\n", program_name);
  exit(-1);
}

void handle_redirect_symbol(char* args[]) {
  int i = 0;
  while (args[i] != NULL) {
    // redirection
    if (args[i][0] == '<') {
      if (args[i + 1] == NULL) {
        printf("Invalid redirect file name\n");
        exit(-1);
      }
      freopen(args[i + 1], "r", stdin);
      args[i] = NULL;
      break;
    }
    if (args[i][0] == '>') {
      if (args[i + 1] == NULL) {
        printf("Invalid redirect file name\n");
        exit(-1);
      }
      freopen(args[i + 1], "w", stdout);
      args[i] = NULL;
      break;
    }
    i++;
  }
}

void exec_program(struct tokens* tokens) {
  bool is_background = check_if_background(tokens);
  int num_tokens = tokens_get_length(tokens);
  if (num_tokens < 1) {
    return;
  }
  int num_pipes = 0;
  for (int i = 0; i < num_tokens; i++) {
    char* token = tokens_get_token(tokens, i);
    if (token[0] == '|') {
      num_pipes++;
    }
  }
  int num_process = num_pipes + 1;
  pid_t pids[num_pipes + 1];
  int program_index = create_pipe_process(pids, num_pipes, is_background);
  /* new process */
  if (program_index >= 0) {
    /* one more location for null terminated */
    char* args[num_tokens + 1];
    /* handle program paramters */
    get_program_args(tokens, program_index, args, is_background);
    handle_redirect_symbol(args);
    exec_program_in_path(args);
  }
  /* main process */
  else {
    // wait all child processes
    if (!is_background) {
      for (int i = 0; i < num_process; i++) {
        waitpid(pids[i], NULL, 0);
      }
    }
    if (tcsetpgrp(0, getpgrp()) == -1) {
      printf("set foreground terminal group id failed\n");
    };
  }
}

int main(unused int argc, unused char* argv[]) {
  init_shell();

  static char line[4096];
  int line_num = 0;

  /* Please only print shell prompts when standard input is not a tty */
  if (shell_is_interactive)
    fprintf(stdout, "%d: ", line_num);

  while (fgets(line, 4096, stdin)) {
    /* Split our line into words. */
    struct tokens* tokens = tokenize(line);

    /* Find which built-in function to run. */
    int fundex = lookup(tokens_get_token(tokens, 0));

    if (fundex >= 0) {
      cmd_table[fundex].fun(tokens);
    } else {
      exec_program(tokens);
    }

    if (shell_is_interactive)
      /* Please only print shell prompts when standard input is not a tty */
      fprintf(stdout, "%d: ", ++line_num);

    /* Clean up memory */
    tokens_destroy(tokens);
  }

  return 0;
}
