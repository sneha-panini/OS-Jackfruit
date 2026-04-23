/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Block while the buffer is full, unless we are shutting down */
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while empty; if shutting down and empty, signal the caller to exit */
    while (buffer->count == 0) {
        if (buffer->shutting_down) {
            pthread_mutex_unlock(&buffer->mutex);
            return -1;   /* tell the consumer it is time to exit */
        }
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char log_path[PATH_MAX];
        int fd;

        /* Build per-container log path: logs/<container_id>.log */
        snprintf(log_path, sizeof(log_path), "%s/%s.log",
                 LOG_DIR, item.container_id);

        fd = open(log_path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd < 0) {
            fprintf(stderr, "[logger] cannot open %s: %s\n",
                    log_path, strerror(errno));
            continue;
        }

        /* Write the chunk; ignore partial-write for simplicity */
        if (write(fd, item.data, item.length) < 0)
            fprintf(stderr, "[logger] write error on %s: %s\n",
                    log_path, strerror(errno));

        close(fd);
    }

    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* ---- UTS: give the container its own hostname ---- */
    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("sethostname");
        return 1;
    }

    /* ---- Mount: make the mount namespace private so we can pivot ---- */
    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0) {
        perror("mount --make-rprivate /");
        return 1;
    }

    /* ---- Bind-mount rootfs onto itself so it becomes a mount point ---- */
    if (mount(cfg->rootfs, cfg->rootfs, NULL, MS_BIND | MS_REC, NULL) < 0) {
        perror("mount --bind rootfs");
        return 1;
    }

    /* ---- Mount /proc inside rootfs ---- */
    {
        char proc_path[PATH_MAX];
        snprintf(proc_path, sizeof(proc_path), "%s/proc", cfg->rootfs);
        mkdir(proc_path, 0555);
        if (mount("proc", proc_path, "proc", 0, NULL) < 0) {
            perror("mount proc");
            /* non-fatal: continue without /proc */
        }
    }

    /* ---- chroot into rootfs ---- */
    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") < 0) {
        perror("chdir /");
        return 1;
    }

    /* ---- Redirect stdout / stderr to the log write fd ---- */
    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }

    /* ---- Apply nice value for scheduler experiments ---- */
    if (cfg->nice_value != 0)
        if (nice(cfg->nice_value) == -1 && errno != 0)
            perror("nice"); /* non-fatal */

    /* ---- Execute the command ---- */
    execl("/bin/sh", "/bin/sh", "-c", cfg->command, (char *)NULL);
    /* execl only returns on error */
    perror("execl");
    return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/* ---------------------------------------------------------------
 * Supervisor helpers
 * --------------------------------------------------------------- */

/* Global pointer used by signal handlers */
static supervisor_ctx_t *g_ctx = NULL;

static void sigchld_handler(int sig)
{
    (void)sig;
    /* Reap all available children in a loop to avoid missing signals */
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (!g_ctx)
            continue;

        pthread_mutex_lock(&g_ctx->metadata_lock);

        container_record_t *rec = g_ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(status)) {
                    rec->state     = CONTAINER_EXITED;
                    rec->exit_code = WEXITSTATUS(status);
                } else if (WIFSIGNALED(status)) {
                    rec->state       = CONTAINER_KILLED;
                    rec->exit_signal = WTERMSIG(status);
                }

                /* Unregister from the kernel monitor */
                if (g_ctx->monitor_fd >= 0)
                    unregister_from_monitor(g_ctx->monitor_fd,
                                            rec->id, pid);
                break;
            }
            rec = rec->next;
        }

        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

/* Spawn one container using clone(2).
 * Returns the host PID of the new container on success, -1 on error.
 * Adds an entry to ctx->containers. */
static pid_t spawn_container(supervisor_ctx_t *ctx,
                             const control_request_t *req)
{
    char *stack;
    char *stack_top;
    pid_t pid;
    int pipe_fds[2];
    container_record_t *rec;

    /* Create the log pipe: child writes to pipe_fds[1],
     * the logging thread reads from pipe_fds[0] via a reader thread
     * spawned below. */
    if (pipe(pipe_fds) < 0) {
        perror("pipe");
        return -1;
    }

    /* Ensure the logs directory exists */
    mkdir(LOG_DIR, 0755);

    /* Build child configuration */
    child_config_t *cfg = calloc(1, sizeof(*cfg));
    if (!cfg) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        return -1;
    }
    strncpy(cfg->id,      req->container_id, sizeof(cfg->id)      - 1);
    strncpy(cfg->rootfs,  req->rootfs,       sizeof(cfg->rootfs)  - 1);
    strncpy(cfg->command, req->command,      sizeof(cfg->command) - 1);
    cfg->nice_value   = req->nice_value;
    cfg->log_write_fd = pipe_fds[1];

    /* Allocate clone stack */
    stack = malloc(STACK_SIZE);
    if (!stack) {
        perror("malloc stack");
        free(cfg);
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        return -1;
    }
    stack_top = stack + STACK_SIZE;

    pid = clone(child_fn, stack_top,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                cfg);

    /* Parent closes the write end of the pipe */
    close(pipe_fds[1]);

    if (pid < 0) {
        perror("clone");
        free(stack);
        free(cfg);
        close(pipe_fds[0]);
        return -1;
    }

    /* The stack and cfg are kept alive until the child exits.
     * For brevity we accept the small leak; a production runtime would
     * store them in the container record and free on SIGCHLD. */

    /* Register with the kernel monitor */
    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd,
                                  req->container_id, pid,
                                  req->soft_limit_bytes,
                                  req->hard_limit_bytes) < 0)
            fprintf(stderr, "[supervisor] warning: monitor registration failed: %s\n",
                    strerror(errno));
    }

    /* Allocate and insert container record */
    rec = calloc(1, sizeof(*rec));
    if (rec) {
        strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
        rec->host_pid          = pid;
        rec->started_at        = time(NULL);
        rec->state             = CONTAINER_RUNNING;
        rec->soft_limit_bytes  = req->soft_limit_bytes;
        rec->hard_limit_bytes  = req->hard_limit_bytes;
        snprintf(rec->log_path, sizeof(rec->log_path),
                 "%s/%s.log", LOG_DIR, req->container_id);

        pthread_mutex_lock(&ctx->metadata_lock);
        rec->next        = ctx->containers;
        ctx->containers  = rec;
        pthread_mutex_unlock(&ctx->metadata_lock);
    }

    /* Spawn a pipe-reader thread to forward container output into the
     * bounded buffer so the logging thread can persist it. */
    {
        typedef struct { int fd; supervisor_ctx_t *ctx; char cid[CONTAINER_ID_LEN]; } pipe_arg_t;
        pipe_arg_t *pa = malloc(sizeof(*pa));
        if (pa) {
            pa->fd  = pipe_fds[0];
            pa->ctx = ctx;
            strncpy(pa->cid, req->container_id, sizeof(pa->cid) - 1);

            pthread_t tid;
            /* Inline lambda via nested function is a GCC extension; use a
             * static helper instead. */
            /* We pass the arg as a void* to a small trampoline defined
             * after this function. */
            extern void *pipe_reader_thread(void *);
            if (pthread_create(&tid, NULL, pipe_reader_thread, pa) != 0) {
                close(pipe_fds[0]);
                free(pa);
            } else {
                pthread_detach(tid);
            }
        } else {
            close(pipe_fds[0]);
        }
    }

    return pid;
}

/* Pipe-reader trampoline: reads chunks from a container's stdout/stderr
 * pipe and pushes them into the bounded buffer. */
typedef struct {
    int fd;
    supervisor_ctx_t *ctx;
    char cid[CONTAINER_ID_LEN];
} pipe_arg_t;

void *pipe_reader_thread(void *arg)
{
    pipe_arg_t *pa = (pipe_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    memset(item.container_id, 0, sizeof(item.container_id));
    strncpy(item.container_id, pa->cid, sizeof(item.container_id) - 1);

    while ((n = read(pa->fd, item.data, sizeof(item.data))) > 0) {
        item.length = (size_t)n;
        bounded_buffer_push(&pa->ctx->log_buffer, &item);
    }

    close(pa->fd);
    free(pa);
    return NULL;
}

/* Send a complete message over a blocking socket fd. */
static int send_all(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) return -1;
        p   += n;
        len -= (size_t)n;
    }
    return 0;
}

/* Receive exactly len bytes from a blocking socket fd. */
static int recv_all(int fd, void *buf, size_t len)
{
    char *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n <= 0) return -1;
        p   += n;
        len -= (size_t)n;
    }
    return 0;
}

/* Handle a single client connection accepted on the control socket. */
static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t  req;
    control_response_t resp;

    memset(&resp, 0, sizeof(resp));

    if (recv_all(client_fd, &req, sizeof(req)) < 0) {
        close(client_fd);
        return;
    }

    switch (req.kind) {

    case CMD_START:
    case CMD_RUN: {
        pid_t pid = spawn_container(ctx, &req);
        if (pid < 0) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Failed to start container '%s': %s",
                     req.container_id, strerror(errno));
        } else {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "Container '%s' started (pid=%d)",
                     req.container_id, pid);

            /* For CMD_RUN: wait for the container to finish */
            if (req.kind == CMD_RUN) {
                int wstatus;
                waitpid(pid, &wstatus, 0);
                snprintf(resp.message, sizeof(resp.message),
                         "Container '%s' (pid=%d) exited with status %d",
                         req.container_id, pid,
                         WIFEXITED(wstatus) ? WEXITSTATUS(wstatus) : -1);
            }
        }
        break;
    }

    case CMD_PS: {
        /* Build a human-readable list of container records */
        char buf[CONTROL_MESSAGE_LEN];
        int  off = 0;

        pthread_mutex_lock(&ctx->metadata_lock);

        container_record_t *rec = ctx->containers;
        if (!rec) {
            off += snprintf(buf + off, sizeof(buf) - (size_t)off,
                            "(no containers)\n");
        }
        while (rec && off < (int)sizeof(buf) - 1) {
            off += snprintf(buf + off, sizeof(buf) - (size_t)off,
                            "%-16s  pid=%-6d  %s\n",
                            rec->id, rec->host_pid,
                            state_to_string(rec->state));
            rec = rec->next;
        }

        pthread_mutex_unlock(&ctx->metadata_lock);

        resp.status = 0;
        strncpy(resp.message, buf, sizeof(resp.message) - 1);
        break;
    }

    case CMD_LOGS: {
        /* Read the log file and stream its contents back */
        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path),
                 "%s/%s.log", LOG_DIR, req.container_id);

        FILE *f = fopen(log_path, "r");
        if (!f) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "No log for container '%s'", req.container_id);
        } else {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "[log for %s]", req.container_id);
            send_all(client_fd, &resp, sizeof(resp));

            /* Stream file contents in chunks */
            char chunk[512];
            size_t n;
            while ((n = fread(chunk, 1, sizeof(chunk), f)) > 0)
                send_all(client_fd, chunk, n);

            fclose(f);
            close(client_fd);
            return;
        }
        break;
    }

    case CMD_STOP: {
        int found = 0;

        pthread_mutex_lock(&ctx->metadata_lock);

        container_record_t *rec = ctx->containers;
        while (rec) {
            if (strncmp(rec->id, req.container_id, CONTAINER_ID_LEN) == 0) {
                if (rec->state == CONTAINER_RUNNING ||
                    rec->state == CONTAINER_STARTING) {
                    kill(rec->host_pid, SIGTERM);
                    rec->state = CONTAINER_STOPPED;
                    found = 1;
                }
                break;
            }
            rec = rec->next;
        }

        pthread_mutex_unlock(&ctx->metadata_lock);

        resp.status = found ? 0 : -1;
        snprintf(resp.message, sizeof(resp.message),
                 found ? "Container '%s' stopped."
                       : "Container '%s' not found or not running.",
                 req.container_id);
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "Unknown command.");
        break;
    }

    send_all(client_fd, &resp, sizeof(resp));
    close(client_fd);
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;
    struct sockaddr_un addr;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;

    /* ---- 1) Install signal handlers before anything else ---- */
    g_ctx = &ctx;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = sigterm_handler;
    sa.sa_flags   = SA_RESTART;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    /* ---- 2) Initialize metadata lock and bounded buffer ---- */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* ---- 3) Open the kernel monitor device (optional) ---- */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr,
                "[supervisor] Warning: cannot open /dev/container_monitor (%s)."
                " Memory limits disabled.\n",
                strerror(errno));

    /* ---- 4) Ensure the logs directory exists ---- */
    mkdir(LOG_DIR, 0755);

    /* ---- 5) Create the UNIX domain socket ---- */
    unlink(CONTROL_PATH);

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        goto cleanup;
    }

    if (listen(ctx.server_fd, 8) < 0) {
        perror("listen");
        goto cleanup;
    }

    /* ---- 6) Spawn the logging consumer thread ---- */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        goto cleanup;
    }

    fprintf(stderr, "[supervisor] ready. base-rootfs=%s control=%s\n",
            rootfs, CONTROL_PATH);

    /* ---- 7) Event loop: accept and serve control requests ---- */
    while (!ctx.should_stop) {
        fd_set rfds;
        struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };

        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);

        int nready = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (nready < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }
        if (nready == 0)
            continue;   /* timeout — check should_stop */

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }

        handle_client(&ctx, client_fd);
    }

    fprintf(stderr, "[supervisor] shutting down.\n");

    /* ---- 8) Graceful teardown ---- */

    /* Stop all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *rec = ctx.containers;
        while (rec) {
            if (rec->state == CONTAINER_RUNNING ||
                rec->state == CONTAINER_STARTING)
                kill(rec->host_pid, SIGTERM);
            rec = rec->next;
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Give them a moment then reap */
    sleep(1);
    while (waitpid(-1, NULL, WNOHANG) > 0)
        ;

    /* Free container records */
    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *rec = ctx.containers;
        while (rec) {
            container_record_t *next = rec->next;
            free(rec);
            rec = next;
        }
        ctx.containers = NULL;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

cleanup:
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    if (ctx.logger_thread)
        pthread_join(ctx.logger_thread, NULL);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.server_fd >= 0)  close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    g_ctx = NULL;
    return 0;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr,
                "Cannot connect to supervisor at %s: %s\n"
                "Is the supervisor running?\n",
                CONTROL_PATH, strerror(errno));
        close(fd);
        return 1;
    }

    /* Send request */
    if (send_all(fd, req, sizeof(*req)) < 0) {
        perror("write");
        close(fd);
        return 1;
    }

    /* Read response */
    if (recv_all(fd, &resp, sizeof(resp)) < 0) {
        perror("read");
        close(fd);
        return 1;
    }

    /* For CMD_LOGS the supervisor streams extra data after the header */
    if (req->kind == CMD_LOGS && resp.status == 0) {
        char buf[512];
        ssize_t n;
        while ((n = read(fd, buf, sizeof(buf))) > 0)
            fwrite(buf, 1, (size_t)n, stdout);
    } else {
        printf("%s\n", resp.message);
    }

    close(fd);
    return (resp.status == 0) ? 0 : 1;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    /*
     * TODO:
     * The supervisor should respond with container metadata.
     * Keep the rendering format simple enough for demos and debugging.
     */
    printf("Expected states include: %s, %s, %s, %s, %s\n",
           state_to_string(CONTAINER_STARTING),
           state_to_string(CONTAINER_RUNNING),
           state_to_string(CONTAINER_STOPPED),
           state_to_string(CONTAINER_KILLED),
           state_to_string(CONTAINER_EXITED));
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
