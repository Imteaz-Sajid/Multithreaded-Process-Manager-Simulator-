/*
 * pm_sim.c  —  Multithreaded Process Manager Simulator
 * CSE321 Lab Project, Spring 2026
 *
 * Compile : gcc -o pm_sim pm_sim.c -lpthread
 * Run     : ./pm_sim thread0.txt thread1.txt thread2.txt
 */

#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>

/* ═══════════════════════════ Constants ═══════════════════════════════════ */
#define MAX_PROCS       64
#define MAX_CHILDREN    63

/* ═══════════════════════════ Process States ══════════════════════════════ */
typedef enum {
    STATE_RUNNING    = 0,
    STATE_BLOCKED    = 1,
    STATE_ZOMBIE     = 2,
    STATE_TERMINATED = 3
} ProcState;

static const char *state_name(ProcState s)
{
    switch (s) {
        case STATE_RUNNING:    return "RUNNING";
        case STATE_BLOCKED:    return "BLOCKED";
        case STATE_ZOMBIE:     return "ZOMBIE";
        case STATE_TERMINATED: return "TERMINATED";
        default:               return "UNKNOWN";
    }
}

/* ═══════════════════════════ PCB ═════════════════════════════════════════ */
typedef struct {
    int       pid;
    int       ppid;
    ProcState state;
    int       exit_status;
    int       children[MAX_CHILDREN];
    int       num_children;
    int       used;
} PCB;

/* ═══════════════════════════ Global Process Table ════════════════════════ */
static PCB             ptable[MAX_PROCS];
static int             next_pid   = 2;          /* PID 1 is init */
static pthread_mutex_t table_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  table_cond = PTHREAD_COND_INITIALIZER;

/* ═══════════════════════════ Snapshot Queue ══════════════════════════════ */
#define SNAP_QUEUE_CAP  256
#define SNAP_DATA_SZ    8192
#define LABEL_SZ        512

typedef struct {
    char label[LABEL_SZ];
    char data[SNAP_DATA_SZ];
} SnapEntry;

static SnapEntry       snap_queue[SNAP_QUEUE_CAP];
static int             snap_head  = 0;
static int             snap_tail  = 0;
static pthread_mutex_t snap_mutex = PTHREAD_MUTEX_INITIALIZER;
static sem_t           snap_sem;
static FILE           *snap_file  = NULL;

/* ═══════════════════════════ Internal Helpers ════════════════════════════ */
/* All helpers below require table_lock to be held by the caller. */

static PCB *find_pcb(int pid)
{
    for (int i = 0; i < MAX_PROCS; i++)
        if (ptable[i].used && ptable[i].pid == pid)
            return &ptable[i];
    return NULL;
}

static PCB *alloc_pcb(void)
{
    for (int i = 0; i < MAX_PROCS; i++)
        if (!ptable[i].used)
            return &ptable[i];
    return NULL;
}

static void remove_child(PCB *parent, int child_pid)
{
    for (int i = 0; i < parent->num_children; i++) {
        if (parent->children[i] == child_pid) {
            parent->children[i] = parent->children[--parent->num_children];
            return;
        }
    }
}

/* Build a printable snapshot of ptable into buf (table_lock must be held). */
static void build_snapshot(char *buf, int bufsz)
{
    int off = 0;
    off += snprintf(buf + off, bufsz - off,
                    "%-10s  %-10s  %-12s  %s\n",
                    "PID", "PPID", "STATE", "EXIT_STATUS");
    off += snprintf(buf + off, bufsz - off,
                    "----------------------------------------------\n");
    for (int i = 0; i < MAX_PROCS && off < bufsz - 1; i++) {
        PCB *p = &ptable[i];
        if (!p->used || p->state == STATE_TERMINATED)
            continue;
        if (p->state == STATE_ZOMBIE)
            off += snprintf(buf + off, bufsz - off,
                            "%-10d  %-10d  %-12s  %d\n",
                            p->pid, p->ppid, state_name(p->state), p->exit_status);
        else
            off += snprintf(buf + off, bufsz - off,
                            "%-10d  %-10d  %-12s  -\n",
                            p->pid, p->ppid, state_name(p->state));
    }
}

/*
 * Capture current ptable and push it onto the snapshot queue.
 * Caller must hold table_lock; this function acquires snap_mutex internally.
 */
static void enqueue_snapshot(const char *label)
{
    pthread_mutex_lock(&snap_mutex);
    int next_tail = (snap_tail + 1) % SNAP_QUEUE_CAP;
    if (next_tail != snap_head) {   /* queue not full */
        snprintf(snap_queue[snap_tail].label, LABEL_SZ, "%s", label);
        build_snapshot(snap_queue[snap_tail].data, SNAP_DATA_SZ);
        snap_tail = next_tail;
        sem_post(&snap_sem);
    }
    pthread_mutex_unlock(&snap_mutex);
}

/* ═══════════════════════════ Process Manager Operations ══════════════════ */

/*
 * pm_fork — create a child process under parent_pid.
 * Returns the new child PID, or -1 on error.
 */
int pm_fork(int parent_pid, int thread_id)
{
    pthread_mutex_lock(&table_lock);

    PCB *parent = find_pcb(parent_pid);
    if (!parent
        || parent->state == STATE_ZOMBIE
        || parent->state == STATE_TERMINATED
        || parent->num_children >= MAX_CHILDREN) {
        pthread_mutex_unlock(&table_lock);
        return -1;
    }

    PCB *child = alloc_pcb();
    if (!child) {
        pthread_mutex_unlock(&table_lock);
        return -1;
    }

    int new_pid = next_pid++;
    memset(child, 0, sizeof(PCB));
    child->pid          = new_pid;
    child->ppid         = parent_pid;
    child->state        = STATE_RUNNING;
    child->exit_status  = -1;
    child->num_children = 0;
    child->used         = 1;

    parent->children[parent->num_children++] = new_pid;

    char label[LABEL_SZ];
    snprintf(label, LABEL_SZ,
             "Thread %d calls pm_fork %d", thread_id, parent_pid);
    enqueue_snapshot(label);

    pthread_cond_broadcast(&table_cond);
    pthread_mutex_unlock(&table_lock);
    return new_pid;
}

/*
 * pm_exit — terminate pid with exit status.
 * Changes state to ZOMBIE, re-parents children to init (PID 1),
 * and wakes any parent waiting on a condition variable.
 */
void pm_exit(int pid, int status, int thread_id)
{
    pthread_mutex_lock(&table_lock);

    PCB *p = find_pcb(pid);
    if (!p || p->state == STATE_ZOMBIE || p->state == STATE_TERMINATED) {
        pthread_mutex_unlock(&table_lock);
        return;
    }

    p->state       = STATE_ZOMBIE;
    p->exit_status = status;

    /* Re-parent any living children to init (PID 1). */
    PCB *init_proc = find_pcb(1);
    for (int i = 0; i < p->num_children; i++) {
        PCB *c = find_pcb(p->children[i]);
        if (c && c->state != STATE_TERMINATED) {
            c->ppid = 1;
            if (init_proc && init_proc->num_children < MAX_CHILDREN)
                init_proc->children[init_proc->num_children++] = c->pid;
        }
    }
    p->num_children = 0;

    char label[LABEL_SZ];
    snprintf(label, LABEL_SZ,
             "Thread %d calls pm_exit %d %d", thread_id, pid, status);
    enqueue_snapshot(label);

    pthread_cond_broadcast(&table_cond);
    pthread_mutex_unlock(&table_lock);
}

/*
 * pm_wait — parent waits for child_pid to terminate.
 * child_pid == -1 means wait for the first child that exits.
 * Blocks if no zombie child is available yet.
 * Returns the reaped child's exit status, or -1 if nothing to wait for.
 */
int pm_wait(int parent_pid, int child_pid, int thread_id)
{
    pthread_mutex_lock(&table_lock);

    PCB *parent = find_pcb(parent_pid);
    if (!parent) {
        pthread_mutex_unlock(&table_lock);
        return -1;
    }

    while (1) {
        /* If parent was killed/exited while blocked, abort. */
        if (parent->state == STATE_ZOMBIE || parent->state == STATE_TERMINATED) {
            pthread_mutex_unlock(&table_lock);
            return -1;
        }

        /* Search for a zombie child. */
        int found_pid    = -1;
        int found_status = -1;

        if (child_pid == -1) {
            for (int i = 0; i < parent->num_children; i++) {
                PCB *c = find_pcb(parent->children[i]);
                if (c && c->state == STATE_ZOMBIE) {
                    found_pid    = c->pid;
                    found_status = c->exit_status;
                    break;
                }
            }
        } else {
            PCB *c = find_pcb(child_pid);
            if (c && c->ppid == parent_pid && c->state == STATE_ZOMBIE) {
                found_pid    = c->pid;
                found_status = c->exit_status;
            }
        }

        if (found_pid != -1) {
            /* Reap the zombie. */
            PCB *c   = find_pcb(found_pid);
            c->state = STATE_TERMINATED;
            c->used  = 0;
            remove_child(parent, found_pid);
            parent->state = STATE_RUNNING;

            char label[LABEL_SZ];
            snprintf(label, LABEL_SZ,
                     "Thread %d calls pm_wait %d %d",
                     thread_id, parent_pid, child_pid);
            enqueue_snapshot(label);

            pthread_cond_broadcast(&table_cond);
            pthread_mutex_unlock(&table_lock);
            return found_status;
        }

        /* Check whether there is still a target to wait for. */
        int has_target = 0;
        if (child_pid == -1) {
            has_target = (parent->num_children > 0);
        } else {
            PCB *c = find_pcb(child_pid);
            has_target = (c && c->ppid == parent_pid
                          && c->state != STATE_TERMINATED);
        }

        if (!has_target) {
            parent->state = STATE_RUNNING;
            pthread_mutex_unlock(&table_lock);
            return -1;
        }

        /* Block until the table changes. */
        parent->state = STATE_BLOCKED;
        pthread_cond_wait(&table_cond, &table_lock);

        /*
         * After wakeup: if still BLOCKED (not killed), set back to RUNNING
         * so the loop re-evaluates normally.
         */
        if (parent->state == STATE_BLOCKED)
            parent->state = STATE_RUNNING;
    }
}

/*
 * pm_kill — send a termination request to pid.
 * Equivalent to an immediate exit with no meaningful exit code.
 */
void pm_kill(int pid, int thread_id)
{
    pthread_mutex_lock(&table_lock);

    PCB *p = find_pcb(pid);
    if (!p || p->state == STATE_ZOMBIE || p->state == STATE_TERMINATED) {
        pthread_mutex_unlock(&table_lock);
        return;
    }

    p->state       = STATE_ZOMBIE;
    p->exit_status = -1;

    /* Re-parent children to init (PID 1). */
    PCB *init_proc = find_pcb(1);
    for (int i = 0; i < p->num_children; i++) {
        PCB *c = find_pcb(p->children[i]);
        if (c && c->state != STATE_TERMINATED) {
            c->ppid = 1;
            if (init_proc && init_proc->num_children < MAX_CHILDREN)
                init_proc->children[init_proc->num_children++] = c->pid;
        }
    }
    p->num_children = 0;

    char label[LABEL_SZ];
    snprintf(label, LABEL_SZ,
             "Thread %d calls pm_kill %d", thread_id, pid);
    enqueue_snapshot(label);

    pthread_cond_broadcast(&table_cond);
    pthread_mutex_unlock(&table_lock);
}

/*
 * pm_ps — print current process table snapshot to stdout.
 */
void pm_ps(void)
{
    pthread_mutex_lock(&table_lock);
    char buf[SNAP_DATA_SZ];
    build_snapshot(buf, SNAP_DATA_SZ);
    printf("%s", buf);
    pthread_mutex_unlock(&table_lock);
}

/* ═══════════════════════════ Monitor Thread ══════════════════════════════ */

/*
 * Waits on snap_sem.  Each enqueue_snapshot() call does sem_post, so the
 * semaphore count exactly mirrors the number of pending entries.
 * When workers finish, main() posts one extra sem_post; the monitor then
 * finds the queue empty and exits cleanly.
 */
static void *monitor_thread(void *arg)
{
    (void)arg;

    while (1) {
        sem_wait(&snap_sem);

        pthread_mutex_lock(&snap_mutex);
        if (snap_head == snap_tail) {
            /* Empty queue after sem_wait → shutdown sentinel received. */
            pthread_mutex_unlock(&snap_mutex);
            break;
        }
        SnapEntry entry = snap_queue[snap_head];
        snap_head = (snap_head + 1) % SNAP_QUEUE_CAP;
        pthread_mutex_unlock(&snap_mutex);

        if (snap_file) {
            fprintf(snap_file, "%s\n", entry.label);
            fprintf(snap_file, "%s\n", entry.data);
            fflush(snap_file);
        }
    }

    return NULL;
}

/* ═══════════════════════════ Worker Thread ═══════════════════════════════ */

typedef struct {
    int   thread_id;
    char *filename;
} WorkerArg;

static void *worker_thread(void *arg)
{
    WorkerArg *wa = (WorkerArg *)arg;

    FILE *f = fopen(wa->filename, "r");
    if (!f) {
        fprintf(stderr, "[ERROR] Thread %d: cannot open '%s'\n",
                wa->thread_id, wa->filename);
        return NULL;
    }

    char line[256];
    while (fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\r\n")] = '\0';
        if (line[0] == '\0' || line[0] == '#')
            continue;

        char cmd[64] = {0};
        sscanf(line, "%63s", cmd);

        if (strcmp(cmd, "fork") == 0) {
            int ppid = 1;
            sscanf(line, "%*s %d", &ppid);
            pm_fork(ppid, wa->thread_id);

        } else if (strcmp(cmd, "exit") == 0) {
            int pid = 0, status = 0;
            sscanf(line, "%*s %d %d", &pid, &status);
            pm_exit(pid, status, wa->thread_id);

        } else if (strcmp(cmd, "wait") == 0) {
            int ppid = 0, cpid = -1;
            sscanf(line, "%*s %d %d", &ppid, &cpid);
            pm_wait(ppid, cpid, wa->thread_id);

        } else if (strcmp(cmd, "kill") == 0) {
            int pid = 0;
            sscanf(line, "%*s %d", &pid);
            pm_kill(pid, wa->thread_id);

        } else if (strcmp(cmd, "sleep") == 0) {
            int ms = 0;
            sscanf(line, "%*s %d", &ms);
            usleep((useconds_t)ms * 1000);

        } else {
            fprintf(stderr, "[WARN] Thread %d: unknown command '%s'\n",
                    wa->thread_id, cmd);
        }
    }

    fclose(f);
    return NULL;
}

/* ═══════════════════════════ main ════════════════════════════════════════ */

int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr,
                "Usage: %s script0.txt [script1.txt ...]\n", argv[0]);
        return 1;
    }

    /* ── Initialise process table with init process (PID=1, PPID=0) ── */
    memset(ptable, 0, sizeof(ptable));
    ptable[0].pid          = 1;
    ptable[0].ppid         = 0;
    ptable[0].state        = STATE_RUNNING;
    ptable[0].exit_status  = -1;
    ptable[0].num_children = 0;
    ptable[0].used         = 1;

    /* ── Open snapshot output file ── */
    snap_file = fopen("snapshots.txt", "w");
    if (!snap_file) {
        perror("fopen snapshots.txt");
        return 1;
    }

    /* ── Initialise semaphore ── */
    if (sem_init(&snap_sem, 0, 0) != 0) {
        perror("sem_init");
        return 1;
    }

    /* ── Write the initial snapshot directly (before monitor starts) ── */
    {
        char buf[SNAP_DATA_SZ];
        pthread_mutex_lock(&table_lock);
        build_snapshot(buf, SNAP_DATA_SZ);
        pthread_mutex_unlock(&table_lock);
        fprintf(snap_file, "Initial Process Table\n");
        fprintf(snap_file, "%s\n", buf);
        fflush(snap_file);
    }

    /* ── Start monitor thread ── */
    pthread_t monitor;
    pthread_create(&monitor, NULL, monitor_thread, NULL);

    /* ── Start one worker thread per script file ── */
    int        nworkers = argc - 1;
    pthread_t *workers  = malloc((size_t)nworkers * sizeof(pthread_t));
    WorkerArg *wargs    = malloc((size_t)nworkers * sizeof(WorkerArg));

    for (int i = 0; i < nworkers; i++) {
        wargs[i].thread_id = i;
        wargs[i].filename  = argv[i + 1];
        pthread_create(&workers[i], NULL, worker_thread, &wargs[i]);
    }

    /* ── Wait for all workers ── */
    for (int i = 0; i < nworkers; i++)
        pthread_join(workers[i], NULL);

    /*
     * ── Signal monitor to finish ──
     * All workers are done so no new entries will be enqueued.
     * One extra sem_post acts as the shutdown sentinel: the monitor will
     * wake, find the queue empty, and exit its loop.
     */
    sem_post(&snap_sem);
    pthread_join(monitor, NULL);

    fclose(snap_file);
    sem_destroy(&snap_sem);
    free(workers);
    free(wargs);

    printf("Simulation complete. Snapshots written to snapshots.txt\n");
    return 0;
}
