/*
 * coordinator.c -- YOU IMPLEMENT THE TODOs IN THIS FILE.
 */
#include "common.h"

#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define POLL_MS 50

enum phase { PH_CP, PH_RECOVER_REDUCER, PH_RECOVER_MAPPER, PH_LAST_CP, PH_EXITING };

struct wstate {
    long last_hb;       /* time of the last heartbeat from the current process */
    int  heard;         /* current process has sent a heartbeat (its sockets are up) */
    long last_cp;       /* newest checkpoint this worker acknowledged */
    int done;
};

static const config_t *cfg;
static job_t         *job;
static struct wstate  ws[MAX_WORKERS];
static enum phase     phase;
static int            rid;              /* current recovery id */
static long           next_cp_id = 1;   /* never reused */

static long checkpoint_in_progress = NO_CHECKPOINT;
static long last_checkpoint_ms;

static const char *phase_name[] = {
    "CP", "RECOVER_REDUCER", "RECOVER_MAPPER", "LAST_CP", "EXITING",
};

static int is_mapper(int w) { return w < cfg->M; }

static const char *wname(int w)
{
    static char buf[4][32];
    static int k;
    char *b = buf[k++ % 4];
    snprintf(b, sizeof buf[0], "%s %d", is_mapper(w) ? "mapper" : "reducer",
             is_mapper(w) ? w : w - cfg->M);
    return b;
}

/* PROVIDED: change phase (and log it). Use this instead of assigning phase. */
static void set_phase(enum phase p)
{
    wlog("phase %s -> %s (rid %d)", phase_name[phase], phase_name[p], rid);
    phase = p;
}

/* Send a message of the given type to worker w, tagged with our recovery id. */
static void send_to(int w, int type, long cp)
{
    msg_t m;
    memset(&m, 0, sizeof m);
    m.type = type;
    m.role = ROLE_COORDINATOR;
    m.recovery_id = rid;
    m.checkpoint_id = cp;
    int role = is_mapper(w) ? ROLE_MAPPER : ROLE_REDUCER;
    int id = is_mapper(w) ? w : w - cfg->M;
    wlog("-> %s: %s cp=%ld rid=%d", wname(w), msg_type_name(type), cp, rid);
    udp_send(job->fd, worker_port(cfg->base_port, cfg->M, role, id), &m);
}

/*
 * TODO (Task 3)
 */
static void start_recovery(void)
{
}

/* One message from a worker. */
static void handle(const msg_t *m, long now)
{
    if ((m->role != ROLE_MAPPER && m->role != ROLE_REDUCER) || m->id < 0 ||
        m->id >= (m->role == ROLE_MAPPER ? cfg->M : cfg->R))
        return;
    int w = worker_index(cfg->M, m->role, m->id);

    if (m->type == MT_HEARTBEAT) {
        /* Ignore heartbeats from a process we already replaced. */
        if (m->count == (int64_t)job->pid[w]) {
            if (!ws[w].heard)
                wlog("%s is up (pid %ld)", wname(w), (long)m->count);
            ws[w].last_hb = now;
            ws[w].heard = 1;
        }
        return;
    }

    wlog("<- %s: %s cp=%ld rid=%d (mine: rid=%d, phase %s)", wname(w),
         msg_type_name(m->type), (long)m->checkpoint_id, m->recovery_id,
         rid, phase_name[phase]);

    /* TODO (Task 3): ignore messages from before the last rollback. */

    switch (m->type) {
        case MT_CHECKPOINT_ACK:
            if(phase == PH_CP && m->recovery_id == rid && checkpoint_in_progress != NO_CHECKPOINT && m->checkpoint_id == checkpoint_in_progress){
                ws[w].last_cp = (long)m->checkpoint_id;
            }
            break;
        case MT_LAST_CHECKPOINT_ACK:
            if(m->recovery_id == rid && m->checkpoint_id == LAST_CHECKPOINT_ID){
                ws[w].last_cp = LAST_CHECKPOINT_ID;
            }
            break;
        case MT_DONE:
            if(is_mapper(w) && m->recovery_id == rid){
                ws[w].done = 1;
            }
            break;
        case MT_RECOVERY_ACK:
            /* TODO (Task 3) */
            break;
        default:
        break;
    }
}

/* Returns 0 when the job is complete; any other value is a fatal error. */
int coordinator_loop(const config_t *c, job_t *j)
{
    cfg = c;
    job = j;
    long now = now_ms();
    for (int w = 0; w < job->n; w++) {
        memset(&ws[w], 0, sizeof ws[w]);
        ws[w].last_hb = now;
        ws[w].last_cp = NO_CHECKPOINT;
    }
    phase = PH_CP;
    rid = 0;

    next_cp_id = 1;
    checkpoint_in_progress = NO_CHECKPOINT;
    last_checkpoint_ms = now;

    for (;;) {
        chaos_tick(job);        /* PROVIDED: do not remove */

        struct pollfd p = { .fd = job->fd, .events = POLLIN, .revents = 0 };
        if (poll(&p, 1, POLL_MS) < 0 && errno != EINTR) {
            perror("poll");
            return 1;
        }
        now = now_ms();

        msg_t m;
        int r;
        while ((r = udp_recv(job->fd, &m)) == 1)
            handle(&m, now);
        if (r < 0) {
            perror("udp_recv");
            return 1;
        }

        reap_children(job);     /* bookkeeping only */

        if (phase == PH_EXITING) {
            int all_exited = 1;
            
            for(int w = 0 ; w< job->n; w++){
                if(!job->exited[w]){
                    all_exited = 0;
                    break;
                }
            }

            if(all_exited){
                return 0;
            }
            continue;
        }

        /* TODO (Task 3) */
        (void)start_recovery;

        if(phase == PH_CP){

            // check whether the active checkpoint progress is complete
            if(checkpoint_in_progress != NO_CHECKPOINT){
                int all_acked = 1;

                for(int w=0 ; w< job->n ; w++){
                    if(ws[w].last_cp != checkpoint_in_progress){
                        all_acked = 0;
                        break;
                    }
                }

                if(all_acked){
                    wlog("checkpoint %ld complete", checkpoint_in_progress);
                    checkpoint_in_progress = NO_CHECKPOINT;
                }

            }            

            // Check whether the all the mappers has finished it's files
            int all_done = 1;

            for(int w=0 ; w<cfg->M; w++){
                if(!ws[w].done){
                    all_done = 0;
                    break;
                }
            }

            if(all_done && checkpoint_in_progress == NO_CHECKPOINT){
                set_phase(PH_LAST_CP);

                //request final checkpoint for each mapper
                for (int w = 0; w < cfg->M; w++) {
                    send_to(w, MT_CHECKPOINT, LAST_CHECKPOINT_ID);
                }
            // otherwise initiate a checkpoint when time
            }else if(!all_done && checkpoint_in_progress == NO_CHECKPOINT && cfg->interval_ms > 0 && now - last_checkpoint_ms >= cfg->interval_ms){
                checkpoint_in_progress = next_cp_id++;
                last_checkpoint_ms = now;
                wlog("starting periodic checkpoint %ld", checkpoint_in_progress);
                for(int w=0; w<cfg->M ; w++){
                    send_to(w, MT_CHECKPOINT, checkpoint_in_progress);
                }
            }

        }else if(phase == PH_LAST_CP){
            //wait for all mappers and reducers to ack checkpoint 0
            int all_acked = 1;
            for(int w = 0; w< job->n ; w++){
                if (ws[w].last_cp != LAST_CHECKPOINT_ID) {
                    all_acked = 0;
                    break;
                }
            }

            if(all_acked){
                for(int i=0 ; i<job->n ; i++){
                    send_to(i, MT_EXIT, LAST_CHECKPOINT_ID);
                }
                set_phase(PH_EXITING);
            }
        }


    }
}
