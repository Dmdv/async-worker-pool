#include "internal.h"

int awp_frame_pool_init(awp_frame_pool_t *p, uint32_t size)
{
    uint32_t i;
    if (!p || size < 1)
        return -EINVAL;
    memset(p, 0, sizeof(*p));
    p->slab = calloc(size, sizeof(awp_frame_t));
    p->free_list = calloc(size, sizeof(awp_frame_t *));
    if (!p->slab || !p->free_list) {
        free(p->slab);
        free(p->free_list);
        return -ENOMEM;
    }
    p->size = size;
    p->free_count = size;
    for (i = 0; i < size; i++)
        p->free_list[i] = &p->slab[i];
    atomic_store(&p->closed, 0);
    if (pthread_mutex_init(&p->mu, NULL) != 0)
        goto fail;
    if (pthread_cond_init(&p->has_free, NULL) != 0) {
        pthread_mutex_destroy(&p->mu);
        goto fail;
    }
    return 0;
fail:
    free(p->slab);
    free(p->free_list);
    return -ENOMEM;
}

void awp_frame_pool_destroy(awp_frame_pool_t *p)
{
    if (!p)
        return;
    pthread_cond_destroy(&p->has_free);
    pthread_mutex_destroy(&p->mu);
    free(p->free_list);
    free(p->slab);
    memset(p, 0, sizeof(*p));
}

void awp_frame_pool_close(awp_frame_pool_t *p)
{
    if (!p)
        return;
    atomic_store(&p->closed, 1);
    pthread_mutex_lock(&p->mu);
    pthread_cond_broadcast(&p->has_free);
    pthread_mutex_unlock(&p->mu);
}

awp_frame_t *awp_frame_pool_acquire(awp_frame_pool_t *p)
{
    awp_frame_t *f;
    if (!p)
        return NULL;
    pthread_mutex_lock(&p->mu);
    while (p->free_count == 0 && !atomic_load(&p->closed))
        pthread_cond_wait(&p->has_free, &p->mu);
    if (p->free_count == 0) {
        pthread_mutex_unlock(&p->mu);
        return NULL;
    }
    f = p->free_list[--p->free_count];
    pthread_mutex_unlock(&p->mu);
    memset(f, 0, sizeof(*f));
    return f;
}

void awp_frame_pool_release(awp_frame_pool_t *p, awp_frame_t *f)
{
    if (!p || !f)
        return;
    pthread_mutex_lock(&p->mu);
    if (p->free_count < p->size) {
        p->free_list[p->free_count++] = f;
        pthread_cond_signal(&p->has_free);
    }
    pthread_mutex_unlock(&p->mu);
}
