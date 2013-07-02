#include "lightkv.h"
#include <sys/mman.h> /* mmap inside */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h> /* file open modes and stuff */
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <math.h>
#include <inttypes.h>
#include "helper.h"
#include "errors.h"
#include "logger.h"
#include <unistd.h>
#include <pthread.h>

freeloc *freelist_add(freeloc *head, freeloc *n) {
    if (head) {
        head->prev = n;
        n->next = head;
    }
    return n;
}

freeloc *freelist_get(freeloc *head, uint32_t size) {
    if (head == NULL) {
        return NULL;
    }

    freeloc *f = NULL;
    uint32_t diff;

    while (head) {
        if (f == NULL) {
            f = head;
            diff = get_slotsize(head->l.l.sclass) - size;
        } else {
            uint32_t t;
            t = get_slotsize(head->l.l.sclass) - size;
            if (t < diff) {
                diff = t;
                f = head;
            }
        }
        head = head->next;
    }

    return f;
}

freeloc *freelist_remove(freeloc *head, freeloc *f) {
    if (head == NULL) {
        return NULL;
    }

    if (f) {
        if (f == head) {
            if (f->next) {
                f->next->prev = NULL;
            }
            head = f->next;
        } else {
            freeloc *tmp;
            tmp = f->next;
            if (tmp) {
                tmp->prev = f->prev;
            }

            f->prev->next = tmp;
        }

        free(f);
    }

    return head;
}

int alloc_file(char *filepath, size_t size) {
    int fd;
    int rv;

    fd = open(filepath, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return fd;
    }

    // Fill the file with zeros
    lseek (fd, size-2, SEEK_SET);
    if ((rv = write(fd, "",1)) < 0) {
        return rv;
    }

    return 0;
}

int map_file(void **map, char *filepath) {
    int fd;
    struct stat st;
    fd = open(filepath, O_RDWR);
    if (fd < 0) {
        return fd;
    }

    errno = 0;
    if (fstat(fd, &st) < 0) {
        return errno;
    }

    errno = 0;
    *map = mmap(0, st.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (errno < 0) {
        return errno;
    }

    close(fd);
    return 0;
}


loc create_nextloc(lightkv *kv, uint32_t size) {
    loc next = kv->end_loc;
    next.l.sclass = 0;
    uint64_t off = kv->end_loc.l.offset + size + 2;
    if (off > MAX_FILESIZE) {
        // FIXME: in prealloc, its different.

        next.l.num++;
        next.l.offset = 1;
        kv->nfiles++;

        char *f = getfilepath(kv->basepath, next.l.num);
        alloc_file(f, MAX_FILESIZE);

        if (map_file(&kv->filemaps[next.l.num], f) < 0) {
            assert(false);
        }
        free(f);
        // Put this space to freelist
        uint32_t remaining = MAX_FILESIZE - (kv->end_loc.l.offset + 1);
        if (remaining >= RECORD_HEADER_SIZE) {
            loc rm;
            rm.l.num = kv->end_loc.l.num;
            rm.l.offset = kv->end_loc.l.offset + 1;
            rm.l.sclass = get_sizeslot(roundsize(remaining));
            // cannot find a suitable bucket
            // Place an end pointer
            // FIXME: split remaining into maximum free buckets
            if (get_slotsize(rm.l.sclass) > remaining) {
                record rh;
                rh.type = RECODE_END;
                rh.len = remaining;
                write_record(kv, rm, (record *) &rh);
            } else {
                lightkv_delete(kv, rm.val);
            }

        }
    } else {
       next.l.offset++;
    }

    return next;
}

int write_record(lightkv *kv, loc l, record *rec) {
    char *dst;

    dst = (char *) kv->filemaps[l.l.num] + l.l.offset;
    memcpy(dst, rec, rec->len);

    return rec->len;
}

int read_record(lightkv *kv, loc l, record **rec) {
    char *src;
    size_t slotsize = get_slotsize(l.l.sclass);
    *rec = (record *) malloc(slotsize);
    src = (char *) kv->filemaps[l.l.num] + l.l.offset;
    memcpy(*rec, src, slotsize);
    print_record(*rec);

    return 0;
}

record read_recheader(lightkv *kv, loc l) {
    record rh;
    char *src = (char *) kv->filemaps[l.l.num] + l.l.offset;
    memcpy((char *) &rh, src, sizeof(rh));
    return rh;
}

int lightkv_init(lightkv **kv, char *base, bool prealloc) {
    // TODO: Add sanity checks

    *kv = (lightkv *) malloc(sizeof(lightkv));
    assert(*kv > 0);

    (*kv)->prealloc = prealloc;
    (*kv)->basepath = base;
    (*kv)->filemaps[0] = NULL;
    (*kv)->nfiles = 1;

    int i;
    for (i=0; i <MAX_SIZES; i++) {
        (*kv)->freelist[i] = NULL;
    }

    char *f = getfilepath(base, 0);
    if (access(f, F_OK ) != -1) {
        int num = 0;
        char *fn;
        (*kv)->has_scanned = false;

        while (1) {
            fn = getfilepath(base, num);
            if (access(fn, F_OK|R_OK|W_OK) != -1) {
                if (map_file(&(*kv)->filemaps[num], fn) < 0) {
                    assert(false);
                }
                num++;
                (*kv)->nfiles = num;
                free(fn);
            } else {
                free(fn);
                break;
            }
        }
    } else {
        (*kv)->has_scanned = true;
        alloc_file(f, MAX_FILESIZE);

        if (map_file(&(*kv)->filemaps[0], f) < 0) {
            assert(false);
        }
    }

    free(f);

    // FIXME: fix loc pointers
    loc x;
    x.l.num = 0;
    x.l.offset = 0;

    (*kv)->end_loc = x;
    x.l.offset = 1;
    (*kv)->start_loc = x;

    return 0;
}

// Create a VAL or DEL record. Pass recsize = 0 for VAL record.
record *create_record(uint8_t type, char *key, char *val, size_t len, size_t recsize) {
    record *rec = NULL;

    if (type == RECORD_VAL) {
        int keylen = strlen(key);
        recsize = RECORD_HEADER_SIZE + keylen + len;
        rec = (record *) calloc(recsize, 1);
        rec->type = type;
        rec->len = recsize;
        rec->extlen = keylen;
        memcpy((char *) rec + RECORD_HEADER_SIZE, key, keylen);
        memcpy((char *) rec + RECORD_HEADER_SIZE + keylen, val, len);
    } else if (type == RECORD_DEL) {
        rec = (record *) calloc(recsize, 1);
        rec->type = type;
        rec->len = recsize;
    }

    return rec;
}

loc find_freeloc(lightkv *kv, size_t size) {

    loc l;
    int slot = get_sizeslot(size);

    freeloc *f = freelist_get(kv->freelist[slot], size);
    if (f) {
        l = f->l;
        kv->freelist[slot] = freelist_remove(kv->freelist[slot], f);
    } else {
        l = create_nextloc(kv, size);
        kv->end_loc.l.num = l.l.num;
        kv->end_loc.l.offset = l.l.offset + size - 1;
    }
    l.l.sclass = slot;

    return l;
}

uint64_t lightkv_insert(lightkv *kv, char *key, char *val, uint32_t len) {
    debug_log("Operation:Insert, key:%s vallen:%d", key, len);
    size_t size, keylen;
    loc diskloc;

    record *rec = create_record(RECORD_VAL, key, val, len, 0);
    int rsize = roundsize(rec->len);
    diskloc = find_freeloc(kv, rsize);
    int l = write_record(kv, diskloc, rec);
    free(rec);

    debug_log("Operation:Insert, completed at target:"LOCSTR, LOCPARAMS(diskloc));
    return diskloc.val;
}

bool lightkv_get(lightkv *kv, uint64_t recid, char **key, char **val, uint32_t *len) {
    bool rv;
    record *rec;
    loc l;
    l.val = recid;
    debug_log("Operation:Get, target:"LOCSTR, LOCPARAMS(l));

    read_record(kv, l, &rec);
    rv = rec->type == RECORD_VAL ? true: false;
    if (rv == false) {
        return false;
    }

    *key = get_key(rec);
    *len = get_val(rec, val);
    free(rec);

    debug_log("Operation:Get, fetched %s of vallen:%d", *key, *len);
    return true;
}

bool lightkv_delete(lightkv *kv, uint64_t recid) {
    // TODO: basic sanity
    loc l;
    l.val = recid;
    debug_log("Operation:Delete, target:"LOCSTR, LOCPARAMS(l));

    size_t slotsize = get_slotsize(l.l.sclass);
    record *rec = create_record(RECORD_DEL, NULL, NULL, 0, slotsize);
    write_record(kv, l, rec);
    freeloc *f = freeloc_new(l);
    kv->freelist[l.l.sclass] = freelist_add(kv->freelist[l.l.sclass], f);
    free(rec);
    return true;
}

uint64_t lightkv_update(lightkv *kv, uint64_t recid, char *key, char *val, uint32_t len) {
    loc l;
    l.val = recid;
    debug_log("Operation:Update, target:"LOCSTR" key:%s vallen:%d", LOCPARAMS(l), key, len);

    size_t slotsize = get_slotsize(l.l.sclass);
    record *rec = create_record(RECORD_VAL, key, val, len, 0);

    // We need to find a new slot
    if (rec->len > slotsize) {
        lightkv_delete(kv, recid);
        int rsize = roundsize(rec->len);
        l = find_freeloc(kv, rsize);
    }

    write_record(kv, l, rec);

    debug_log("Operation:Update, completed at target:"LOCSTR, LOCPARAMS(l));
    return l.val;
}

lightkv_iter *lightkv_iterator(lightkv *kv) {
    lightkv_iter *iter = (lightkv_iter *) malloc(sizeof(lightkv_iter));
    iter->store = kv;
    iter->current = kv->start_loc;
    return iter;
}

bool lightkv_next(lightkv_iter *iter, char **key, char **val, uint32_t *len) {
    record *rec;
    bool rv, cont = true;

    while (cont) {
        cont = false;
        if ((uint64_t) iter->current.l.offset + RECORD_HEADER_SIZE >= MAX_FILESIZE) {
            if (iter->current.l.num + 1 < iter->store->nfiles) {
                iter->current.l.num++;
                iter->current.l.offset = 1;
            } else {
                return false;
            }
        }

        record rh = read_recheader(iter->store, iter->current);
        size_t rsize = roundsize(rh.len);
        iter->current.l.sclass = get_sizeslot(rsize);

        if (rh.type == RECORD_NULL) {
            iter->store->has_scanned = true;
            rv = false;
        } else if (rh.type == RECODE_END) {
            cont = true;
        } else if (rh.type == RECORD_VAL) {
            read_record(iter->store, iter->current, &rec);
            *key = get_key(rec);
            *len = get_val(rec, val);
            free(rec);
            rv = true;

        } else if (rh.type == RECORD_DEL) {
            if (iter->store->has_scanned == false) {
                freeloc *f = freeloc_new(iter->current);
                iter->store->freelist[iter->current.l.sclass] = freelist_add(iter->store->freelist[iter->current.l.sclass], f);
            }
            cont = true;
        }

        if (iter->store->has_scanned == false) {
            iter->store->end_loc = iter->current;
            iter->store->end_loc.l.offset += rsize - 1;
        }

        iter->current.l.offset += rsize;

        if (cont) continue;

        return rv;
    }

    return false;
}

void lightkv_free_iter(lightkv_iter *iter) {
    free(iter);
}

void lightkv_sync(lightkv *kv) {
    int i;
    for (i=0; i < kv->nfiles; i++) {
        msync(kv->filemaps[i], MAX_FILESIZE, MS_SYNC);
    }
}

main() {
    lightkv *kv;
    lightkv_init(&kv,(char *)  "/tmp/", true);
    uint64_t rid;
    char *k,*v;
    uint32_t l;

    /*
    rid = lightkv_insert(kv, "test_key1", "hello", 5);

    rid = lightkv_insert(kv, "test_key2", "helli", 5);
    lightkv_delete(kv, rid);



    lightkv_get(kv, rid, &k, &v, &l);

    rid = lightkv_update(kv, rid, "test_upd", "updat", 5);
    rid = lightkv_update(kv, rid, "test_update-large", "1234567890", 10);
    */

    int i;
    for (i=0; i < 5000; i++) {
        char *st = (char *) calloc(10,1);
        sprintf(st, "key_%d", i);

        rid = lightkv_insert(kv, st, (char *) "hell3", 5);
    }
    lightkv_iter *it = lightkv_iterator(kv);

    i = 0;
    while (lightkv_next(it, &k, &v, &l)) {
        i++;
    }
    debug_log("read %d items", i);

}
