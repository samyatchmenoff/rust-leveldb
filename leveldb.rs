#[link(name = "leveldb",
       vers = "0.1.0",
       uuid = "122bed0b-c19b-4b82-b0b7-7ae8aead7297",
       url = "http://github.com/thestinger/rust-leveldb")];

#[comment = "Rust binding for LevelDB"];
#[license = "BSD"];
#[crate_type = "lib"];

extern mod std;

use core::result::{Err, Ok, Result};
use core::ptr::{is_null, null};
use core::libc::{c_char, c_int, c_void, size_t};

pub struct db {
    priv db: *leveldb_t,
}

unsafe fn consume_buf(s: *c_char, len: size_t) -> Option<~[u8]> {
    if is_null(s) {
        None
    } else {
        let e = vec::raw::from_buf_raw(s as *u8, len as uint);
        leveldb_free(s as *c_void);
        Some(e)
    }
}

unsafe fn consume_s(s: *c_char) -> Option<~str> {
    if is_null(s) {
        None
    } else {
        let e = str::raw::from_c_str(s);
        leveldb_free(s as *c_void);
        Some(e)
    }
}

pub fn open(opts: &[Flag], name: &str) -> Result<db, ~str> {
    unsafe {
        let err: *c_char = null();
        str::as_c_str(name, |cname| {
            let copts = to_c_options(opts);
            let r = leveldb_open(copts, cname, &err);
            leveldb_options_destroy(copts);
            match consume_s(err) {
                Some(e) => Err(e),
                None => Ok(db{db: r})
            }
        })
    }
}

enum leveldb_t {}
enum leveldb_cache_t {}
enum leveldb_comparator_t {}
enum leveldb_env_t {}
enum leveldb_filelock_t {}
enum leveldb_iterator_t {}
enum leveldb_logger_t {}
enum leveldb_options_t {}
enum leveldb_randomfile_t {}
enum leveldb_readoptions_t {}
enum leveldb_seqfile_t {}
enum leveldb_snapshot_t {}
enum leveldb_writablefile_t {}
enum leveldb_writebatch_t {}
enum leveldb_writeoptions_t {}

#[link_args="-lpthread -lstdc++ -lleveldb -Wl,--no-as-needed -lsnappy"]
extern "C" {
    // DB operations

    fn leveldb_open(options: *const leveldb_options_t, name: *const c_char,
                    errptr: **c_char) -> *leveldb_t;

    fn leveldb_close(db: *leveldb_t);

    fn leveldb_get(db: *leveldb_t, options: *const leveldb_readoptions_t,
                   key: *const u8, keylen: size_t, vlen: *const size_t,
                   errptr: **c_char) -> *c_char;

    fn leveldb_put(db: *leveldb_t, options: *const leveldb_writeoptions_t,
                   key: *const u8, keylen: size_t, val: *const u8,
                   vallen: size_t, errptr: **c_char);

    fn leveldb_delete(db: *leveldb_t, options: *const leveldb_writeoptions_t,
                      key: *const u8, keylen: size_t, errptr: **c_char);

    fn leveldb_write(db: *leveldb_t, options: *const leveldb_writeoptions_t,
                     batch: *leveldb_writebatch_t, errptr: **c_char);

    fn leveldb_create_iterator(db: *leveldb_t,
                               options: *const leveldb_readoptions_t) ->
     *leveldb_iterator_t;

    fn leveldb_create_snapshot(db: *leveldb_t) -> *const leveldb_snapshot_t;

    fn leveldb_release_snapshot(db: *leveldb_t,
                                snapshot: *const leveldb_snapshot_t);

    fn leveldb_property_value(db: *leveldb_t, propname: *const c_char);

    fn leveldb_approximate_sizes(db: *leveldb_t, num_ranges: c_int,
                                 range_start_key: *const *const u8,
                                 range_start_key_len: *const size_t,
                                 range_limit_key: *const *const u8,
                                 range_limit_key_len: *const size_t,
                                 sizes: *u64);

    // Management operations
    fn leveldb_destroy_db(options: *const leveldb_options_t,
                          name: *const c_char, errptr: **c_char);
    fn leveldb_repair_db(options: *const leveldb_options_t,
                         name: *const c_char, errptr: **c_char);

    // Iterator
    fn leveldb_iter_destroy(it: *leveldb_iterator_t);
    fn leveldb_iter_valid(it: *const leveldb_iterator_t) -> u8;
    fn leveldb_iter_seek_to_first(it: *leveldb_iterator_t);
    fn leveldb_iter_seek_to_last(it: *leveldb_iterator_t);
    fn leveldb_iter_seek(it: *leveldb_iterator_t, k: *const u8, klen: size_t);
    fn leveldb_iter_next(it: *leveldb_iterator_t);
    fn leveldb_iter_prev(it: *leveldb_iterator_t);
    fn leveldb_iter_key(it: *const leveldb_iterator_t, klen: size_t) ->
     *const u8;
    fn leveldb_iter_value(it: *const leveldb_iterator_t, vlen: *size_t) ->
     *const u8;
    fn leveldb_iter_get_error(it: *const leveldb_iterator_t,
                              errptr: **c_char);

    // Write batch

    fn leveldb_writebatch_create() -> *leveldb_writebatch_t;
    fn leveldb_writebatch_destroy(batch: *leveldb_writebatch_t);
    fn leveldb_writebatch_clear(batch: *leveldb_writebatch_t);
    fn leveldb_writebatch_put(batch: *leveldb_writebatch_t, key: *const u8,
                              klen: size_t, val: *const u8, klen: size_t);
    fn leveldb_writebatch_delete(batch: *leveldb_writebatch_t, key: *const u8,
                                 klen: size_t);

    /* TODO: requires support for exposing code to C
    fn leveldb_writebatch_iterate(batch: *leveldb_writebatch_t, state: *u8,
        void (*put)(void*, const char* k, size_t klen,
        const char* v, size_t vlen),
        void (*deleted)(void*, const char* k, size_t klen));
        put: *u8, delete: *u8);
    */

    // Options
    fn leveldb_options_create() -> *leveldb_options_t;
    fn leveldb_options_destroy(options: *leveldb_options_t);
    fn leveldb_options_set_comparator(options: *leveldb_options_t,
                                      c: *leveldb_comparator_t);
    fn leveldb_options_set_create_if_missing(options: *leveldb_options_t,
                                             x: u8);
    fn leveldb_options_set_error_if_exists(options: *leveldb_options_t,
                                           x: u8);
    fn leveldb_options_set_paranoid_checks(options: *leveldb_options_t,
                                           x: u8);
    fn leveldb_options_set_env(options: *leveldb_options_t,
                               env: *leveldb_env_t);
    fn leveldb_options_set_info_log(options: *leveldb_options_t,
                                    g: *leveldb_logger_t);
    fn leveldb_options_set_write_buffer_size(options: *leveldb_options_t,
                                             x: size_t);
    fn leveldb_options_set_max_open_files(options: *leveldb_options_t,
                                          x: c_int);
    fn leveldb_options_set_block_size(options: *leveldb_options_t, x: size_t);
    fn leveldb_options_set_block_restart_interval(options: *leveldb_options_t,
                                                  x: c_int);
    fn leveldb_options_set_compression(options: *leveldb_options_t, z: c_int);

    // Read options
    fn leveldb_readoptions_create() -> *leveldb_readoptions_t;
    fn leveldb_readoptions_destroy(ropts: *leveldb_readoptions_t);
    fn leveldb_readoptions_set_verify_checksums(ropts: *leveldb_readoptions_t,
                                                v: u8);
    fn leveldb_readoptions_set_fill_cache(ropts: *leveldb_readoptions_t,
                                          v: u8);
    fn leveldb_readoptions_set_snapshot(ropts: *leveldb_readoptions_t,
                                        snapshot: *leveldb_snapshot_t);

    // Write options
    fn leveldb_writeoptions_create() -> *leveldb_writeoptions_t;
    fn leveldb_writeoptions_destroy(options: *leveldb_writeoptions_t);
    fn leveldb_writeoptions_set_sync(options: *leveldb_writeoptions_t, v: u8);

    // Cache
    fn leveldb_cache_create_lru(capacity: size_t) -> *leveldb_cache_t;
    fn leveldb_cache_destroy(cache: *leveldb_cache_t);

    // Env
    fn leveldb_create_default_env() -> *leveldb_env_t;
    fn leveldb_env_destroy(env: *leveldb_env_t);

    // Utility
    fn leveldb_free(ptr: *c_void);
}

type write_batch = *leveldb_writebatch_t;

// type compression_type = int;

static no_compression: c_int = 0;
static snappy_compression: c_int = 1;

pub enum Flag {
    create_if_missing,
    error_if_exists,
    paranoid_checks,
    // env,
    // log,
    write_buffer_size(size_t),
    max_open_files(c_int),
    // block_cache(),
    block_size(size_t),
    block_restart_interval(c_int),
    compression(c_int),
}

type snapshot = *leveldb_snapshot_t;

pub enum ReadFlag { verify_checksum, full_cache, use_snapshot(snapshot), }

pub enum WriteFlag { sync, }

fn to_c_options(opts: &[Flag]) -> *leveldb_options_t {
    unsafe {
        let copts = leveldb_options_create();
        for opts.each |&o| {
            match o {
              create_if_missing => {
                leveldb_options_set_create_if_missing(copts, 1);
              }
              error_if_exists => {
                leveldb_options_set_error_if_exists(copts, 1);
              }
              paranoid_checks => {
                leveldb_options_set_paranoid_checks(copts, 1);
              }
              // env;
              // log
              write_buffer_size(sz) => {
                leveldb_options_set_write_buffer_size(copts, sz);
              }
              max_open_files(num) => {
                leveldb_options_set_max_open_files(copts, num);
              }
              // block_cache();
              block_size(sz) => {
                leveldb_options_set_block_size(copts, sz);
              }
              block_restart_interval(int) => {
                leveldb_options_set_block_restart_interval(copts, int);
              }
              compression(ct) => { leveldb_options_set_compression(copts, ct); }
            }
        }
        copts
    }
}

fn to_c_readoptions(opts: &[ReadFlag]) -> *leveldb_readoptions_t {
    unsafe {
        let copts = leveldb_readoptions_create();
        for opts.each |&o| {
            match o {
              verify_checksum => {
                leveldb_readoptions_set_verify_checksums(copts, 1);
              }
              full_cache => { leveldb_readoptions_set_fill_cache(copts, 1); }
              use_snapshot(snapshot) => {
                leveldb_readoptions_set_snapshot(copts, snapshot);
              }
            }
        }
        copts
    }
}

fn to_c_writeoptions(opts: &[WriteFlag]) -> *leveldb_writeoptions_t {
    unsafe {
        let copts = leveldb_writeoptions_create();
        for opts.each |&o| {
            match o { sync => { leveldb_writeoptions_set_sync(copts, 1); } }
        }
        copts
    }
}

impl Drop for db {
    fn finalize(&self) { unsafe { leveldb_close(self.db) } }
}

impl db {
    fn get(&self, ropts: &[ReadFlag], key: &[u8]) -> Option<~[u8]> {
        unsafe {
            let vlen: size_t = 0;
            let err: *c_char = null();
            vec::as_imm_buf(key, |kb, klen| {
                let copts = to_c_readoptions(ropts);
                let r = leveldb_get(self.db, copts, kb, klen as size_t,
                                    &vlen, &err);
                leveldb_readoptions_destroy(copts);

                match consume_s(err) {
                    Some(e) => fail!(e),
                    None => consume_buf(r, vlen)
                }
            })
        }
    }

    fn put(&self, opts: &[WriteFlag], key: &[u8], val: &[u8]) {
        unsafe {
        let err: *c_char = null();
            vec::as_imm_buf(key, |bk, klen| {
                vec::as_imm_buf(val, |bv, vlen| {
                    let copts = to_c_writeoptions(opts);
                    leveldb_put(self.db, copts, bk, klen as size_t, bv,
                                vlen as size_t, &err);
                    leveldb_writeoptions_destroy(copts);
                });
            });

            match consume_s(err) { Some(e) => fail!(e), None => () }
        }
    }

    fn delete(&self, opts: &[WriteFlag], key: &[u8]) {
        unsafe {
            let err: *c_char = null();
            vec::as_imm_buf(key, |bk, klen| {
                let copts = to_c_writeoptions(opts);
                leveldb_delete(self.db, copts, bk, klen as size_t, &err);
                leveldb_writeoptions_destroy(copts);
            });
            match consume_s(err) { Some(e) => fail!(e), None => () }
        }
    }

    fn write(&self, opts: &[WriteFlag], wb: write_batch) {
        unsafe {
            let copts = to_c_writeoptions(opts);
            let err: *c_char = null();
            leveldb_write(self.db, copts, wb, &err);
            match consume_s(err) { Some(e) => fail!(e), None => () }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::str::as_bytes_slice;

    // TODO: should use a proper temporary directory

    #[test]
    fn test() {
        let db = open([create_if_missing], "/tmp/testdb").unwrap();
        let key = as_bytes_slice("foo");
        db.put([], key, as_bytes_slice("bar"));
        assert!(db.get([], key) == Some(vec::from_slice(as_bytes_slice("bar"))));
        db.put([], key, as_bytes_slice("baz"));
        assert!(db.get([], key) == Some(vec::from_slice(as_bytes_slice("baz"))));
        db.delete([], key);
        assert!(db.get([], key).is_none());
        db.delete([], key);
    }

    #[test]
    fn test_missing() { assert!(open([], "/tmp/testdb-missing").is_err()) }

    #[test]
    fn test_error_if_exists() {
        assert!(open([create_if_missing], "/tmp/testdb-exists").is_ok());
        assert!(open([error_if_exists], "/tmp/testdb-missing").is_err());
    }
}
