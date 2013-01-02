Rust bindings for [LevelDB](https://code.google.com/p/leveldb/)

Sample Code
-----------

```rust
extern mod leveldb;

use leveldb::create_if_missing;

fn main() {
    match leveldb::open([create_if_missing], "sample.leveldb") {
      result::Ok(db) => {
        db.put([], "key", "value");
        let value = db.get([], "key");
        io::println(fmt!("value for key is \"%s\"", value.unwrap()));
      }
      result::Err(e) => { io::println(fmt!("open error: %s" , e)); }
    }
}
```
