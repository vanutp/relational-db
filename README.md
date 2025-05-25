Run tests with `cargo test --release`

Run cli with `cargo run --release`

You can change the data directory by setting the `STORAGE_DIR` environment variable

Example queries:

- `create table meow (id int, name string, height double, is_fox bool)`
- `insert into meow values (1, 'vanutp', 182.5, true)`
- `select * from meow where name = 'vanutp'`
- `update meow set height = -1. where name = 'vanutp'`
- `delete from meow where is_fox = false`
- `drop table meow`
