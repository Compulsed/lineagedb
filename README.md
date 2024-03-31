# Lineage DB

Lineage DB is an educational MVCC database and has the following functionality:
1. Supports ACID transactions (* everything currently runs as serialization)
1. Utilizes a WAL for performant writes / supports trimming the WAL
1. Time travel; query the database at any given transaction id (* assuming the previous transactions are untrimmed)
1. For any given item can look at all revisions (* assuming the previous transactions are untrimmed)
1. Supports index based queries

Current limitations:
1. Does not support session based transactions, statements in a transaction must be sent all at once
1. Does not support DDL statements, at the moment the system is limited to a single entity (Person)
1. Database Multi-threading is limited to a single writer / multiple readers (via a Reader Writer database lock).
   1. This limits reading whilst writing, once correctly implemented MVCC should allow reads while there are writes
1. The working dataset must fit entirely within memory, there is no storage pool / disk paging
1. Does not have an SQL frontend
1. Has limited querying capabilities, just `AND`, no `OR`, `IN`, etc.
1. Does not clean up older item versions -- should implement this by looking at the oldest transaction and cleaning up items before that TX id
1. Version compression, for each new version we make a clean copy of all of the previous versions' data


## How to use 

To play around / interact with the database I have provided a GraphQL / TCP client, though, you could implement your own frontend.

The database is sufficiently isolated, this means it exists in its own crate / is independent from any clients.

**Start the database**
`cargo run`

Open `http://0.0.0.0:9000/graphiql`

Can use the below mutations to persist / get data
```
# Create
mutation writeHuman {
  createHuman(newHuman: { fullName: "Frank Walker" }) {
    id
    fullName
    email
  }
}

# Create builk
mutation createHumans ($newHumans: [NewHuman!]!) {
  createHumans(newHumans: $newHumans) {
    id
    fullName
    email
  }
}

{
  "newHumans": [
    { "fullName": "test1", "email": "dalejohnsalter@gmail.com" },
    { "fullName": "test2", "email": null }
  ]
}

# Update
mutation updateHuman {
  updateHuman(id: "53db1e6f-4b90-4d3d-8871-b24288bf9192", updateHuman: { email: "1233@gmail.com"}) {
    id
    fullName
    email
  }
}

# Use ID in mutation response to get the human
query queryHuman {
  human (id: "bf5567e4-1d4e-4451-aeb3-449cdd2970be") {
    id
    fullName
    email
  }
}

# List
query listHuman {
  listHuman {
    id
    fullName
    email
  }
}

query listHumanWithQuery {
  listHuman(query: { fullName: "test1" }) {
    id
    fullName
    email
  }
}

mutation dbSnapshot {
  snapshot
}


mutation dbReset {
  reset
}
```

**CLI**
An optional CLI is provided for various configuration options

```
📀 Lineagedb GraphQL Server, provides a simple GraphQL interface for interacting with the database

Usage: lineagedb [OPTIONS]

Options:
  -d, --data <DATA>        Location of the database. Reads / writes to this directory. Note: Does not support shell paths, e.g. ~ [default: data]
  -p, --port <PORT>        Port the graphql server will run on [default: 9000]
  -a, --address <ADDRESS>  Address the graphql server will run on [default: 0.0.0.0]
  -h, --help               Print help
```

**Debugging**

```
# Prints out logs from the database (skips GraphQL)
RUST_LOG=lineagedb cargo run

# Prints out full exception strings
RUST_BACKTRACE=1 cargo run
```

**Other binaries**

```
cargo run --package tcp-server --bin lineagedb-tcp-server
```

**Performance**

Tested on an M1 Mac.

```
Read speed ~800k statements/s
Write speed ~80k statements/s
```

**Testing / Benchmarking**

```
# Quick functional unit tests
cargo test --all

# Running performance unit tests
# Notes:
# 1. Running these tests one after another will yield different results to
#   running them individually. I suspect this could be because the OS' cleaning up allocated memory.
# 2. These tests will yield different results based on whether the laptop is charging or not
cargo test --package database "database::database::tests::bulk" -- --nocapture --ignored --test-threads=1

# Benchmarking https://bheisler.github.io/criterion.rs/book/user_guide/command_line_options.html#baselines
# There appears to be an issue with 'benchmark' that spin up multiple
#   threads in the database. It causes 100x performance lags. 800us to 80ms
# Seems that the performance unit tests are a more reliable indicator
cargo bench --all
cargo bench -- --save-baseline no-fsync # Saves the baseline to compare to another branch
```


## Notes

### Concurrent Read / Write

The database single threaded, this means neither reads or writes can be concurrent. Once they are concurrent we will need to address the following challenges:
1. Will need to move away from vectors to linked lists (unless we use a RW Lock). Resizing vectors is not thread 'safe'
1. Must create rust data structures that are both sync + send w/ some internal unsafe operations

### Session Transactions

Handling session based transactions will have the following challenges
1. Write-write conflicts, i.e. when two transactions update the same item, one will need to be rolled back
1. GraphQL might not be the right mechanism for managing transaction BEGIN; COMMIT; 

### Usage of MVCC

By using MVCC we do not need to implement the more complicated 2PL (2 Phase Locking) protocol.

## Areas of improvement

**GraphQL Feature**
- Get ✅
- GetVersion ✅
- List 
  - Basic ✅
  - Filtering 
    - AND ✅
    - OR
  - Using indexes ✅
- List at transaction id
- Create 
  - Single ✅ 
  - Bulk ✅
- Update
  - Single ✅
  - Bulk
- Delete
  - Single
  - Bulk
- Implement create / update / delete via GraphQL alias' (might be hard with existing library)

**DB Features**
- Transaction rollbacks ✅
- Transactions with multiple actions ✅
- Dynamic schema
- Lower level transaction levels (currently have max)
- Multiple tables support
- Counter (id counter)
- Multiple updates based on a condition (select)
- Where clause in list
- Update conditions
- Transaction queue (max length, 5s timeout)
- Referential integrity
- Does not support changes to the underlying snapshot / transaction else serialization / deserialization will fail

**Architecture**
- Split the database / clients components into their own libraries ✅
- Transaction log listener (can run another db in another location)
- Run on cloud via docker / lambda

**Performance**
- Create a tx/s metrics (1ms for ~100 reads / writes) ✅
- WAL ✅
- Is there a way to monitor rust performance? Like where are we spending the most time
- Is there a way to improve the performance of transaction writes?
  - i.e. we set the transaction log file to be larger than what we need
- Read at a transaction id whilst there is a writer — may require thread safe data structures
- Move away from a single thread per request (could implement a thread pool w/ channels?)
- Reduce the amount of rust clones
- Investigate ~6k TX stall from load testing (was using AB, and running on a Mac)
- Anywhere we would clone attempt to use an RC -- this happens with Actions (check performance after doing this)

**Design Improvements -- Internals**
- Clippy ✅
- CI/CD Pipeline ✅
- Improve error types -- it is not clear what part of the application can throw an error vs. an enum type response ✅
- Improve change the send_request to be 'action aware', as in, a single action should return a single response ✅
- Tests 
  - Areas:
    - GraphQL
    - Database 
      - Transaction Management ✅
    - Table (Applying / Rolling back changes)
      - Should test all exceptions
    - Row
  - Tooling
    - Rstest (can we use the fixture functionality to run the tests against different database states? empty, few transactions, etc)
    - Code coverage?
- CLI
    - Specify port to bind ✅
    - Specify IP to bind ✅
    - List database version (https://github.com/rust-lang/cargo/issues/6583)
- Turn index into a class
- Create a 'storage engine' abstraction. At the moment this is the responsibility of the transaction manager
- Transaction that just contain queries should not be persisted to the transaction log
- Do not need to maintain the transaction log in memory -- Transation log can just use a reference
- Updating action format (e.g. adding additional params to list) causes parsing to break
- Try a faster / binary serialization format. JSON might be slow
- Versions are full clones of the data, if we use RC we would be be able to save on clones

## Resource
- https://www.youtube.com/watch?v=s19G6n0UjsM (explains epoch, lock free data structures)
- https://docs.rs/evmap/latest/evmap/
- https://github.com/penberg/tihku (MVCC database implemented w/ rust)
- https://github.com/crossbeam-rs/crossbeam/tree/master/crossbeam-skiplist

## My learnings

**To read**
- https://rust-unofficial.github.io/patterns/patterns/creational/builder.html
- https://rust-unofficial.github.io/patterns/additional_resources/design-principles.html

**Rust learnings**
1. NewType is great
2. match .into_iter().next() is a great way to get ownership / get the first item
3. When evaluating nested types in e.g. DatabaseResponseAction, it is better to assert that the enum is of X value (matches!) is useful too
4. Error handling
  1. Enums for problems common problems with user input
  1. Results for issues with the network, supports propagation via ? and error type mapping
  1. Panics for logical errors / bugs in the code
5. Prefer infallable logic, e.g. try not to create methods that hide unwraps 
6. Lifetimes > (A)Rc > Clone
7. Rust the ternary if / else can be very clean


```
    let search_method = if use_index {
        determine_search_method(table, transaction_id, query_person_data)
    } else {
        SearchMethod::RequiresFullScan
    };
```
