# Lineage DB

Lineage DB is an experimental time traveling database. The database will have the following functionality:
1. Query the database at any transaction id
1. For any given item can look at all revisions
1. Supports ACID transactions (it is single threaded / time traveling so this is almost 'free')

The database is sufficiently isolated, this means it exists in its own crate / is independent from any clients.

To play around / interact with the database I have provided a GraphQL / TCP client.

## How to use 

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
```

**CLI**
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

**Testing / Benchmarking**

```
cargo test --all
cargo bench --all
```

## Features
1. Input parser ✅
1. Transaction Processor (Query, Add) ✅
1. Read ✅
1. Write ✅ 
1. Apply ✅
    1. World state ✅
    1. Version history ✅
    1. Transaction list ✅
1. Multiple producers single consumer ✅
1. Uniqueness constraints ✅
1. Restore ✅
1. Persist to file transaction log to a file ✅
1. Pass result back to caller ✅
1. Transaction return type with data (latch?) ✅
1. Network based requests ✅

**GraphQL Feature**
- Get ✅
- GetVersion ✅
- List ✅
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

**Architecture**
- Transaction log listener (can run another db in another location)
- Run on cloud via docker / lambda
- Split the database / clients components into their own libraries

**Performance**
- Create a tx/s metrics (1ms for ~100 reads / writes) ✅
- Is there a way to monitor rust performance? Like where are we spending the most time
- Is there a way to improve the performance of transaction writes?
  - i.e. we set the transaction log file to be larger than what we need
- Read at a transaction id whilst there is a writer — may require thread safe data structures
- Move away from a single thread per request (could implement a thread pool w/ channels?)
- Reduce the amount of rust clones
- State backups
    - Maybe trim the transaction log
    - Perform a state backup every N number of TXs (called a 'backup copy') -- (DI P12). 
      - This is usually async / buffered. I suspect the async part is the state backup and not TX log as the log has to be written
        to consider the transaction as 'committed'.
- Investigate ~6k TX stall from load testing (was using AB, and running on a Mac)
- Anywhere we would clone attempt to use an RC -- this happens with Actions (check performance after doing this)

**Design Improvements -- Internals**
- Clippy ✅
- CI/CD Pipeline ✅
- Improve error types -- it is not clear what part of the application can throw an error vs. an enum type response ✅
- Improve change the send_request to be 'action aware', as in, a single action should return a single response ✅
- Tests 
  - GraphQL
  - Database (Transaction Management) ✅
  - Table (Applying / Rolling back changes)
  - Row
- CLI
    - Specify port to bind ✅
    - Specify IP to bind ✅
    - List database version (https://github.com/rust-lang/cargo/issues/6583)
- Turn index into a class
- Create a 'storage engine' abstraction. At the moment this is the responsibility of the transaction manager
- Transaction that just contain queries should not be persisted to the transaction log
- Do not need to maintain the transaction log in memory -- Transation log can just use a reference

## My learnings

**To read**
- https://rust-unofficial.github.io/patterns/patterns/creational/builder.html

**Rust learnings**
1. NewType is great
2. match .into_iter().next() is a great way to get ownership / get the first item
3. When evaluating nested types in e.g. DatabaseResponseAction, it is better to assert that the enum is of X value (matches!) is useful too
4. Error handling
  1. Enums for problems common problems with user input
  1. Results for issues with the network, supports propagation via ? and error type mapping
  1. Panics for logical errors / bugs in the code
5. Prefer infallable logic, e.g. try not to create methods that hide unwraps 
6. Can use Rc instead of clone (unlike ARC RC is likely 0 cost)