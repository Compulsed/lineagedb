
## Notes

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
- Transaction levels
  - Serializable
  - Repeatable read
  - Read committed ✅
  - Read uncommitted (will not implement due to MVCC)
- Multiple tables support
- Counter (id counter)
- Multiple updates based on a condition (select)
- Where clause in list (limited) ✅
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
- Move away from a single thread per request (could implement a thread pool w/ channels?) ✅
- Investigate ~6k TX stall from load testing (was using AB, and running on a Mac) ✅
- Is there a way to monitor rust performance? Like where are we spending the most time
- Reduce the amount of rust clones
- Anywhere we would clone attempt to use an RC -- this happens with Actions (check performance after doing this)

**Design Improvements -- Internals**
- Clippy ✅
- CI/CD Pipeline ✅
- Improve error types -- it is not clear what part of the application can throw an error vs. an enum type response ✅
- Improve change the send_request to be 'action aware', as in, a single action should return a single response ✅
- Try a faster / binary serialization format. ✅
  - Tried bare, it was not faster / the bottleneck ✅ https://github.com/Compulsed/lineagedb/blob/feat/bare-serialization/database/src/database/transaction.rs
- Do not need to maintain the transaction log in memory -- Transation log can just use a reference ✅
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
- Create a 'storage engine' abstraction. At the moment this is the responsibility of the transaction manager ✅
- Transaction that just contain queries should not be persisted to the transaction log ✅
- Updating action format (e.g. adding additional params to list) causes parsing to break
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
6. Lifetimes > (A)Rc > Clone (cost)
7. Rust the ternary if let / else to unwrap rather than a match
8. Struct to capture all common dependencies and a .run() method to execute them (Control)
9. Deref trait to wrap items into an Arc so that there is only a single reference to something when cloning
10. Nesting test impl under a mod, makes it easier to ensure that cfg deps are correctly scoped
  1. This might be good for attempting to swap our implementations
11. Avoid option, prefer an enum, so you can describe the use case
12. 'static as a lifetype means that it runs for entire length of program, 'static for a trait means that it is not a ref type
13. .then for accessing non-error, .map_err for accessing error, .map for the response
14. Can use multi-line comments w/ panic to describe the why
  1. If you have to unwrap the same reason in multiple places you should share a function OR a &str


```
    let search_method = if use_index {
        determine_search_method(table, transaction_id, query_person_data)
    } else {
        SearchMethod::RequiresFullScan
    };
```


Ideas:
1. Snapshot isolation
    1. Unsure if we're technically there (are we encoding that value somewhere?)
        1. Which timestamps are there and what do they mean? tx start, and tx commit? -- look into whitepaper
    1. Query at any clock time
1. Review transaction lifecycle (as described in the white paper)
1. Long lived transactions
    1. Setting up the Infrastructure w/ GraphQL (transaction identifier, clock time)
    1. Query at a point in time
1. Database write lock barrier sync poorman's serializable writes
1. Indexes
    1. Uniqueness
    2. Query performance