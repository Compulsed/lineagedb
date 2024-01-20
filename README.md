# Lineage DB

## How to use 

**Start the database**
`cargo run`

Open `http://localhost:9000/graphiql`

Can use the below mutations to persist / get data
```
mutation writeHuman {
  createHuman(newHuman: { fullName: "Frank Walker", email: "fwalker@gmail.com" }) {
    id
    fullName
    email
  }
}

# Use ID in mutation response to get the human
query queryHuman {
  human (id: "7e4f0ec4-eb4a-4fd2-a6e0-5c67ec89056e") {
    id
    fullName
    email
  }
}

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
- Create ✅ 
- Get ✅
- List
- Update
- GetVersion
- Delete

**DB Features**
- Multiple tables support
- Counter (id counter)
- Multiple updates based on a condition (select)
- Where clause in list
- Update conditions
- Transaction rollbacks
- Transaction queue (max length, 5s timeout)
- Transaction levels
- Transactions with multiple actions
- Referential integrity

**Architecture**
- Transaction log listener (can run another db in another location)
- Run on cloud via docker / lambda

**Performance**
- Create a tx/s metrics
- Is there a way to improve the performance of transaction writes?
  - i.e. we set the transaction log file to be larger than what we need
- Is there a way to monitor rust performance? Like where are we spending the most time
- Read at a transaction id whilst there is a writer — may require thread safe data structures
- Move away from a single thread per request (could implement a thread pool w/ channels?)
- Reduce the amount of rust clones
- State backups
    - Maybe trim the transaction log
    - Perform a state backup every N number of TXs (called a 'backup copy') -- (DI P12). 
      - This is usually async / buffered. I suspect the async part is the state backup and not TX log as the log has to be written
        to consider the transaction as 'committed'.
- Investigate ~6k TX stall from load testing (was using AB, and running on a Mac)

**Design Improvements**
- Clippy ✅
- CI/CD Pipeline ✅
- CLI
    - List version
    - Specify port to bind
    - Specify IP to bind
- Turn index into a class
- Tests
- Create a 'storage engine' abstraction. At the moment this is the responsibility of the transaction manager

**Current Performance**
- ~1-2ms for a create call