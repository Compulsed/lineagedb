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
- Read at a transaction id whilst there is a writer — may require thread safe data structures
- Move away from a single thread per request (could implement a thread pool w/ channels?)
- Reduce the amount of clones
- State backups
    - Maybe trim the transaction log
    - Perform a state backup every N number of TXs
- Investigate ~6k TX stall from AB

**Design Improvements**
- Clippy ✅
- CI/CD Pipeline ✅
- CLI
    - List version
    - Specify port to bind
    - Specify IP to bind
- Turn index into a class
- Tests

**Current Performance**
- ~1-2ms for a create call