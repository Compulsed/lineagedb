# Lineage DB

## How to use 

**Start the database**
`cargo run`

**Run the database**
`echo "a" | nc 127.0.0.1 8080` # Adds
`echo "l" | nc 127.0.0.1 8080` # Lists
`echo "u" | nc 127.0.0.1 8080` # Updates
`echo "l" | nc 127.0.0.1 8080` # Lists

**Performance test**
`echo "a" | nc 127.0.0.1 8080`
`while true; do echo "u" | nc 127.0.0.1 8080; done`

## Features
1. Input parser
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

**Features**
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
- Read at a transaction id whilst there is a writer — may require thread safe data structures
- Move away from a single thread per request (could implement a thread pool w/ channels)

**Design Improvements**
- Turn index into a class
- Clippy
- CI/CD Pipeline
- Tests