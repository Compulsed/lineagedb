# Lineage DB

1. Input parser
1. Transaction Processor (Query, Add) ✅
1. Read ✅
1. Write ✅ 
1. Apply ✅
    1. World state ✅
    1. Version history ✅
    1. Transaction list ✅
1. Multiple producers single consumer

1. Uniqueness constraints
1. Restore
1. Persist to file
1. WAL
- Transaction queue (max length, 5s timeout)
- Transaction return type with data (latch?)
- Transaction levels
- Transactions with multiple actions
- Transaction rollbacks
- Where clause in list
- Update conditions
- Multiple updates based on a condition (select)

- Counter (id counter)
- Multiple tables support
- Referential integrity
- Read whilst writing — may require thread safe data structures 
- Writing out transaction log to file, world state 
- Transaction log listener (can run another db in another location)
