# Lineage DB

1. Input parser
1. Transaction Processor (Query, Add)
1. Read
1. Write
    1. Apply
        1. World state
        1. Version history
        1. Transaction list
    1. Restore
    1. Persist to file
    1. WAL