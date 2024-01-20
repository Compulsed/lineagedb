Design points:
- An on-disk transaction log is committed to on every transaction
- There is an async process which takes those transaction logs and builds a checkpoint / backup

MVCC:
- Database eventually delete old versions, they remove old state when
    - There are no transactions which reference old state
    - Coordinator subscriptions have processes those old states
- In our case we will keep all changes, this means we need to figure out a way to flush all old versions to disk,
    our trim mechanism is just removing them from RAM. 