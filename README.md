# wal-python

## Steps for setup

```
uv init
uv run wal_basic.py

# For tests
uv run -m unittest wal_tests.py
```

## Plan

Today's work:

- Add thread safety and using last sequence number in case of wal instance restart
- Implement a simple K-V data store that can use the wal implementation
- How does recovery work for the data store using wal?

Implementation Notes:

- Recovery function kept in wal class.. Why? You just pass in the datastore instance, log related functions are available in wal class
