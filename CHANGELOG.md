# Changelog

## 0.2.0

- `claim` waits on `{path}-claim` so one process can take the file during a switch.
- `close` folds the WAL once, then drops the claim.
- `busy_timeout` is seconds. The default is 5.
- `Replace` is gone. A comparison with `None` raises `WriterError`.

## 0.1.1

- Windows loops have no `add_reader`. One callback still carries the batch.
- The writer sleeps until the commit floor. One `sleep` can return early.

## 0.1.0

- One SQLite file. One writer thread. Reads stay on the calling thread.
- `isolated=True` is the default. A failed write undoes only that write.
- `Select`, `Insert`, `Update`, and `Delete`. `col`, `lit`, `param`, and `.as_()` build expressions.
- `db.watch` yields again when a commit changes a column the query reads.
