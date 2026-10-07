# Changelog

## 0.1.1

- Windows loops have no `add_reader`. One callback still carries the batch.
- The writer sleeps until the commit floor. One `sleep` can return early.

## 0.1.0

- One SQLite file. One writer thread. Reads stay on the calling thread.
- `isolated=True` is the default. A failed write undoes only that write.
- `Select`, `Insert`, `Update`, and `Delete`. `col`, `lit`, `param`, and `.as_()` build expressions.
- `db.watch` yields again when a commit changes a column the query reads.
