# Changelog

## 0.1.0

- One SQLite file. One writer thread. Reads stay on the calling thread.
- `isolated=True` is the default. A failed write undoes only that write.
- `Select`, `Insert`, `Update`, and `Delete`. `col`, `lit`, `param`, and `.as_()` build expressions.
- `db.watch` yields again when a commit changes a column the query reads.
