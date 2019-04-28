# zx-node-challenge
Node Challenge for ZX Ventures

- `src/index.js` bulk-import stream file
- `src/small-file-option.js` import stream file, simple version, yet not appropriate for large files
- `src/db.js` sample data generator and DB setup considerations. This was originally set-up _prior_ to noticing that you wanted non-customers excluded prior to import, instead of using a foreign key to ignore these rows.