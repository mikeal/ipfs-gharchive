# GitHub Archive Data in IPFS

This project loads github archive data into IPFS and provides additional tools for
querying that data.

### Storage Model

Data is stored using the ipld-cbor-dag. Each object is stored in three paths.

```
/repos/:shard1/:shard2/:owner/:repo/:year/:month/:day/:time
/actors/:shard1/:shard2/:login/:year/:month/:day/:time
/timestamps/:year/:month/:day/:hour/:minute/:hash
```

The shards are integers determined statically by hashing the `owner` and `login` strings.

The values are IPFS blocks of gzipped JSON, which reduces the size of the blocks by 70%.

Additionally, receipts are stored for every tarball in gharchive.

```
/receipts/:year/:month/:day/:filename
```
