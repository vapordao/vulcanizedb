# Watching Contract Storage

Using the `extractDiffs` command VulcanizeDB can ingest raw contract storage values and store them in the `public.storage_diffs` table in postgres, allowing for transformation by the `execute` command.

## Assumptions

We have [a branch of go-ethereum (a.k.a geth)](https://github.com/makerdao/go-ethereum) that enables running a node that provides storage diffs via a websocket, emitting them as nodes are added to the chain, and is required for using `extractDiffs`. That command will continuously read from the websocket and persist the results to postgres, in the `public.storage_diffs` table to be transformed.

Looking forward, we would like to eliminate this single point of failure, potentially writing the diffs to an intermediate data store in the event the socket connection is lost. Currently in the event this happens the `backfillStorage` process can be used to backfill the lost diffs.

### Storage Watcher

The `execute` command will use the storage watcher to continuously delegate entries in `public.storage_diffs` to the appropriate transformer as they are being written by the ethereum node. Entries are marked as `transformed` when the process is complete, and `unrecognized` if its key is unknown to vulcanize. `[u]nrecognized` diffs are re-checked periodically.

Storage watchers can be loaded with plugin storage transformers and executed using the `compose` then `execute` commands.

### Storage Transformer

The storage transformer is responsible for converting raw contract storage hex values into useful data and writing them to postgres.

The storage transformer depends on contract-specific implementations of code capable of recognizing storage keys and writing the matching (decoded) storage value to disk. To see examples see the [maker storage transformers](https://github.com/makerdao/vdb-mcd-transformers/tree/prod/transformers/storage).


This is the high-level Execute function. You must fill in the Transformer interface.

```golang
func (transformer Transformer) Execute(diff types.PersistedDiff) error {
	metadata, lookupErr := transformer.StorageKeysLookup.Lookup(diff.StorageKey)
	if lookupErr != nil {
		return fmt.Errorf("error getting metadata for storage key: %w", lookupErr)
	}
	value := storage.Decode(diff, metadata)
	return transformer.Repository.Create(diff.ID, diff.HeaderID, metadata, value)
}
```


## Custom Code

In order to watch an additional smart contract, a developer must create three things:

1. KeysLoader - identify keys in the contract's storage trie, providing metadata to describe how associated values should be decoded.
1. Repository - specify how to persist a parsed version of the storage value matching the recognized storage key.
1. Instance - create an instance of the storage transformer that uses your mappings and repository.

### KeysLoader

A `KeysLoader` is used by the `StorageKeysLookup` object on a storage transformer. You'll implement this interface.

```go
type KeysLoader interface {
	LoadMappings() (map[common.Hash]types.ValueMetadata, error)
	SetDB(db *postgres.DB)
}
```

When a key is not found, the lookup object refreshes its known keys by calling the loader.

```go
func (lookup *keysLookup) refreshMappings() error {
	newMappings, err := lookup.loader.LoadMappings()
	if err != nil {
		return fmt.Errorf("error loading mappings: %w", err)
	}
	lookup.mappings = newMappings
	return nil
}
```

A contract-specific implementation of the loader enables the storage transformer to fetch metadata associated with a storage key. Here is the version for the `flap` contract.

``` go
func (loader *keysLoader) LoadMappings() (map[common.Hash]types.ValueMetadata, error) {
	mappings := loadStaticKeys()
	mappings, wardsErr := loader.addWardsKeys(mappings)
	if wardsErr != nil {
		return nil, fmt.Errorf("error adding wards keys to flap keys loader: %w", wardsErr)
	}
	mappings, bidErr := loader.loadBidKeys(mappings)
	if bidErr != nil {
		return nil, fmt.Errorf("error adding bid keys to flap keys loader: %w", bidErr)
	}
	return mappings, nil
}
```

Storage metadata contains: the name of the variable matching the storage key, a raw version of any keys associated with the variable (if the variable is a mapping), the variable's type, and optional `PackedNames` and `PackedTypes`.

```go
type ValueMetadata struct {
	Name        string
	Keys        map[Key]string
	Type        ValueType
	PackedNames map[int]string    //zero indexed position in map => name of packed item
	PackedTypes map[int]ValueType //zero indexed position in map => type of packed item
}
```

The `Keys` field on the metadata is only relevant if the variable is a mapping. For example, in the following Solidity code:

```solidity
pragma solidity ^0.4.0;

contract Contract {
  uint x;
  mapping(address => uint) y;
}
```

The metadata for variable `x` would not have any associated keys, but the metadata for a storage key associated with `y` would include the address used to specify that key's index in the mapping.

The `SetDB` function is required for the storage key loader to connect to the database.
A database connection may be desired when keys in a mapping variable need to be read from log events (e.g. to lookup what addresses may exist in `y`, above).

### Repository

```go
type Repository interface {
	Create(diffID, headerID int64, metadata types.ValueMetadata, value interface{}) error
	SetDB(db *postgres.DB)
}
```

A contract-specific implementation of the repository interface enables the transformer to write the decoded storage value to the appropriate table in postgres.

The `Create` function is expected to recognize and persist a given storage value by the variable's name, as indicated on the row's metadata.
Note: we advise silently discarding duplicates in `Create` - as it's possible that you may read the same diff several times.

The `SetDB` function is required for the repository to connect to the database. Here is another example from the `flap` contract. It's mostly calling other functions, but should give you a rough idea what `Create` would look like.

``` go
func (repository *StorageRepository) Create(diffID, headerID int64, metadata types.ValueMetadata, value interface{}) error {
	switch metadata.Name {
	case storage.Vat:
		return repository.insertVat(diffID, headerID, value.(string))
	case storage.Gem:
		return repository.insertGem(diffID, headerID, value.(string))
	case storage.Beg:
		return repository.insertBeg(diffID, headerID, value.(string))
	case storage.Kicks:
		return repository.insertKicks(diffID, headerID, value.(string))
	case storage.Live:
		return repository.insertLive(diffID, headerID, value.(string))
	case wards.Wards:
		return wards.InsertWards(diffID, headerID, metadata, repository.ContractAddress, value.(string), repository.db)
	case storage.BidBid:
		return repository.insertBidBid(diffID, headerID, metadata, value.(string))
	case storage.BidLot:
		return repository.insertBidLot(diffID, headerID, metadata, value.(string))
	case storage.Packed:
		return repository.insertPackedValueRecord(diffID, headerID, metadata, value.(map[int]string))
	default:
		panic(fmt.Sprintf("unrecognized flap contract storage name: %s", metadata.Name))
	}
}
```

### Instance

```go
type Transformer struct {
	Address    			common.Address
	StorageKeysLookup 	KeysLookup
	Repository 			Repository
}
```

A new instance of the storage transformer is initialized with the contract-specific key lookup and repository, as well as the contract's address.
The contract's address is included so that the watcher can query that value from the transformer in order to build up its mapping of addresses to transformers.

## Summary

To begin watching an additional smart contract, create a new mappings file for looking up storage keys on that contract, a repository for writing storage values from the contract, and initialize a new storage transformer instance with the mappings, repository, and contract address.

The new instance, wrapped in an initializer that calls `SetDB` on the mappings and repository, should be passed to the `AddTransformers` function on the storage watcher.
