# Storage Transformer Example

In the Storage Transformer README, we went over code that needs to be written to add a new storage transformer to VulcanizeDB.
In this document, we'll go over an example contract and discuss how one would go about watching its storage.

## Example Contract

For the purposes of this document, we'll be assuming that we're interested in watching the following contract:

```solidity
pragma solidity ^0.5.1;

contract Contract {
  uint256 public num_addresses;
  mapping(address => uint) public addresses;
  
  event AddressAdded(
    address addr,
    uint256 num_addrs
  );
  
  constructor() public {
    addresses[msg.sender] = 1;
    num_addresses = 1;
  }
  
  function add_address(address addr) public {
    bool exists = addresses[addr] > 0;
    addresses[addr]++;
    if (!exists) {
      emit AddressAdded(addr, ++num_addresses);
    }
  }
}
```

Disclaimer: this contract has not been audited and is not intended to be modeled or used in production. :)

This contract persists two values in its storage:

1. `num_addresses`: the total number of unique addresses known to the contract.
2. `addresses`: a mapping that records the number of times an address has been added to the contract.

It also emits an event each time a new address is added into the contract's storage.

## Custom Code

In order to monitor the state of this smart contract, we'd need to implement: an event transformer, a KeysLoader interface, and a repository. Event transformers are out of scope for this doc, but you can see the [event transformer README](PUT LINK HERE BEFORE MERGING).

### Mappings

If we point an ethereum node at a blockchain hosting this contract and our node is emitting storage diffs happening on this contract, we will expect such changes to appear each time `add_address` (which modifies the `addresses` mapping) is called.

In order for those changes - which include raw hex versions of storage keys and storage values, to be useful for us - we need to know how to recognize and parse them, and we do that via the `KeysLoader` interface and the `LoadMappings` function which returns a map of key hashes to `ValueMetadata`.

In this example the the mapping should contain the storage key for `num_addresses` as well as a storage key for each `addresses` key known to be associated with a non-zero value.

#### num_addresses

`num_addresses` is the first variable declared on the contract, and it is a simple (non-array, non-mapping) type.
Therefore, we know that its storage key is `0000000000000000000000000000000000000000000000000000000000000000`.

The storage key for non-array and non-mapping variables is (usually*) the index of the variable on the contract's storage.

If we see a storage diff being emitted from this contract with this storage key, we know that the `num_addresses` variable has been modified.

In this case,  we would expect that the call `mappings[common.HexToHash("0000000000000000000000000000000000000000000000000000000000000000")]` would return metadata corresponding to the `num_addresses` variable. Note the call to `common.HexToHash`. The mapping maps a hash value, and not the raw index, to the meta data so this call is necessary.

This metadata would probably look something like:

```go
types.ValueMetadata{
    Name: "num_addresses",
    Keys: nil,
    Type: shared.Uint256,
}
```

<sup>*</sup> Occasionally, multiple variables may be packed into one storage slot, which complicates a direct translation of the index of the variable on the contract to its storage key.

#### addresses

`addresses` is the second variable declared on the contract, but it is a mapping.
Since it is a mapping, the storage key is more complex than `0000000000000000000000000000000000000000000000000000000000000001` (which would be the key for the variable if it were not an array or mapping).
Having a single storage slot for an entire mapping would not work, since there can be an arbitrary number of entries in a mapping, and a single storage value slot is constrained to 32 bytes.

The way that smart contract mappings are maintained in storage (in Solidity) is by creating a new storage key/value pair for each entry in the mapping, where the storage key is a hash of the occupied slot's key concatenated with the mapping's index on the contract.  Fortunately you do not have to manage that and can use a helper method:

```go
import (
  ... 
	vdbStorage "github.com/makerdao/vulcanizedb/libraries/shared/storage"
)

vdbStorage.GetKeyForMapping(addressesIndex, mappingIndex)
```
Where addressIndex here would be `0000000000000000000000000000000000000000000000000000000000000001` and the mappingIndex would be the index into that mapping you desired.

### Repository

Once we have recognized a storage diff, we can decode the storage value to the data's known type.
Since the metadata tells us that the above values are `uint256`, we can decode a value like `0000000000000000000000000000000000000000000000000000000000000001` to `1`.

The purpose of the contract-specific repository is to write that value to the database in a way that makes it useful for future queries.
Typically, this involves writing the block hash, block number, decoded value, and any keys in the metadata to a table.

The current repository interface has a generalized `Create` function that can accept any arbitrary storage row along with its metadata.
This is deliberate, to facilitate shared use of the common storage transformer.
An implication of this decision is that the `Create` function typically includes a `switch` statement that selects which table to write to, as well as what data to include, based on the name of the variable as defined in the metadata.

An example implementation of `Create` for our example contract above might look like:

```go
func (repository AddressStorageRepository) Create(blockNumber int, blockHash string, metadata shared.StorageValueMetadata, value interface{}) error {
    switch metadata.Name {
    case "num_addresses":
        _, err := repository.db.Exec(`INSERT INTO storage.num_addresses (block_hash, block_number, n) VALUES ($1, $2, $3)`,
            blockHash, blockNumber, value)
        return err
    case "addresses":
        _, err := repository.db.Exec(`INSERT INTO storage.addresses (block_hash, block_number, address, n) VALUES ($1, $2, $3, $4)`,
            blockHash, blockNumber, metadata.Keys[Address], value)
        return err
    default:
        panic(fmt.Sprintf("unrecognized contract storage name: %s", metadata.Name))
    }
}
```

## Summary

With our very simple address storing contract, we would be able to read its storage diffs by implementing an event transformer, a mappings, and a repository.

The mappings would be able to lookup storage keys reflecting `num_addresses` or any slot in `addresses`, using addresses derived from watching the `AddressAdded` event for the latter.

The repository would be able to persist the value or `num_addresses` or any slot in `addresses`, using metadata returned from the mappings.

The mappings and repository could be plugged into the common storage transformer, enabling us to know the contract's state as it is changing.
