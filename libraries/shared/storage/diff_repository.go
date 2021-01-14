// VulcanizeDB
// Copyright © 2019 Vulcanize

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package storage

import (
	"fmt"

	"github.com/makerdao/vulcanizedb/libraries/shared/storage/types"
	"github.com/makerdao/vulcanizedb/pkg/datastore/postgres"
)

type DiffRepository interface {
	CreateStorageDiff(rawDiff types.RawDiff) (int64, error)
	CreateBackFilledStorageValue(rawDiff types.RawDiff) error
	GetNewDiffs(minID, limit int) ([]types.PersistedDiff, error)
	GetUnrecognizedDiffs(minID, limit int) ([]types.PersistedDiff, error)
	GetPendingDiffs(minID, limit int) ([]types.PersistedDiff, error)
	MarkTransformed(id int64) error
	MarkNoncanonical(id int64) error
	MarkNoncanonicalDiffsAsNew(blockNumber int64) error
	MarkUnrecognized(id int64) error
	MarkUnwatched(id int64) error
	MarkPending(id int64) error
	GetFirstDiffIDForBlockHeight(blockHeight int64) (int64, error)
}

var (
	New          = `new`
	Pending      = `pending`
	Noncanonical = `noncanonical`
	Transformed  = `transformed`
	Unrecognized = `unrecognized`
	Unwatched    = `unwatched`
)

type diffRepository struct {
	db *postgres.DB
}

func NewDiffRepository(db *postgres.DB) diffRepository {
	return diffRepository{db: db}
}

// CreateStorageDiff writes a raw storage diff to the database
func (repository diffRepository) CreateStorageDiff(rawDiff types.RawDiff) (int64, error) {
	var storageDiffID int64
	row := repository.db.QueryRowx(`INSERT INTO public.storage_diff
		(address, block_height, block_hash, storage_key, storage_value, eth_node_id) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT DO NOTHING RETURNING id`, rawDiff.Address.Bytes(), rawDiff.BlockHeight, rawDiff.BlockHash.Bytes(),
		rawDiff.StorageKey.Bytes(), rawDiff.StorageValue.Bytes(), repository.db.NodeID)
	err := row.Scan(&storageDiffID)
	if err != nil {
		return 0, fmt.Errorf("error creating storage diff: %w", err)
	}
	return storageDiffID, nil
}

func (repository diffRepository) CreateBackFilledStorageValue(rawDiff types.RawDiff) error {
	_, err := repository.db.Exec(`SELECT * FROM public.create_back_filled_diff($1, $2, $3, $4, $5, $6)`,
		rawDiff.BlockHeight, rawDiff.BlockHash.Bytes(), rawDiff.Address.Bytes(),
		rawDiff.StorageKey.Bytes(), rawDiff.StorageValue.Bytes(), repository.db.NodeID)
	if err != nil {
		return fmt.Errorf("error creating back filled storage value: %w", err)
	}
	return nil
}

func (repository diffRepository) GetNewDiffs(minID, limit int) ([]types.PersistedDiff, error) {
	return repository.getDiffsByStatus(New, minID, limit)
}

func (repository diffRepository) GetUnrecognizedDiffs(minID, limit int) ([]types.PersistedDiff, error) {
	return repository.getDiffsByStatus(Unrecognized, minID, limit)
}

func (repository diffRepository) GetPendingDiffs(minID, limit int) ([]types.PersistedDiff, error) {
	return repository.getDiffsByStatus(Pending, minID, limit)
}

func (repository diffRepository) getDiffsByStatus(status string, minID, limit int) ([]types.PersistedDiff, error) {
	var result []types.PersistedDiff
	err := repository.db.Select(
		&result,
		`SELECT id, address, block_height, block_hash, storage_key, storage_value, eth_node_id, status, from_backfill
				FROM public.storage_diff
				WHERE status = $1 AND id > $2 ORDER BY id ASC LIMIT $3`,
		status, minID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("error getting %s storage diffs with id greater than %d: %w", status, minID, err)
	}
	return result, nil
}

func (repository diffRepository) MarkTransformed(id int64) error {
	_, err := repository.db.Exec(`UPDATE public.storage_diff SET status = $1 WHERE id = $2`, Transformed, id)
	if err != nil {
		return fmt.Errorf("error marking diff %d transformed: %w", id, err)
	}
	return nil
}

func (repository diffRepository) MarkUnrecognized(id int64) error {
	_, err := repository.db.Exec(`UPDATE public.storage_diff SET status = $1 WHERE id = $2`, Unrecognized, id)
	if err != nil {
		return fmt.Errorf("error marking diff %d checked: %w", id, err)
	}
	return nil
}

func (repository diffRepository) MarkNoncanonical(id int64) error {
	_, err := repository.db.Exec(`UPDATE public.storage_diff SET status = $1 WHERE id = $2`, Noncanonical, id)
	if err != nil {
		return fmt.Errorf("error marking diff %d checked: %w", id, err)
	}
	return nil
}

func (repository diffRepository) MarkNoncanonicalDiffsAsNew(blockNumber int64) error {
	_, err := repository.db.Exec(`UPDATE public.storage_diff SET status = $1 WHERE block_height = $2 AND status = $3`, New, blockNumber, Noncanonical)
	return err
}

func (repository diffRepository) MarkUnwatched(id int64) error {
	_, err := repository.db.Exec(`UPDATE public.storage_diff SET status = $1 WHERE id = $2`, Unwatched, id)
	if err != nil {
		return fmt.Errorf("error marking diff %d checked: %w", id, err)
	}
	return nil
}

func (repository diffRepository) MarkPending(id int64) error {
	_, err := repository.db.Exec(`UPDATE public.storage_diff SET status = $1 WHERE id = $2`, Pending, id)
	if err != nil {
		return fmt.Errorf("error marking diff %d checked: %w", id, err)
	}
	return nil
}

func (repository diffRepository) GetFirstDiffIDForBlockHeight(blockHeight int64) (int64, error) {
	var diffID int64
	err := repository.db.Get(&diffID,
		`SELECT id FROM public.storage_diff WHERE block_height >= $1 LIMIT 1`, blockHeight)
	if err != nil {
		return diffID, fmt.Errorf("error getting first diff ID for block height %d: %w", blockHeight, err)
	}
	return diffID, nil
}
