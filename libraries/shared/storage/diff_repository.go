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
	"database/sql"
	"time"

	"github.com/makerdao/vulcanizedb/libraries/shared/storage/types"
	"github.com/makerdao/vulcanizedb/pkg/datastore/postgres"
	"github.com/sirupsen/logrus"
)

var ErrDuplicateDiff = sql.ErrNoRows

type DiffRepository interface {
	CreateStorageDiff(rawDiff types.RawDiff) (int64, error)
	CreateBackFilledStorageValue(rawDiff types.RawDiff) error
	GetNewDiffs(diffs chan types.PersistedDiff, errs chan error, done chan bool)
	MarkChecked(id int64) error
}

type diffRepository struct {
	db *postgres.DB
}

func NewDiffRepository(db *postgres.DB) diffRepository {
	return diffRepository{db: db}
}

// CreateStorageDiff writes a raw storage diff to the database
func (repository diffRepository) CreateStorageDiff(rawDiff types.RawDiff) (int64, error) {
	var storageDiffID int64
	start := time.Now()
	row := repository.db.QueryRowx(`INSERT INTO public.storage_diff
		(hashed_address, block_height, block_hash, storage_key, storage_value) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING RETURNING id`, rawDiff.HashedAddress.Bytes(), rawDiff.BlockHeight, rawDiff.BlockHash.Bytes(),
		rawDiff.StorageKey.Bytes(), rawDiff.StorageValue.Bytes())
	err := row.Scan(&storageDiffID)
	if err != nil && err == sql.ErrNoRows {
		return 0, ErrDuplicateDiff
	}
	elapsed := time.Since(start)
	logrus.Info("Duration of CreateStorageDiff: ", elapsed, "diff ", rawDiff.BlockHeight, rawDiff.StorageKey)
	return storageDiffID, err
}

func (repository diffRepository) CreateBackFilledStorageValue(rawDiff types.RawDiff) error {
	_, err := repository.db.Exec(`SELECT * FROM public.create_back_filled_diff($1, $2, $3, $4, $5)`,
		rawDiff.BlockHeight, rawDiff.BlockHash.Bytes(), rawDiff.HashedAddress.Bytes(),
		rawDiff.StorageKey.Bytes(), rawDiff.StorageValue.Bytes())
	return err
}

func (repository diffRepository) GetNewDiffs(diffs chan types.PersistedDiff, errs chan error, done chan bool) {
	startQuery := time.Now()
	rows, queryErr := repository.db.Queryx(`SELECT * FROM public.storage_diff WHERE checked = false`)
	elapsedQuery := time.Since(startQuery)
	logrus.Info("Time it took to query for unchecked diffs", elapsedQuery)
	if queryErr != nil {
		logrus.Errorf("error getting unchecked storage diffs: %s", queryErr.Error())
		if rows != nil {
			closeErr := rows.Close()
			if closeErr != nil {
				logrus.Errorf("error closing rows: %s", closeErr.Error())
			}
		}
		errs <- queryErr
		return
	}

	if rows != nil {
		for rows.Next() {
			start := time.Now()

			var diff types.PersistedDiff
			scanErr := rows.StructScan(&diff)
			if scanErr != nil {
				logrus.Errorf("error scanning diff: %s", scanErr.Error())
				closeErr := rows.Close()
				if closeErr != nil {
					logrus.Errorf("error closing rows: %s", closeErr.Error())
				}
				errs <- scanErr
			}
			elapsed := time.Since(start)
			logrus.Info("Diff being passed to diffs channel. Block height: ", diff.BlockHeight, "Storage Key:", diff.StorageKey)
			logrus.Info("Elapsed time between rows.Next and sending diff: ", elapsed)
			diffs <- diff
		}
	}

	done <- true
}

func (repository diffRepository) MarkChecked(id int64) error {
	_, err := repository.db.Exec(`UPDATE public.storage_diff SET checked = true WHERE id = $1`, id)
	return err
}
