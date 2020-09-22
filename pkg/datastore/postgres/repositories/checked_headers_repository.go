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

package repositories

import (
	"fmt"

	"github.com/makerdao/vulcanizedb/pkg/core"
	"github.com/makerdao/vulcanizedb/pkg/datastore/postgres"
)

const (
	insertCheckedHeaderQuery = `
INSERT INTO %s.checked_headers (check_count, header_id)
VALUES (1, $1)
ON CONFLICT (header_id) DO
	UPDATE SET check_count =
		(SELECT checked_headers.check_count WHERE checked_headers.header_id = $1) + 1
	WHERE checked_headers.header_id = $1`
)

type SchemaQuery struct {
	SchemaName string `db:"schema_name"`
}

type CheckedHeadersRepository struct {
	db         *postgres.DB
	schemaName string
}

func NewCheckedHeadersRepository(db *postgres.DB, schemaName string) (*CheckedHeadersRepository, error) {
	var sq SchemaQuery
	err := db.Get(&sq, "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1", schemaName)
	if err != nil {
		return nil, fmt.Errorf("error creating checked headers repository - invalid schema %w", err)
	}

	// Use the SchemaName from the actual information_schema, just in case the
	// passed in schemaName doesn't exist
	return &CheckedHeadersRepository{db: db, schemaName: sq.SchemaName}, nil
}

// Increment check_count for header
func (repo CheckedHeadersRepository) MarkHeaderChecked(headerID int64) error {
	queryString := fmt.Sprintf(insertCheckedHeaderQuery, repo.schemaName)
	_, err := repo.db.Exec(queryString, headerID)
	return err
}

// Zero out check count for header with the given block number
func (repo CheckedHeadersRepository) MarkSingleHeaderUnchecked(blockNumber int64) error {
	queryString := fmt.Sprintf(`UPDATE %s.checked_headers ch
								SET check_count = 0
								FROM public.headers h
								WHERE ch.header_id = h.id
								AND h.block_number = $1`, repo.schemaName)

	_, err := repo.db.Exec(queryString, blockNumber)
	return err
}

// Return header if check_count  < passed checkCount
func (repo CheckedHeadersRepository) UncheckedHeaders(startingBlockNumber, endingBlockNumber, checkCount int64) ([]core.Header, error) {
	var (
		result                  []core.Header
		err                     error
		recheckOffsetMultiplier = 15
	)

	joinQuery := fmt.Sprintf(`
WITH checked_headers AS (
	SELECT h.id, h.block_number, h.hash, COALESCE(ch.check_count, 0) AS check_count
	FROM public.headers h
	LEFT JOIN %s.checked_headers ch
	ON ch.header_id = h.id
    WHERE h.block_number >= $1
)
SELECT id, block_number, hash
FROM checked_headers
WHERE ( check_count < 1
	OR (check_count < $2
		AND block_number <= ((SELECT MAX(block_number) FROM public.headers) - ($3 * check_count * (check_count + 1) / 2))))
`, repo.schemaName)

	if endingBlockNumber == -1 {
		err = repo.db.Select(&result, joinQuery, startingBlockNumber, checkCount, recheckOffsetMultiplier)
	} else {
		endingBlockQuery := fmt.Sprintf(`%s AND block_number <= $4`, joinQuery)
		err = repo.db.Select(&result, endingBlockQuery, startingBlockNumber, checkCount, recheckOffsetMultiplier, endingBlockNumber)
	}

	return result, err
}
