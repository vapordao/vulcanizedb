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

package mocks

import (
	"github.com/makerdao/vulcanizedb/libraries/shared/storage/types"
)

type MockStorageDiffRepository struct {
	CreateBackFilledStorageValuePassedRawDiffs []types.RawDiff
	CreateBackFilledStorageValueReturnError    error
	CreatePassedRawDiffs                       []types.RawDiff
	GetNewDiffsToReturn                        []types.PersistedDiff
	GetNewDiffsErrors                          []error
	GetNewDiffsPassedMinIDs                    []int
	GetNewDiffsPassedLimits                    []int
	GetPendingDiffsToReturn                    []types.PersistedDiff
	GetPendingDiffsErrors                      []error
	GetPendingDiffsPassedMinIDs                []int
	GetPendingDiffsPassedLimits                []int
	GetUnrecognizedDiffsToReturn               []types.PersistedDiff
	GetUnrecognizedDiffsErrors                 []error
	GetUnrecognizedDiffsPassedMinIDs           []int
	GetUnrecognizedDiffsPassedLimits           []int
	MarkTransformedPassedID                    int64
	MarkUnrecognizedPassedID                   int64
	MarkNoncanonicalPassedID                   int64
	MarkPendingPassedID                        int64
	MarkUnwatchedPassedID                      int64
	GetFirstDiffIDToReturn                     int64
	GetFirstDiffIDErr                          error
	GetFirstDiffBlockHeightPassed              int64
}

func (repository *MockStorageDiffRepository) CreateStorageDiff(rawDiff types.RawDiff) (int64, error) {
	repository.CreatePassedRawDiffs = append(repository.CreatePassedRawDiffs, rawDiff)
	return 0, nil
}

func (repository *MockStorageDiffRepository) CreateBackFilledStorageValue(rawDiff types.RawDiff) error {
	repository.CreateBackFilledStorageValuePassedRawDiffs = append(repository.CreateBackFilledStorageValuePassedRawDiffs, rawDiff)
	return repository.CreateBackFilledStorageValueReturnError
}

func (repository *MockStorageDiffRepository) GetNewDiffs(minID, limit int) ([]types.PersistedDiff, error) {
	repository.GetNewDiffsPassedMinIDs = append(repository.GetNewDiffsPassedMinIDs, minID)
	repository.GetNewDiffsPassedLimits = append(repository.GetNewDiffsPassedLimits, limit)
	err := repository.GetNewDiffsErrors[0]
	if len(repository.GetNewDiffsErrors) > 1 {
		repository.GetNewDiffsErrors = repository.GetNewDiffsErrors[1:]
	}
	return repository.GetNewDiffsToReturn, err
}

func (repository *MockStorageDiffRepository) GetUnrecognizedDiffs(minID, limit int) ([]types.PersistedDiff, error) {
	repository.GetUnrecognizedDiffsPassedMinIDs = append(repository.GetUnrecognizedDiffsPassedMinIDs, minID)
	repository.GetUnrecognizedDiffsPassedLimits = append(repository.GetUnrecognizedDiffsPassedLimits, limit)
	err := repository.GetUnrecognizedDiffsErrors[0]
	if len(repository.GetUnrecognizedDiffsErrors) > 1 {
		repository.GetUnrecognizedDiffsErrors = repository.GetUnrecognizedDiffsErrors[1:]
	}
	return repository.GetUnrecognizedDiffsToReturn, err
}

func (repository *MockStorageDiffRepository) GetPendingDiffs(minID, limit int) ([]types.PersistedDiff, error) {
	repository.GetPendingDiffsPassedMinIDs = append(repository.GetPendingDiffsPassedMinIDs, minID)
	repository.GetPendingDiffsPassedLimits = append(repository.GetPendingDiffsPassedLimits, limit)
	err := repository.GetPendingDiffsErrors[0]
	if len(repository.GetPendingDiffsErrors) > 1 {
		repository.GetPendingDiffsErrors = repository.GetPendingDiffsErrors[1:]
	}
	return repository.GetPendingDiffsToReturn, err
}

func (repository *MockStorageDiffRepository) MarkTransformed(id int64) error {
	repository.MarkTransformedPassedID = id
	return nil
}

func (repository *MockStorageDiffRepository) MarkNoncanonical(id int64) error {
	repository.MarkNoncanonicalPassedID = id
	return nil
}

func (repository *MockStorageDiffRepository) MarkNoncanonicalDiffsAsNew(blockNumber int64) error {
	panic("implement me")
}

func (repository *MockStorageDiffRepository) MarkUnrecognized(id int64) error {
	repository.MarkUnrecognizedPassedID = id
	return nil
}

func (repository *MockStorageDiffRepository) MarkUnwatched(id int64) error {
	repository.MarkUnwatchedPassedID = id
	return nil
}

func (repository *MockStorageDiffRepository) MarkPending(id int64) error {
	repository.MarkPendingPassedID = id
	return nil
}

func (repository *MockStorageDiffRepository) GetFirstDiffIDForBlockHeight(blockHeight int64) (int64, error) {
	repository.GetFirstDiffBlockHeightPassed = blockHeight
	return repository.GetFirstDiffIDToReturn, repository.GetFirstDiffIDErr
}
