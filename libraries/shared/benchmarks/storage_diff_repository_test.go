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

package benchmarks

import (
	"fmt"
	"math/rand"

	"github.com/ethereum/go-ethereum/common"
	"github.com/makerdao/vulcanizedb/libraries/shared/storage"
	"github.com/makerdao/vulcanizedb/libraries/shared/storage/types"
	"github.com/makerdao/vulcanizedb/libraries/shared/test_data"
	"github.com/makerdao/vulcanizedb/pkg/datastore/postgres"
	"github.com/makerdao/vulcanizedb/test_config"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Storage diffs repository", func() {
	var (
		db              = test_config.NewTestDB(test_config.NewTestNode())
		repo            storage.DiffRepository
		fakeStorageDiff types.RawDiff
	)

	BeforeEach(func() {
		//test_config.CleanTestDB(db)
		repo = storage.NewDiffRepository(db)
		fakeStorageDiff = types.RawDiff{
			HashedAddress: test_data.FakeHash(),
			BlockHash:     test_data.FakeHash(),
			BlockHeight:   rand.Int(),
			StorageKey:    test_data.FakeHash(),
			StorageValue:  test_data.FakeHash(),
		}
	})

	Describe("Benchmarks", func() {
		var (
		//numberOfCheckedDiffs = 20949597
		//numberOfUncheckedDiffs = 1396488
		)
		BeforeEach(func() {
			//err := seedDatabase(db, fakeStorageDiff, numberOfCheckedDiffs, numberOfUncheckedDiffs)
			//Expect(err).NotTo(HaveOccurred())
		})

		Measure("CreateStorageDiff", func(b Benchmarker) {
			fakeStorageDiff = types.RawDiff{
				HashedAddress: test_data.FakeHash(),
				BlockHash:     test_data.FakeHash(),
				BlockHeight:   rand.Int(),
				StorageKey:    test_data.FakeHash(),
				StorageValue:  test_data.FakeHash(),
			}

			b.Time("runtime", func() {
				hex := test_data.FakeHash().Hex()
				fmt.Println(len(common.HexToHash(hex).Bytes()))

				id, err := repo.CreateStorageDiff(fakeStorageDiff)
				fmt.Println(id)
				Expect(err).NotTo(HaveOccurred())
			})

		}, 1)

		Measure("MarkChecked", func(b Benchmarker) {
			var markCheckedId = int64(20949597 + 1 + 1000)
			b.Time("runtime", func() {
				fmt.Println("MarkCheckedId: ", markCheckedId)
				err := repo.MarkChecked(markCheckedId)
				Expect(err).NotTo(HaveOccurred())
				markCheckedId++
			})
		}, 10000)

		Measure("GetNewDiffs", func(b Benchmarker) {
			runtime := b.Time("runtime", func() {
				diffs := make(chan types.PersistedDiff)
				errs := make(chan error)
				done := make(chan bool)

				defer close(diffs)
				defer close(errs)
				defer close(done)

				go repo.GetNewDiffs(diffs, errs, done)

				var diffCount = 0
				for {
					select {
					case <-diffs:
						diffCount++
					case err := <-errs:
						fmt.Println("err", err)
						break
					case <-done:
						fmt.Println("done")
						return
					}
				}
			})
			Expect(runtime).To(Not(BeZero()))

		}, 10)
	})
})


func seedDatabase(db *postgres.DB, testDiff types.RawDiff, numberOfCheckedDiffs, numberOfUncheckedDiffs int) error {
	fmt.Println("seeding db with checked diffs")
	for i := 1; i <= numberOfCheckedDiffs; i++ {
		blockHeight := i
		_, err := db.Exec(`INSERT INTO public.storage_diff
		(hashed_address, block_height, block_hash, storage_key, storage_value, checked) VALUES ($1, $2, $3, $4, $5, $6)`,
			testDiff.HashedAddress.Bytes(), blockHeight, testDiff.BlockHash.Bytes(),
			testDiff.StorageKey.Bytes(), testDiff.StorageValue.Bytes(), true)
		if err != nil {
			return err
		}
	}

	fmt.Println("seeding db with unchecked diffs")
	for i := 1; i <= numberOfUncheckedDiffs; i++ {
		blockHeight := numberOfCheckedDiffs + i
		_, err := db.Exec(`INSERT INTO public.storage_diff
		(hashed_address, block_height, block_hash, storage_key, storage_value, checked) VALUES ($1, $2, $3, $4, $5, $6)`,
			testDiff.HashedAddress.Bytes(), blockHeight, testDiff.BlockHash.Bytes(),
			testDiff.StorageKey.Bytes(), testDiff.StorageValue.Bytes(), false)
		if err != nil {
			return err
		}
	}
	return nil
}
