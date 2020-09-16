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

package repositories_test

import (
	"fmt"
	"math/rand"

	"github.com/makerdao/vulcanizedb/pkg/core"
	"github.com/makerdao/vulcanizedb/pkg/datastore"
	"github.com/makerdao/vulcanizedb/pkg/datastore/postgres"
	"github.com/makerdao/vulcanizedb/pkg/datastore/postgres/repositories"
	"github.com/makerdao/vulcanizedb/pkg/fakes"
	"github.com/makerdao/vulcanizedb/test_config"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func selectCheckedHeaders(db *postgres.DB, schemaName string, headerID int64) (int, error) {
	var checkedCount int
	queryString := fmt.Sprintf(`SELECT check_count FROM %s.checked_headers WHERE header_id = $1`, schemaName)
	fetchErr := db.Get(&checkedCount, queryString, headerID)
	return checkedCount, fetchErr
}

func createPluginSchema(db *postgres.DB, schemaName string) error {
	prepareSchema := `
CREATE SCHEMA IF NOT EXISTS %[1]s;

CREATE TABLE %[1]s.checked_headers (
	id SERIAL PRIMARY KEY,
	check_count INTEGER  NOT NULL DEFAULT 0,
	header_id INTEGER NOT NULL REFERENCES public.headers(id) ON DELETE CASCADE,
	UNIQUE (header_id)
);
`
	_, schemaError := db.Exec(fmt.Sprintf(prepareSchema, schemaName))
	return schemaError
}

func clearPluginSchema(db *postgres.DB, schemaName string) error {
	_, err := db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE;", schemaName))
	return err
}

var _ = Describe("Checked Headers repository", func() {
	var (
		db               = test_config.NewTestDB(test_config.NewTestNode())
		repo             datastore.CheckedHeadersRepository
		pluginSchemaName = "plugin"
	)

	BeforeEach(func() {
		test_config.CleanTestDB(db)
		schemaErr := createPluginSchema(db, pluginSchemaName)
		Expect(schemaErr).NotTo(HaveOccurred())

		repo = repositories.NewCheckedHeadersRepository(db, pluginSchemaName)
	})

	AfterEach(func() {
		err := clearPluginSchema(db, pluginSchemaName)
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("MarkHeaderChecked", func() {
		It("marks passed header as checked on insert", func() {
			headerRepository := repositories.NewHeaderRepository(db)
			headerID, headerErr := headerRepository.CreateOrUpdateHeader(fakes.FakeHeader)
			Expect(headerErr).NotTo(HaveOccurred())

			err := repo.MarkHeaderChecked(headerID)

			Expect(err).NotTo(HaveOccurred())
			checkedCount, fetchErr := selectCheckedHeaders(db, pluginSchemaName, headerID)
			Expect(fetchErr).NotTo(HaveOccurred())
			Expect(checkedCount).To(Equal(1))
		})

		It("increments check count on update", func() {
			headerRepository := repositories.NewHeaderRepository(db)
			headerID, headerErr := headerRepository.CreateOrUpdateHeader(fakes.FakeHeader)
			Expect(headerErr).NotTo(HaveOccurred())

			insertErr := repo.MarkHeaderChecked(headerID)
			Expect(insertErr).NotTo(HaveOccurred())

			updateErr := repo.MarkHeaderChecked(headerID)
			Expect(updateErr).NotTo(HaveOccurred())

			checkedCount, fetchErr := selectCheckedHeaders(db, pluginSchemaName, headerID)
			Expect(fetchErr).NotTo(HaveOccurred())
			Expect(checkedCount).To(Equal(2))
		})
	})

	Describe("MarkSingleHeaderUnchecked", func() {
		It("marks headers with matching block number as unchecked", func() {
			blockNumber := rand.Int63()
			fakeHeader := fakes.GetFakeHeader(blockNumber)
			headerRepository := repositories.NewHeaderRepository(db)
			headerID, insertHeaderErr := headerRepository.CreateOrUpdateHeader(fakeHeader)
			Expect(insertHeaderErr).NotTo(HaveOccurred())
			markHeaderCheckedErr := repo.MarkHeaderChecked(headerID)
			Expect(markHeaderCheckedErr).NotTo(HaveOccurred())

			err := repo.MarkSingleHeaderUnchecked(blockNumber)
			Expect(err).NotTo(HaveOccurred())

			headerCheckCount, getHeaderErr := selectCheckedHeaders(db, pluginSchemaName, headerID)
			Expect(getHeaderErr).NotTo(HaveOccurred())
			Expect(headerCheckCount).To(BeZero())
		})

		It("leaves headers without a matching block number alone", func() {
			checkedBlockNumber := rand.Int63()
			uncheckedBlockNumber := checkedBlockNumber + 1
			checkedHeader := fakes.GetFakeHeader(checkedBlockNumber)
			uncheckedHeader := fakes.GetFakeHeader(uncheckedBlockNumber)
			headerRepository := repositories.NewHeaderRepository(db)
			checkedHeaderID, insertCheckedHeaderErr := headerRepository.CreateOrUpdateHeader(checkedHeader)
			Expect(insertCheckedHeaderErr).NotTo(HaveOccurred())
			uncheckedHeaderID, insertUncheckedHeaderErr := headerRepository.CreateOrUpdateHeader(uncheckedHeader)
			Expect(insertUncheckedHeaderErr).NotTo(HaveOccurred())

			// mark both headers as checked
			markCheckedHeaderErr := repo.MarkHeaderChecked(checkedHeaderID)
			Expect(markCheckedHeaderErr).NotTo(HaveOccurred())
			markUncheckedHeaderErr := repo.MarkHeaderChecked(uncheckedHeaderID)
			Expect(markUncheckedHeaderErr).NotTo(HaveOccurred())

			// re-mark unchecked header as unchecked
			err := repo.MarkSingleHeaderUnchecked(uncheckedBlockNumber)
			Expect(err).NotTo(HaveOccurred())

			// Verify the other block was ignored
			checkedHeaderCount, checkedHeaderErr := selectCheckedHeaders(db, pluginSchemaName, checkedHeaderID)
			Expect(checkedHeaderErr).NotTo(HaveOccurred())
			Expect(checkedHeaderCount).To(Equal(1))
		})
	})

	Describe("UncheckedHeaders", func() {
		var (
			headerRepository datastore.HeaderRepository
			firstBlock,
			secondBlock,
			thirdBlock,
			lastBlock,
			secondHeaderID,
			thirdHeaderID int64
			blockNumbers        []int64
			headerIDs           []int64
			err                 error
			uncheckedCheckCount = int64(1)
			recheckCheckCount   = int64(2)
		)

		BeforeEach(func() {
			headerRepository = repositories.NewHeaderRepository(db)

			lastBlock = rand.Int63()
			thirdBlock = lastBlock - 15
			secondBlock = lastBlock - (15 + 30)
			firstBlock = lastBlock - (15 + 30 + 45)

			blockNumbers = []int64{firstBlock, secondBlock, thirdBlock, lastBlock}

			headerIDs = []int64{}
			for _, n := range blockNumbers {
				headerID, err := headerRepository.CreateOrUpdateHeader(fakes.GetFakeHeader(n))
				headerIDs = append(headerIDs, headerID)
				Expect(err).NotTo(HaveOccurred())
			}
			secondHeaderID = headerIDs[1]
			thirdHeaderID = headerIDs[2]
		})

		Describe("when ending block is specified", func() {
			It("excludes headers that are out of range", func() {
				headers, err := repo.UncheckedHeaders(firstBlock, thirdBlock, uncheckedCheckCount)
				Expect(err).NotTo(HaveOccurred())

				headerBlockNumbers := getBlockNumbers(headers)
				Expect(headerBlockNumbers).To(ConsistOf(firstBlock, secondBlock, thirdBlock))
				Expect(headerBlockNumbers).NotTo(ContainElement(lastBlock))
			})

			It("excludes headers that have been checked more than the check count", func() {
				err = repo.MarkHeaderChecked(secondHeaderID)
				Expect(err).NotTo(HaveOccurred())

				headers, err := repo.UncheckedHeaders(firstBlock, thirdBlock, uncheckedCheckCount)
				Expect(err).NotTo(HaveOccurred())

				headerBlockNumbers := getBlockNumbers(headers)
				Expect(headerBlockNumbers).To(ConsistOf(firstBlock, thirdBlock))
				Expect(headerBlockNumbers).NotTo(ContainElement(secondBlock))
			})

			Describe("when header has already been checked", func() {
				It("includes header with block number > 15 back from latest with check count of 1", func() {
					err := repo.MarkHeaderChecked(thirdHeaderID)
					Expect(err).NotTo(HaveOccurred())

					headers, err := repo.UncheckedHeaders(firstBlock, lastBlock, recheckCheckCount)
					Expect(err).NotTo(HaveOccurred())

					headerBlockNumbers := getBlockNumbers(headers)
					Expect(headerBlockNumbers).To(ContainElement(thirdBlock))
				})

				It("excludes header with block number < 15 back from latest with check count of 1", func() {
					excludedHeader := fakes.GetFakeHeader(thirdBlock + 1)
					excludedHeaderID, createHeaderErr := headerRepository.CreateOrUpdateHeader(excludedHeader)
					Expect(createHeaderErr).NotTo(HaveOccurred())
					updateHeaderErr := repo.MarkHeaderChecked(excludedHeaderID)
					Expect(updateHeaderErr).NotTo(HaveOccurred())

					headers, err := repo.UncheckedHeaders(firstBlock, lastBlock, recheckCheckCount)
					Expect(err).NotTo(HaveOccurred())

					headerBlockNumbers := getBlockNumbers(headers)
					Expect(headerBlockNumbers).NotTo(ContainElement(excludedHeader.BlockNumber))
				})

				It("includes header with block number > 45 back from latest with check count of 2", func() {
					err := repo.MarkHeaderChecked(secondHeaderID)
					Expect(err).NotTo(HaveOccurred())

					headers, err := repo.UncheckedHeaders(firstBlock, lastBlock, recheckCheckCount)
					Expect(err).NotTo(HaveOccurred())

					headerBlockNumbers := getBlockNumbers(headers)
					Expect(headerBlockNumbers).To(ContainElement(secondBlock))
				})

				It("excludes header with block number < 45 back from latest with check count of 2", func() {
					excludedHeader := fakes.GetFakeHeader(secondBlock + 1)
					excludedHeaderID, createHeaderErr := headerRepository.CreateOrUpdateHeader(excludedHeader)
					Expect(createHeaderErr).NotTo(HaveOccurred())

					firstCheckErr := repo.MarkHeaderChecked(excludedHeaderID)
					Expect(firstCheckErr).NotTo(HaveOccurred())
					secondCheckErr := repo.MarkHeaderChecked(excludedHeaderID)
					Expect(secondCheckErr).NotTo(HaveOccurred())

					headers, err := repo.UncheckedHeaders(firstBlock, lastBlock, 3)
					Expect(err).NotTo(HaveOccurred())

					headerBlockNumbers := getBlockNumbers(headers)
					Expect(headerBlockNumbers).NotTo(ContainElement(excludedHeader.BlockNumber))
				})
			})

			It("only returns headers associated with any node", func() {
				dbTwo := test_config.NewTestDB(core.Node{ID: "second"})
				headerRepositoryTwo := repositories.NewHeaderRepository(dbTwo)
				repoTwo := repositories.NewCheckedHeadersRepository(dbTwo, pluginSchemaName)
				for _, n := range blockNumbers {
					_, err = headerRepositoryTwo.CreateOrUpdateHeader(fakes.GetFakeHeader(n + 10))
					Expect(err).NotTo(HaveOccurred())
				}
				allHeaders := []int64{firstBlock, firstBlock + 10, secondBlock, secondBlock + 10, thirdBlock, thirdBlock + 10}

				nodeOneMissingHeaders, err := repo.UncheckedHeaders(firstBlock, thirdBlock+10, uncheckedCheckCount)
				Expect(err).NotTo(HaveOccurred())
				nodeOneHeaderBlockNumbers := getBlockNumbers(nodeOneMissingHeaders)
				Expect(nodeOneHeaderBlockNumbers).To(ConsistOf(allHeaders))

				nodeTwoMissingHeaders, err := repoTwo.UncheckedHeaders(firstBlock, thirdBlock+10, uncheckedCheckCount)
				Expect(err).NotTo(HaveOccurred())
				nodeTwoHeaderBlockNumbers := getBlockNumbers(nodeTwoMissingHeaders)
				Expect(nodeTwoHeaderBlockNumbers).To(ConsistOf(allHeaders))
			})
		})

		Describe("when ending block is -1", func() {
			It("includes all non-checked headers when ending block is -1 ", func() {
				headers, err := repo.UncheckedHeaders(firstBlock, -1, uncheckedCheckCount)
				Expect(err).NotTo(HaveOccurred())

				headerBlockNumbers := getBlockNumbers(headers)
				Expect(headerBlockNumbers).To(ConsistOf(firstBlock, secondBlock, thirdBlock, lastBlock))
			})

			It("excludes headers that have been checked more than the check count", func() {
				err := repo.MarkHeaderChecked(headerIDs[1])
				Expect(err).NotTo(HaveOccurred())

				headers, err := repo.UncheckedHeaders(firstBlock, -1, uncheckedCheckCount)
				Expect(err).NotTo(HaveOccurred())

				headerBlockNumbers := getBlockNumbers(headers)
				Expect(headerBlockNumbers).To(ConsistOf(firstBlock, thirdBlock, lastBlock))
				Expect(headerBlockNumbers).NotTo(ContainElement(secondBlock))
			})

			Describe("when header has already been checked", func() {
				It("includes header with block number > 15 back from latest with check count of 1", func() {
					err := repo.MarkHeaderChecked(thirdHeaderID)
					Expect(err).NotTo(HaveOccurred())

					headers, err := repo.UncheckedHeaders(firstBlock, -1, recheckCheckCount)
					Expect(err).NotTo(HaveOccurred())

					headerBlockNumbers := getBlockNumbers(headers)
					Expect(headerBlockNumbers).To(ContainElement(thirdBlock))
				})

				It("excludes header with block number < 15 back from latest with check count of 1", func() {
					excludedHeader := fakes.GetFakeHeader(thirdBlock + 1)
					excludedHeaderID, createHeaderErr := headerRepository.CreateOrUpdateHeader(excludedHeader)
					Expect(createHeaderErr).NotTo(HaveOccurred())
					updateHeaderErr := repo.MarkHeaderChecked(excludedHeaderID)
					Expect(updateHeaderErr).NotTo(HaveOccurred())

					headers, err := repo.UncheckedHeaders(firstBlock, -1, recheckCheckCount)
					Expect(err).NotTo(HaveOccurred())

					headerBlockNumbers := getBlockNumbers(headers)
					Expect(headerBlockNumbers).NotTo(ContainElement(excludedHeader.BlockNumber))
				})

				It("includes header with block number > 45 back from latest with check count of 1", func() {
					err := repo.MarkHeaderChecked(secondHeaderID)
					Expect(err).NotTo(HaveOccurred())

					headers, err := repo.UncheckedHeaders(firstBlock, -1, recheckCheckCount)
					Expect(err).NotTo(HaveOccurred())

					headerBlockNumbers := getBlockNumbers(headers)
					Expect(headerBlockNumbers).To(ContainElement(secondBlock))
				})

				It("excludes header with block number < 45 back from latest with check count of 2", func() {
					excludedHeader := fakes.GetFakeHeader(secondBlock + 1)
					excludedHeaderID, createHeaderErr := headerRepository.CreateOrUpdateHeader(excludedHeader)
					Expect(createHeaderErr).NotTo(HaveOccurred())

					updateHeaderErr := repo.MarkHeaderChecked(excludedHeaderID)
					Expect(updateHeaderErr).NotTo(HaveOccurred())
					updateHeaderErr = repo.MarkHeaderChecked(excludedHeaderID)
					Expect(updateHeaderErr).NotTo(HaveOccurred())

					headers, err := repo.UncheckedHeaders(firstBlock, -1, 3)
					Expect(err).NotTo(HaveOccurred())

					headerBlockNumbers := getBlockNumbers(headers)
					Expect(headerBlockNumbers).NotTo(ContainElement(excludedHeader.BlockNumber))
				})
			})

			It("returns headers associated with any node", func() {
				dbTwo := test_config.NewTestDB(core.Node{ID: "second"})
				headerRepositoryTwo := repositories.NewHeaderRepository(dbTwo)
				repoTwo := repositories.NewCheckedHeadersRepository(dbTwo, pluginSchemaName)
				for _, n := range blockNumbers {
					_, err = headerRepositoryTwo.CreateOrUpdateHeader(fakes.GetFakeHeader(n + 10))
					Expect(err).NotTo(HaveOccurred())
				}
				allHeaders := []int64{firstBlock, firstBlock + 10, secondBlock, secondBlock + 10, thirdBlock, thirdBlock + 10, lastBlock, lastBlock + 10}

				nodeOneMissingHeaders, err := repo.UncheckedHeaders(firstBlock, -1, uncheckedCheckCount)
				Expect(err).NotTo(HaveOccurred())
				nodeOneBlockNumbers := getBlockNumbers(nodeOneMissingHeaders)
				Expect(nodeOneBlockNumbers).To(ConsistOf(allHeaders))

				nodeTwoMissingHeaders, err := repoTwo.UncheckedHeaders(firstBlock, -1, uncheckedCheckCount)
				Expect(err).NotTo(HaveOccurred())
				nodeTwoBlockNumbers := getBlockNumbers(nodeTwoMissingHeaders)
				Expect(nodeTwoBlockNumbers).To(ConsistOf(allHeaders))
			})
		})
	})
})

func getBlockNumbers(headers []core.Header) []int64 {
	var headerBlockNumbers []int64
	for _, header := range headers {
		headerBlockNumbers = append(headerBlockNumbers, header.BlockNumber)
	}
	return headerBlockNumbers
}
