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

var _ = Describe("Checked Headers repository", func() {
	var (
		db               = test_config.NewTestDB(test_config.NewTestNode())
		repo             datastore.CheckedHeadersRepository
		pluginSchemaName = "plugin"
	)

	Describe("without a valid setup", func() {
		BeforeEach(func() {
			test_config.CleanTestDB(db)
		})

		It("errors for a schema that isn't present", func() {
			var err error
			repo, err = repositories.NewCheckedHeadersRepository(db, pluginSchemaName)
			Expect(err.Error()).To(ContainSubstring("invalid schema"))
			Expect(repo).To(BeNil())
		})
	})

	Describe("with a valid database schema", func() {

		BeforeEach(func() {
			test_config.CleanTestDB(db)
			Expect(createPluginCheckedHeadersTable(db, pluginSchemaName)).To(Succeed())

			var repoErr error
			repo, repoErr = repositories.NewCheckedHeadersRepository(db, pluginSchemaName)
			Expect(repoErr).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			Expect(clearPluginSchema(db, pluginSchemaName)).To(Succeed())
		})

		Describe("MarkHeaderChecked", func() {
			It("marks passed header as checked on insert", func() {
				headerRepository := repositories.NewHeaderRepository(db)
				headerID, headerErr := headerRepository.CreateOrUpdateHeader(fakes.FakeHeader)
				Expect(headerErr).NotTo(HaveOccurred())

				Expect(repo.MarkHeaderChecked(headerID)).To(Succeed())

				Expect(selectCheckedHeaders(db, pluginSchemaName, headerID)).To(Equal(1))
			})

			It("increments check count on update", func() {
				headerRepository := repositories.NewHeaderRepository(db)
				headerID, headerErr := headerRepository.CreateOrUpdateHeader(fakes.FakeHeader)
				Expect(headerErr).NotTo(HaveOccurred())

				Expect(repo.MarkHeaderChecked(headerID)).To(Succeed())
				Expect(repo.MarkHeaderChecked(headerID)).To(Succeed())

				Expect(selectCheckedHeaders(db, pluginSchemaName, headerID)).To(Equal(2))
			})
		})

		Describe("MarkSingleHeaderUnchecked", func() {
			It("marks headers with matching block number as unchecked", func() {
				blockNumber := rand.Int63()
				fakeHeader := fakes.GetFakeHeader(blockNumber)
				headerRepository := repositories.NewHeaderRepository(db)
				headerID, insertHeaderErr := headerRepository.CreateOrUpdateHeader(fakeHeader)
				Expect(insertHeaderErr).NotTo(HaveOccurred())
				Expect(repo.MarkHeaderChecked(headerID)).To(Succeed())

				Expect(repo.MarkSingleHeaderUnchecked(blockNumber)).To(Succeed())

				Expect(selectCheckedHeaders(db, pluginSchemaName, headerID)).To(BeZero())
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
				Expect(repo.MarkHeaderChecked(checkedHeaderID)).To(Succeed())
				Expect(repo.MarkHeaderChecked(uncheckedHeaderID)).To(Succeed())

				// re-mark unchecked header as unchecked
				Expect(repo.MarkSingleHeaderUnchecked(uncheckedBlockNumber)).To(Succeed())

				// Verify the other block was not checked (1 checked header)
				Expect(selectCheckedHeaders(db, pluginSchemaName, checkedHeaderID)).To(Equal(1))
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
					Expect(err).NotTo(HaveOccurred())
					headerIDs = append(headerIDs, headerID)
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
					Expect(repo.MarkHeaderChecked(secondHeaderID)).To(Succeed())

					headers, err := repo.UncheckedHeaders(firstBlock, thirdBlock, uncheckedCheckCount)
					Expect(err).NotTo(HaveOccurred())

					headerBlockNumbers := getBlockNumbers(headers)
					Expect(headerBlockNumbers).To(ConsistOf(firstBlock, thirdBlock))
					Expect(headerBlockNumbers).NotTo(ContainElement(secondBlock))
				})

				Describe("when header has already been checked", func() {
					It("includes header with block number >= 15 back from latest with check count of 1", func() {
						Expect(repo.MarkHeaderChecked(thirdHeaderID)).To(Succeed())

						headers, err := repo.UncheckedHeaders(firstBlock, lastBlock, recheckCheckCount)
						Expect(err).NotTo(HaveOccurred())

						headerBlockNumbers := getBlockNumbers(headers)
						Expect(headerBlockNumbers).To(ContainElement(thirdBlock))
					})

					It("excludes header with block number < 15 back from latest with check count of 1", func() {
						excludedHeader := fakes.GetFakeHeader(thirdBlock + 1)
						excludedHeaderID, createHeaderErr := headerRepository.CreateOrUpdateHeader(excludedHeader)
						Expect(createHeaderErr).NotTo(HaveOccurred())
						Expect(repo.MarkHeaderChecked(excludedHeaderID)).To(Succeed())

						headers, err := repo.UncheckedHeaders(firstBlock, lastBlock, recheckCheckCount)
						Expect(err).NotTo(HaveOccurred())

						headerBlockNumbers := getBlockNumbers(headers)
						Expect(headerBlockNumbers).NotTo(ContainElement(excludedHeader.BlockNumber))
					})

					It("includes header with block number > 45 back from latest with check count of 2", func() {
						Expect(repo.MarkHeaderChecked(secondHeaderID)).To(Succeed())
						Expect(repo.MarkHeaderChecked(secondHeaderID)).To(Succeed())

						headers, err := repo.UncheckedHeaders(firstBlock, lastBlock, 3)
						Expect(err).NotTo(HaveOccurred())

						headerBlockNumbers := getBlockNumbers(headers)
						Expect(headerBlockNumbers).To(ContainElement(secondBlock))
					})

					It("excludes header with block number < 45 back from latest with check count of 2", func() {
						excludedHeader := fakes.GetFakeHeader(secondBlock + 1)
						excludedHeaderID, createHeaderErr := headerRepository.CreateOrUpdateHeader(excludedHeader)
						Expect(createHeaderErr).NotTo(HaveOccurred())

						Expect(repo.MarkHeaderChecked(excludedHeaderID)).To(Succeed())
						Expect(repo.MarkHeaderChecked(excludedHeaderID)).To(Succeed())

						headers, err := repo.UncheckedHeaders(firstBlock, lastBlock, 3)
						Expect(err).NotTo(HaveOccurred())

						headerBlockNumbers := getBlockNumbers(headers)
						Expect(headerBlockNumbers).NotTo(ContainElement(excludedHeader.BlockNumber))
					})
				})

				It("only returns headers associated with any node", func() {
					dbTwo := test_config.NewTestDB(core.Node{ID: "second"})
					headerRepositoryTwo := repositories.NewHeaderRepository(dbTwo)
					repoTwo, repoErr := repositories.NewCheckedHeadersRepository(dbTwo, pluginSchemaName)
					Expect(repoErr).NotTo(HaveOccurred())

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
					Expect(repo.MarkHeaderChecked(headerIDs[1])).To(Succeed())

					headers, err := repo.UncheckedHeaders(firstBlock, -1, uncheckedCheckCount)
					Expect(err).NotTo(HaveOccurred())

					headerBlockNumbers := getBlockNumbers(headers)
					Expect(headerBlockNumbers).To(ConsistOf(firstBlock, thirdBlock, lastBlock))
					Expect(headerBlockNumbers).NotTo(ContainElement(secondBlock))
				})

				Describe("when header has already been checked", func() {
					It("includes header with block number > 15 back from latest with check count of 1", func() {
						Expect(repo.MarkHeaderChecked(thirdHeaderID)).To(Succeed())

						headers, err := repo.UncheckedHeaders(firstBlock, -1, recheckCheckCount)
						Expect(err).NotTo(HaveOccurred())

						headerBlockNumbers := getBlockNumbers(headers)
						Expect(headerBlockNumbers).To(ContainElement(thirdBlock))
					})

					It("excludes header with block number < 15 back from latest with check count of 1", func() {
						excludedHeader := fakes.GetFakeHeader(thirdBlock + 1)
						excludedHeaderID, createHeaderErr := headerRepository.CreateOrUpdateHeader(excludedHeader)
						Expect(createHeaderErr).NotTo(HaveOccurred())
						Expect(repo.MarkHeaderChecked(excludedHeaderID)).To(Succeed())

						headers, err := repo.UncheckedHeaders(firstBlock, -1, recheckCheckCount)
						Expect(err).NotTo(HaveOccurred())

						headerBlockNumbers := getBlockNumbers(headers)
						Expect(headerBlockNumbers).NotTo(ContainElement(excludedHeader.BlockNumber))
					})

					It("includes header with block number > 45 back from latest with check count of 2", func() {
						Expect(repo.MarkHeaderChecked(secondHeaderID)).To(Succeed())
						Expect(repo.MarkHeaderChecked(secondHeaderID)).To(Succeed())

						headers, err := repo.UncheckedHeaders(firstBlock, -1, 3)
						Expect(err).NotTo(HaveOccurred())

						headerBlockNumbers := getBlockNumbers(headers)
						Expect(headerBlockNumbers).To(ContainElement(secondBlock))
					})

					It("excludes header with block number < 45 back from latest with check count of 2", func() {
						excludedHeader := fakes.GetFakeHeader(secondBlock + 1)
						excludedHeaderID, createHeaderErr := headerRepository.CreateOrUpdateHeader(excludedHeader)
						Expect(createHeaderErr).NotTo(HaveOccurred())

						Expect(repo.MarkHeaderChecked(excludedHeaderID)).To(Succeed())
						Expect(repo.MarkHeaderChecked(excludedHeaderID)).To(Succeed())

						headers, err := repo.UncheckedHeaders(firstBlock, -1, 3)
						Expect(err).NotTo(HaveOccurred())

						headerBlockNumbers := getBlockNumbers(headers)
						Expect(headerBlockNumbers).NotTo(ContainElement(excludedHeader.BlockNumber))
					})
				})

				It("returns headers associated with any node", func() {
					dbTwo := test_config.NewTestDB(core.Node{ID: "second"})
					headerRepositoryTwo := repositories.NewHeaderRepository(dbTwo)
					repoTwo, repoErr := repositories.NewCheckedHeadersRepository(dbTwo, pluginSchemaName)
					Expect(repoErr).NotTo(HaveOccurred())

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
})

func getBlockNumbers(headers []core.Header) []int64 {
	var headerBlockNumbers []int64
	for _, header := range headers {
		headerBlockNumbers = append(headerBlockNumbers, header.BlockNumber)
	}
	return headerBlockNumbers
}

func selectCheckedHeaders(db *postgres.DB, schemaName string, headerID int64) (int, error) {
	var checkedCount int
	queryString := fmt.Sprintf(`SELECT check_count FROM %s.checked_headers WHERE header_id = $1`, schemaName)
	fetchErr := db.Get(&checkedCount, queryString, headerID)
	return checkedCount, fetchErr
}

func createPluginSchema(db *postgres.DB, schemaName string) error {
	prepareSchema := `
CREATE SCHEMA IF NOT EXISTS %[1]s;
`

	_, schemaError := db.Exec(fmt.Sprintf(prepareSchema, schemaName))
	return schemaError
}

func createPluginCheckedHeadersTable(db *postgres.DB, schemaName string) error {
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
