/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.HiveMetastore;
import io.trino.operator.OperatorStats;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metric;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.parquet.Parquet;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.io.Resources.getResource;
import static io.trino.parquet.reader.ParquetReader.COLUMN_INDEX_ROWS_FILTERED;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getParquetFileMetadata;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_ROW_LIMIT;
import static org.apache.iceberg.TableProperties.PARQUET_PAGE_SIZE_BYTES;
import static org.apache.iceberg.mapping.NameMappingParser.toJson;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergParquetPageSkipping
        extends AbstractTestQueryFramework
{
    private TrinoFileSystem fileSystem;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.file-format", "PARQUET")
                .addIcebergProperty("parquet.use-column-index", "true")
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        fileSystem = getFileSystemFactory(getQueryRunner()).create(SESSION);
        metastore = getHiveMetastore(getQueryRunner());
    }

    @Test
    public void testPageSkippingMatchesDisabledSession()
            throws Exception
    {
        String tableName = createTableWithIndexedFile();
        @Language("SQL") String query = "SELECT * FROM " + tableName +
                " WHERE totalprice BETWEEN 100000 AND 131280 AND clerk = 'Clerk#000000624'";
        assertThat(assertColumnIndexResults(query)).isGreaterThan(0);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRowGroupPruningFromPageIndexes()
            throws Exception
    {
        String tableName = createTableWithIndexedFile();
        // totalprice BETWEEN 51890 AND 51900 lies between row-group min/max but outside page min/max
        assertRowGroupPruning("SELECT * FROM " + tableName + " WHERE totalprice BETWEEN 51890 AND 51900 AND orderkey > 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPageSkippingFiltersRows()
            throws Exception
    {
        String tableName = createLineitemTableWithIndexedFile();
        compareScanWork("SELECT * FROM " + tableName + " WHERE suppkey = 10");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartitionEvolutionDoesNotReturnEmpty()
            throws Exception
    {
        // Unpartitioned indexed file, then add an orderstatus partition spec and a second
        // indexed file. Filter on orderstatus is unenforced across specs. The new-spec file
        // serves orderstatus as a constant, so the page-skip predicate must drop it or
        // ColumnIndexFilter returns zero rows (#13584 / mwong77).
        String tableName = createTableWithIndexedFile();
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES partitioning = ARRAY['orderstatus']");
        appendIndexedFileWithPartition(
                tableName,
                "parquet_page_skipping/orders_sorted_by_totalprice/data.parquet",
                "O");
        @Language("SQL") String query = "SELECT orderkey FROM " + tableName +
                " WHERE orderstatus = 'O' AND totalprice BETWEEN 100000 AND 131280";
        assertThat(assertColumnIndexResults(query)).isGreaterThan(0);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPositionDeletesDisablePageSkipping()
            throws Exception
    {
        String tableName = createParquetV2IndexedTable();
        @Language("SQL") String neighbors = "SELECT id, payload FROM " + tableName + " WHERE id BETWEEN 8 AND 12 ORDER BY id";
        assertQuery(neighbors, "VALUES (8, 'row-8'), (9, 'row-9'), (10, 'row-10'), (11, 'row-11'), (12, 'row-12')");

        assertUpdate("DELETE FROM " + tableName + " WHERE id = 10", 1);
        assertQuery(neighbors, "VALUES (8, 'row-8'), (9, 'row-9'), (11, 'row-11'), (12, 'row-12')");
        assertQueryReturnsEmptyResult("SELECT id FROM " + tableName + " WHERE id = 10");

        assertIdentityPathSafe("SELECT id, payload FROM " + tableName + " WHERE id BETWEEN 8 AND 12");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testParquetV2PageSkipping()
            throws Exception
    {
        String tableName = createParquetV2IndexedTable();
        assertParquetV2Pages(tableName);
        compareScanWork("SELECT * FROM " + tableName + " WHERE id = 10");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRowIdDisablesPageSkipping()
            throws Exception
    {
        String tableName = createParquetV2IndexedTable();
        @Language("SQL") String query = "SELECT id, \"$row_id\" FROM " + tableName + " WHERE id BETWEEN 8 AND 12";
        assertIdentityPathSafe(query);
        assertQuery(query, "VALUES (8, 8), (9, 9), (10, 10), (11, 11), (12, 12)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMergeDisablesPageSkipping()
            throws Exception
    {
        String tableWithSkip = createParquetV2IndexedTable();
        String tableWithoutSkip = createParquetV2IndexedTable();
        @Language("SQL") String mergeSql =
                "MERGE INTO %s t USING (VALUES BIGINT '10') s(id) ON t.id = s.id " +
                        "WHEN MATCHED THEN UPDATE SET payload = 'updated'";
        QueryRunner queryRunner = getDistributedQueryRunner();
        MaterializedResultWithPlan mergeWithSkip = queryRunner.executeWithPlan(
                getSession(),
                mergeSql.formatted(tableWithSkip));
        assertThat(getScanOperatorStats(mergeWithSkip.queryId())
                .getConnectorMetrics()
                .getMetrics())
                .doesNotContainKey(COLUMN_INDEX_ROWS_FILTERED);
        assertUpdate(noParquetColumnIndexFiltering(getSession()), mergeSql.formatted(tableWithoutSkip), 1);
        assertQuery(
                "SELECT id, payload FROM " + tableWithSkip + " WHERE id BETWEEN 8 AND 12 ORDER BY id",
                "VALUES (8, 'row-8'), (9, 'row-9'), (10, 'updated'), (11, 'row-11'), (12, 'row-12')");
        assertEqualsIgnoreOrder(
                computeActual("SELECT * FROM " + tableWithSkip),
                computeActual("SELECT * FROM " + tableWithoutSkip));
        assertUpdate("DROP TABLE " + tableWithSkip);
        assertUpdate("DROP TABLE " + tableWithoutSkip);
    }

    @Test
    public void testSessionKillSwitch()
            throws Exception
    {
        String tableName = createTableWithIndexedFile();
        @Language("SQL") String query = "SELECT * FROM " + tableName +
                " WHERE totalprice BETWEEN 100000 AND 131280 AND clerk = 'Clerk#000000624'";
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(
                noParquetColumnIndexFiltering(getSession()),
                query);
        Map<String, Metric<?>> metrics = getScanOperatorStats(result.queryId())
                .getConnectorMetrics()
                .getMetrics();
        assertThat(metrics).doesNotContainKey(COLUMN_INDEX_ROWS_FILTERED);
        assertUpdate("DROP TABLE " + tableName);
    }

    private String createTableWithIndexedFile()
            throws Exception
    {
        String tableName = "test_iceberg_page_skipping_" + randomNameSuffix();
        assertUpdate(
                """
                CREATE TABLE %s (
                   orderkey bigint,
                   custkey bigint,
                   orderstatus varchar,
                   totalprice double,
                   orderdate date,
                   orderpriority varchar,
                   clerk varchar,
                   shippriority integer,
                   comment varchar,
                   rvalues array(double))
                WITH (format = 'PARQUET')
                """.formatted(tableName));
        assertUpdate("INSERT INTO " + tableName + " SELECT *, ARRAY[rand(), rand(), rand()] FROM tpch.tiny.orders", 15000);
        replaceDataFile(tableName, "parquet_page_skipping/orders_sorted_by_totalprice/data.parquet");
        return tableName;
    }

    private String createLineitemTableWithIndexedFile()
            throws Exception
    {
        String tableName = "test_iceberg_page_skipping_lineitem_" + randomNameSuffix();
        assertUpdate(
                """
                CREATE TABLE %s (
                   suppkey bigint,
                   extendedprice decimal(12, 2),
                   shipmode varchar,
                   comment varchar)
                WITH (format = 'PARQUET')
                """.formatted(tableName));
        assertUpdate("INSERT INTO " + tableName + " SELECT suppkey, extendedprice, shipmode, comment FROM tpch.tiny.lineitem", 60175);
        replaceDataFile(tableName, "parquet_page_skipping/lineitem_sorted_by_suppkey/data.parquet");
        return tableName;
    }

    private String createParquetV2IndexedTable()
            throws Exception
    {
        String tableName = "test_iceberg_page_skipping_v2_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, payload varchar) WITH (format = 'PARQUET', format_version = 3)");
        BaseTable table = IcebergTestUtils.loadTable(tableName, metastore, getFileSystemFactory(getQueryRunner()), ICEBERG_CATALOG, "tpch");
        Schema schema = table.schema();
        String dataPath = table.location() + "/data/v2-indexed-" + randomNameSuffix() + ".parquet";
        FileAppender<Record> writer = Parquet.write(table.io().newOutputFile(dataPath))
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::create)
                .writerVersion(PARQUET_2_0)
                .set(PARQUET_PAGE_ROW_LIMIT, "32")
                .set(PARQUET_PAGE_SIZE_BYTES, "256")
                .build();
        try {
            Record record = GenericRecord.create(schema);
            for (long id = 0; id < 2000; id++) {
                record.setField("id", id);
                record.setField("payload", "row-" + id);
                writer.add(record);
            }
        }
        finally {
            writer.close();
        }
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(dataPath)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(writer.length())
                .withMetrics(writer.metrics())
                .build();
        table.newAppend()
                .appendFile(dataFile)
                .commit();
        return tableName;
    }

    private void assertParquetV2Pages(String tableName)
            throws Exception
    {
        String filePath = (String) computeScalar(format("SELECT file_path FROM \"%s$files\"", tableName));
        ParquetMetadata parquetMetadata = getParquetFileMetadata(fileSystem.newInputFile(Location.of(filePath)));
        assertThat(parquetMetadata.getBlocks()).isNotEmpty();
        boolean usesV2Pages = parquetMetadata.getBlocks().stream()
                .flatMap(block -> block.columns().stream())
                .anyMatch(column -> column.getEncodingStats() != null && column.getEncodingStats().usesV2Pages());
        assertThat(usesV2Pages).isTrue();
        boolean hasColumnIndex = parquetMetadata.getBlocks().stream()
                .flatMap(block -> block.columns().stream())
                .anyMatch(column -> column.getColumnIndexReference() != null);
        assertThat(hasColumnIndex).isTrue();
    }

    private void replaceDataFile(String tableName, String resourceName)
            throws Exception
    {
        String parquetFilePath = (String) computeScalar(format("SELECT DISTINCT file_path FROM \"%s$files\"", tableName));
        byte[] parquetFileData = Resources.toByteArray(getResource(resourceName));
        fileSystem.newOutputFile(Location.of(parquetFilePath)).createOrOverwrite(parquetFileData);
        fileSystem.deleteFiles(List.of(Location.of(parquetFilePath.replaceAll("/([^/]*)$", ".$1.crc"))));

        BaseTable table = IcebergTestUtils.loadTable(tableName, metastore, getFileSystemFactory(getQueryRunner()), ICEBERG_CATALOG, "tpch");
        table.updateProperties()
                .set(DEFAULT_NAME_MAPPING, toJson(MappingUtil.create(table.schema())))
                .commit();
    }

    private void appendIndexedFileWithPartition(String tableName, String resourceName, String orderstatus)
            throws Exception
    {
        BaseTable table = IcebergTestUtils.loadTable(tableName, metastore, getFileSystemFactory(getQueryRunner()), ICEBERG_CATALOG, "tpch");
        String dataPath = table.location() + "/data/part-" + randomNameSuffix() + ".parquet";
        byte[] parquetFileData = Resources.toByteArray(getResource(resourceName));
        fileSystem.newOutputFile(Location.of(dataPath)).createOrOverwrite(parquetFileData);
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(dataPath)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(parquetFileData.length)
                .withRecordCount(15000)
                .withPartition(new PartitionData(new Object[] {orderstatus}))
                .build();
        table.newAppend()
                .appendFile(dataFile)
                .commit();
        table.updateProperties()
                .set(DEFAULT_NAME_MAPPING, toJson(MappingUtil.create(table.schema())))
                .commit();
    }

    private int assertColumnIndexResults(String query)
    {
        MaterializedResult withColumnIndexing = computeActual(query);
        MaterializedResult withoutColumnIndexing = computeActual(noParquetColumnIndexFiltering(getSession()), query);
        assertEqualsIgnoreOrder(withColumnIndexing, withoutColumnIndexing);
        return withoutColumnIndexing.getRowCount();
    }

    private void compareScanWork(@Language("SQL") String query)
    {
        QueryRunner queryRunner = getDistributedQueryRunner();
        MaterializedResultWithPlan resultWithoutColumnIndex = queryRunner.executeWithPlan(
                noParquetColumnIndexFiltering(getSession()),
                query);
        QueryStats off = getQueryStats(resultWithoutColumnIndex.queryId());
        Map<String, Metric<?>> metricsOff = getScanOperatorStats(resultWithoutColumnIndex.queryId())
                .getConnectorMetrics()
                .getMetrics();

        MaterializedResultWithPlan resultWithColumnIndex = queryRunner.executeWithPlan(getSession(), query);
        QueryStats on = getQueryStats(resultWithColumnIndex.queryId());
        Map<String, Metric<?>> metricsOn = getScanOperatorStats(resultWithColumnIndex.queryId())
                .getConnectorMetrics()
                .getMetrics();
        long rowsFiltered = metricsOn.containsKey(COLUMN_INDEX_ROWS_FILTERED)
                ? ((Count<?>) metricsOn.get(COLUMN_INDEX_ROWS_FILTERED)).getTotal()
                : 0;

        assertThat(metricsOff).doesNotContainKey(COLUMN_INDEX_ROWS_FILTERED);
        assertThat(rowsFiltered).isGreaterThan(0);
        assertThat(on.getPhysicalInputPositions())
                .isGreaterThan(0)
                .isLessThan(off.getPhysicalInputPositions());
        assertThat(on.getPhysicalInputDataSize().toBytes())
                .isLessThanOrEqualTo(off.getPhysicalInputDataSize().toBytes());
        assertEqualsIgnoreOrder(resultWithColumnIndex.result(), resultWithoutColumnIndex.result());
    }

    private void assertIdentityPathSafe(@Language("SQL") String query)
    {
        QueryRunner queryRunner = getDistributedQueryRunner();
        MaterializedResultWithPlan resultWithColumnIndex = queryRunner.executeWithPlan(getSession(), query);
        Map<String, Metric<?>> metrics = getScanOperatorStats(resultWithColumnIndex.queryId())
                .getConnectorMetrics()
                .getMetrics();
        assertThat(metrics).doesNotContainKey(COLUMN_INDEX_ROWS_FILTERED);
        assertThat(resultWithColumnIndex.result().getRowCount()).isGreaterThan(0);

        MaterializedResultWithPlan resultWithoutColumnIndex = queryRunner.executeWithPlan(
                noParquetColumnIndexFiltering(getSession()),
                query);
        assertEqualsIgnoreOrder(resultWithColumnIndex.result(), resultWithoutColumnIndex.result());
    }

    private void assertRowGroupPruning(@Language("SQL") String sql)
    {
        assertQueryStats(
                noParquetColumnIndexFiltering(getSession()),
                sql,
                queryStats -> {
                    assertThat(queryStats.getPhysicalInputPositions()).isGreaterThan(0);
                    assertThat(queryStats.getProcessedInputPositions()).isEqualTo(queryStats.getPhysicalInputPositions());
                },
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        assertQueryStats(
                getSession(),
                sql,
                queryStats -> {
                    assertThat(queryStats.getPhysicalInputPositions()).isEqualTo(0);
                    assertThat(queryStats.getProcessedInputPositions()).isEqualTo(0);
                },
                results -> assertThat(results.getRowCount()).isEqualTo(0));
    }

    private Session noParquetColumnIndexFiltering(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "parquet_use_column_index", "false")
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "use_file_size_from_metadata", "false")
                .build();
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setCatalogSessionProperty(super.getSession().getCatalog().orElseThrow(), "use_file_size_from_metadata", "false")
                .build();
    }

    private QueryStats getQueryStats(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats();
    }

    private OperatorStats getScanOperatorStats(QueryId queryId)
    {
        return getQueryStats(queryId)
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().startsWith("TableScan") || summary.getOperatorType().startsWith("Scan"))
                .collect(onlyElement());
    }
}
