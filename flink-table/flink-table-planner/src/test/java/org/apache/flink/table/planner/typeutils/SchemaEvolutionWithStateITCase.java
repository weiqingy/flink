/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.typeutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINT_STORAGE;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for schema evolution with nested ROW types in state serializers.
 *
 * <p>This test verifies schema evolution capabilities when adding nullable fields to nested ROW types:
 * <ul>
 *   <li>Run job with original schema</li>
 *   <li>Create savepoint</li>
 *   <li>Evolve schema (add nullable fields to nested ROWs)</li>
 *   <li>Restore from savepoint with evolved schema</li>
 *   <li>Verify successful execution and checkpointing</li>
 * </ul>
 */
public class SchemaEvolutionWithStateITCase extends TestLogger {

    // Cluster configuration
    private static final int NUM_TMS = 1;
    private static final int NUM_SLOTS_PER_TM = 1;

    // Checkpointing configuration
    private static final int CHECKPOINT_INTERVAL_MS = 1000;
    private static final int CHECKPOINT_TIMEOUT_MS = 30000;
    private static final int WAIT_STATE_TIME_MS = 5000;

    // SQL definitions for test tables
    private static final String ORIGINAL_SQL = createOriginalSql();
    private static final String EVOLVED_SQL = createEvolvedSql();
    private static final String JOIN_QUERY =
            "INSERT INTO order_shipments\n" +
                    "SELECT o.order_id, s.shipment_id\n" +
                    "FROM orders o JOIN shipments s\n" +
                    "ON o.order_id = s.order_id";

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    // Test directories and resources
    private File checkpointDir;
    private File savepointDir;
    private MiniClusterWithClientResource miniClusterResource;

    @Before
    public void before() throws Exception {
        // Create test directories
        checkpointDir = TEMPORARY_FOLDER.newFolder("checkpoints");
        savepointDir = TEMPORARY_FOLDER.newFolder("savepoints");

        // Create and configure the cluster
        Configuration config = createClusterConfiguration();
        miniClusterResource = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(config)
                        .setNumberTaskManagers(NUM_TMS)
                        .setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
                        .build());
        miniClusterResource.before();
    }

    /**
     * Creates the base configuration for the test cluster.
     */
    private Configuration createClusterConfiguration() {
        Configuration config = new Configuration();

        // Configure checkpoint and savepoint storage
        config.setString(CHECKPOINTS_DIRECTORY.key(), checkpointDir.toURI().toString());
        config.setString(SAVEPOINT_DIRECTORY.key(), savepointDir.toURI().toString());
        config.setString(CHECKPOINT_STORAGE.key(), "filesystem");

        return config;
    }

    @After
    public void after() {
        if (miniClusterResource != null) {
            miniClusterResource.after();
        }
    }

    @Test
    public void testSchemaEvolutionWithNestedRowTypes() throws Exception {
        ClusterClient<?> client = miniClusterResource.getClusterClient();
        Configuration config = new Configuration(miniClusterResource.getMiniCluster().getConfiguration());

        JobID jobId = null;
        JobID restoreJobId = null;

        try {
            // Step 1: Run original job
            StreamTableEnvironment tableEnv = setupEnvironment(config);
            executeSqlStatements(tableEnv, ORIGINAL_SQL);

            TableResult tableResult = tableEnv.executeSql(JOIN_QUERY);
            jobId = tableResult.getJobClient().get().getJobID();
            System.out.println("Job running with ID: " + jobId);

            waitForJobRunning(client, jobId);
            Thread.sleep(WAIT_STATE_TIME_MS);

            // Create savepoint
            String savepointPath = takeSavepoint(client, jobId);
            System.out.println("Savepoint created and job stopped: " + savepointPath);

            // Step 2: Restore with evolved schema
            StreamTableEnvironment restoreTableEnv = setupEnvironment(config);
            executeSqlStatements(restoreTableEnv, EVOLVED_SQL);

            // Configure savepoint restoration
            Configuration restoreConfig = restoreTableEnv.getConfig().getConfiguration();
            restoreConfig.setString("execution.savepoint.path", savepointPath);
            TableResult restoreResult = restoreTableEnv.executeSql(JOIN_QUERY);
            restoreJobId = restoreResult.getJobClient().get().getJobID();

            waitForJobRunning(client, restoreJobId);
            Thread.sleep(WAIT_STATE_TIME_MS);

            // Verify checkpoints are created after restore
            String checkpointPath = verifyCheckpointsCreated(restoreJobId);
            System.out.println("Schema evolution successful. Checkpoint created: " + checkpointPath);

            client.cancel(restoreJobId).get(10, TimeUnit.SECONDS);
        } finally {
            // Clean up resources
            cancelJobIfRunning(client, jobId);
            cancelJobIfRunning(client, restoreJobId);
        }
    }

    /**
     * Sets up the execution environment with state backend and checkpointing configured.
     */
    private StreamTableEnvironment setupEnvironment(Configuration config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        try {
            // Use RocksDB state backend
            EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
            File rocksdbLocalDir = TEMPORARY_FOLDER.newFolder("rocksdb-data-" + System.nanoTime());
            rocksDBStateBackend.setDbStoragePath(rocksdbLocalDir.getAbsolutePath());
            env.setStateBackend(rocksDBStateBackend);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up RocksDB state backend. " +
                    "Please check Java version and RocksDB native library.", e);
        }

        // Configure checkpointing
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
        env.getConfig().setGlobalJobParameters(config);
        env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_TIMEOUT_MS);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        // Create table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().set("table.exec.resource.default-parallelism", "1");
        return tableEnv;
    }

    /**
     * Executes SQL statements separated by semicolon.
     */
    private void executeSqlStatements(StreamTableEnvironment tableEnv, String sql) {
        for (String stmt : sql.split(";")) {
            stmt = stmt.trim();
            if (!stmt.isEmpty()) {
                tableEnv.executeSql(stmt + ";");
            }
        }
    }

    /**
     * Takes a savepoint and returns the path.
     */
    private String takeSavepoint(ClusterClient<?> client, JobID jobId) throws Exception {
        String savepointPath = client.stopWithSavepoint(
                jobId,
                false,
                savepointDir.toURI().toString(),
                SavepointFormatType.CANONICAL).get(60, TimeUnit.SECONDS);

        // Handle savepoint path format
        if (savepointPath.startsWith("file:")) {
            savepointPath = savepointPath.substring(5);
        }

        // Find actual savepoint directory
        File[] candidates = savepointDir.listFiles((dir, name) -> name.contains("savepoint"));
        boolean anySavepointExists = candidates != null && candidates.length > 0;

        assertTrue("No savepoint was created", anySavepointExists);
        return anySavepointExists ? candidates[0].getAbsolutePath() : savepointPath;
    }

    /**
     * Waits for a job to reach RUNNING or FINISHED state.
     */
    private void waitForJobRunning(ClusterClient<?> client, JobID jobId) throws Exception {
        long deadline = System.currentTimeMillis() + 60000;

        while (System.currentTimeMillis() < deadline) {
            JobStatus status = client.getJobStatus(jobId).get(5, TimeUnit.SECONDS);

            if (status == JobStatus.RUNNING || status == JobStatus.FINISHED) {
                return;
            } else if (status.isTerminalState()) {
                printJobFailureDetails(miniClusterResource.getMiniCluster(), jobId);
                throw new RuntimeException("Job entered terminal state: " + status);
            }

            Thread.sleep(1000);
        }

        throw new RuntimeException("Job did not enter RUNNING state within timeout of 60 seconds");
    }

    /**
     * Verifies that checkpoints were created for the given job and returns the first checkpoint path.
     */
    private String verifyCheckpointsCreated(JobID jobId) throws Exception {
        Path checkpointsDir = Paths.get(checkpointDir.getAbsolutePath(), jobId.toString());
        long deadline = System.currentTimeMillis() + 30000;
        boolean checkpointsFound = false;
        String firstCheckpointPath = null;

        while (System.currentTimeMillis() < deadline && !checkpointsFound) {
            if (Files.exists(checkpointsDir)) {
                try (java.util.stream.Stream<Path> paths = Files.list(checkpointsDir)) {
                    Optional<Path> checkpointPath = paths
                            .filter(p -> p.toString().contains("chk"))
                            .findFirst();

                    if (checkpointPath.isPresent()) {
                        checkpointsFound = true;
                        firstCheckpointPath = checkpointPath.get().toString();
                    }
                }
            }
            if (!checkpointsFound) {
                Thread.sleep(1000);
            }
        }

        assertTrue("No checkpoints created after restoring from savepoint with schema evolution",
                checkpointsFound);
        return firstCheckpointPath;
    }

    /**
     * Cancels a job if it is running.
     */
    private void cancelJobIfRunning(ClusterClient<?> client, JobID jobId) {
        try {
            if (jobId == null) {
                return;
            }

            JobStatus status = client.getJobStatus(jobId).get(5, TimeUnit.SECONDS);
            if (status != JobStatus.FINISHED && status != JobStatus.FAILED && status != JobStatus.CANCELED) {
                client.cancel(jobId).get(10, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    /**
     * Prints information about a failed job.
     */
    private void printJobFailureDetails(MiniCluster miniCluster, JobID jobId) throws Exception {
        try {
            // Check execution vertices for errors
            miniCluster.getExecutionGraph(jobId)
                    .get(5, TimeUnit.SECONDS)
                    .getAllExecutionVertices()
                    .forEach(vertex -> {
                        if (vertex.getCurrentExecutionAttempt() != null) {
                            vertex.getCurrentExecutionAttempt().getFailureInfo().ifPresent(info ->
                                    System.err.println("Failure in vertex " + vertex.getTaskNameWithSubtaskIndex() +
                                            ": " + info.getException().getFullStringifiedStackTrace()));
                        }
                    });

            // Check job result for exception
            miniCluster.requestJobResult(jobId)
                    .get(5, TimeUnit.SECONDS)
                    .getSerializedThrowable()
                    .ifPresent(throwable ->
                            System.err.println("Job result exception: " + throwable));
        } catch (Exception e) {
            System.err.println("Error retrieving job failure details: " + e.getMessage());
        }
    }

    /**
     * Creates the original schema SQL definition.
     */
    private static String createOriginalSql() {
        return "CREATE TABLE orders (\n" +
                "    order_id INT,\n" +
                "    customer_detail ROW<id INT, name STRING>\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '2',\n" +
                "    'fields.order_id.kind' = 'sequence',\n" +
                "    'fields.order_id.start' = '1',\n" +
                "    'fields.order_id.end' = '1000',\n" +
                "    'fields.customer_detail.kind' = 'random'\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE shipments (\n" +
                "    shipment_id INT,\n" +
                "    order_id INT,\n" +
                "    shipper_detail ROW<id INT, name STRING>\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '2',\n" +
                "    'fields.shipment_id.kind' = 'sequence',\n" +
                "    'fields.shipment_id.start' = '1',\n" +
                "    'fields.shipment_id.end' = '1000',\n" +
                "    'fields.order_id.kind' = 'sequence',\n" +
                "    'fields.order_id.start' = '1',\n" +
                "    'fields.order_id.end' = '1000',\n" +
                "    'fields.shipper_detail.kind' = 'random'\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE order_shipments (\n" +
                "    order_id INT,\n" +
                "    shipment_id INT\n" +
                ") WITH (\n" +
                "    'connector' = 'blackhole'\n" +
                ");";
    }

    /**
     * Creates the evolved schema SQL definition with additional nullable fields.
     */
    private static String createEvolvedSql() {
        return "CREATE TABLE orders (\n" +
                "    order_id INT,\n" +
                "    customer_detail ROW<id INT, email STRING NULL, name STRING, address STRING NULL>\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '2',\n" +
                "    'fields.order_id.kind' = 'sequence',\n" +
                "    'fields.order_id.start' = '1',\n" +
                "    'fields.order_id.end' = '1000',\n" +
                "    'fields.customer_detail.kind' = 'random'\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE shipments (\n" +
                "    shipment_id INT,\n" +
                "    order_id INT,\n" +
                "    shipper_detail ROW<id INT, company STRING NULL, name STRING, tracking_id STRING NULL>\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '2',\n" +
                "    'fields.shipment_id.kind' = 'sequence',\n" +
                "    'fields.shipment_id.start' = '1',\n" +
                "    'fields.shipment_id.end' = '1000',\n" +
                "    'fields.order_id.kind' = 'sequence',\n" +
                "    'fields.order_id.start' = '1',\n" +
                "    'fields.order_id.end' = '1000',\n" +
                "    'fields.shipper_detail.kind' = 'random'\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE order_shipments (\n" +
                "    order_id INT,\n" +
                "    shipment_id INT\n" +
                ") WITH (\n" +
                "    'connector' = 'blackhole'\n" +
                ");";
    }
}
