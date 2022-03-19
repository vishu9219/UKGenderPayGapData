package com.vsrivastava.loadtobigquery.data;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.vsrivastava.loadtobigquery.config.ApplicationProperties;

import java.io.InputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

public class LoadPartitionedTable {
    private static final Logger LOG = Logger.getLogger(LoadPartitionedTable.class.getName());
    private final Properties properties;

    public LoadPartitionedTable(Properties properties) {
        this.properties = properties;
    }

    public void runLoadPartitionedTable() throws Exception {
        String datasetName = properties.getProperty(ApplicationProperties.DATASET_NAME);
        String tableName = properties.getProperty(ApplicationProperties.TABLE_NAME);
        String sourceUri = properties.getProperty(ApplicationProperties.SOURCE_URI);
        String projectName = properties.getProperty(ApplicationProperties.PROJECT_NAME);
        loadPartitionedTable(datasetName, tableName, sourceUri, projectName);
    }

    public void loadPartitionedTable(String datasetName, String tableName, String sourceUri, String projectName)
            throws Exception {
        try {
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            try (InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("key.json")) {
                assert resourceAsStream != null;
                BigQuery bigquery = BigQueryOptions.newBuilder()
                        .setProjectId(projectName)
                        .setCredentials(ServiceAccountCredentials.fromStream(resourceAsStream))
                        .build().getService();

                TableId tableId = TableId.of(projectName, datasetName, tableName);

                //Improvement load sql type and column name from config
                Schema schema =
                        Schema.of(
                                Field.of("EmployerName", StandardSQLTypeName.STRING),
                                Field.of("EmployerId", StandardSQLTypeName.INT64),
                                Field.of("Address", StandardSQLTypeName.STRING),
                                Field.of("PostCode", StandardSQLTypeName.STRING),
                                Field.of("CompanyNumber", StandardSQLTypeName.STRING),
                                Field.of("SicCodes", StandardSQLTypeName.STRING),
                                Field.of("DiffMeanHourlyPercent", StandardSQLTypeName.NUMERIC),
                                Field.of("DiffMedianHourlyPercent", StandardSQLTypeName.NUMERIC),
                                Field.of("DiffMeanBonusPercent", StandardSQLTypeName.NUMERIC),
                                Field.of("DiffMedianBonusPercent", StandardSQLTypeName.NUMERIC),
                                Field.of("MaleBonusPercent", StandardSQLTypeName.NUMERIC),
                                Field.of("FemaleBonusPercent", StandardSQLTypeName.NUMERIC),
                                Field.of("MaleLowerQuartile", StandardSQLTypeName.NUMERIC),
                                Field.of("FemaleLowerQuartile", StandardSQLTypeName.NUMERIC),
                                Field.of("MaleLowerMiddleQuartile", StandardSQLTypeName.NUMERIC),
                                Field.of("FemaleLowerMiddleQuartile", StandardSQLTypeName.NUMERIC),
                                Field.of("MaleUpperMiddleQuartile", StandardSQLTypeName.NUMERIC),
                                Field.of("FemaleUpperMiddleQuartile", StandardSQLTypeName.NUMERIC),
                                Field.of("MaleTopQuartile", StandardSQLTypeName.NUMERIC),
                                Field.of("FemaleTopQuartile", StandardSQLTypeName.NUMERIC),
                                Field.of("CompanyLinkToGPGInfo", StandardSQLTypeName.STRING),
                                Field.of("ResponsiblePerson", StandardSQLTypeName.STRING),
                                Field.of("EmployerSize", StandardSQLTypeName.STRING),
                                Field.of("CurrentName", StandardSQLTypeName.STRING),
                                Field.of("SubmittedAfterTheDeadline", StandardSQLTypeName.BOOL),
                                Field.of("DueDate", StandardSQLTypeName.TIMESTAMP),
                                Field.of("DateSubmitted", StandardSQLTypeName.TIMESTAMP)
                        );

                // Configure time partitioning. For full list of options, see:
                // https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TimePartitioning
                TimePartitioning partitioning =
                        TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                                .setField("DueDate")
                                .setExpirationMs(Duration.of(90, ChronoUnit.DAYS).toMillis())
                                .build();

                LoadJobConfiguration loadJobConfig =
                        LoadJobConfiguration.builder(tableId, sourceUri)
                                .setFormatOptions(CsvOptions.newBuilder()
                                        .setSkipLeadingRows(1)
                                        .setFieldDelimiter(",")
                                        .setAllowQuotedNewLines(true)
                                        .setQuote("\"")
                                        .build())
                                .setSchema(schema)
                                .setTimePartitioning(partitioning)
                                .build();

                // Create a job ID so that we can safely retry.
                JobId jobId = JobId.of(UUID.randomUUID().toString());
                Job loadJob = bigquery.create(JobInfo.newBuilder(loadJobConfig).setJobId(jobId).build());

                // Load data from a GCS parquet file into the table
                // Blocks until this load table job completes its execution, either failing or succeeding.
                Job completedJob = loadJob.waitFor();

                // Check for errors
                if (completedJob == null) {
                    throw new Exception("Job not executed since it no longer exists.");
                } else if (completedJob.getStatus().getError() != null) {
                    // You can also look at queryJob.getStatus().getExecutionErrors() for all
                    // errors, not just the latest one.
                    throw new Exception(
                            "BigQuery was unable to load into the table due to an error: \n"
                                    + loadJob.getStatus().getError());
                }
                LOG.info("Data successfully loaded into time partitioned table during load job");
            }
        } catch (BigQueryException | InterruptedException e) {
            LOG.info(
                    "Data not loaded into time partitioned table during load job \n" + e.toString());
        }
    }
}
