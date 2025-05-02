/**
 * StarRocks Data Migration Integrity Verification Script (Node.js)
 * --------------------------------------------------------
 * This script connects to both source (AWS) and target (Hetzner) StarRocks clusters,
 * and performs multiple integrity checks to ensure data was migrated correctly.
 *
 * Validation checks include:
 * - Database existence verification
 * - Table existence verification
 * - Row count comparison
 * - Schema validation
 * - Statistical validation (MIN, MAX, AVG, SUM)
 * - Data sampling comparison
 * - Checksum verification
 *
 * Requirements:
 * - mysql2 (npm install mysql2)
 * - fs (built-in)
 * - console-table-printer (npm install console-table-printer)
 */

const mysql = require("mysql2/promise");
const fs = require("fs");
const { Table } = require("console-table-printer");
const yargs = require("yargs");

// Configuration Constants
const CONFIG = {
  // Thresholds
  LARGE_TABLE_CHECKSUM_THRESHOLD: 1000000, // 1M rows
  SKIP_CHECKSUM_THRESHOLD: 10000000, // 10M rows
  MAX_COLUMNS_FOR_STATS: 50,
  MAX_NUMERIC_COLUMNS_TO_CHECK: 5,
  FLOAT_COMPARISON_TOLERANCE: 0.0001, // 0.01%
  SAMPLE_SIZE: 100,

  // Connection settings
  CONNECT_TIMEOUT: 30000,
  QUERY_TIMEOUT: 60000,

  // Report settings
  MAX_TABLE_CELL_WIDTH: 50,
  SHOW_FULL_VALUES_IN_REPORT: false,
};

// Parse command line arguments
const argv = yargs
  .option("config", {
    alias: "c",
    description: "Path to configuration file",
    type: "string",
  })
  .option("databases", {
    alias: "d",
    description: "Comma-separated list of databases to verify",
    type: "string",
  })
  .option("skip-tables", {
    alias: "s",
    description: "Comma-separated list of tables to skip",
    type: "string",
  })
  .option("sample-size", {
    description: "Number of samples for data verification",
    type: "number",
    default: CONFIG.SAMPLE_SIZE,
  })
  .help()
  .alias("help", "h").argv;

// Load configuration from file if provided
let sourceConfig, targetConfig, databasesToVerify, tablesToSkip;
if (argv.config) {
  const configFile = require(argv.config);
  sourceConfig = configFile.source;
  targetConfig = configFile.target;
  databasesToVerify = configFile.databases || [];
  tablesToSkip = configFile.skipTables || [];
} else {
  // Default configuration
  sourceConfig = {
    host: "source-starrocks-aws.example.com",
    port: 9030,
    user: "your_username",
    password: "your_password",
    database: null,
    connectTimeout: CONFIG.CONNECT_TIMEOUT,
  };

  targetConfig = {
    host: "target-starrocks-hetzner.example.com",
    port: 9030,
    user: "your_username",
    password: "your_password",
    database: null,
    connectTimeout: CONFIG.CONNECT_TIMEOUT,
  };

  databasesToVerify = argv.databases
    ? argv.databases.split(",")
    : ["db1", "db2", "db3"];
  tablesToSkip = argv.skipTables ? argv.skipTables.split(",") : [];
}

// Report file path
const REPORT_FILE =
  "results/" +
  `${new Date().toISOString().replace(/:/g, "-").replace(/\..+/, "")}.md`;

class StarRocksVerifier {
  constructor(sourceConfig, targetConfig, databasesToVerify, tablesToSkip) {
    this.sourceConfig = { ...sourceConfig };
    this.targetConfig = { ...targetConfig };
    this.databasesToVerify = databasesToVerify;
    this.tablesToSkip = tablesToSkip;
    this.verificationResults = [];
    this.errors = [];
    this.startTime = null;
    this.endTime = null;
    this.sourceConn = null;
    this.targetConn = null;
  }

  /**
   * Initialize connections to source and target
   */
  async initializeConnections() {
    try {
      this.sourceConn = await mysql.createConnection(this.sourceConfig);
      this.targetConn = await mysql.createConnection(this.targetConfig);
      console.log("Successfully connected to both source and target clusters");
    } catch (error) {
      throw new Error(`Connection initialization failed: ${error.message}`);
    }
  }

  /**
   * Close connections
   */
  async closeConnections() {
    if (this.sourceConn) {
      await this.sourceConn.end();
    }
    if (this.targetConn) {
      await this.targetConn.end();
    }
    console.log("Connections closed");
  }

  /**
   * Execute a query with timeout
   */
  async executeQuery(connection, query, params = []) {
    try {
      const [rows] = await connection.execute(query, params);
      return rows;
    } catch (error) {
      throw new Error(`Query error (${query}): ${error.message}`);
    }
  }

  /**
   * Get all databases from the connection
   */
  async getDatabases(connection) {
    const [rows] = await connection.query("SHOW DATABASES");
    return rows
      .map((row) => Object.values(row)[0])
      .filter((name) => !["information_schema", "mysql"].includes(name));
  }

  /**
   * Get all tables in the specified database
   */
  async getTables(connection, database) {
    await connection.query(`USE \`${database}\``);
    const [rows] = await connection.query("SHOW TABLES");
    return rows
      .map((row) => Object.values(row)[0])
      .filter((name) => !this.tablesToSkip.includes(name));
  }

  /**
   * Get table schema
   */
  async getTableSchema(connection, database, table) {
    await connection.query(`USE \`${database}\``);
    const [rows] = await connection.query(`DESCRIBE \`${table}\``);
    return rows;
  }

  /**
   * Get row count for a table
   */
  async getRowCount(connection, database, table) {
    try {
      await connection.query(`USE \`${database}\``);
      const [rows] = await connection.query(
        `SELECT COUNT(*) as count FROM \`${table}\``
      );
      return rows[0].count;
    } catch (error) {
      this.errors.push(
        `Error getting row count for ${database}.${table}: ${error.message}`
      );
      return 0;
    }
  }

  /**
   * Get column names for a table
   */
  async getTableColumns(connection, database, table) {
    const schema = await this.getTableSchema(connection, database, table);
    return schema.map((row) => row.Field);
  }

  /**
   * Get columns that can be used for ordering (exclude JSON and complex types)
   */
  async getOrderableColumns(connection, database, table) {
    const schema = await this.getTableSchema(connection, database, table);
    return schema
      .filter((row) => {
        const type = row.Type.toLowerCase();
        return (
          !type.includes("json") &&
          !type.includes("nested") &&
          !type.includes("percentile") &&
          !type.includes("hll") &&
          !type.includes("bitmap") &&
          !type.includes("struct") &&
          !type.includes("map")
        );
      })
      .map((row) => row.Field);
  }

  /**
   * Get a sample of data from the table with memory optimization
   */
  async getDataSample(connection, database, table, sampleSize = 100) {
    try {
      await connection.query(`USE \`${database}\``);

      // Get total rows to calculate appropriate sampling ratio
      const rowCount = await this.getRowCount(connection, database, table);

      if (rowCount === 0) {
        return [];
      }

      // Get orderable columns
      const orderableColumns = await this.getOrderableColumns(
        connection,
        database,
        table
      );

      if (orderableColumns.length === 0) {
        this.errors.push(
          `No orderable columns found for ${database}.${table}, skipping data sample`
        );
        return [];
      }

      // For large tables, use a more efficient sampling method
      if (rowCount > CONFIG.LARGE_TABLE_CHECKSUM_THRESHOLD) {
        // Use a smaller sample size for large tables
        const actualSampleSize = Math.min(10, sampleSize);
        const step = Math.floor(rowCount / actualSampleSize);

        // Create ORDER BY clause using only orderable columns
        const orderByClause = orderableColumns
          .map((col) => `\`${col}\``)
          .join(", ");

        // Use a more memory-efficient query with LIMIT and OFFSET
        const query = `
          SELECT * FROM \`${table}\`
          ORDER BY ${orderByClause}
          LIMIT ${actualSampleSize}
          OFFSET ${step}
        `;

        const [rows] = await connection.query(query);
        return rows;
      }

      // For smaller tables, use the original sampling method
      const orderByClause = orderableColumns
        .map((col) => `\`${col}\``)
        .join(", ");
      const query = `
        SELECT * FROM \`${table}\`
        ORDER BY ${orderByClause}
        LIMIT ${sampleSize}
      `;

      const [rows] = await connection.query(query);
      return rows;
    } catch (error) {
      this.errors.push(
        `Error getting data sample for ${database}.${table}: ${error.message}`
      );
      return [];
    }
  }

  /**
   * Calculate checksum for a table using a more robust method
   */
  async computeChecksum(connection, database, table) {
    try {
      await connection.query(`USE \`${database}\``);
      const columns = await this.getTableColumns(connection, database, table);
      const rowCount = await this.getRowCount(connection, database, table);

      // For large tables, use a more efficient sampling method
      if (rowCount > CONFIG.LARGE_TABLE_CHECKSUM_THRESHOLD) {
        console.log(
          `Using sampling for large table ${database}.${table} (${rowCount} rows)`
        );
        return await this.computeSampledChecksum(
          connection,
          database,
          table,
          columns
        );
      }

      // For smaller tables, compute full checksum
      return await this.computeFullChecksum(
        connection,
        database,
        table,
        columns
      );
    } catch (error) {
      this.errors.push(
        `Error computing checksum for ${database}.${table}: ${error.message}`
      );
      return "ERROR";
    }
  }

  /**
   * Compute checksum for a large table using sampling
   */
  async computeSampledChecksum(connection, database, table, columns) {
    // Get orderable columns
    const orderableColumns = await this.getOrderableColumns(
      connection,
      database,
      table
    );

    if (orderableColumns.length === 0) {
      this.errors.push(
        `No orderable columns found for ${database}.${table}, skipping checksum`
      );
      return "SKIPPED";
    }

    // Get row count first
    const rowCount = await this.getRowCount(connection, database, table);
    if (rowCount === 0) {
      return "EMPTY";
    }

    // Use a deterministic sampling method
    const sampleRatio = Math.min(10, (1000000 / rowCount) * 100); // Sample at most 10% or enough to get 1M rows

    // Create ORDER BY clause using only orderable columns
    const orderByClause = orderableColumns
      .map((col) => `\`${col}\``)
      .join(", ");

    const query = `
      SELECT MD5(GROUP_CONCAT(row_hash ORDER BY row_num)) as checksum
      FROM (
        SELECT 
          ROW_NUMBER() OVER (ORDER BY ${orderByClause}) as row_num,
          MD5(CONCAT_WS('|', ${columns
            .map((col) => `IFNULL(CAST(\`${col}\` AS STRING), 'NULL')`)
            .join(", ")})) as row_hash
        FROM \`${table}\`
        ORDER BY ${orderByClause}
        LIMIT ${Math.floor(rowCount * (sampleRatio / 100))}
      ) t
    `;

    const [rows] = await connection.query(query);
    return rows[0].checksum || "N/A";
  }

  /**
   * Compute full checksum for a table
   */
  async computeFullChecksum(connection, database, table, columns) {
    // Get orderable columns
    const orderableColumns = await this.getOrderableColumns(
      connection,
      database,
      table
    );

    if (orderableColumns.length === 0) {
      this.errors.push(
        `No orderable columns found for ${database}.${table}, skipping checksum`
      );
      return "SKIPPED";
    }

    // Create ORDER BY clause using only orderable columns
    const orderByClause = orderableColumns
      .map((col) => `\`${col}\``)
      .join(", ");

    const query = `
      SELECT MD5(GROUP_CONCAT(row_hash ORDER BY row_num)) as checksum
      FROM (
        SELECT 
          ROW_NUMBER() OVER (ORDER BY ${orderByClause}) as row_num,
          MD5(CONCAT_WS('|', ${columns
            .map((col) => `IFNULL(CAST(\`${col}\` AS STRING), 'NULL')`)
            .join(", ")})) as row_hash
        FROM \`${table}\`
        ORDER BY ${orderByClause}
      ) t
    `;

    const [rows] = await connection.query(query);
    return rows[0].checksum || "N/A";
  }

  /**
   * Compare values based on their data type
   */
  compareValues(sourceVal, targetVal, columnType) {
    if (sourceVal === null && targetVal === null) return true;
    if (sourceVal === null || targetVal === null) return false;

    // Handle numeric types
    if (/int|float|double|decimal|numeric/i.test(columnType)) {
      const sourceNum = parseFloat(sourceVal);
      const targetNum = parseFloat(targetVal);

      if (isNaN(sourceNum) || isNaN(targetNum)) {
        return sourceVal === targetVal;
      }

      if (sourceNum === 0) {
        return Math.abs(targetNum) < CONFIG.FLOAT_COMPARISON_TOLERANCE;
      }

      const relDiff = Math.abs((sourceNum - targetNum) / sourceNum);
      return relDiff <= CONFIG.FLOAT_COMPARISON_TOLERANCE;
    }

    // Handle date/time types
    if (/date|time|timestamp/i.test(columnType)) {
      const sourceDate = new Date(sourceVal);
      const targetDate = new Date(targetVal);
      return (
        !isNaN(sourceDate.getTime()) &&
        !isNaN(targetDate.getTime()) &&
        sourceDate.getTime() === targetDate.getTime()
      );
    }

    // Default to string comparison
    return sourceVal === targetVal;
  }

  /**
   * Compare data samples with type-aware comparison
   */
  compareDataSamples(sourceSample, targetSample, schema) {
    if (!sourceSample.length || !targetSample.length) {
      return false;
    }

    // Ensure both samples have the same columns
    const sourceColumns = Object.keys(sourceSample[0]);
    const targetColumns = Object.keys(targetSample[0]);
    const commonColumns = sourceColumns
      .filter((col) => targetColumns.includes(col))
      .sort();

    if (!commonColumns.length) {
      return false;
    }

    // Simple count check
    if (sourceSample.length !== targetSample.length) {
      return false;
    }

    // Sort both data sets by common columns
    const sortedSource = [...sourceSample].sort((a, b) => {
      for (const col of commonColumns) {
        if (a[col] < b[col]) return -1;
        if (a[col] > b[col]) return 1;
      }
      return 0;
    });

    const sortedTarget = [...targetSample].sort((a, b) => {
      for (const col of commonColumns) {
        if (a[col] < b[col]) return -1;
        if (a[col] > b[col]) return 1;
      }
      return 0;
    });

    // Compare each row with type awareness
    for (let i = 0; i < sortedSource.length; i++) {
      const sourceRow = sortedSource[i];
      const targetRow = sortedTarget[i];

      for (const col of commonColumns) {
        const columnType =
          schema.find((field) => field.Field === col)?.Type || "string";
        if (!this.compareValues(sourceRow[col], targetRow[col], columnType)) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Get statistics for a numeric column
   */
  async getColumnStats(connection, database, table, column) {
    try {
      await connection.query(`USE \`${database}\``);
      const query = `
        SELECT 
          MIN(\`${column}\`) as min_val, 
          MAX(\`${column}\`) as max_val, 
          AVG(\`${column}\`) as avg_val, 
          SUM(\`${column}\`) as sum_val
        FROM \`${table}\`
      `;

      const [rows] = await connection.query(query);
      return [
        rows[0].min_val,
        rows[0].max_val,
        rows[0].avg_val,
        rows[0].sum_val,
      ];
    } catch (error) {
      this.errors.push(
        `Error getting stats for ${database}.${table}.${column}: ${error.message}`
      );
      return [null, null, null, null];
    }
  }

  /**
   * Verify that all databases exist in both source and target
   */
  async verifyDatabaseExistence() {
    console.log("Verifying database existence...");

    const sourceConn = await this.sourceConn;
    const targetConn = await this.targetConn;

    try {
      const sourceDbs = await this.getDatabases(sourceConn);
      const targetDbs = await this.getDatabases(targetConn);

      for (const dbMapping of this.databasesToVerify) {
        const sourceDb = dbMapping.source;
        const targetDb = dbMapping.target;

        if (!sourceDbs.includes(sourceDb)) {
          this.verificationResults.push({
            check_type: "Database Existence",
            database: `${sourceDb} -> ${targetDb}`,
            table: "N/A",
            source: "Missing",
            target: "N/A",
            result: "FAILED",
          });
        } else if (!targetDbs.includes(targetDb)) {
          this.verificationResults.push({
            check_type: "Database Existence",
            database: `${sourceDb} -> ${targetDb}`,
            table: "N/A",
            source: "Present",
            target: "Missing",
            result: "FAILED",
          });
        } else {
          this.verificationResults.push({
            check_type: "Database Existence",
            database: `${sourceDb} -> ${targetDb}`,
            table: "N/A",
            source: "Present",
            target: "Present",
            result: "PASSED",
          });
        }
      }
    } catch (error) {
      throw error;
    }
  }

  /**
   * Verify that all tables exist in both source and target
   */
  async verifyTableExistence() {
    console.log("Verifying table existence...");

    for (const dbMapping of this.databasesToVerify) {
      const sourceDb = dbMapping.source;
      const targetDb = dbMapping.target;

      const sourceConn = await this.sourceConn;
      const targetConn = await this.targetConn;

      try {
        const sourceTables = await this.getTables(sourceConn, sourceDb);
        const targetTables = await this.getTables(targetConn, targetDb);

        // Check if all source tables exist in target
        for (const table of sourceTables) {
          if (!targetTables.includes(table)) {
            this.verificationResults.push({
              check_type: "Table Existence",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: "Present",
              target: "Missing",
              result: "FAILED",
            });
          } else {
            this.verificationResults.push({
              check_type: "Table Existence",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: "Present",
              target: "Present",
              result: "PASSED",
            });
          }
        }
      } catch (error) {
        throw error;
      }
    }
  }

  /**
   * Verify row counts for all tables
   */
  async verifyRowCounts() {
    console.log("Verifying row counts...");

    for (const dbMapping of this.databasesToVerify) {
      const sourceDb = dbMapping.source;
      const targetDb = dbMapping.target;

      const sourceConn = await this.sourceConn;
      const targetConn = await this.targetConn;

      try {
        const sourceTables = await this.getTables(sourceConn, sourceDb);

        for (const table of sourceTables) {
          if (this.tablesToSkip.includes(table)) {
            continue;
          }

          const sourceCount = await this.getRowCount(
            sourceConn,
            sourceDb,
            table
          );
          const targetCount = await this.getRowCount(
            targetConn,
            targetDb,
            table
          );

          if (sourceCount === targetCount) {
            this.verificationResults.push({
              check_type: "Row Count",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: sourceCount,
              target: targetCount,
              result: "PASSED",
            });
          } else {
            this.verificationResults.push({
              check_type: "Row Count",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: sourceCount,
              target: targetCount,
              result: "FAILED",
            });
          }
        }
      } catch (error) {
        throw error;
      }
    }
  }

  /**
   * Verify table schemas match between source and target
   */
  async verifyTableSchemas() {
    console.log("Verifying table schemas...");

    for (const dbMapping of this.databasesToVerify) {
      const sourceDb = dbMapping.source;
      const targetDb = dbMapping.target;

      const sourceConn = await this.sourceConn;
      const targetConn = await this.targetConn;

      try {
        const sourceTables = await this.getTables(sourceConn, sourceDb);

        for (const table of sourceTables) {
          if (this.tablesToSkip.includes(table)) {
            continue;
          }

          const sourceSchema = await this.getTableSchema(
            sourceConn,
            sourceDb,
            table
          );
          const targetSchema = await this.getTableSchema(
            targetConn,
            targetDb,
            table
          );

          // Convert to comparable format
          const sourceCols = sourceSchema.map((row) => [row.Field, row.Type]);
          const targetCols = targetSchema.map((row) => [row.Field, row.Type]);

          // Compare column names and types
          const sourceColMap = new Map(sourceCols);
          const targetColMap = new Map(targetCols);

          let schemaMatch = true;
          const missingCols = [];
          const diffTypes = [];

          // Check all source columns exist in target with same type
          for (const [colName, colType] of sourceColMap.entries()) {
            if (!targetColMap.has(colName)) {
              schemaMatch = false;
              missingCols.push(colName);
            } else if (targetColMap.get(colName) !== colType) {
              schemaMatch = false;
              diffTypes.push(colName);
            }
          }

          if (schemaMatch) {
            this.verificationResults.push({
              check_type: "Schema Check",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: sourceCols.length,
              target: targetCols.length,
              result: "PASSED",
            });
          } else {
            const notes = [];
            if (missingCols.length) {
              notes.push(`Missing columns: ${missingCols.join(", ")}`);
            }
            if (diffTypes.length) {
              notes.push(`Different types: ${diffTypes.join(", ")}`);
            }

            this.verificationResults.push({
              check_type: "Schema Check",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: sourceCols.length,
              target: targetCols.length,
              result: "FAILED",
              notes: notes.join("; "),
            });
          }
        }
      } catch (error) {
        throw error;
      }
    }
  }

  /**
   * Verify table checksums match between source and target
   */
  async verifyChecksums() {
    console.log("Verifying checksums (this may take time for large tables)...");

    for (const dbMapping of this.databasesToVerify) {
      const sourceDb = dbMapping.source;
      const targetDb = dbMapping.target;

      const sourceConn = await this.sourceConn;
      const targetConn = await this.targetConn;

      try {
        const sourceTables = await this.getTables(sourceConn, sourceDb);

        for (const table of sourceTables) {
          if (this.tablesToSkip.includes(table)) {
            continue;
          }

          // Skip very large tables or use alternative methods
          const rowCount = await this.getRowCount(sourceConn, sourceDb, table);
          if (rowCount > 10000000) {
            // 10M+ rows
            this.verificationResults.push({
              check_type: "Table Checksum",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: "Skipped (large table)",
              target: "Skipped (large table)",
              result: "SKIPPED",
              notes: `Table too large (${rowCount} rows), skipping full checksum`,
            });
            continue;
          }

          const sourceChecksum = await this.computeChecksum(
            sourceConn,
            sourceDb,
            table
          );
          const targetChecksum = await this.computeChecksum(
            targetConn,
            targetDb,
            table
          );

          if (sourceChecksum === targetChecksum) {
            this.verificationResults.push({
              check_type: "Table Checksum",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: `${sourceChecksum.substring(0, 8)}...`,
              target: `${targetChecksum.substring(0, 8)}...`,
              result: "PASSED",
            });
          } else {
            this.verificationResults.push({
              check_type: "Table Checksum",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: `${sourceChecksum.substring(0, 8)}...`,
              target: `${targetChecksum.substring(0, 8)}...`,
              result: "FAILED",
            });
          }
        }
      } catch (error) {
        throw error;
      }
    }
  }

  /**
   * Verify column statistics for numeric columns
   */
  async verifyColumnStatistics() {
    console.log("Verifying column statistics...");

    for (const dbMapping of this.databasesToVerify) {
      const sourceDb = dbMapping.source;
      const targetDb = dbMapping.target;

      const sourceConn = await this.sourceConn;
      const targetConn = await this.targetConn;

      try {
        const sourceTables = await this.getTables(sourceConn, sourceDb);

        for (const table of sourceTables) {
          if (this.tablesToSkip.includes(table)) {
            continue;
          }

          // Get column information
          const columns = await this.getTableColumns(
            sourceConn,
            sourceDb,
            table
          );

          // Skip tables with too many columns to avoid excessive checks
          if (columns.length > 50) {
            this.verificationResults.push({
              check_type: "Column Statistics",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: "Skipped",
              target: "Skipped",
              result: "SKIPPED",
              notes: `Too many columns (${columns.length})`,
            });
            continue;
          }

          // Find numeric columns
          const schema = await this.getTableSchema(sourceConn, sourceDb, table);
          const numericColumns = schema
            .filter((row) => /int|float|double|decimal|numeric/i.test(row.Type))
            .map((row) => row.Field);

          // If there are too many numeric columns, sample some
          let sampleColumns = numericColumns;
          if (numericColumns.length > 5) {
            // Deterministic sampling
            sampleColumns = [];
            const step = Math.floor(numericColumns.length / 5);
            for (let i = 0; i < 5; i++) {
              sampleColumns.push(numericColumns[i * step]);
            }
          }

          for (const column of sampleColumns) {
            const sourceStats = await this.getColumnStats(
              sourceConn,
              sourceDb,
              table,
              column
            );
            const targetStats = await this.getColumnStats(
              targetConn,
              targetDb,
              table,
              column
            );

            // Compare statistics with tolerance for floating point differences
            let statsMatch = true;
            for (let i = 0; i < 4; i++) {
              if (sourceStats[i] === null && targetStats[i] === null) {
                continue;
              }
              if (sourceStats[i] === null || targetStats[i] === null) {
                statsMatch = false;
                break;
              }

              // Use relative tolerance for non-zero values
              if (sourceStats[i] !== 0) {
                const relDiff = Math.abs(
                  (sourceStats[i] - targetStats[i]) / sourceStats[i]
                );
                if (relDiff > 0.0001) {
                  // 0.01% tolerance
                  statsMatch = false;
                  break;
                }
              }
              // Use absolute tolerance for zero values
              else if (Math.abs(targetStats[i]) > 0.0001) {
                statsMatch = false;
                break;
              }
            }

            if (statsMatch) {
              this.verificationResults.push({
                check_type: "Column Statistics",
                database: `${sourceDb} -> ${targetDb}`,
                table: table,
                column: column,
                source: `MIN=${sourceStats[0]}, MAX=${sourceStats[1]}`,
                target: `MIN=${targetStats[0]}, MAX=${targetStats[1]}`,
                result: "PASSED",
              });
            } else {
              this.verificationResults.push({
                check_type: "Column Statistics",
                database: `${sourceDb} -> ${targetDb}`,
                table: table,
                column: column,
                source: `MIN=${sourceStats[0]}, MAX=${sourceStats[1]}`,
                target: `MIN=${targetStats[0]}, MAX=${targetStats[1]}`,
                result: "FAILED",
              });
            }
          }
        }
      } catch (error) {
        throw error;
      }
    }
  }

  /**
   * Verify random data samples from tables
   */
  async verifyDataSamples() {
    console.log("Verifying data samples...");

    for (const dbMapping of this.databasesToVerify) {
      const sourceDb = dbMapping.source;
      const targetDb = dbMapping.target;

      const sourceConn = await this.sourceConn;
      const targetConn = await this.targetConn;

      try {
        const sourceTables = await this.getTables(sourceConn, sourceDb);

        for (const table of sourceTables) {
          if (this.tablesToSkip.includes(table)) {
            continue;
          }

          // Skip tables with too many rows for efficient sampling
          const rowCount = await this.getRowCount(sourceConn, sourceDb, table);
          if (rowCount === 0) {
            this.verificationResults.push({
              check_type: "Data Sampling",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: "Empty table",
              target: "Empty table",
              result: "SKIPPED",
              notes: "Table is empty",
            });
            continue;
          }

          const sourceSample = await this.getDataSample(
            sourceConn,
            sourceDb,
            table,
            CONFIG.SAMPLE_SIZE
          );
          const targetSample = await this.getDataSample(
            targetConn,
            targetDb,
            table,
            CONFIG.SAMPLE_SIZE
          );

          if (
            this.compareDataSamples(
              sourceSample,
              targetSample,
              await this.getTableSchema(sourceConn, sourceDb, table)
            )
          ) {
            this.verificationResults.push({
              check_type: "Data Sampling",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: `${sourceSample.length} samples`,
              target: `${targetSample.length} samples`,
              result: "PASSED",
            });
          } else {
            this.verificationResults.push({
              check_type: "Data Sampling",
              database: `${sourceDb} -> ${targetDb}`,
              table: table,
              source: `${sourceSample.length} samples`,
              target: `${targetSample.length} samples`,
              result: "FAILED",
            });
          }
        }
      } finally {
        await this.closeConnections();
      }
    }
  }

  /**
   * Format a value for display in the report
   */
  formatValue(value) {
    if (value === null || value === undefined) return "N/A";
    if (
      typeof value === "string" &&
      value.length > CONFIG.MAX_TABLE_CELL_WIDTH
    ) {
      return value.substring(0, CONFIG.MAX_TABLE_CELL_WIDTH - 3) + "...";
    }
    return value;
  }

  /**
   * Generate a markdown report of verification results
   */
  generateReport() {
    // Count results by type
    const resultsByType = {};
    const resultsByDb = {};

    for (const result of this.verificationResults) {
      const checkType = result.check_type;
      const db = result.database;

      // Count by type
      if (!resultsByType[checkType]) {
        resultsByType[checkType] = {
          PASSED: 0,
          FAILED: 0,
          SKIPPED: 0,
          ERROR: 0,
        };
      }
      resultsByType[checkType][result.result]++;

      // Group by database
      if (!resultsByDb[db]) {
        resultsByDb[db] = [];
      }
      resultsByDb[db].push(result);
    }

    // Create summary report
    let report = "";

    // Header
    report += "# StarRocks Data Migration Verification Report\n\n";
    report += `Generated: ${new Date().toISOString()}\n\n`;
    report += "---\n\n";

    // Summary
    report += "## Summary\n\n";
    report +=
      "| Check Type | Passed | Failed | Skipped | Errors | Total | Pass Rate |\n";
    report +=
      "|------------|-------:|-------:|--------:|-------:|------:|----------:|\n";

    for (const [checkType, counts] of Object.entries(resultsByType)) {
      const total = counts.PASSED + counts.FAILED + counts.ERROR; // Exclude SKIPPED from total
      const passPercentage =
        total > 0 ? ((counts.PASSED / total) * 100).toFixed(2) : "100.00";

      report += `| ${checkType} | ${counts.PASSED} | ${counts.FAILED} | ${counts.SKIPPED} | ${counts.ERROR} | ${total} | ${passPercentage}% |\n`;
    }

    report += "\n";

    // First list all failures and errors
    const issues = this.verificationResults.filter(
      (r) => r.result === "FAILED" || r.result === "ERROR"
    );
    if (issues.length) {
      report += "## Issues\n\n";
      report +=
        "| Check Type | Database | Table | Column | Source | Target | Result | Notes |\n";
      report +=
        "|------------|----------|-------|--------|--------|--------|--------|-------|\n";

      for (const issue of issues) {
        const column = issue.column || "N/A";
        const notes = issue.notes || "";
        report += `| ${issue.check_type} | ${issue.database} | ${
          issue.table
        } | ${column} | ${this.formatValue(issue.source)} | ${this.formatValue(
          issue.target
        )} | ${issue.result} | ${this.formatValue(notes)} |\n`;
      }

      report += "\n";
    }

    // Detailed Results by Database
    report += "## Detailed Results\n\n";

    for (const [db, results] of Object.entries(resultsByDb)) {
      report += `### Database: ${db}\n\n`;

      // Group results by table
      const resultsByTable = {};
      for (const result of results) {
        if (!resultsByTable[result.table]) {
          resultsByTable[result.table] = [];
        }
        resultsByTable[result.table].push(result);
      }

      for (const [table, tableResults] of Object.entries(resultsByTable)) {
        report += `#### Table: ${table}\n\n`;
        report +=
          "| Check Type | Column | Source | Target | Result | Notes |\n";
        report +=
          "|------------|--------|--------|--------|--------|-------|\n";

        for (const result of tableResults) {
          const column = result.column || "N/A";
          const notes = result.notes || "";
          report += `| ${result.check_type} | ${column} | ${this.formatValue(
            result.source
          )} | ${this.formatValue(result.target)} | ${
            result.result
          } | ${this.formatValue(notes)} |\n`;
        }

        report += "\n";
      }
    }

    // Error Log
    if (this.errors.length > 0) {
      report += "## Error Log\n\n";
      for (const error of this.errors) {
        report += `- ${error}\n`;
      }
      report += "\n";
    }

    // Performance Summary
    if (this.startTime && this.endTime) {
      const duration = (this.endTime - this.startTime) / 1000;
      report += "## Performance\n\n";
      report += `- Start Time: ${this.startTime.toISOString()}\n`;
      report += `- End Time: ${this.endTime.toISOString()}\n`;
      report += `- Total Duration: ${duration.toFixed(2)} seconds\n\n`;
    }

    // Write report to file
    fs.writeFileSync(REPORT_FILE, report);
    console.log(`Report written to ${REPORT_FILE}`);

    return report;
  }

  /**
   * Log progress with timestamp
   */
  logProgress(message) {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] ${message}`);
  }

  /**
   * Run all verification checks
   */
  async runAllVerifications() {
    this.startTime = new Date();
    this.logProgress("Starting verification process");

    try {
      await this.initializeConnections();

      this.logProgress("Verifying database existence");
      await this.verifyDatabaseExistence();

      this.logProgress("Verifying table existence");
      await this.verifyTableExistence();

      this.logProgress("Verifying row counts");
      await this.verifyRowCounts();

      this.logProgress("Verifying table schemas");
      await this.verifyTableSchemas();

      this.logProgress("Verifying checksums");
      await this.verifyChecksums();

      this.logProgress("Verifying column statistics");
      await this.verifyColumnStatistics();

      this.logProgress("Verifying data samples");
      await this.verifyDataSamples();

      this.endTime = new Date();
      const duration = (this.endTime - this.startTime) / 1000;
      this.logProgress(
        `Verification completed in ${duration.toFixed(2)} seconds`
      );

      return this.verificationResults;
    } finally {
      await this.closeConnections();
    }
  }
}

// Example usage
async function main() {
  try {
    // Create verifier instance
    const verifier = new StarRocksVerifier(
      sourceConfig,
      targetConfig,
      databasesToVerify,
      tablesToSkip
    );

    // Run verifications
    await verifier.runAllVerifications();

    // Generate report
    const report = verifier.generateReport();

    // Print summary to console
    console.log("\nVerification Summary:");
    const table = new Table({
      title: "Verification Results",
      columns: [
        { name: "Check Type", alignment: "left" },
        { name: "Passed", alignment: "right" },
        { name: "Failed", alignment: "right" },
        { name: "Skipped", alignment: "right" },
        { name: "Errors", alignment: "right" },
        { name: "Total", alignment: "right" },
        { name: "Pass Rate", alignment: "right" },
      ],
    });

    // Count results by type
    const resultsByType = {};
    for (const result of verifier.verificationResults) {
      const checkType = result.check_type;
      if (!resultsByType[checkType]) {
        resultsByType[checkType] = {
          PASSED: 0,
          FAILED: 0,
          SKIPPED: 0,
          ERROR: 0,
        };
      }
      resultsByType[checkType][result.result]++;
    }

    // Add rows to table
    for (const [checkType, counts] of Object.entries(resultsByType)) {
      const total = counts.PASSED + counts.FAILED + counts.ERROR; // Exclude SKIPPED from total
      const passPercentage =
        total > 0 ? ((counts.PASSED / total) * 100).toFixed(2) : "100.00";

      table.addRow({
        "Check Type": checkType,
        Passed: counts.PASSED,
        Failed: counts.FAILED,
        Skipped: counts.SKIPPED,
        Errors: counts.ERROR,
        Total: total,
        "Pass Rate": `${passPercentage}%`,
      });
    }

    table.printTable();

    // Print any errors
    if (verifier.errors.length > 0) {
      console.log("\nErrors encountered:");
      for (const error of verifier.errors) {
        console.log(`- ${error}`);
      }
    }

    console.log(`\nDetailed report written to: ${REPORT_FILE}`);
  } catch (error) {
    console.error("Verification failed:", error);
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  main().catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
  });
}
