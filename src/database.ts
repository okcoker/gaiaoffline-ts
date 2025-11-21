import { Database } from "@db/sqlite";
import type { CLIConfig } from "./config.ts";
import { Logger } from "./types.ts";
import { createLogger } from "./utils.ts";

export interface FileTrackingRecord {
  url: string;
  status: "pending" | "completed" | "failed";
}

export interface GaiaRecord {
  source_id: string;
  ra: number;
  dec: number;
  [key: string]: string | number | null;
}

export interface TmassXmatchRecord {
  gaiadr3_source_id: string;
  tmass_source_id: string;
}

export interface TmassRecord {
  gaiadr3_source_id: string;
  tmass_source_id: string;
  j_m: number | null;
  h_m: number | null;
  k_m: number | null;
}

export class GaiaDatabase {
  private db: Database;
  private config: CLIConfig;
  private logger: Logger;

  constructor(config: CLIConfig) {
    this.db = new Database(config.databasePath);
    this.config = config;
    this.logger = createLogger(config.logLevel, "Database");
  }

  /**
   * Initialize database schema
   */
  initialize(): void {
    // Create main Gaia table
    const columnDefs = this.config.storedColumns
      .map((col) => {
        if (col === "source_id") {
          return `${col} TEXT PRIMARY KEY`;
        }
        return `${col} REAL`;
      })
      .join(", ");

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS gaiadr3 (
        ${columnDefs}
      );
    `);

    // Create 2MASS crossmatch table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS tmass_xmatch (
        gaiadr3_source_id TEXT PRIMARY KEY,
        tmass_source_id TEXT NOT NULL
      );
    `);

    // Create 2MASS photometry table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS tmass (
        gaiadr3_source_id TEXT PRIMARY KEY,
        tmass_source_id TEXT NOT NULL,
        j_m REAL,
        h_m REAL,
        k_m REAL
      );
    `);

    // Create file tracking tables
    this.createTrackingTable("file_tracking_gaiadr3");
    this.createTrackingTable("file_tracking_tmass_xmatch");
    this.createTrackingTable("file_tracking_tmass");
  }

  /**
   * Create a tracking table for file processing
   */
  private createTrackingTable(tableName: string): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS ${tableName} (
        url TEXT PRIMARY KEY,
        status TEXT DEFAULT 'pending'
      );
    `);
  }

  /**
   * Initialize tracking table with URLs
   */
  initializeTracking(
    tableName: string,
    urls: string[],
    overwrite = false,
  ): void {
    const stmt = overwrite
      ? this.db.prepare(
        `INSERT OR REPLACE INTO ${tableName} (url, status) VALUES (?, 'pending')`,
      )
      : this.db.prepare(
        `INSERT OR IGNORE INTO ${tableName} (url, status) VALUES (?, 'pending')`,
      );

    this.db.transaction(() => {
      for (const url of urls) {
        stmt.run(url);
      }
    })();

    stmt.finalize();
  }

  /**
   * Check if a file has already been processed
   */
  isFileProcessed(tableName: string, url: string): boolean {
    const result = this.db.prepare(
      `SELECT status FROM ${tableName} WHERE url = ?`,
    ).get<{ status: string }>(url);

    return result?.status === "completed";
  }

  /**
   * Mark a file as completed
   */
  markFileCompleted(tableName: string, url: string): void {
    this.db.prepare(
      `UPDATE ${tableName} SET status = 'completed' WHERE url = ?`,
    )
      .run(url);
  }

  /**
   * Mark a file as failed
   */
  markFileFailed(tableName: string, url: string): void {
    this.db.prepare(`UPDATE ${tableName} SET status = 'failed' WHERE url = ?`)
      .run(url);
  }

  /**
   * Get tracking progress
   */
  getTrackingProgress(tableName: string): {
    total: number;
    completed: number;
    failed: number;
    pending: number;
  } {
    const result = this.db.prepare(`
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending
      FROM ${tableName}
    `).get<{
      total: number;
      completed: number;
      failed: number;
      pending: number;
    }>();

    return result ?? {
      total: 0,
      completed: 0,
      failed: 0,
      pending: 0,
    };
  }

  /**
   * Insert Gaia records from CSV data (sequential, transactional)
   */
  insertGaiaRecords(records: GaiaRecord[]): number {
    if (records.length === 0) return 0;

    // Build dynamic INSERT statement based on columns
    const columns = this.config.storedColumns.join(", ");
    const placeholders = this.config.storedColumns.map(() => "?").join(", ");

    const stmt = this.db.prepare(
      `INSERT OR IGNORE INTO gaiadr3 (${columns}) VALUES (${placeholders})`,
    );

    let insertedCount = 0;

    this.db.transaction(() => {
      for (const record of records) {
        const values = this.config.storedColumns.map((col) => record[col]);
        stmt.run(...values);
        insertedCount++;
      }
    })();

    stmt.finalize();
    return insertedCount;
  }

  /**
   * Insert 2MASS crossmatch records
   */
  insertTmassXmatchRecords(records: TmassXmatchRecord[]): number {
    if (records.length === 0) return 0;

    const stmt = this.db.prepare(
      `INSERT OR IGNORE INTO tmass_xmatch (gaiadr3_source_id, tmass_source_id) VALUES (?, ?)`,
    );

    let insertedCount = 0;

    this.db.transaction(() => {
      for (const record of records) {
        stmt.run(record.gaiadr3_source_id, record.tmass_source_id);
        insertedCount++;
      }
    })();

    stmt.finalize();
    return insertedCount;
  }

  /**
   * Insert 2MASS photometry records
   */
  insertTmassRecords(records: TmassRecord[]): number {
    if (records.length === 0) return 0;

    const stmt = this.db.prepare(
      `INSERT OR IGNORE INTO tmass (gaiadr3_source_id, tmass_source_id, j_m, h_m, k_m) VALUES (?, ?, ?, ?, ?)`,
    );

    let insertedCount = 0;

    this.db.transaction(() => {
      for (const record of records) {
        stmt.run(
          record.gaiadr3_source_id,
          record.tmass_source_id,
          record.j_m,
          record.h_m,
          record.k_m,
        );
        insertedCount++;
      }
    })();

    stmt.finalize();
    return insertedCount;
  }

  /**
   * Check if 2MASS table exists
   */
  hasTmassTable(): boolean {
    const result = this.db.prepare(
      `SELECT name FROM sqlite_master WHERE type='table' AND name='tmass'`,
    ).get() as { name: string } | undefined;

    return result !== undefined;
  }

  /**
   * Get database handle for direct queries (used by utils)
   */
  getDb(): Database {
    return this.db;
  }

  /**
   * Prepare a statement
   */
  prepare(sql: string) {
    return this.db.prepare(sql);
  }

  /**
   * Create indices on commonly queried columns
   */
  createIndices(): void {
    this.logger.debug("Creating database indices…");

    const indices = [
      "CREATE INDEX IF NOT EXISTS idx_source_id ON gaiadr3(source_id)",
      "CREATE INDEX IF NOT EXISTS idx_ra ON gaiadr3(ra)",
      "CREATE INDEX IF NOT EXISTS idx_dec ON gaiadr3(dec)",
      "CREATE INDEX IF NOT EXISTS idx_ra_dec ON gaiadr3(ra, dec)",
      "CREATE INDEX IF NOT EXISTS idx_phot_g_mean_flux ON gaiadr3(phot_g_mean_flux)",
      "CREATE INDEX IF NOT EXISTS idx_tmass_xmatch_gaiadr3 ON tmass_xmatch(gaiadr3_source_id)",
      "CREATE INDEX IF NOT EXISTS idx_tmass_xmatch_tmass ON tmass_xmatch(tmass_source_id)",
      "CREATE INDEX IF NOT EXISTS idx_tmass_gaiadr3 ON tmass(gaiadr3_source_id)",
      "CREATE INDEX IF NOT EXISTS idx_tmass_tmass ON tmass(tmass_source_id)",
    ];

    for (const indexSql of indices) {
      try {
        this.db.exec(indexSql);
      } catch (error) {
        this.logger.error(`Failed to create index: ${error}`);
      }
    }

    this.logger.debug("Indices created successfully");
  }

  /**
   * Vacuum and optimize the database
   */
  optimize(): void {
    this.logger.debug("Optimizing database…");
    this.db.exec("VACUUM");
    this.db.exec("ANALYZE");
    this.logger.debug("Database optimized");
  }

  /**
   * Execute a cone search query
   */
  coneSearch(
    ra: number,
    dec: number,
    radius: number,
    magnitudeLimit?: [number, number],
    tmassCrossmatch = false,
  ): GaiaRecord[] {
    const radiusRad = (radius * Math.PI) / 180;
    const raRad = (ra * Math.PI) / 180;
    const decRad = (dec * Math.PI) / 180;

    const sinDec = Math.sin(decRad);
    const cosDec = Math.cos(decRad);
    const cosRadius = Math.cos(radiusRad);

    // Calculate bounding box
    const deltaRa = (radius * 180) / (Math.PI * Math.cos(decRad));
    const deltaDec = radius;

    const decMin = Math.max(dec - deltaDec, -90);
    const decMax = Math.min(dec + deltaDec, 90);
    const raMin = (ra - deltaRa + 360) % 360;
    const raMax = (ra + deltaRa) % 360;

    // Build SELECT clause with 2MASS join if needed
    let selectClause = "g.*";
    let fromClause = "gaiadr3 g";

    if (tmassCrossmatch) {
      selectClause += ", t.tmass_source_id, t.j_m, t.h_m, t.k_m";
      fromClause += " LEFT JOIN tmass t ON g.source_id = t.gaiadr3_source_id";
    }

    // Build query with magnitude filter if provided
    let whereClause = `g.dec BETWEEN ${decMin} AND ${decMax}`;

    if (raMin > raMax) {
      whereClause +=
        ` AND (g.ra BETWEEN ${raMin} AND 360 OR g.ra BETWEEN 0 AND ${raMax})`;
    } else {
      whereClause += ` AND g.ra BETWEEN ${raMin} AND ${raMax}`;
    }

    if (magnitudeLimit) {
      const [minMag, maxMag] = magnitudeLimit;
      const zp = this.config.zeropoints[0];
      const maxFlux = Math.round(10 ** ((zp - minMag) / 2.5));
      const minFlux = Math.round(10 ** ((zp - maxMag) / 2.5));

      whereClause +=
        ` AND g.phot_g_mean_flux < ${maxFlux} AND g.phot_g_mean_flux > ${minFlux}`;
    }

    // Add spherical cap check
    whereClause += ` AND (
      sin(radians(g.dec)) * ${sinDec} +
      cos(radians(g.dec)) * ${cosDec} * cos(radians(g.ra) - ${raRad})
    ) >= ${cosRadius}`;

    const query =
      `SELECT ${selectClause} FROM ${fromClause} WHERE ${whereClause}`;

    return this.db.prepare(query).all() as GaiaRecord[];
  }

  /**
   * Get total record count
   */
  getRecordCount(table = "gaiadr3"): number {
    const result = this.db.prepare(`SELECT COUNT(*) as count FROM ${table}`)
      .get<{ count: number }>();
    return result?.count ?? 0;
  }

  /**
   * Close database connection
   */
  close(): void {
    this.db.close();
  }
}
