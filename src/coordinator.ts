import { GaiaDatabase } from "./database.ts";
import { getCSVUrls, ParallelDownloader } from "./downloader.ts";
import {
  createLogger,
  formatDuration,
  processCSVFile,
  renderProgressBar,
} from "./utils.ts";
import type { CLIConfig } from "./config.ts";
import { Logger } from "./types.ts";

export interface PopulateStats {
  totalFiles: number;
  completedFiles: number;
  failedFiles: number;
  totalRecords: number;
  duration: number;
}

/**
 * Coordinates parallel downloads with sequential database inserts
 */
export class PopulateCoordinator {
  private db: GaiaDatabase;
  private downloader: ParallelDownloader;
  private config: CLIConfig;
  private stats: PopulateStats = {
    totalFiles: 0,
    completedFiles: 0,
    failedFiles: 0,
    totalRecords: 0,
    duration: 0,
  };
  private logger: Logger;

  constructor(db: GaiaDatabase, config: CLIConfig) {
    this.db = db;
    this.config = config;
    this.logger = createLogger(config.logLevel, "GaiaPopulate");
    this.downloader = new ParallelDownloader(
      config.downloadDir,
      config.maxParallelDownloads,
      createLogger(config.logLevel, "Downloader"),
    );
  }

  /**
   * Populate the Gaia DR3 database
   */
  async populateGaiaDR3(fileLimit?: number): Promise<PopulateStats> {
    this.logger.info("🌌 Starting Gaia DR3 population…");

    const startTime = Date.now();

    // Initialize
    await this.downloader.initialize();
    this.db.initialize();

    // Get all CSV URLs
    this.logger.info("📋 Fetching list of Gaia DR3 files…");
    const allUrls = await getCSVUrls(
      "https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/",
    );

    const urls = fileLimit ? allUrls.slice(0, fileLimit) : allUrls;
    this.stats.totalFiles = urls.length;

    this.logger.debug(`Found ${urls.length} files to process…`);

    // Initialize tracking
    this.db.initializeTracking("file_tracking_gaiadr3", urls);

    // Filter out already processed files
    const pendingUrls = urls.filter(
      (url) => !this.db.isFileProcessed("file_tracking_gaiadr3", url),
    );

    this.logger.debug(
      `${
        urls.length - pendingUrls.length
      } files already processed, ${pendingUrls.length} remaining\n`,
    );

    // Process in batches: download N files in parallel, then insert sequentially
    await this.processBatchedPipeline(pendingUrls, "file_tracking_gaiadr3");

    // Create indices
    this.logger.debug("\n📊 Creating database indices…");
    this.db.createIndices();

    // Optimize
    this.logger.debug("🔧 Optimizing database…");
    this.db.optimize();

    this.stats.duration = Date.now() - startTime;

    this.printSummary();

    return this.stats;
  }

  /**
   * Process files in a batched pipeline:
   * - Download batch N in parallel
   * - While inserting batch N, download batch N+1
   */
  private async processBatchedPipeline(
    urls: string[],
    trackingTable: string,
  ): Promise<void> {
    const batchSize = this.config.maxParallelDownloads;
    const totalBatches = Math.ceil(urls.length / batchSize);

    for (let i = 0; i < urls.length; i += batchSize) {
      const batchUrls = urls.slice(i, i + batchSize);
      const batchNum = Math.floor(i / batchSize) + 1;

      this.logger.info(`\n📦 Batch ${batchNum}/${totalBatches}`);
      this.logger.debug(
        `⬇️ Downloading ${batchUrls.length} files in parallel…`,
      );

      // Download batch
      // const downloadResults = await this.downloader.downloadBatch(batchUrls);

      const downloadResults = [
        {
          "url":
            "https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/GaiaSource_000000-003111.csv.gz",
          "filePath":
            "/var/folders/qj/lxh91jfx75n0z_k9nlsxfh9h0000gn/T/GaiaSource_000000-003111.csv.gz",
          "success": true,
        },
        {
          "url":
            "https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/GaiaSource_003112-005263.csv.gz",
          "filePath":
            "/var/folders/qj/lxh91jfx75n0z_k9nlsxfh9h0000gn/T/GaiaSource_003112-005263.csv.gz",
          "success": true,
        },
        {
          "url":
            "https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/GaiaSource_005264-006601.csv.gz",
          "filePath":
            "/var/folders/qj/lxh91jfx75n0z_k9nlsxfh9h0000gn/T/GaiaSource_005264-006601.csv.gz",
          "success": true,
        },
        {
          "url":
            "https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/GaiaSource_006602-007952.csv.gz",
          "filePath":
            "/var/folders/qj/lxh91jfx75n0z_k9nlsxfh9h0000gn/T/GaiaSource_006602-007952.csv.gz",
          "success": true,
        },
        {
          "url":
            "https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/GaiaSource_007953-010234.csv.gz",
          "filePath":
            "/var/folders/qj/lxh91jfx75n0z_k9nlsxfh9h0000gn/T/GaiaSource_007953-010234.csv.gz",
          "success": true,
        },
      ];

      if (downloadResults.length > 0) {
        // Process successful downloads sequentially
        this.logger.debug(`💾 Inserting batch into database…`);
      }

      for (const result of downloadResults) {
        if (result.success) {
          const processResult = await processCSVFile(
            result.filePath,
            result.url,
            this.db,
            this.config,
            trackingTable,
          );

          if (processResult.success) {
            this.stats.completedFiles++;
            this.stats.totalRecords += processResult.recordCount;
          } else {
            this.stats.failedFiles++;
            this.logger.error(
              `❌ Failed to process ${result.url}: ${processResult.error}`,
            );
          }
        } else {
          this.stats.failedFiles++;
          this.logger.error(
            `❌ Failed to download ${result.url}: ${result.error}`,
          );
        }
      }

      // Show progress
      const progress = this.db.getTrackingProgress(trackingTable);
      console.log(
        `\n${renderProgressBar(progress.completed, progress.total)}`,
      );
      this.logger.info(
        `✅ Completed: ${progress.completed} | ❌ Failed: ${progress.failed} | 📊 Total records: ${this.stats.totalRecords.toLocaleString()}`,
      );
    }
  }

  /**
   * Print final summary
   */
  private printSummary(): void {
    this.logger.info("\n" + "=".repeat(60));
    this.logger.info("📊 Population Summary");
    this.logger.info("=".repeat(60));
    this.logger.info(`Total files:      ${this.stats.totalFiles}`);
    this.logger.info(`Completed:        ${this.stats.completedFiles}`);
    this.logger.info(`Failed:           ${this.stats.failedFiles}`);
    this.logger.info(
      `Total records:    ${this.stats.totalRecords.toLocaleString()}`,
    );
    this.logger.info(
      `Duration:         ${formatDuration(this.stats.duration)}`,
    );
    this.logger.info(`Database path:    ${this.config.databasePath}`);
    this.logger.info("=".repeat(60) + "\n");
  }

  /**
   * Clean up resources
   */
  async cleanup(): Promise<void> {
    await this.downloader.cleanup();
  }
}
