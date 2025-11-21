import { GaiaDatabase } from "./database.ts";
import { type DownloadProgress, ParallelDownloader } from "./downloader.ts";
import {
  createLogger,
  formatBytes,
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
  private interval: number = 0;

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

    await this.downloader.initialize();
    this.db.initialize();

    this.logger.info("📋 Fetching list of Gaia DR3 files…");
    const allUrls = await getCSVUrls(
      "https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/",
    );

    const urls = fileLimit ? allUrls.slice(0, fileLimit) : allUrls;
    this.stats.totalFiles = urls.length;

    this.logger.debug(`Found ${urls.length} files to process…`);

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

    this.db.createIndices();
    this.db.optimize();

    this.stats.duration = Date.now() - startTime;

    this.printSummary();

    return this.stats;
  }

  private printDownloadProgress() {
    if (this.config.logLevel !== "INFO") {
      return;
    }

    const aggregated = this.downloader.getProgress().reduce(
      (acc, progress) => {
        acc.status[progress.status] = (acc.status[progress.status] || 0) +
          1;
        acc.downloaded += progress.bytesDownloaded;
        acc.total += progress.totalBytes;
        return acc;
      },
      {
        status: {} as Record<DownloadProgress["status"], number>,
        downloaded: 0,
        total: 1,
      },
    );
    this.logger.info(
      `Download progress: ${formatBytes(aggregated.downloaded)} / ${
        formatBytes(aggregated.total)
      } (${
        Object.entries(aggregated.status).map(([status, count]) =>
          `${status}: ${count}`
        ).join(", ")
      })`,
    );
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

      this.logger.info(`📦 Batch ${batchNum}/${totalBatches}`);
      this.logger.info(
        `⬇️ Downloading ${batchUrls.length} files in parallel to ${this.config.downloadDir}…`,
      );
      this.interval = setInterval(() => {
        this.printDownloadProgress();
      }, 15000);

      // Download batch
      const downloadResults = await this.downloader.downloadBatch(batchUrls);

      this.printDownloadProgress();
      clearInterval(this.interval);

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
      this.logger.info(
        `${renderProgressBar(progress.completed, progress.total)}`,
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
    this.logger.info("=".repeat(60));
    this.logger.info("Population Summary");
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
  cleanup() {
    // await this.downloader.cleanup();
    clearInterval(this.interval);
  }
}

/**
 * Fetch all CSV URLs from a Gaia directory listing
 */
export async function getCSVUrls(baseUrl: string): Promise<string[]> {
  const response = await fetch(baseUrl);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${baseUrl}: ${response.statusText}`);
  }

  const html = await response.text();

  // Simple regex to extract .csv.gz or .gz links
  const linkRegex = /href="([^"]+\.(csv\.gz|gz))"/g;
  const urls: string[] = [];

  let match;
  while ((match = linkRegex.exec(html)) !== null) {
    const link = match[1];
    // Build full URL if it's a relative path
    const fullUrl = link.startsWith("http") ? link : baseUrl + link;
    urls.push(fullUrl);
  }

  return urls;
}
