import { GaiaDatabase, type GaiaRecord } from "./database.ts";
import { ParallelDownloader } from "./downloader.ts";
import {
  createLogger,
  formatBytes,
  formatDuration,
  streamAndFilterCSV,
} from "./utils.ts";
import type { CLIConfig } from "./config.ts";
import { Logger } from "./types.ts";
import type { DownloadProgress } from "./downloader.ts";

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

    const totalFiles = fileLimit ?? allUrls.length;
    this.stats.totalFiles = totalFiles;

    this.logger.debug(`Found ${totalFiles} files to process…`);

    this.db.initializeTracking("file_tracking_gaiadr3", allUrls);

    // Filter out already processed files
    let pendingUrls = allUrls.filter(
      (url) => !this.db.isFileProcessed("file_tracking_gaiadr3", url),
    );

    if (fileLimit) {
      pendingUrls = pendingUrls.slice(0, fileLimit);
    }

    this.logger.debug(
      `${
        allUrls.length - pendingUrls.length
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
      const startTime = Date.now();
      const emoji = batchSize === 1 ? "⬇️" : "📦";

      let processResults: Array<{
        url: string;
        records: GaiaRecord[] | null;
        error: string | null;
      }>;

      if (this.config.useStreaming) {
        this.logger.info(
          `${emoji} Streaming batch ${batchNum}/${totalBatches} (${batchUrls.length} files)`,
        );

        const streamResults = await this.downloader.streamBatch(batchUrls);

        // Process all streams in parallel (decompress, parse, filter)
        const processPromises = streamResults.map(async (streamResult) => {
          if (!streamResult.success) {
            this.stats.failedFiles++;
            this.logger.error(
              `❌ Failed to download ${streamResult.url}: ${streamResult.error}`,
            );
            return {
              url: streamResult.url,
              records: null,
              error: streamResult.error,
            };
          }

          try {
            if (this.db.isFileProcessed(trackingTable, streamResult.url)) {
              this.logger.debug(
                `Skipping already processed: ${streamResult.url}`,
              );
              return { url: streamResult.url, records: [], error: null };
            }

            const records = await streamAndFilterCSV(
              streamResult.stream,
              this.config,
            );

            return { url: streamResult.url, records, error: null };
          } catch (error) {
            const errorMessage = error instanceof Error
              ? error.message
              : String(error);

            return {
              url: streamResult.url,
              records: null,
              error: errorMessage,
            };
          }
        });

        processResults = await Promise.all(processPromises);
      } else {
        // FILE MODE: Download to disk, then process from files
        if (totalBatches > 0) {
          this.logger.info(
            `${emoji} Downloading batch ${batchNum}/${totalBatches} (${batchUrls.length} files) to ${this.config.downloadDir}`,
          );
        }

        this.interval = setInterval(() => {
          this.printDownloadProgress();
        }, 15000);

        const downloadResults = await this.downloader.downloadBatch(batchUrls);

        clearInterval(this.interval);

        // Process files in parallel (read from disk)
        const processPromises = downloadResults.map(async (result) => {
          if (!result.success) {
            this.stats.failedFiles++;
            this.logger.error(
              `❌ Failed to download ${result.url}: ${result.error}`,
            );
            return { url: result.url, records: null, error: result.error };
          }

          try {
            if (this.db.isFileProcessed(trackingTable, result.url)) {
              this.logger.debug(`Skipping already processed: ${result.url}`);
              return { url: result.url, records: [], error: null };
            }

            const csvStartTime = Date.now();

            // Use FFI parser if enabled, otherwise TypeScript
            let records: GaiaRecord[];
            if (this.config.useCParser) {
              // Dynamically import C FFI only when needed (fastest option)
              const { streamAndFilterCSVC } = await import("./utils-c.ts");
              records = await streamAndFilterCSVC(result.filePath, this.config);
            } else if (this.config.useRustParser) {
              // Dynamically import Rust FFI only when needed
              const { streamAndFilterCSVRust } = await import(
                "./utils-rust.ts"
              );
              records = await streamAndFilterCSVRust(
                result.filePath,
                this.config,
              );
            } else {
              records = await streamAndFilterCSV(result.filePath, this.config);
            }

            this.logger.info(
              `${result.url} processed in ${Date.now() - csvStartTime}ms`,
            );

            // Clean up file if configured
            if (this.config.cleanUpDownloadedFiles) {
              try {
                await Deno.remove(result.filePath);
              } catch {
                // Ignore cleanup errors
              }
            }

            return { url: result.url, records, error: null };
          } catch (error) {
            const errorMessage = error instanceof Error
              ? error.message
              : String(error);

            // If gzip is corrupt, delete the file
            if (errorMessage.includes("corrupt gzip stream")) {
              try {
                await Deno.remove(result.filePath);
              } catch {
                // Ignore if already deleted
              }
            }

            return { url: result.url, records: null, error: errorMessage };
          }
        });

        processResults = await Promise.all(processPromises);
      }

      if (processResults.length > 0) {
        const totalDuration = Date.now() - startTime;
        this.logger.debug(
          `Processing took ${formatDuration(totalDuration)}`,
        );
      }

      // Accumulate all records for bulk insert
      const allRecords: GaiaRecord[] = [];
      const networkErrors: string[] = [];

      for (const result of processResults) {
        if (result.records) {
          allRecords.push(...result.records);
        } else if (result.error) {
          this.stats.failedFiles++;
          this.db.markFileFailed(trackingTable, result.url);

          // Track network errors separately for helpful message
          const isNetworkError = result.error.toLowerCase().includes(
            "connection",
          ) ||
            result.error.toLowerCase().includes("network") ||
            result.error.toLowerCase().includes("body from connection");

          if (isNetworkError) {
            networkErrors.push(result.url);
          }

          this.logger.error(
            `❌ Failed to process ${result.url}: ${result.error}`,
          );
        }
      }

      // Show helpful message for network errors
      if (networkErrors.length > 0) {
        this.logger.info(
          `💡 ${networkErrors.length} file(s) failed due to network issues. Run the command again to retry failed files.`,
        );
      }

      // Single bulk insert for entire batch
      if (allRecords.length > 0) {
        const insertedCount = this.db.insertGaiaRecords(allRecords);
        this.stats.totalRecords += insertedCount;

        // Mark all successful files as completed
        for (const result of processResults) {
          if (result.records && result.records.length >= 0) {
            this.db.markFileCompleted(trackingTable, result.url);
            this.stats.completedFiles++;
          }
        }
      }

      // Show progress
      const progress = this.db.getTrackingProgress(trackingTable);
      const percentage = progress.total > 0
        ? (progress.completed / progress.total) * 100
        : 0;

      const parts = [
        `✅ Completed: ${progress.completed}`,
        progress.failed > 0 ? `❌ Failed: ${progress.failed}` : null,
        `🗄️ Total records: ${this.stats.totalRecords.toLocaleString()}`,
        `${percentage.toFixed(1)}% (${progress.completed}/${progress.total})`,
      ].filter(Boolean).join(" | ");

      this.logger.info(parts + "\n");
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
