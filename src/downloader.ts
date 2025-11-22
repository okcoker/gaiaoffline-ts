import { join } from "@std/path";
import { ensureDir, exists } from "@std/fs";
import { Logger } from "./types.ts";
import { formatBytes } from "./utils.ts";

export interface DownloadProgress {
  url: string;
  status: "pending" | "downloading" | "completed" | "failed";
  bytesDownloaded: number;
  totalBytes: number;
  error?: string;
}

export type DownloadResult = {
  url: string;
  filePath: string;
  success: true;
} | {
  url: string;
  success: false;
  error: string;
};

export type StreamResult = {
  url: string;
  stream: ReadableStream<Uint8Array>;
  success: true;
} | {
  url: string;
  success: false;
  error: string;
};

export class ParallelDownloader {
  private tempDir: string;
  private parallelLimit: number;
  private resumable: boolean;
  private progress: Map<string, DownloadProgress> = new Map();
  private logger: Logger;

  constructor(tempDir: string, parallelLimit: number, logger: Logger) {
    this.tempDir = tempDir;
    this.parallelLimit = parallelLimit;
    this.resumable = true;
    this.logger = logger;
  }

  async initialize(): Promise<void> {
    await ensureDir(this.tempDir);
  }

  /**
   * Download multiple URLs in parallel
   */
  async downloadBatch(urls: string[]): Promise<DownloadResult[]> {
    const results: DownloadResult[] = [];

    for (let i = 0; i < urls.length; i += this.parallelLimit) {
      const batch = urls.slice(i, i + this.parallelLimit);
      const batchResults = await Promise.all(
        batch.map((url) => this.downloadFile(url)),
      );
      results.push(...batchResults);
    }

    return results;
  }

  /**
   * Start downloads and return streams immediately for processing
   * This allows processing to happen concurrently with downloading
   */
  async streamBatch(urls: string[]): Promise<StreamResult[]> {
    // Start all downloads in parallel up to limit
    const streamPromises = urls.map(async (url): Promise<StreamResult> => {
      try {
        const stream = await this.streamDownload(url);
        return { url, stream, success: true };
      } catch (error) {
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        this.logger.error(
          `Failed to create download stream for ${url}: ${errorMessage}`,
        );
        return { url, success: false, error: errorMessage };
      }
    });

    return await Promise.all(streamPromises);
  }

  /**
   * Download a single file with resume capability
   */
  private async downloadFile(url: string): Promise<DownloadResult> {
    const fileName = url.split("/").pop() || `file_${Date.now()}.csv.gz`;
    const filePath = join(this.tempDir, fileName);

    this.logger.debug(`Downloading file ${url} to ${filePath}`);
    // Initialize progress tracking
    this.progress.set(url, {
      url,
      status: "downloading",
      bytesDownloaded: 0,
      totalBytes: 0,
    });

    try {
      // Check if file already exists (resume capability)
      let existingSize = 0;
      if (this.resumable && (await exists(filePath))) {
        const stat = await Deno.stat(filePath);
        existingSize = stat.size;
        this.logger.debug(
          `File ${url} already exists, attempting to resume from ${
            formatBytes(existingSize)
          }`,
        );
      }

      // Setup request headers for resume
      const headers: HeadersInit = {};
      if (existingSize > 0) {
        headers["Range"] = `bytes=${existingSize}-`;
      }
      const response = await fetch(url, { headers });

      // Handle 416 Range Not Satisfiable - partial file is invalid
      if (response.status === 416) {
        this.logger.debug(
          `Range not satisfiable for ${url}, deleting partial file and retrying from scratch`,
        );
        // Delete corrupted/invalid partial file
        try {
          await Deno.remove(filePath);
        } catch {
          // Ignore if file doesn't exist
        }
        return this.downloadFile(url);
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      // Validate Content-Range header when resuming
      if (existingSize > 0 && response.status === 206) {
        const contentRange = response.headers.get("content-range");
        if (contentRange && !contentRange.includes(`${existingSize}-`)) {
          this.logger.debug(
            `Server didn't honor range request for ${url}, starting over`,
          );
          // Server returned wrong range, delete and start over
          try {
            await Deno.remove(filePath);
          } catch {
            // Ignore if file doesn't exist
          }
          return this.downloadFile(url);
        }
      }

      // Get total file size
      const contentLength = response.headers.get("content-length");
      const totalBytes = contentLength
        ? parseInt(contentLength) + existingSize
        : 0;

      this.logger.debug(`Total file size: ${formatBytes(totalBytes)}`);

      // Update progress
      this.progress.set(url, {
        url,
        status: "downloading",
        bytesDownloaded: existingSize,
        totalBytes,
      });

      // Stream to file
      if (response.body) {
        const file = await Deno.open(filePath, {
          create: true,
          write: true,
          append: existingSize > 0,
        });

        const reader = response.body.getReader();
        let downloadedBytes = existingSize;

        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) {
              this.logger.debug(
                `Downloaded ${url}: ${formatBytes(downloadedBytes)} ✅`,
              );
              break;
            }

            await file.write(value);
            downloadedBytes += value.length;

            this.logger.debug(
              `Downloading ${url}: ${formatBytes(downloadedBytes)}…`,
            );

            // Update progress
            this.progress.set(url, {
              url,
              status: "downloading",
              bytesDownloaded: downloadedBytes,
              totalBytes,
            });
          }
        } catch (error: unknown) {
          this.logger.error(`Failed to download file ${url}: ${error}`);
        } finally {
          file.close();
        }
      }

      // Mark as completed
      this.progress.set(url, {
        url,
        status: "completed",
        bytesDownloaded: totalBytes,
        totalBytes,
      });

      return { url, filePath, success: true };
    } catch (error: unknown) {
      const errorMessage = error instanceof Error
        ? error.message
        : String(error);

      const currentProgress = this.progress.get(url);
      const currentBytes = currentProgress?.bytesDownloaded ?? 0;
      const totalBytes = currentProgress?.totalBytes ?? 0;

      this.logger.error(
        `Failed to download file ${url}.\n${formatBytes(currentBytes)}/${
          formatBytes(totalBytes)
        }\nError: ${errorMessage}`,
      );

      this.progress.set(url, {
        url,
        status: "failed",
        bytesDownloaded: currentBytes,
        totalBytes: totalBytes,
        error: errorMessage,
      });

      return {
        url,
        success: false,
        error: errorMessage,
      };
    }
  }

  /**
   * Stream download without saving to disk
   * Returns ReadableStream for immediate processing
   * Includes retry logic with exponential backoff
   */
  async streamDownload(
    url: string,
    maxRetries = 3,
    retryDelay = 1000,
  ): Promise<ReadableStream<Uint8Array>> {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        if (attempt > 0) {
          const delay = retryDelay * Math.pow(2, attempt - 1);
          this.logger.debug(
            `Retry attempt ${attempt}/${maxRetries} for ${url} after ${delay}ms`,
          );
          await new Promise((resolve) => setTimeout(resolve, delay));
        }

        const response = await fetch(url);

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        if (!response.body) {
          throw new Error(`No response body for ${url}`);
        }

        return response.body;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        const isRetryable = this.isRetryableError(lastError);

        if (!isRetryable || attempt === maxRetries) {
          this.logger.error(
            `Failed to stream ${url} after ${attempt + 1} attempts: ${lastError.message}`,
          );
          throw lastError;
        }

        this.logger.debug(
          `Retryable error for ${url}: ${lastError.message}`,
        );
      }
    }

    throw lastError || new Error("Unknown error");
  }

  /**
   * Check if an error is retryable (network issues, timeouts, etc.)
   */
  private isRetryableError(error: Error): boolean {
    const message = error.message.toLowerCase();
    return (
      message.includes("connection") ||
      message.includes("timeout") ||
      message.includes("network") ||
      message.includes("econnreset") ||
      message.includes("enotfound") ||
      message.includes("socket") ||
      message.includes("aborted") ||
      message.includes("body from connection")
    );
  }

  /**
   * Get current progress for all downloads
   */
  getProgress(): DownloadProgress[] {
    return Array.from(this.progress.values());
  }

  /**
   * Get progress for a specific URL
   */
  getUrlProgress(url: string): DownloadProgress | undefined {
    return this.progress.get(url);
  }

  /**
   * Clear progress tracking
   */
  clearProgress(): void {
    this.progress.clear();
  }
}
