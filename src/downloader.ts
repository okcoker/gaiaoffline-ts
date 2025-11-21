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

export interface DownloadResult {
  url: string;
  filePath: string;
  success: boolean;
  error?: string;
}

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
   * Download multiple URLs in parallel with batching
   */
  async downloadBatch(urls: string[]): Promise<DownloadResult[]> {
    const results: DownloadResult[] = [];

    // Process in batches to avoid overwhelming the network
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
          `File ${url} already exists, resuming from ${
            formatBytes(existingSize)
          } bytes`,
        );
      }

      // Setup request headers for resume
      const headers: HeadersInit = {};
      if (existingSize > 0) {
        headers["Range"] = `bytes=${existingSize}-`;
      }
      const response = await fetch(url, { headers });

      if (!response.ok && response.status !== 206) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
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
                `✅ Downloaded ${url}: ${formatBytes(downloadedBytes)}`,
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
        filePath: "",
        success: false,
        error: errorMessage,
      };
    }
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

  /**
   * Clean up temp directory
   */
  async cleanup(): Promise<void> {
    try {
      const files = Array.from(this.progress.values()).map((progress) =>
        progress.url
      );
      await Promise.all(files.map((file) => Deno.remove(file)));
    } catch (error: unknown) {
      this.logger.error("Failed to cleanup temp directory:", error);
    }
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
