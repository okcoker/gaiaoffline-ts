// How to run:
// gcc -O3 -o test-c test.c -lz && ./test-c

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <zlib.h>

int main()
{
    const char *file_path = "./test.csv.gz";
    printf("Reading: %s\n", file_path);

    clock_t start = clock();

    // Open gzipped file
    gzFile file = gzopen(file_path, "rb");
    if (!file)
    {
        fprintf(stderr, "Failed to open file\n");
        return 1;
    }

    // Read and decompress file
    char buffer[8192];
    char *decompressed = malloc(1024 * 1024 * 100); // 100MB
    size_t total_size = 0;
    size_t buffer_capacity = 1024 * 1024 * 100;
    int bytes_read;

    while ((bytes_read = gzread(file, buffer, sizeof(buffer))) > 0)
    {
        if (total_size + bytes_read >= buffer_capacity)
        {
            buffer_capacity *= 2;
            decompressed = realloc(decompressed, buffer_capacity);
        }
        memcpy(decompressed + total_size, buffer, bytes_read);
        total_size += bytes_read;
    }

    gzclose(file);
    decompressed[total_size] = '\0';

    // Count rows (simple line counting, ignoring comments)
    long count = 0;
    int is_first_line = 1;
    char *line = strtok(decompressed, "\n");

    while (line != NULL)
    {
        if (strlen(line) > 0 && line[0] != '#')
        {
            if (is_first_line)
            {
                is_first_line = 0; // Skip header
            }
            else
            {
                count++;
            }
        }
        line = strtok(NULL, "\n");
    }

    free(decompressed);

    clock_t end = clock();
    double duration = (double)(end - start) / CLOCKS_PER_SEC;

    printf("\nParsed %ld rows in %.2fs\n", count, duration);
    printf("Rate: %ld rows/sec\n", (long)(count / duration));

    return 0;
}
