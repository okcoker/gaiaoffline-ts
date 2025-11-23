#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <ctype.h>

// Simple JSON array builder
typedef struct {
    char* data;
    size_t size;
    size_t capacity;
} JsonBuilder;

void json_builder_init(JsonBuilder* builder) {
    builder->capacity = 1024 * 1024 * 10; // 10MB initial
    builder->data = malloc(builder->capacity);
    builder->size = 0;
    builder->data[0] = '\0';
}

void json_builder_append(JsonBuilder* builder, const char* str) {
    size_t len = strlen(str);
    if (builder->size + len + 1 >= builder->capacity) {
        builder->capacity *= 2;
        builder->data = realloc(builder->data, builder->capacity);
    }
    memcpy(builder->data + builder->size, str, len);
    builder->size += len;
    builder->data[builder->size] = '\0';
}

void json_builder_append_escaped(JsonBuilder* builder, const char* str) {
    char buffer[4096];
    size_t j = 0;
    buffer[j++] = '"';

    for (size_t i = 0; str[i] && j < sizeof(buffer) - 3; i++) {
        if (str[i] == '"' || str[i] == '\\') {
            buffer[j++] = '\\';
        }
        buffer[j++] = str[i];
    }
    buffer[j++] = '"';
    buffer[j] = '\0';

    json_builder_append(builder, buffer);
}

// Parse a gzipped CSV file and return JSON array
char* parse_gzipped_csv(const char* file_path, const char* columns_json, size_t chunk_size) {
    // Open gzipped file
    gzFile file = gzopen(file_path, "rb");
    if (!file) {
        return strdup("{\"error\":\"Failed to open file\"}");
    }

    // Read and decompress file into memory
    char buffer[8192];
    char* decompressed = malloc(1024 * 1024 * 100); // 100MB initial
    size_t total_size = 0;
    size_t buffer_capacity = 1024 * 1024 * 100;
    int bytes_read;

    while ((bytes_read = gzread(file, buffer, sizeof(buffer))) > 0) {
        if (total_size + bytes_read >= buffer_capacity) {
            buffer_capacity *= 2;
            decompressed = realloc(decompressed, buffer_capacity);
        }
        memcpy(decompressed + total_size, buffer, bytes_read);
        total_size += bytes_read;
    }

    gzclose(file);
    decompressed[total_size] = '\0';

    // Parse columns to keep from JSON array
    // Simple parser: ["col1","col2"] -> extract column names
    char columns_to_keep[100][256];
    int num_columns_to_keep = 0;

    const char* p = strchr(columns_json, '[');
    if (p) {
        p++;
        while (*p && *p != ']') {
            // Skip whitespace
            while (*p && isspace(*p)) p++;

            // Check for quoted string
            if (*p == '"') {
                p++;
                size_t i = 0;
                while (*p && *p != '"' && i < 255) {
                    columns_to_keep[num_columns_to_keep][i++] = *p++;
                }
                columns_to_keep[num_columns_to_keep][i] = '\0';
                num_columns_to_keep++;

                // Skip closing quote and comma
                if (*p == '"') p++;
                while (*p && (*p == ',' || isspace(*p))) p++;
            } else {
                break;
            }
        }
    }

    // Parse CSV
    JsonBuilder json;
    json_builder_init(&json);
    json_builder_append(&json, "[");

    // Parse line by line manually instead of using strtok
    int* column_indices = NULL;
    int num_indices = 0;
    char** headers = NULL;
    int record_count = 0;
    int line_num = 0;


    // Process each line
    char* line_start = decompressed;
    char* line_end;

    while (*line_start) {
        // Find end of line
        line_end = strchr(line_start, '\n');
        if (!line_end) {
            line_end = line_start + strlen(line_start);
        }

        // Extract line
        size_t line_len = line_end - line_start;
        if (line_len == 0 || *line_start == '#') {
            line_start = (*line_end == '\n') ? line_end + 1 : line_end;
            continue;
        }

        char* line = strndup(line_start, line_len);
        line_num++;

        if (line_num == 1) {
            // Parse header to find column indices
            column_indices = malloc(num_columns_to_keep * sizeof(int));
            headers = malloc(num_columns_to_keep * sizeof(char*));

            char* header_copy = strdup(line);
            char* token = strtok(header_copy, ",");
            int col_idx = 0;

            while (token) {
                // Check if this column should be kept
                for (int i = 0; i < num_columns_to_keep; i++) {
                    if (strcmp(token, columns_to_keep[i]) == 0) {
                        column_indices[num_indices] = col_idx;
                        headers[num_indices] = strdup(token);
                        num_indices++;
                        break;
                    }
                }
                token = strtok(NULL, ",");
                col_idx++;
            }

            free(header_copy);
            free(line);
        } else {
            // Parse data row
            if (record_count > 0) {
                json_builder_append(&json, ",");
            }

            json_builder_append(&json, "{");

            char* row_copy = strdup(line);
            char* token = strtok(row_copy, ",");
            int col_idx = 0;
            int field_count = 0;

            while (token) {
                // Check if this column should be included
                for (int i = 0; i < num_indices; i++) {
                    if (column_indices[i] == col_idx) {
                        if (field_count > 0) {
                            json_builder_append(&json, ",");
                        }

                        json_builder_append(&json, "\"");
                        json_builder_append(&json, headers[i]);
                        json_builder_append(&json, "\":");

                        // Check if value is a number or string
                        if (strlen(token) == 0 || strcmp(token, "null") == 0 || strcmp(token, "NULL") == 0) {
                            json_builder_append(&json, "null");
                        } else if (strspn(token, "0123456789.-+eE") == strlen(token)) {
                            // Looks like a number
                            json_builder_append(&json, token);
                        } else {
                            // String value
                            json_builder_append_escaped(&json, token);
                        }

                        field_count++;
                        break;
                    }
                }
                token = strtok(NULL, ",");
                col_idx++;
            }

            json_builder_append(&json, "}");
            free(row_copy);
            free(line);
            record_count++;
        }

        // Move to next line
        line_start = (*line_end == '\n') ? line_end + 1 : line_end;
    }

    json_builder_append(&json, "]");

    // Cleanup
    free(decompressed);
    if (column_indices) free(column_indices);
    if (headers) {
        for (int i = 0; i < num_indices; i++) {
            free(headers[i]);
        }
        free(headers);
    }

    return json.data;
}

// Free a string allocated by this library
void free_string(char* ptr) {
    if (ptr) {
        free(ptr);
    }
}
