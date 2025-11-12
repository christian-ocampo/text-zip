#include <dirent.h> 
#include <stdio.h> 
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>

#define BUFFER_SIZE 1048576 // 1MB
#define THREADS 15

// Global index
volatile int i = 0;
int nfiles;
char **files;
volatile int total_in, total_out;
FILE *f_out;

// Used by threads to lock critical sections 
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

// Structure to store compressed data for each file
typedef struct {
    int size;
    unsigned char *data;
} compressed_file_t;

compressed_file_t *compressed_files;

void *compress_file_thread(void *arg) {
	char *directory_name = (char *)arg;

	while (1) {
		// Create lock for threads to use
		//This is a critical section because we need to ensure no one index is used by more than one thread
		pthread_mutex_lock(&mutex);
        int local_i = i; // Retrieve the current file index
        i++;             // Increment the global index
        pthread_mutex_unlock(&mutex);

		// Exit if there are no more files to process
        if (local_i >= nfiles) break; 

		// Get path of file
		int len = strlen(directory_name)+strlen(files[local_i])+2;
		char *full_path = malloc(len*sizeof(char));
		assert(full_path != NULL);
		strcpy(full_path, directory_name);
		strcat(full_path, "/");
		strcat(full_path, files[local_i]);

		unsigned char buffer_in[BUFFER_SIZE];
		unsigned char buffer_out[BUFFER_SIZE];

		// load file
		FILE *f_in = fopen(full_path, "r");
		assert(f_in != NULL);
		int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
		fclose(f_in);
		total_in += nbytes;

		// zip file
		z_stream strm;
		int ret = deflateInit(&strm, 9);
		assert(ret == Z_OK);
		strm.avail_in = nbytes;
		strm.next_in = buffer_in;
		strm.avail_out = BUFFER_SIZE;
		strm.next_out = buffer_out;

		ret = deflate(&strm, Z_FINISH);
		assert(ret == Z_STREAM_END);

		// dump zipped file
		int nbytes_zipped = BUFFER_SIZE-strm.avail_out;
		// Store bytes and data in each struct instance
		compressed_files[local_i].size = nbytes_zipped;
        compressed_files[local_i].data = malloc(nbytes_zipped);
        memcpy(compressed_files[local_i].data, buffer_out, nbytes_zipped);

		total_out += nbytes_zipped;

		free(full_path);
	}
}

int cmp(const void *a, const void *b) {
	return strcmp(*(char **) a, *(char **) b);
}

int compress_directory(char *directory_name) {
	DIR *d;
	struct dirent *dir;
	// Changed to global variable for all threads to access
	files = NULL; 
	// Changed to global variable for all threads to access
	nfiles = 0; 

	d = opendir(directory_name);
	if(d == NULL) {
		printf("An error has occurred\n");
		return 0;
	}

	// create sorted list of text files
	while ((dir = readdir(d)) != NULL) {
		files = realloc(files, (nfiles+1)*sizeof(char *));
		assert(files != NULL);

		int len = strlen(dir->d_name);
		if(dir->d_name[len-4] == '.' && dir->d_name[len-3] == 't' && dir->d_name[len-2] == 'x' && dir->d_name[len-1] == 't') {
			files[nfiles] = strdup(dir->d_name);
			assert(files[nfiles] != NULL);

			nfiles++;
		}
	}
	closedir(d);
	qsort(files, nfiles, sizeof(char *), cmp);

	// Allocate memory for the compressed_files array
	compressed_files = malloc(nfiles * sizeof(compressed_file_t));
	// Check that it was allocated correctly
	assert(compressed_files != NULL);

	// Initialize each entry in the compressed_files array
	for (int i = 0; i < nfiles; i++) {
		compressed_files[i].size = 0;
		compressed_files[i].data = NULL;
	}

	// create a single zipped package with all text files in lexicographical order
	total_in = 0, total_out = 0; // Changed to global variables.
	f_out = fopen("text.tzip", "w"); // Changed to global variable.
	assert(f_out != NULL);

	// Create list of threads that will be used to process each file
	pthread_t threads[THREADS];

    // Create threads via pthread_create
    for (int i = 0; i < THREADS; i++) {
		// We pass in the an element of threads to store the thread id
		// We pass in the compress_file_thread function to be ran by each thread
        pthread_create(&threads[i], NULL, compress_file_thread, directory_name);
    }

    // Join threads and wait for all of them to finish
    for (int i = 0; i < THREADS; i++) {
        pthread_join(threads[i], NULL);
    }


	for (int i = 0; i < nfiles; i++) {
		// Write each compressed file to f_out
        fwrite(&compressed_files[i].size, sizeof(int), 1, f_out);
        fwrite(compressed_files[i].data, sizeof(unsigned char), compressed_files[i].size, f_out);
        free(compressed_files[i].data); // Free the allocated memory for compressed data
    }
	fclose(f_out);

	printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/total_in);

	// release list of files
	for(i=0; i < nfiles; i++)
		free(files[i]);
	free(files);

	// do not modify the main function after this point!
	return 0;
}
