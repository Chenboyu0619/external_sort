# external_sort
Features

	•	Supports files larger than RAM.
	•	Reads the input file in chunks.
	•	Uses external merge sort.
	•	Uses multiprocessing.Pool for parallel sorting and parallel merging.
	•	Creates the output file in the same directory as the input file.
	•	Does not require any third-party Python libraries.
	•	Works with positive and negative integers.
	•	Supports empty lines in the input file; they are ignored.

Requirements

	•	Python 3.9 or newer.
	•	No additional dependencies are required.

Usage

    python external_sort.py input.txt 1000000
    Where:
	•	input.txt is the source file.
	•	1000000 is the maximum number of integers that may be loaded into memory at the same time.
	
Example:

    python external_sort.py random_numbers.txt 500000

Algorithm
    The program uses external merge sort.

    1. Splitting the input file

    The input file is scanned and divided into ranges. Each range contains no more than the allowed number of integers per worker process.

    2. Sorting chunks in parallel

    Each process reads one part of the file, loads only that chunk into memory, sorts it, and writes the result into a temporary sorted run file.
    This phase uses: multiprocessing.Pool
    so several chunks can be sorted at the same time.

    3. Merging sorted runs

    After all chunks are sorted, the program repeatedly merges pairs of sorted temporary files.
    The merge phase is also parallelized: multiple file pairs can be merged at the same time.

    4. Writing the final file

    After the last merge pass, the final sorted temporary file is moved to the output path.
    The output file is placed in the same directory as the input file.
