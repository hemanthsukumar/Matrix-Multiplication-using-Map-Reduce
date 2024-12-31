# Matrix Multiplication using Map-Reduce

## Project Overview

This project implements matrix multiplication using two Map-Reduce jobs. The task involves multiplying two matrices, `M` and `N`, and follows a specific procedure outlined in the pseudo code. The input matrices are sparse, and the multiplication process is performed using two Map-Reduce jobs to calculate the products and then sum them for the final output matrix.

The matrix multiplication procedure involves:
1. **First Map-Reduce Job**: It computes all possible products `Mik * Nkj` by joining matrices `M` and `N`.
2. **Second Map-Reduce Job**: It adds all the products for each pair `(i, j)` to get the final result of matrix multiplication.

This project provides an efficient way to multiply large sparse matrices using Map-Reduce, and it can be tested both in local mode and distributed mode on Expanse.

## Input and Output Format

### Input
- **Matrix M (M-matrix-small.txt / M-matrix-large.txt)**: This matrix contains the rows and columns in the format:
  ```
  i,k,m
  ```
  where `i` is the row index, `k` is the column index, and `m` is the matrix element.

- **Matrix N (N-matrix-small.txt / N-matrix-large.txt)**: This matrix contains the rows and columns in the format:
  ```
  k,j,n
  ```
  where `k` is the column index, `j` is the row index, and `n` is the matrix element.

### Output
- **Matrix Multiplication Result (solution-small.txt / solution-large.txt)**: After running the two Map-Reduce jobs, the output will be a matrix where each entry `(i, j)` is the sum of products of corresponding elements from matrices `M` and `N`.

## Project Steps

### 1. Map-Reduce Job 1: Matrix Product Calculation

#### Mapper for Matrix `M`
- **Input**: `i,k,m` (row index, column index, value in matrix `M`)
- **Output**: `(k, new Triple(0, i, m))`
  - Emit the key-value pair where `k` is the key and the value is a `Triple` containing:
    - `0` for matrix `M`
    - `i` (row index)
    - `m` (value at `(i,k)`)

#### Mapper for Matrix `N`
- **Input**: `k,j,n` (column index, row index, value in matrix `N`)
- **Output**: `(k, new Triple(1, j, n))`
  - Emit the key-value pair where `k` is the key and the value is a `Triple` containing:
    - `1` for matrix `N`
    - `j` (row index)
    - `n` (value at `(k,j)`)

#### Reducer
- **Input**: `(k, [Triple(0, i, m), Triple(1, j, n)])`
  - For each `k`, collect all values from matrices `M` and `N` that share the same column index `k`.
  - **Output**: `(new Pair(i, j), m * n)`
    - For each combination of values from `M` and `N`, multiply and emit the result with the pair `(i, j)` as the key.

### 2. Map-Reduce Job 2: Sum Products for Each Pair `(i, j)`

#### Mapper
- **Input**: `(Pair(i, j), value)`
  - Emit `(Pair(i, j), value)` as-is.

#### Reducer
- **Input**: `(Pair(i, j), [values])`
  - Sum all values associated with each `(i, j)` pair to get the final result for that matrix position.
  - **Output**: `(Pair(i, j), sum)`

## Running the Project

### On Your Laptop

1. **Download and Setup**: 
   Download the project, extract it, and navigate to the project directory:
   ```bash
   wget https://lambda.uta.edu/cse6332/project2.tgz
   tar xfz project2.tgz
   cd project2
   ```

2. **Compile the Project**: 
   Use Maven to compile the project:
   ```bash
   mvn install
   ```

3. **Run the Project in Local Mode**: 
   Test the project with small test matrices:
   ```bash
   rm -rf tmp output
   ~/hadoop-3.2.2/bin/hadoop jar target/*.jar Multiply M-matrix-small.txt N-matrix-small.txt tmp output
   ```

4. **View the Output**: 
   Check the output in the `output/part-r-00000` file to verify the results:
   ```bash
   cat output/part-r-00000
   ```

5. **Log the Output**: 
   Run the program with logging enabled:
   ```bash
   rm -rf tmp output
   ~/hadoop-3.2.2/bin/hadoop jar target/*.jar Multiply M-matrix-small.txt N-matrix-small.txt tmp output &> multiply.local.out
   ```

### On Expanse (Distributed Mode)

1. **Transfer the Files**: 
   Copy the project to Expanse if you developed it on your laptop.

2. **Extract and Set Permissions**: 
   On Expanse, extract the project files and set appropriate permissions:
   ```bash
   tar xfz project2.tgz
   chmod -R g-wrx,o-wrx project2
   ```

3. **Compile on Expanse**: 
   Compile the Java program on Expanse:
   ```bash
   run multiply.build
   ```

4. **Run the Program in Local Mode**: 
   Test the program with the small matrices on Expanse:
   ```bash
   sbatch multiply.local.run
   ```

5. **Run the Program in Distributed Mode**: 
   Once local mode works, run it in distributed mode:
   ```bash
   sbatch multiply.distr.run
   ```

6. **Check the Results**: 
   After running in distributed mode, check the log for the first and last lines of the result:
   ```bash
   cat multiply.distr.out
   ```

## Requirements

- **Java**: The project requires Java for Map-Reduce job implementation.
- **Hadoop**: Hadoop must be installed to run Map-Reduce jobs.
- **Maven**: Used for building the project.

## References

- **Map-Reduce and the New Software Stack**: Section 2.3.9 (page 38) for detailed explanation of matrix multiplication with Map-Reduce.

