# MapReduce - Block Matrix Multiplication
The central aim of this project is, given a large matrix and a vector, we want to find their product using Generalized Iterative Matrix-Vector (or GIM-V) multiplication, a MapReduce algorithm.

This two-stage algorithm takes inputs as an edge file (matrix) and a node file (vector). Stage 1 combines the columns of the matrix with the rows of the vector and generates (key, value) pairs which are the input to Stage 2. Stage 2 combines all partial results from Stage 1 and assigns the new vector to the old vector.

The main idea is to group elements of the input matrix into blocks of size b x b and the elements of input vectors into blocks of length b. This block encoding forces nearby edges in the adjacency matrix to be closely located. After grouping, GIM-V is performed on blocks, not on individual elements.

The experiments conducted involve running over 30 iterations of the algorithm.
