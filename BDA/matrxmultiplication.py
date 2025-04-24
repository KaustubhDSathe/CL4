from mrjob.job import MRJob

class MRMatrixMultiplicationSimple(MRJob):

    def mapper(self, _, line):
        # Extract matrix identifier, row, column, and value from the line
        matrix, i, j, value = line.split(',')
        i, j, value = int(i), int(j), float(value)

        if matrix == 'A':
            # Emit keys for multiplying A's row with B's column
            for k in range(1, 3):  # Assuming B has 2 columns
                yield (i, k), ('A', j, value)
        elif matrix == 'B':
            # Emit keys for multiplying B's column with A's row
            for k in range(1, 3):  # Assuming A has 2 rows
                yield (k, j), ('B', i, value)

    def reducer(self, key, values):
        # Split values by their matrix of origin
        A = {j: v for m, j, v in values if m == 'A'}
        B = {i: v for m, i, v in values if m == 'B'}
        # Compute the sum of products for the cell (key)
        result = sum(A[k] * B[k] for k in A if k in B)
        yield key, result

if __name__ == '__main__':
    MRMatrixMultiplicationSimple.run()
