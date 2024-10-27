import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.distributed import Client

# Generate dummy data (100 rows)
np.random.seed(0)
data = np.random.rand(100, 5)
columns = list('ABCDE')
df = pd.DataFrame(data, columns=columns)

# Create a Dask DataFrame from the pandas DataFrame
ddf = dd.from_pandas(df, npartitions=10)  # Adjust npartitions as per your system

# Advantages of using Dask for processing data in huge parallel:

# 1. **Parallel Processing:** Dask allows you to leverage multiple cores or even distributed clusters to process data parallelly, speeding up computations significantly.

# 2. **Lazy Evaluation:** Dask performs computations lazily, meaning operations are only executed when needed. This delayed execution approach optimizes memory usage and allows for more efficient processing.

# 3. **Handling Large Datasets:** Dask is designed to handle large datasets that cannot fit into memory of a single machine. It breaks down the data into smaller partitions, allowing processing in chunks.

# Example code to perform a simple aggregation (sum) on the Dask DataFrame:

if __name__ == "__main__":

	# Start a Dask client (local cluster for demonstration)
	client = Client()

	print("Computing sum using Dask:")
	result = ddf.sum().compute()
	print("Result:", result)

	# Stop the Dask client
	client.close()
