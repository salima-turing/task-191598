import numpy as np
import dask.array as da
from dask.distributed import Client

# Dummy data: Generate a large 10000x10000 NumPy array of random numbers
np.random.seed(0)
data = np.random.rand(10000, 10000)

# Convert the NumPy array to a Dask array
dask_data = da.from_array(data, chunks=(1000, 1000))

# Define a custom function to apply to each element in the Dask array
def custom_function(x):
   return np.sin(x) ** 2 + np.cos(x) ** 3

# Create a Dask delayed function to apply the custom function
custom_func_delayed = da.delayed(custom_function)

# Apply the custom function to each element in the Dask array using map_blocks
result = dask_data.map_blocks(custom_func_delayed, dtype=np.float64)

# Compute the result using Dask's distributed scheduler
if __name__ == "__main__":
   client = Client(n_workers=4)  # Number of workers can be adjusted based on your system's resources
   result = result.compute()

   # Display the result
   print(result.shape)
   print(result)
