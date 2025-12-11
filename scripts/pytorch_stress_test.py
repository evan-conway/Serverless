import torch
import time
import sys

duration_seconds = 10

def pytorch_gpu_stress_test():
    if not torch.cuda.is_available():
        print("CUDA not available. PyTorch GPU stress test requires a GPU.")
        return

    device = torch.device("cuda")
    print(f"Using GPU: {torch.cuda.get_device_name(0)}")

    # Create two large random matrices on the GPU
    matrix_size = (4096, 4096) # Adjust size as needed to stress GPU
    try:
        a = torch.randn(matrix_size, device=device)
        b = torch.randn(matrix_size, device=device)
    except RuntimeError as e:
        print(f"Error creating tensors on GPU: {e}")
        print("Try reducing matrix_size or check GPU memory.")
        return

    start_time = time.time()
    iterations = 0

    print(f"Starting PyTorch GPU stress test for {duration_seconds} seconds...")

    while (time.time() - start_time) < duration_seconds:
        # Perform matrix multiplication
        c = torch.matmul(a, b)
        # Ensure the operation completes (optional, but good for stress)
        torch.cuda.synchronize()
        iterations += 1
        if iterations % 100 == 0:
            print(f"Iteration {iterations} completed. Elapsed time: {time.time() - start_time:.2f}s")

    end_time = time.time()
    print(f"PyTorch GPU stress test finished.")
    print(f"Total iterations: {iterations}")
    print(f"Total duration: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    pytorch_gpu_stress_test()
