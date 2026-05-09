import torch

def get_device(use_gpu: bool) -> str:
    """Return 'cuda' if use_gpu and CUDA is available, else 'cpu'."""
    if use_gpu and torch.cuda.is_available():
        return "cuda"
    return "cpu"