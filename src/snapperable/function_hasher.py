"""Function hashing utilities for version detection."""

import hashlib
import inspect
from typing import Callable


class FunctionHasher:
    """
    Computes a version hash for functions to detect when their implementation changes.
    """
    
    @staticmethod
    def compute_hash(fn: Callable) -> str:
        """
        Compute a version hash for a function based on its source code or bytecode.
        This helps detect when the function implementation has changed.
        
        Args:
            fn: The function to compute a version for.
            
        Returns:
            A string hash representing the function version.
        """
        try:
            # Get the source code of the function
            source = inspect.getsource(fn)
            # Hash the source code
            return hashlib.sha256(source.encode()).hexdigest()
        except (OSError, TypeError):
            # If we can't get source (e.g., built-in function, lambda),
            # use the function's code object
            try:
                code = fn.__code__
                # Hash the bytecode
                return hashlib.sha256(code.co_code).hexdigest()
            except AttributeError:
                # If all else fails, return a constant hash
                # This means we won't detect changes, but at least we won't crash
                return "unknown"
