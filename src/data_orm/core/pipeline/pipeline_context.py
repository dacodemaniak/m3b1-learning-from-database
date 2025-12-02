from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import pandas as pd

@dataclass
class PipelineContext:
    """Shared context for passing data between processors in the chain"""
    
    # Input/output configuration
    input_file: Optional[str] = None
    output_file: Optional[str] = None
    
    # Processing configuration
    config: Dict[str, Any] = field(default_factory=dict)
    
    # Results and metrics from each processor
    results: Dict[str, Any] = field(default_factory=dict)
    
    # Statistics and reports
    statistics: Dict[str, Any] = field(default_factory=dict)
    
    # Error handling
    errors: List[str] = field(default_factory=list)
    
    def add_result(self, processor_name: str, result: Any):
        """Add result from a processor"""
        self.results[processor_name] = result
    
    def add_statistic(self, key: str, value: Any):
        """Add statistical information"""
        self.statistics[key] = value
    
    def add_error(self, error: str):
        """Add error message"""
        self.errors.append(error)

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from the context.

        Lookup order:
        1. `results` dict
        2. `statistics` dict
        3. `config` dict
        4. direct attribute on the context (e.g., `input_file`, `output_file`, `errors`)

        Supports dot-separated nested keys for dict-like values, e.g. "relevance_filter_report.final_columns".
        """
        # support nested keys like 'a.b.c'
        parts = key.split('.') if isinstance(key, str) else [key]

        def _lookup(container, parts_list):
            cur = container
            for p in parts_list:
                if isinstance(cur, dict) and p in cur:
                    cur = cur[p]
                else:
                    return None
            return cur

        # 1. results
        val = _lookup(self.results, parts)
        if val is not None:
            return val

        # 2. statistics
        val = _lookup(self.statistics, parts)
        if val is not None:
            return val

        # 3. config
        val = _lookup(self.config, parts)
        if val is not None:
            return val

        # 4. attributes
        # try top-level attribute (no nested parts)
        if len(parts) == 1:
            attr = parts[0]
            if hasattr(self, attr):
                return getattr(self, attr)

        return default