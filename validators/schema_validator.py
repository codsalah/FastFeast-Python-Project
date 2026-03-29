"""
Multi-Stage Schema Validator using Vectorized Pandas Operations.

Execution Flow:
1. Structural: Check for missing required columns.
2. Stage 1: Critical Checks (Nullability & Type parity).
3. Stage 2: Logical Checks (Regex, Ranges, Categorical constraints).

Decisions:
- Collects ALL failures per record (no early exit).
- Performance optimized for high-volume batches.
- Supports audit schema validation.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from .schema_registry import SchemaContract, ColumnContract

@dataclass
class ValidationError:
    """Represents a single validation failure."""
    row_index: int
    field: str
    value: Any
    reason: str
    level: str = "critical"  # critical | logical


class SchemaValidator:
    """Handles multi-stage data validation against SchemaContracts."""

    def __init__(self, contract: SchemaContract):
        self.contract = contract

    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        """
        Executes multi-stage validation:
        1. Structural: Missing required columns.
        2. Stage 1: Critical (Nullability & Type parity).
        3. Stage 2: Logical (Regex, Ranges, Categorical constraints).
        """
        if df.empty:
            return []

        errors: List[ValidationError] = []

        # --------------------------- Stage 1: Structural Integrity ---------------------------
        missing_cols = set(self.contract.required_columns()) - set(df.columns)
        if missing_cols:
            for col in missing_cols:
                errors.append(ValidationError(-1, col, None, "Structural: Missing required column"))

        # ------------------------- Stage 2 & 3: Field-Level Validations ------------------------
        for col_contract in self.contract.columns:
            if col_contract.name not in df.columns:
                continue

            series = df[col_contract.name]

            # --- Stage 2: Critical Checks (Nullability & Type) ---
            
            # 1. Nullability
            if not col_contract.nullable:
                null_mask = series.isna()
                if null_mask.any():
                    self._collect_errors(errors, df, col_contract.name, null_mask, "Critical: Null value not allowed", level="critical")

            # 2. Type Parity
            type_mask = self._check_type(series, col_contract.dtype)
            if type_mask is not None and type_mask.any():
                self._collect_errors(errors, df, col_contract.name, type_mask, f"Critical: Type mismatch (expected {col_contract.dtype})", level="critical")

            # --- Stage 3: Logical Checks (Regex, Ranges, Categorical) ---
            
            # We ONLY perform logical checks on rows that passed Stage 2 (not null & correct type)
            valid_for_logical = series.notna()
            if type_mask is not None:
                valid_for_logical &= ~type_mask
                
            if not valid_for_logical.any():
                continue

            # 3. Regex Pattern Check
            if col_contract.regex:
                regex_mask = ~series.astype(str).str.match(col_contract.regex, na=False) & valid_for_logical
                if regex_mask.any():
                    reason = f"Logical: Regex mismatch (Pattern: {col_contract.regex})"
                    self._collect_errors(errors, df, col_contract.name, regex_mask, reason, level="logical")

            # 4. Numerical Range Check
            if col_contract.min_value is not None or col_contract.max_value is not None:
                # We know these are numeric because they passed valid_for_logical type parity
                numeric_series = pd.to_numeric(series, errors='coerce')
                range_mask = pd.Series(False, index=df.index)
                
                if col_contract.min_value is not None:
                    range_mask |= (numeric_series < col_contract.min_value)
                if col_contract.max_value is not None:
                    range_mask |= (numeric_series > col_contract.max_value)
                
                range_mask &= valid_for_logical
                if range_mask.any():
                    msg = []
                    if col_contract.min_value is not None: msg.append(f"min: {col_contract.min_value}")
                    if col_contract.max_value is not None: msg.append(f"max: {col_contract.max_value}")
                    reason = f"Logical: Range violation ({', '.join(msg)})"
                    self._collect_errors(errors, df, col_contract.name, range_mask, reason, level="logical")

            # 5. Categorical Check (Allowed Values)
            if col_contract.allowed_values:
                cat_mask = ~series.astype(str).isin(list(col_contract.allowed_values)) & valid_for_logical
                if cat_mask.any():
                    reason = f"Logical: Categorical violation (Expected one of: {col_contract.allowed_values})"
                    self._collect_errors(errors, df, col_contract.name, cat_mask, reason, level="logical")

        return errors

    def _collect_errors(self, 
                        errors: List[ValidationError], 
                        df: pd.DataFrame, 
                        field: str, 
                        mask: pd.Series, 
                        reason: str, 
                        level: str = "critical"):
        """Extracts failing records and appends to the error collection."""
        failed_indices = df.index[mask].tolist()
        for idx in failed_indices:
            errors.append(ValidationError(
                row_index=idx,
                field=field,
                value=df.at[idx, field],
                reason=reason,
                level=level
            ))

    def _check_type(self, series: pd.Series, expected_dtype: str) -> Optional[pd.Series]:
        """Performs vectorized type checking. Returns a mask of FAILURES."""
        if series.isna().all():
            return None
            
        non_null_mask = series.notna()

        if expected_dtype == "int" or expected_dtype == "bigint":
            # Attempt numeric conversion and check for float-ness
            num_series = pd.to_numeric(series, errors='coerce')
            # Fail if conversion failed (NaN) OR if it has decimals (num != floor(num))
            return (num_series.isna() | (num_series % 1 != 0)) & non_null_mask
            
        elif expected_dtype == "float" or expected_dtype == "numeric":
            return pd.to_numeric(series, errors='coerce').isna() & non_null_mask
            
        elif expected_dtype == "bool":
            if series.dtype == 'bool':
                return None
            # Standardize bool representations
            allowed_bools = {True, False, 'True', 'False', '1', '0', 1, 0, 1.0, 0.0}
            return ~series.isin(allowed_bools) & non_null_mask
            
        elif expected_dtype == "datetime" or expected_dtype == "date":
            # Attempt vectorized parsing
            return pd.to_datetime(series, errors='coerce').isna() & non_null_mask
        
        return None

def validate_entity(df: pd.DataFrame, entity_name: str) -> List[ValidationError]:
    """Helper function to validate a specific entity."""
    from .schema_registry import get_contract
    contract = get_contract(entity_name)
    validator = SchemaValidator(contract)
    return validator.validate(df)
