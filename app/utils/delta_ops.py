from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import polars as pl
from deltalake import DeltaTable, write_deltalake

from ..core.config import get_settings

class DeltaTableError(Exception):
    
    pass

class DeltaOps:
    
    
    def __init__(self):
        
        self.settings = get_settings()
    
    def _get_table_path(self, layer: str, table_name: str) -> Path:
        
        layer_paths = {
            "bronze": self.settings.BRONZE_PATH,
            "silver": self.settings.SILVER_PATH,
            "gold": self.settings.GOLD_PATH,
        }
        
        if layer not in layer_paths:
            raise DeltaTableError(f"Invalid layer: {layer}. Must be one of {list(layer_paths.keys())}")
        
        return layer_paths[layer] / table_name
    
    def table_exists(self, layer: str, table_name: str) -> bool:
        
        table_path = self._get_table_path(layer, table_name)
        
        delta_log = table_path / "_delta_log"
        return delta_log.exists() and delta_log.is_dir()
    
    def write_to_delta(
        self,
        df: pl.DataFrame,
        layer: str,
        table_name: str,
        mode: str = "append",
        partition_by: Optional[list[str]] = None
    ) -> int:
        
        if df.is_empty():
            return 0
        
        table_path = self._get_table_path(layer, table_name)
        
        table_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            arrow_table = df.to_arrow()
            
            write_deltalake(
                str(table_path),
                arrow_table,
                mode=mode,
                partition_by=partition_by
            )
            
            return len(df)
            
        except Exception as e:
            raise DeltaTableError(f"Failed to write to Delta table {table_name}: {e}")
    
    def read_delta_as_polars(
        self,
        layer: str,
        table_name: str,
        columns: Optional[list[str]] = None,
        filter_expr: Optional[str] = None
    ) -> pl.DataFrame:
        
        table_path = self._get_table_path(layer, table_name)
        
        if not self.table_exists(layer, table_name):
            return pl.DataFrame()
        
        try:
            df = pl.read_delta(str(table_path), columns=columns)
            
            if filter_expr:
                df = df.filter(pl.Expr.deserialize(filter_expr))
            
            return df
            
        except Exception as e:
            raise DeltaTableError(f"Failed to read Delta table {table_name}: {e}")
    
    def upsert_delta(
        self,
        df: pl.DataFrame,
        layer: str,
        table_name: str,
        merge_keys: list[str]
    ) -> tuple[int, int]:
        
        if df.is_empty():
            return 0, 0
        
        table_path = self._get_table_path(layer, table_name)
        
        if not self.table_exists(layer, table_name):
            rows_written = self.write_to_delta(df, layer, table_name, mode="overwrite")
            return rows_written, 0
        
        try:
            existing_df = self.read_delta_as_polars(layer, table_name)
            
            def create_key(frame: pl.DataFrame) -> pl.DataFrame:
                key_expr = pl.concat_str([pl.col(k).cast(pl.Utf8) for k in merge_keys], separator="_")
                return frame.with_columns(key_expr.alias("_merge_key"))
            
            existing_with_key = create_key(existing_df)
            new_with_key = create_key(df)
            
            existing_keys = set(existing_with_key["_merge_key"].to_list())
            
            new_rows = new_with_key.filter(~pl.col("_merge_key").is_in(list(existing_keys)))
            update_rows = new_with_key.filter(pl.col("_merge_key").is_in(list(existing_keys)))
            
            keys_to_update = set(update_rows["_merge_key"].to_list())
            unchanged_rows = existing_with_key.filter(~pl.col("_merge_key").is_in(list(keys_to_update)))
            
            final_df = pl.concat([
                unchanged_rows.drop("_merge_key"),
                update_rows.drop("_merge_key"),
                new_rows.drop("_merge_key")
            ])
            
            self.write_to_delta(final_df, layer, table_name, mode="overwrite")
            
            return len(new_rows), len(update_rows)
            
        except Exception as e:
            raise DeltaTableError(f"Failed to upsert Delta table {table_name}: {e}")
    
    def get_table_metadata(self, layer: str, table_name: str) -> dict:
        
        table_path = self._get_table_path(layer, table_name)
        
        if not self.table_exists(layer, table_name):
            return {
                "exists": False,
                "row_count": 0,
                "last_modified": None,
                "version": 0,
                "size_bytes": 0,
            }
        
        try:
            dt = DeltaTable(str(table_path))
            
            df = pl.read_delta(str(table_path))
            row_count = len(df)
            
            metadata = dt.metadata()
            
            size_bytes = sum(
                f.stat().st_size
                for f in table_path.rglob("*.parquet")
                if f.is_file()
            )
            
            delta_log = table_path / "_delta_log"
            latest_log = max(delta_log.glob("*.json"), key=lambda p: p.stat().st_mtime, default=None)
            last_modified = datetime.fromtimestamp(latest_log.stat().st_mtime) if latest_log else None
            
            return {
                "exists": True,
                "row_count": row_count,
                "last_modified": last_modified,
                "version": dt.version(),
                "size_bytes": size_bytes,
                "description": metadata.description if metadata else None,
            }
            
        except Exception as e:
            return {
                "exists": True,
                "row_count": 0,
                "last_modified": None,
                "version": 0,
                "size_bytes": 0,
                "error": str(e),
            }
    
    def list_tables(self, layer: str) -> list[str]:
        
        layer_path = self._get_table_path(layer, "").parent
        
        if not layer_path.exists():
            return []
        
        tables = []
        for item in layer_path.iterdir():
            if item.is_dir() and (item / "_delta_log").exists():
                tables.append(item.name)
        
        return sorted(tables)
    
    def delete_table(self, layer: str, table_name: str) -> bool:
        
        import shutil
        
        table_path = self._get_table_path(layer, table_name)
        
        if not table_path.exists():
            return False
        
        try:
            shutil.rmtree(table_path)
            return True
        except Exception as e:
            raise DeltaTableError(f"Failed to delete table {table_name}: {e}")

_delta_ops: Optional[DeltaOps] = None

def get_delta_ops() -> DeltaOps:
    
    global _delta_ops
    if _delta_ops is None:
        _delta_ops = DeltaOps()
    return _delta_ops
