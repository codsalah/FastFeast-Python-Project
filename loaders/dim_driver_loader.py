import pandas as pd
from loaders.base_scd2_loader import BaseSCD2Loader

class DimDriverLoader(BaseSCD2Loader):
    def __init__(self, batch_dir: str = "data/input/batch"):
        self.batch_dir = batch_dir
    @property
    def table_name(self) -> str: return "warehouse.dim_driver"
    @property
    def natural_key(self) -> str: return "driver_id"
    @property
    def source_entity(self) -> str: return "source_drivers"
    @property
    def tracked_fields(self) -> list[str]: 
        return ["driver_name", "vehicle_type", "shift", "region_name", "city_name", "is_active"]

    def load(self, df, batch_date, source_file, pipeline_run_id=None):
        import os, json
        regs = pd.read_csv(os.path.join(self.batch_dir, "regions.csv"))[["region_id", "region_name", "city_id"]]
        
        # Load cities from JSON
        with open(os.path.join(self.batch_dir, "cities.json")) as f:
            cities_data = json.load(f)
        cits = pd.DataFrame(cities_data)[["city_id", "city_name"]]
        
        geo = regs.merge(cits, on="city_id")
        df = df.merge(geo, on="region_id", how="left")
        return super().load(df, batch_date, source_file, pipeline_run_id)

    def _build_insert_row(self, record, batch_date):
        return {**self._build_update_fields(record), "driver_id": record["driver_id"],
                "valid_from": batch_date, "valid_to": None, "is_current": True}

    def _build_update_fields(self, record):
        return {
            "driver_name": record.get("driver_name"),
            "vehicle_type": record.get("vehicle_type"),
            "shift": record.get("shift"),
            "region_name": record.get("region_name"),
            "city_name": record.get("city_name"),
            "is_active": self._coerce_bool_like(record.get("is_active"))
        }