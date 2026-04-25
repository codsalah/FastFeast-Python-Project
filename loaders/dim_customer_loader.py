import pandas as pd
from loaders.base_scd2_loader import BaseSCD2Loader
from utils import PII_handler

class DimCustomerLoader(BaseSCD2Loader):
    def __init__(self, batch_dir: str = "data/master"):
        self.batch_dir = batch_dir

    @property
    def table_name(self) -> str: return "warehouse.dim_customer"
    @property
    def natural_key(self) -> str: return "customer_id"
    @property
    def source_entity(self) -> str: return "source_customers"
    @property
    def tracked_fields(self) -> list[str]: 
        return ["customer_name_masked", "gender", "segment_name", "region_name", "city_name", "signup_date"]

    def load(self, df, batch_date, source_file, pipeline_run_id=None):
        import os, json
        # Join with lookups from batch directory
        segs = pd.read_csv(os.path.join(self.batch_dir, "segments.csv"))[["segment_id", "segment_name"]]
        regs = pd.read_csv(os.path.join(self.batch_dir, "regions.csv"))[["region_id", "region_name", "city_id"]]
        
        # Load cities from JSON
        with open(os.path.join(self.batch_dir, "cities.json")) as f:
            cities_data = json.load(f)
        cits = pd.DataFrame(cities_data)[["city_id", "city_name"]]
        
        geo = regs.merge(cits, on="city_id")
        df = df.merge(segs, on="segment_id", how="left").merge(geo, on="region_id", how="left")
        
        # PII Masking: full_name -> first letter + *** (e.g. A. ***)
        df["customer_name_masked"] = PII_handler.partial_masking(df["full_name"], keep_first=1)
        
        return super().load(df, batch_date, source_file, pipeline_run_id)

    def _build_insert_row(self, record, batch_date):
        return {**self._build_update_fields(record), "customer_id": record["customer_id"], 
                "valid_from": batch_date, "valid_to": None, "is_current": True}

    def _build_update_fields(self, record):
        return {f: record.get(f) for f in self.tracked_fields}