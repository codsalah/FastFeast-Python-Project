import pandas as pd
from loaders.base_scd2_loader import BaseSCD2Loader

class DimCustomerLoader(BaseSCD2Loader):
    @property
    def table_name(self) -> str: return "warehouse.dim_customer"
    @property
    def natural_key(self) -> str: return "customer_id"
    @property
    def tracked_fields(self) -> list[str]: 
        return ["customer_name_masked", "gender", "segment_name", "region_name", "city_name", "signup_date"]

    def load(self, df, batch_date, source_file):
        # Join with lookups
        segs = pd.read_csv("data/master/segments.csv")[["segment_id", "segment_name"]]
        regs = pd.read_csv("data/master/regions.csv")[["region_id", "region_name", "city_id"]]
        cits = pd.read_csv("data/master/cities.csv")[["city_id", "city_name"]]
        
        geo = regs.merge(cits, on="city_id")
        df = df.merge(segs, on="segment_id", how="left").merge(geo, on="region_id", how="left")
        
        # (FOR NOW ONLY and will be updated)
        # PII Masking: full_name -> first letter + *** (e.g. A. ***)
        def mask_name(name):
            if not name or pd.isna(name): return None
            return f"{str(name)[0]}. ***"
        
        df["customer_name_masked"] = df["full_name"].apply(mask_name)
        
        return super().load(df, batch_date, source_file)

    def _build_insert_row(self, record, batch_date):
        return {**self._build_update_fields(record), "customer_id": record["customer_id"], 
                "valid_from": batch_date, "valid_to": None, "is_current": True}

    def _build_update_fields(self, record):
        return {f: record.get(f) for f in self.tracked_fields}