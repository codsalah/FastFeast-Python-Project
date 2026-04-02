import pandas as pd
from loaders.base_scd2_loader import BaseSCD2Loader

class DimRestaurantLoader(BaseSCD2Loader):
    def __init__(self, batch_dir: str = "data/master"):
        self.batch_dir = batch_dir
    @property
    def table_name(self) -> str: return "warehouse.dim_restaurant"
    @property
    def natural_key(self) -> str: return "restaurant_id"
    @property
    def source_entity(self) -> str: return "source_restaurants"
    @property
    def tracked_fields(self) -> list[str]: 
        return ["restaurant_name", "category_name", "price_tier", "region_name", "city_name", "is_active"]

    def load(self, df, batch_date, source_file, pipeline_run_id=None):
        import os, json
        # Perform all lookups in one pass (to avoid multiple passes over the same data)
        cats = pd.read_csv(os.path.join(self.batch_dir, "categories.csv"))[["category_id", "category_name"]]
        regs = pd.read_csv(os.path.join(self.batch_dir, "regions.csv"))[["region_id", "region_name", "city_id"]]
        
        # Load cities from JSON
        with open(os.path.join(self.batch_dir, "cities.json")) as f:
            cities_data = json.load(f)
        cits = pd.DataFrame(cities_data)[["city_id", "city_name"]]
        
        geo = regs.merge(cits, on="city_id")
        df = df.merge(cats, on="category_id", how="left").merge(geo, on="region_id", how="left")
        return super().load(df, batch_date, source_file, pipeline_run_id)

    def _scd1_condition(self, incoming, active_row) -> bool:
        inc_rating = self._safe_float(incoming.get("rating_avg"))
        act_rating = self._safe_float(active_row.get("rating_avg"))
        return inc_rating != act_rating

    def _build_insert_row(self, record, batch_date):
        row = self._build_update_fields(record)
        row.update({
            "restaurant_id": record["restaurant_id"],
            "rating_avg": self._safe_float(record.get("rating_avg")),
            "valid_from": batch_date, "valid_to": None, "is_current": True
        })
        return row

    def _build_update_fields(self, record):
        return {
            "restaurant_name": record.get("restaurant_name"),
            "category_name": record.get("category_name"),
            "price_tier": record.get("price_tier"),
            "region_name": record.get("region_name"),
            "city_name": record.get("city_name"),
            "is_active": self._coerce_bool_like(record.get("is_active")),
            "rating_avg": self._safe_float(record.get("rating_avg"))
        }

    @staticmethod
    def _safe_float(v):
        try: return round(float(v), 2)
        except: return None