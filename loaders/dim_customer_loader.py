from loaders.base_scd2_loader import BaseSCD2Loader

class DimCustomerLoader(BaseSCD2Loader):
    @property
    def table_name(self) -> str: return "warehouse.dim_customer"
    @property
    def natural_key(self) -> str: return "customer_id"
    @property
    def tracked_fields(self) -> list[str]: 
        return ["customer_name_masked", "gender", "segment_name", "region_name", "city_name"]

    def _build_insert_row(self, record, batch_date):
        return {**self._build_update_fields(record), "customer_id": record["customer_id"], 
                "signup_date": record.get("signup_date"), "valid_from": batch_date, 
                "valid_to": None, "is_current": True}

    def _build_update_fields(self, record):
        return {f: record.get(f) for f in self.tracked_fields}