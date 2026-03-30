import pandas as pd
import os
from loaders.base_scd2_loader import BaseSCD2Loader

class DimAgentLoader(BaseSCD2Loader):
    @property
    def table_name(self) -> str: return "warehouse.dim_agent"
    @property
    def natural_key(self) -> str: return "agent_id"
    @property
    def tracked_fields(self) -> list[str]: return ["agent_name", "skill_level", "team_name"]

    def load(self, df, batch_date, source_file):
        teams = pd.read_csv("data/master/teams.csv")
        df = df.merge(teams[["team_id", "team_name"]], on="team_id", how="left")
        return super().load(df, batch_date, source_file)

    def _build_insert_row(self, record, batch_date):
        return {**self._build_update_fields(record), "agent_id": record["agent_id"], 
                "valid_from": batch_date, "valid_to": None, "is_current": True}

    def _build_update_fields(self, record):
        return {f: record.get(f) for f in self.tracked_fields}