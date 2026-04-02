import pandas as pd
import os
from loaders.base_scd2_loader import BaseSCD2Loader

class DimAgentLoader(BaseSCD2Loader):
    def __init__(self, batch_dir: str = "data/master"):
        self.batch_dir = batch_dir
    @property
    def table_name(self) -> str: return "warehouse.dim_agent"
    @property
    def natural_key(self) -> str: return "agent_id"
    @property
    def source_entity(self) -> str: return "source_agents"
    @property
    def tracked_fields(self) -> list[str]: 
        return ["skill_level", "team_name", "is_active"]

    def load(self, df, batch_date, source_file, pipeline_run_id=None):
        teams = pd.read_csv(os.path.join(self.batch_dir, "teams.csv"))[["team_id", "team_name"]]
        df = df.merge(teams, on="team_id", how="left")
        return super().load(df, batch_date, source_file, pipeline_run_id)

    def _build_insert_row(self, record, batch_date):
        return {**self._build_update_fields(record), "agent_id": record["agent_id"], 
                "valid_from": batch_date, "valid_to": None, "is_current": True}

    def _build_update_fields(self, record):
        return {
            "agent_name": record.get("agent_name"),
            "skill_level": record.get("skill_level"),
            "team_name": record.get("team_name"),
            "is_active": self._coerce_bool_like(record.get("is_active")),
        }