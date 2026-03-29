"""
FastFeast Data Loader

Loads dimension and fact data from CSV/JSON files into PostgreSQL DWH.
Handles column mapping, PII filtering, and foreign key dependencies.

----------------- JUST A DUMP LOADER TO TEST THE DWH  -----------------
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, List
from datetime import datetime

import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ---------------- Logging Setup ----------------
class ColorFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': "\033[94m",
        'INFO': "\033[92m",
        'WARNING': "\033[93m",
        'ERROR': "\033[91m",
        'CRITICAL': "\033[41m",
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        record.msg = f"{color}{record.msg}{self.RESET}"
        return super().format(record)


console_handler = logging.StreamHandler()
console_handler.setFormatter(ColorFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

file_handler = logging.FileHandler('data_loader.log')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)
logger.addHandler(file_handler)
# ------------------------------------------------

COLUMN_MAPPINGS = {
    'dim_cities': {'city_name': 'city_name', 'country': 'country', 'timezone': None},
    'dim_regions': {'region_name': 'region_name', 'city_id': 'city_id', 'delivery_base_fee': 'delivery_base_fee'},
    'dim_customers': {'customer_id': 'customer_id', 'segment_id': 'segment_id', 'account_created_at': 'account_created_at',
                      'is_active': 'is_active', 'full_name': None, 'email': None, 'phone': None},
    'dim_restaurants': {'restaurant_id': 'restaurant_id', 'category_id': 'category_id', 'region_id': 'region_id',
                        'rating_avg': 'rating_avg', 'prep_time_avg_min': 'prep_time_avg_min', 'is_active': 'is_active',
                        'restaurant_name': None},
    'dim_drivers': {'driver_id': 'driver_id', 'region_id': 'region_id', 'shift': 'shift', 'vehicle_type': 'vehicle_type',
                    'hire_date': 'hire_date', 'rating_avg': 'rating_avg', 'on_time_rate': 'on_time_rate',
                    'cancel_rate': 'cancel_rate', 'completed_deliveries': 'completed_deliveries', 'is_active': 'is_active',
                    'driver_name': None, 'driver_phone': None, 'national_id': None},
    'dim_agents': {'agent_id': 'agent_id', 'team_id': 'team_id', 'skill_level': 'skill_level', 'hire_date': 'hire_date',
                   'avg_handle_time_min': 'avg_handle_time_min', 'resolution_rate': 'resolution_rate', 'csat_score': 'csat_score',
                   'is_active': 'is_active', 'agent_name': None, 'agent_email': None, 'agent_phone': None},
}

SCHEMA_COLUMNS = {
    'dim_cities': ['city_id', 'city_name', 'country', 'created_at', 'updated_at'],
    'dim_regions': ['region_id', 'region_name', 'city_id', 'delivery_base_fee', 'created_at', 'updated_at'],
    'dim_customers': ['customer_id', 'segment_id', 'account_created_at', 'is_active', 'created_at', 'updated_at'],
    'dim_restaurants': ['restaurant_id', 'category_id', 'region_id', 'rating_avg', 'prep_time_avg_min', 'is_active', 
                        'created_at', 'updated_at'],
    'dim_drivers': ['driver_id', 'region_id', 'shift', 'vehicle_type', 'hire_date', 'rating_avg', 'on_time_rate', 
                    'cancel_rate', 'completed_deliveries', 'is_active', 'created_at', 'updated_at'],
    'dim_agents': ['agent_id', 'team_id', 'skill_level', 'hire_date', 'avg_handle_time_min', 'resolution_rate', 
                    'csat_score', 'is_active', 'created_at', 'updated_at'],
    'dim_segments': ['segment_id', 'segment_name', 'discount_pct', 'priority_support', 'created_at', 'updated_at'],
    'dim_categories': ['category_id', 'category_name', 'created_at', 'updated_at'],
    'dim_teams': ['team_id', 'team_name', 'created_at', 'updated_at'],
    'dim_reason_categories': ['reason_category_id', 'category_name', 'created_at', 'updated_at'],
    'dim_reasons': ['reason_id', 'reason_name', 'reason_category_id', 'severity_level', 'typical_refund_pct', 
                    'created_at', 'updated_at'],
    'dim_channels': ['channel_id', 'channel_name', 'created_at', 'updated_at'],
    'dim_priorities': ['priority_id', 'priority_code', 'priority_name', 'sla_first_response_min', 'sla_resolution_min', 
                        'created_at', 'updated_at'],
}


def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', 5432)),
            database=os.getenv('POSTGRES_DB', 'fastfeast_db'),
            user=os.getenv('POSTGRES_USER', 'fastfeast'),
            password=os.getenv('POSTGRES_PASSWORD', 'fastfeast_pass')
        )
        logger.info("Connected to PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        sys.exit(1)


class DataLoader:

    def __init__(self, data_root: str = "data"):
        self.data_root = Path(data_root)
        self.master_dir = self.data_root / "master"
        self.batch_dir = self.data_root / "input" / "batch"
        self.stream_dir = self.data_root / "input" / "stream"
        self.conn = get_db_connection()
        self.cursor = self.conn.cursor()

        logger.info(f"Data root: {self.data_root}")
        logger.info(f"Master dir: {self.master_dir}")
        logger.info(f"Batch dir: {self.batch_dir}")

    def filter_and_map_columns(self, df: pd.DataFrame, table: str) -> pd.DataFrame:
        mapping = COLUMN_MAPPINGS.get(table, {})
        rename_dict = {csv_col: schema_col for csv_col, schema_col in mapping.items() if csv_col in df.columns and schema_col is not None}
        df = df.rename(columns=rename_dict)
        schema_cols = SCHEMA_COLUMNS.get(table, [])
        keep_cols = [col for col in schema_cols if col not in ['created_at', 'updated_at']]
        df = df[[col for col in keep_cols if col in df.columns]]
        return df

    def load_csv(self, filename: str) -> pd.DataFrame:
        filepath = self.master_dir / filename
        if not filepath.exists():
            logger.warning(f"File not found: {filepath}")
            return pd.DataFrame()
        try:
            df = pd.read_csv(filepath)
            logger.info(f"Loaded {len(df)} rows from {filename}")
            return df
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            return pd.DataFrame()

    def upsert_dimension(self, table: str, df: pd.DataFrame, natural_key: str):
        if df.empty:
            logger.warning(f"Empty DataFrame for {table}")
            return
        try:
            df = self.filter_and_map_columns(df, table)
            if df.empty:
                logger.warning(f"No valid columns for {table}")
                return

            df = df.applymap(lambda x: int(x) if pd.api.types.is_integer_dtype(type(x)) else x)
            df = df.astype(object).where(pd.notnull(df), None)
            rows = [tuple(row) for row in df.values]
            columns = df.columns.tolist()

            placeholders = ','.join(['%s'] * len(columns))
            col_names = ','.join(columns)
            set_clause = ','.join([f"{col}=EXCLUDED.{col}" for col in columns if col != natural_key]) + ",updated_at=NOW()"

            query = f"""
            INSERT INTO warehouse.{table} ({col_names})
            VALUES ({placeholders})
            ON CONFLICT ({natural_key}) DO UPDATE SET {set_clause}
            """
            self.cursor.executemany(query, rows)
            self.conn.commit()
            logger.info(f"Upserted {len(rows)} rows into warehouse.{table}")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error upserting {table}: {e}")

    def load_all_dimensions(self):
        logger.info("=" * 60)
        logger.info("LOADING DIMENSIONS")
        logger.info("=" * 60)

        dimensions = {
            'dim_cities': ('cities.csv', 'city_id'),
            'dim_regions': ('regions.csv', 'region_id'),
            'dim_segments': ('segments.csv', 'segment_id'),
            'dim_categories': ('categories.csv', 'category_id'),
            'dim_teams': ('teams.csv', 'team_id'),
            'dim_reason_categories': ('reason_categories.csv', 'reason_category_id'),
            'dim_reasons': ('reasons.csv', 'reason_id'),
            'dim_channels': ('channels.csv', 'channel_id'),
            'dim_priorities': ('priorities.csv', 'priority_id'),
        }

        for table, (filename, natural_key) in dimensions.items():
            df = self.load_csv(filename)
            if not df.empty:
                self.upsert_dimension(table, df, natural_key)

        logger.info("=" * 60)
        logger.info("DIMENSIONS LOADED")
        logger.info("=" * 60)

    def load_all_entities(self):
        logger.info("=" * 60)
        logger.info("LOADING ENTITY DIMENSIONS")
        logger.info("=" * 60)

        entities = {
            'dim_customers': ('customers.csv', 'customer_id'),
            'dim_restaurants': ('restaurants.csv', 'restaurant_id'),
            'dim_drivers': ('drivers.csv', 'driver_id'),
            'dim_agents': ('agents.csv', 'agent_id'),
        }

        for table, (filename, natural_key) in entities.items():
            df = self.load_csv(filename)
            if not df.empty:
                self.upsert_dimension(table, df, natural_key)

        logger.info("=" * 60)
        logger.info("ENTITY DIMENSIONS LOADED")
        logger.info("=" * 60)

    def verify_loads(self):
        logger.info("=" * 60)
        logger.info("VERIFYING DATA")
        logger.info("=" * 60)

        tables = [
            'dim_cities', 'dim_regions', 'dim_segments', 'dim_categories',
            'dim_teams', 'dim_reason_categories', 'dim_reasons', 'dim_channels',
            'dim_priorities', 'dim_customers', 'dim_restaurants', 'dim_drivers', 'dim_agents'
        ]

        for table in tables:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM warehouse.{table}")
                count = self.cursor.fetchone()[0]
                status = "SUCCESS" if count > 0 else "WARNING"
                logger.info(f"{status}: warehouse.{table} — {count} rows")
            except Exception as e:
                logger.error(f"Error counting {table}: {e}")

        logger.info("=" * 60)

    def run(self):
        logger.info("\n" + "=" * 60)
        logger.info("FASTFEAST DATA LOADER - STARTING")
        logger.info("=" * 60 + "\n")

        try:
            self.load_all_dimensions()
            self.load_all_entities()
            self.verify_loads()
            logger.info("\n" + "=" * 60)
            logger.info("DATA LOAD COMPLETE")
            logger.info("=" * 60 + "\n")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            sys.exit(1)
        finally:
            self.cursor.close()
            self.conn.close()


if __name__ == '__main__':
    loader = DataLoader(data_root="data")
    loader.run()