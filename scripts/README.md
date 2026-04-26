# Scripts Directory

## Definition

The `scripts/` directory contains utility scripts for database setup, maintenance, and administrative tasks. Currently, this directory is minimal and primarily reserved for future expansion.

## What It Does

The scripts directory is intended to provide:

- **Database Setup**: SQL scripts for database initialization
- **Maintenance Scripts**: Scripts for database maintenance and cleanup
- **Administrative Tasks**: Utility scripts for common administrative operations
- **Migration Scripts**: Database schema migration scripts

## Why It Exists

The scripts directory exists to:

- **Separate Concerns**: Keep administrative scripts separate from application code
- **Reusability**: Provide reusable scripts for common tasks
- **Version Control**: Track database schema changes in version control
- **Documentation**: Document database setup and maintenance procedures

## Current State

Currently, the `scripts/` directory contains:
- `setup_db.sql/`: An empty directory reserved for future database setup scripts

Note: Database DDL (Data Definition Language) scripts are currently located in the `warehouse/` directory:
- `warehouse/dwh_ddl.sql`: Data warehouse schema
- `warehouse/audit_ddl.sql`: Audit schema
- `warehouse/analytics_ddl.sql`: Analytics views
- `warehouse/seed.sql`: Seed data

## Relationship with Architecture

### Position in Data Flow
```
┌─────────────────┐
│  scripts/       │
│  (Utility       │
│   Scripts)      │
└────────┬────────┘
         │
         │ (Manual execution)
         ▼
┌─────────────────┐
│  Database       │
│  (PostgreSQL)   │
└─────────────────┘
```

### Dependencies
- **warehouse/**: Currently contains the DDL scripts that could be moved here
- **PostgreSQL**: Target database for script execution

### Used By
- **Database Administrators**: For database setup and maintenance
- **DevOps Engineers**: For deployment and configuration
- **Developers**: For local development setup

### Integration Points
1. **Database Initialization**: Scripts can be used to initialize database schema
2. **Maintenance**: Scripts for routine maintenance tasks
3. **Migration**: Scripts for schema versioning and migration
4. **Backup/Restore**: Scripts for backup and restore operations

## Potential Future Scripts

### Database Setup
- `init_database.sql`: Complete database initialization
- `create_schemas.sql`: Schema creation scripts
- `create_users.sql`: Database user and role creation
- `grant_permissions.sql`: Permission grants

### Maintenance
- `vacuum_analyze.sql`: Database vacuum and analyze
- `reindex.sql`: Reindex scripts
- `cleanup_old_data.sql`: Data cleanup scripts
- `archive_data.sql`: Data archival scripts

### Monitoring
- `health_check.sql`: Database health check queries
- `performance_stats.sql`: Performance statistics
- `space_usage.sql`: Disk space usage queries
- `connection_stats.sql`: Connection statistics

### Backup/Restore
- `backup_schema.sql`: Schema backup script
- `restore_schema.sql`: Schema restore script
- `backup_data.sql`: Data backup script
- `restore_data.sql`: Data restore script

## Running Scripts

Scripts in this directory can be executed using:
```bash
# Using psql
psql -h localhost -U fastfeast -d fastfeast_db -f scripts/script_name.sql

# Using docker exec
docker exec -i fastfeast_postgres psql -U fastfeast -d fastfeast_db < scripts/script_name.sql
```

## Best Practices

### Script Organization
- Use descriptive filenames
- Include comments explaining purpose
- Add version information to migration scripts
- Use transaction blocks for atomic operations

### Script Safety
- Test scripts in development first
- Use BEGIN/COMMIT for transaction safety
- Include ROLLBACK on error handling
- Backup before running destructive scripts

### Documentation
- Add header comments to each script
- Document dependencies between scripts
- Include execution order if multiple scripts
- Note any prerequisites or configuration needed

## Migration from warehouse/

Consider moving DDL scripts from `warehouse/` to `scripts/`:
- `warehouse/dwh_ddl.sql` → `scripts/ddl/dwh_ddl.sql`
- `warehouse/audit_ddl.sql` → `scripts/ddl/audit_ddl.sql`
- `warehouse/analytics_ddl.sql` → `scripts/ddl/analytics_ddl.sql`
- `warehouse/seed.sql` → `scripts/data/seed.sql`

This would provide better separation of concerns and make the scripts directory the single source of truth for database schema.

## Security Considerations

- **Access Control**: Limit access to scripts directory
- **Credentials**: Avoid hardcoding credentials in scripts
- **Permissions**: Ensure scripts have appropriate file permissions
- **Audit Logging**: Log script execution for audit trail

## Configuration

Scripts may need configuration:
- Database connection parameters
- Schema names
- Table names
- File paths for data imports/exports

Configuration can be handled via:
- Environment variables
- Configuration files
- Command-line arguments
- SQL variables

## Testing

Scripts should be tested:
- In development environment first
- With test data before production
- For idempotency (can be run multiple times safely)
- For rollback capability

## Extending Scripts Directory

### Adding New Scripts
1. Create script file with descriptive name
2. Add header comments with purpose and usage
3. Include error handling and transactions
4. Test in development environment
5. Document in this README

### Script Template
```sql
-- Script Name: descriptive_name.sql
-- Purpose: Brief description of what the script does
-- Author: Your name
-- Date: YYYY-MM-DD
-- Version: 1.0
-- Dependencies: List of other scripts this depends on

BEGIN;

-- Your SQL statements here

COMMIT;

-- ROLLBACK on error handling
```
