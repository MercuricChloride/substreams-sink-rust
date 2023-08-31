#!/usr/bin/bash
# Drop all tables from a PostgreSQL database

read -p "Database username: " username
read -p "Database name: " dbname
read -s -p "Database password: " dbpassword

export PGPASSWORD=$dbpassword

# Generate the SQL statements to drop the tables
drop_sql=$(psql -h localhost -U $username -d $dbname -t -c "SELECT 'DROP TABLE IF EXISTS \"' || tablename || '\" CASCADE;' FROM pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")

# Execute the SQL statements
IFS=$'\n'
for sql in $drop_sql; do
  psql -h localhost -U $username -d $dbname -c "$sql"
done
