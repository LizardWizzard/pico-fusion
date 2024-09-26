
Run picodata:

`target/release/picodata run -i --listen 127.0.0.1:3301 --pg-listen 127.0.0.1:5432`


Switch to sql:

`\s l sql`

Then:
```sql
CREATE USER "alice" WITH PASSWORD 'Admin1234' USING md5
GRANT READ TABLE TO "alice"
GRANT CREATE TABLE TO "alice"
```

pg connection string:
`postgres://alice:Admin1234@127.0.0.1:5432`

```bash
psql "postgres://alice:Admin1234@127.0.0.1:5432" -f data/picodata_seed.sql
```