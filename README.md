# Data Leech :)

### Why ?

Redshift is a good way to analysis big informations :)

AWS RedShift it's awesome :) But and now ? If I already have a traditional relational data warehouse ?

OK.. don't wory... Leech  will help you to migrate from traditional database to RedShift :)

### Important.
Remember: Leech is not a ELT tool!! To this purpose, i suggest use Pentaho PDI or Luiggi (Spotify).
Leech will help you to make a first data load from your big database to empity RedShift (One shot)

### Features
- Create automatically table on RedShift based on source table.
- Export table from source to Bucket S3 with Manifest json file
- Connect on Redshift and run the COPY command for load from S3


### Pre-requiste:
- Python3  with aditionals packs: smart_open, boto, psycopg2 and sqlalchemy (use the pip command to install)
For mysql datasource you need include the mysqldb

### RoadMap
- Work with Python log