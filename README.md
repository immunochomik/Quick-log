# Quick-log
Elasticsearch 5, Kibana 3 and Kibana 5 plus the secret sauce.

It will take a data file plus a bit of metadata and:

- start all the servers needed (inside of the container),
- create Elasticsearch mapping,
- insert the data into the Elasticsearch,
- prepare dashboard in Kibana 3. # coming soon
So you can just open a browser and look at the data in the Kibana of your choice.

It support csv and it will tsv and JSON logs (log where each line is a JSON object).

Start container with command:

```bash
sudo docker run -p 8080:80 -p 9200:9200 -p 5601:5601 -v <path to dir with the data>:/inserts -d -e LOG_LEVEL=20 immunoglobul/quick-log:latest
```

## Inserting data
Imagine that you want to insert a csv called:

__sales_2015.csv__

```csv
my_id,sold_at,product,price
3775,2017-04-20 11:15.01,Eggs,4.32
...
```

You will want to add meta information regarding the unique id field, therefore, you add a file with extension .meta and the same name as the data file:

!!! Warning the id field has to be lower case even if the real column name is not.

__sales_2015.meta__

that file will contain JSON as follows:

```json
{
    "id_fields":["my_id"]
}
```

And you will need to transform the data on the insert as the Elasticsearch does not support used date format YYYY-MM-DD HH:MI:SS, Elasticsearch needs a \T instead of the space, We can fix it with a transform function,

__sales_2015.py__

```python
def transform(row):
    for timeitem in ['sold_at']:
        row[timeitem] = row[timeitem].replace(' ', 'T')
   return row
```
So we end up with three files in the watched directory:

sales_2015.csv
sales_2015.meta
sales_2015.py

Once the container is started you can just add data to the watched directory it will insert it automatically.
