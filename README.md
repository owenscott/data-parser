##To Export from OpenOffice
 - Open the database
 - SQL statements
 - SCRIPT "FILEPATH"
 - manually cut-and-paste just the select statements


##What are all of these scripts?!?

I barely know :/. But...

- cleanForAnjesh.js gets all of the data out of the repo and puts it in a usable json format
- extract-geo-data.js was made to get structured geographic data from CSVs of GADM locations
- app.js takes everything in the "data" folder and runs a full comparison/merge on it
- write-to-mongo.js writes the results of the comparison/merge to mongo (warning: hardcoded db name)
