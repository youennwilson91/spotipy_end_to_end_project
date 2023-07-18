# spotipy_end_to_end_project

This project aims to visualise my own spotify data. It uses the spotify api through the spotipy module to get my top artist, top songs and most recent song streamed. I also get info on the related artist of the artists I listen to.

All this data is stored into a local postgresql database. I use Azure function to schedule a job execution 20 minutes. Thus, the database will be refreshed every 20 minutes.

Finally, I use Power BI to connect to the postgresql database and make visualisations. The report will be uploaded on Power BI Service, and refreshed every 20 minutes also. I did not design the report yet, but I will soon.

You'll find the whole code in the two following files:
- main_sqlalchemy.py which is the script that feeds the database.
- sql_tables.py which creates the required tables in the database.

Don't hesitate to give any feedback or recommendations.

Thank you for reading.
