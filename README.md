# spotipy_end_to_end_project

This project aims to visualise my own spotify data. It uses the spotify api through the spotipy module to get my top artist, top songs and most recent song streamed. I also get info on the related artist of the artists I listen to.

All this data is stored into a local postgresql database. I use Windows Task Scheduler to run the python script every hour. Thus, the database is refreshed every hour.

Finally, I use Power BI to connect to the postgresql database and make visualisations. Here is what the dashboard looks like:

![image](https://github.com/youennwilson91/spotipy_end_to_end_project/assets/117467104/8eebd872-c3a9-4b79-a314-90409cfbf4f9)
![image](https://github.com/youennwilson91/spotipy_end_to_end_project/assets/117467104/a0eb856b-e6e0-4256-b813-105d1c5be343)
![image](https://github.com/youennwilson91/spotipy_end_to_end_project/assets/117467104/f6dc9d91-cfae-4e92-a035-0e2133cfbfbd)


You'll find the whole code in the two following files:
- main_sqlalchemy.py which is the script that gathers spotify data and feeds the database.
- sql_tables.py which creates the required tables in the database.

Don't hesitate to give any feedback or recommendations.

Thank you for reading.
