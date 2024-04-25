#### Youtube_Data_Harvesting_and_Warehousing

Domain : Social Media

Problem Statement :
The problem statement is to create a Streamlit application that allows users to access
and analyze data from multiple YouTube channels. The application should have the
following features:
1. Ability to input a YouTube channel ID and retrieve all the relevant data
(Channel name, subscribers, total video count, playlist ID, video ID, likes,
dislikes, comments of each video) using Google API.
2. Option to store the data in a MongoDB database as a data lake.
3. Ability to collect data for up to 10 different YouTube channels and store them in
the data lake by clicking a button.
4. Option to select a channel name and migrate its data from the data lake to a
SQL database as tables.
5. Ability to search and retrieve data from the SQL database using different
search options, including joining tables to get channel details.


Technologies used :
•	Python
•	MongoDB
•	MySQL
•	YouTube Data API
•	Streamlit
•	Pandas

Overview :
This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.

Workflow :
Connect to the YouTube API this API is used to retrieve channel, videos, comments data. I have used the Google API client library for Python to make requests to the API.
The user will able to extract the Youtube channel's data using the Channel ID. Once the channel id is provided the data will be extracted using the API.
Once the data is retrieved from the YouTube API, I've stored it in a MongoDB as data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.
After collected data for multiple channels,it is then migrated/transformed it to a structured MySQL as data warehouse.
Then used SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input.
With the help of SQL query I have got many interesting insights about the youtube channels.
Finally, the retrieved data is displayed in the Streamlit app.
Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB datalake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.
