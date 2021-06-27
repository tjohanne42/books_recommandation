# books_recommandation

## Project goals:

- from a relational database of books from the website [goodreads.com](https://www.goodreads.com/), their ratings by users and their user-assigned tags, create a recommendation engine that can recommend books for any user, whether they already rated books on the website, or are new readers.
- create a SQL database that would allow a backend team to access the informations they want.

## What we did

For the SQL database, we decided to use the free database system provided by MySQL.

For the recommendation engine, we used a mix of popularity based recommendation engine, a system based on a correlation matrix, and a SVD model provided by [the Surprise library](http://surpriselib.com/).

## Setup

- create a new virtual environment *(recommended)* with python 3.9.
- clone the github repository.
- install all requirements: `pip install -r requirements.txt`
- install MySQL (we recommend [LAMP](https://bitnami.com/stack/lamp) on linux), and start the server.
- setup the database: `python setup_database.py` (you will need to provide the MySQL host name, the user name, their password, and the name of the new SQL database).

## How to use