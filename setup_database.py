import os
import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


def get_confirm_token(response):                                      
    for key, value in response.cookies.items():                       
        if key.startswith('download_warning'):                        
            return value                                              
                                                                      
    return None

                                                                    
def save_response_content(response, destination):                     
    CHUNK_SIZE = 3000                                                
                                                                      
    with open(destination, "wb") as f:                                
        for chunk in tqdm(response.iter_content(CHUNK_SIZE)):         
            if chunk: # filter out keep-alive new chunks              
                f.write(chunk)                                        
                                                                      
                                                                      
def download_file_from_google_drive(id, destination):                 
    URL = "https://docs.google.com/uc?export=download"                
                                                                      
    session = requests.Session()                                      
                                                                      
    response = session.get(URL, params = { 'id' : id }, stream = True)
    token = get_confirm_token(response)                               
                                                                      
    if token:                                                         
        params = { 'id' : id, 'confirm' : token }                     
        response = session.get(URL, params = params, stream = True)   
                                                                      
    save_response_content(response, destination)

def download_raw_file_from_github(name, destination):
    
    URL = "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/" + name
    
    session = requests.Session()
    response = session.get(URL, stream=True)
    
    save_response_content(response, Path(destination))

def create_mysql_info_file():
    f = open("mysql_info.txt", "w")
    while True:
        hostname = input("Enter mysql hostname (ex: localhost:3306):\n")
        uname = input("Enter mysql username (ex: root):\n")
        pwd = input("Enter mysql password (ex: root):\n")
        db_name = input("Enter mysql database (ex: book_recommendation):\n")
        connection_str = f"mysql+pymysql://{uname}:{pwd}@{hostname}/{db_name}"
        try:
            engine = create_engine(connection_str)
            break
        except:
            print("Connection failed, try again")
    f.write("\n".join([hostname, uname, pwd, db_name]))
    f.close()
    return engine


def read_mysql_info_file():
    if not os.path.isfile("mysql_info.txt"):
        return create_mysql_info_file()
    file = open('mysql_info.txt', 'r')
    lines = file.readlines()
    file.close()
    if len(lines) < 4:
        return create_mysql_info_file()
    hostname = lines[0].strip("\n")
    uname = lines[1].strip("\n")
    pwd = lines[2].strip("\n")
    db_name = lines[3].strip("\n")
    connection_str = f"mysql+pymysql://{uname}:{pwd}@{hostname}/{db_name}"
    engine = create_engine(connection_str)
    try:
        engine = create_engine(connection_str)
    except:
        print("Login info in mysql_info.txt incorrect")
        return create_mysql_info_file()
    return engine


if __name__ == "__main__":

    if not os.path.isdir(Path("databases")):
        print("Creating databases directory")
        os.system("mkdir databases")
    
    if not os.path.isfile(Path("databases/books.csv")):
        print("Downloading books.csv")
        # download_file_from_google_drive("1YnXO0GeZ_AwZ8XIY8tsQ-oyXH7RWhrjn", "databases/books.csv")
        download_raw_file_from_github("books.csv", "./databases/books.csv")

    if not os.path.isfile(Path("databases/ratings.csv")):
        print("Downloading ratings.csv")
        # download_file_from_google_drive("1p8sB9hW5fDn6JUjKTYTGgrHwy2gWNu5s", "databases/ratings.csv")
        download_raw_file_from_github("ratings.csv", "./databases/ratings.csv")
        
    if not os.path.isfile(Path("databases/book_tags.csv")):
        print("Downloading book_tags.csv")
        # download_file_from_google_drive("1jUX5wnikcgp55vpvBqcadUcJYrEghJyt", "databases/book_tags.csv")
        download_raw_file_from_github("book_tags.csv", "./databases/book_tags.csv")
        
    if not os.path.isfile(Path("databases/tags.csv")):
        print("Downloading tags.csv")
        # download_file_from_google_drive("1lyZ_hOt4S2cMJ0tdo5Khc6rT_U4c60M0", "databases/tags.csv")
        download_raw_file_from_github("tags.csv", "./databases/tags.csv")
        
    if not os.path.isfile("svd.pkl"):
        print("Downloading svd pickle file")
        download_file_from_google_drive("10j-l6WcTjdnzHkpZtwlJOiVoYk7oi-kZ", "svd.pkl")
        
    df_r = pd.read_csv("databases/ratings.csv")
    df_b = pd.read_csv("databases/books.csv")
    df_bt = pd.read_csv("databases/book_tags.csv")
    df_t = pd.read_csv("databases/tags.csv")

    # connect to mysql
    engine = read_mysql_info_file()

    # Create SQLAlchemy engine to connect to MySQL Database
    print("checking if database exists ...")
    if not database_exists(engine.url):
        print("database doesnt exist")
        create_database(engine.url)
        print("database created")
    else:
        print("database already exist")

    # Convert dataframe to sql table

    with engine.begin() as connection:

        try:
            print("uploading ratings ...")
            df_r.to_sql('ratings', connection, index=False)
            print("ratings uploaded")
        except:
            print("ratings already exists")

        try:
            print("uploading books ...")
            df_b.to_sql('books', connection, index=False)
            print("books uploaded")
        except:
            print("books already exists")

        try:
            print("uploading book_tags ...")
            df_bt.to_sql('book_tags', connection, index=False)
            print("book_tags uploaded")
        except:
            print("book_tags already exists")

        try:
            print("uploading tags ...")
            df_t.to_sql('tags', connection, index=False)
            print("tags uploaded")
        except:
            print("tags already exists")
