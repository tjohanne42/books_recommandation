import os
import requests
import pandas as pd
from tqdm import tqdm
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


if __name__ == "__main__":

    if not os.path.isdir("databases"):
        os.system("mkdir databases")
    
    if not os.path.isfile("databases/books.csv"):
        download_file_from_google_drive("1YnXO0GeZ_AwZ8XIY8tsQ-oyXH7RWhrjn", "databases/books.csv")

    if not os.path.isfile("databases/ratings.csv"):
        download_file_from_google_drive("1p8sB9hW5fDn6JUjKTYTGgrHwy2gWNu5s", "databases/ratings.csv")
        
    # TODO : CHANGE PATH GOOGLE DRIVE
    if not os.path.isfile("databases/book_tags.csv"):
        download_file_from_google_drive("1jUX5wnikcgp55vpvBqcadUcJYrEghJyt", "databases/book_tags.csv")
        
    if not os.path.isfile("databases/tags.csv"):
        download_file_from_google_drive("1lyZ_hOt4S2cMJ0tdo5Khc6rT_U4c60M0", "databases/tags.csv")
        
    if not os.path.isfile("svd_pickle_file"):
        download_file_from_google_drive("10j-l6WcTjdnzHkpZtwlJOiVoYk7oi-kZ", "svd_pickle_file")
        
    df_r = pd.read_csv("databases/ratings.csv")
    df_b = pd.read_csv("databases/books.csv")
    df_bt = pd.read_csv("databases/book_tags.csv")
    df_t = pd.read_csv("databases/tags.csv")
    
    ret = input("Enter mysql port (default=3307):\n")
    hostname="localhost:" + ret
    dbname="book_recommendation"
    uname = input("Enter mysql username:\n")
    #uname="root"
    pwd = input("Enter mysql password:\n")
    #pwd="azerty"

    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine(f"mysql+pymysql://{uname}:{pwd}@{hostname}/{dbname}")
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
