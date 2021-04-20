import os
import requests
from tqdm import tqdm


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
        os.system("mkdir csv")
    
    if not os.path.isfile("databases/books.csv"):
        download_file_from_google_drive("1YnXO0GeZ_AwZ8XIY8tsQ-oyXH7RWhrjn", "databases/books.csv")

    if not os.path.isfile("databases/ratings.csv"):
        download_file_from_google_drive("1v_Xl9n3J4eHGPs5ZIyNyrZIvgVDDqlf1", "databases/ratings.csv")
        
    # TODO : CHANGE PATH GOOGLE DRIVE
    if not os.path.isfile("databases/book_tags.csv"):
        download_file_from_google_drive("1v_Xl9n3J4eHGPs5ZIyNyrZIvgVDDqlf1", "databases/book_tags.csv")
        
    if not os.path.isfile("databases/tags.csv"):
        download_file_from_google_drive("1v_Xl9n3J4eHGPs5ZIyNyrZIvgVDDqlf1", "databases/tags.csv")
