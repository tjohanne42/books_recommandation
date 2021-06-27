import numpy as np
import pandas as pd
import tqdm
import time
import random
import atexit
import difflib
import pickle
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

from surprise import Dataset
from surprise import Reader
from surprise import SVD
from surprise.model_selection import cross_validate

from download_databases import *


class BookRecommendation(object):

    def __init__(self, verbose=0):
        """
        params:
            verbose -> int
                if verbose > 0:
                    print progress
        """
        self.engine = read_mysql_info_file()

        init_timer = time.time()
        self.svd_file = "svd_pickle_file"
        
        self.new_user_id = []
        
        self.df_r, self.df_b = self._load_df()
        #self.svd = self.train_svd(verbose=verbose)
        self.svd = self.load_svd(verbose=verbose)
        if verbose > 0:
            print("Time to init:", time.time() - init_timer, "sec")
        # use atexit if you want to update the database at the end of script
        atexit.register(self.exit)
        self.rating_mean = self.df_r["rating"].mean()

        
    def update_database(self):
        if len(self.new_user_id) > 0:
            print("Update database")
            self.df_r.to_csv("databases/ratings.csv", index=False)
            with self.engine.begin() as connection:
                self.df_r.to_sql('ratings', connection, index=False, if_exists="replace")
            self.new_user_id = []
    
    
    def exit(self):
        self.update_database()
        

    def _load_df(self):
        with self.engine.begin() as connection:
            df_r = pd.read_sql_table("ratings", connection)
            df_b = pd.read_sql_table("books", connection)
        return df_r, df_b

    
    def train_svd(self, verbose=0):
        if verbose:
            verbose = True
        else:
            verbose = False
        reader = Reader(rating_scale=(1, 5))
        data = Dataset.load_from_df(self.df_r[['user_id', 'book_id', 'rating']], reader)
        if verbose:
            print("Cross validation SVD ...")
        svd = SVD(verbose=verbose, n_epochs=10)
        cross_validate(svd, data, measures=['RMSE', 'MAE'], cv=3, verbose=verbose)
        if verbose:
            print("Done\nFitting SVD ...")
        trainset = data.build_full_trainset()
        svd.fit(trainset)
        if verbose:
            print("Done")
            print("Saving svd ...")
        with open('svd_pickle_file', 'wb') as f1:
            pickle.dump(svd, f1)
        if verbose:
            print("Done")
        return svd
    
    
    def load_svd(self, verbose=0):
        if verbose:
            print("Loading svd ...")
        with open(self.svd_file, 'rb') as f1:
            svd = pickle.load(f1)
        if verbose:
            print("Done")
        return svd

        
    def show_book_title_from_id(self, book_id):
        book_title = self.df_b.loc[self.df_b["book_id"] == book_id, "title"].values[0]
        print(book_id, book_title)


    def show_books(self, start, end):
        while start < end and start < len(self.title_series):
            print("book_id", start+1, "title", self.title_series[start])
            start += 1

            
    def related_books(self, book, n_books=10, unwanted_id=[]):
        idx = []
        if type(book) == str:
            book_id = self.df_b.loc[self.df_b["title"] == book, "book_id"].values[0]
        elif type(book) == int:
            book_id = book
        else:
            return idx
        
        book_corr = self.corr[book - 1]
        idx = (-book_corr).argsort()
        
        i = 0
        while i < len(idx):
            idx[i] += 1
            i += 1

        new_idx = []
        i = 0
        while len(new_idx) < n_books and i < len(idx):
            if idx[i] not in unwanted_id:
                new_idx.append(idx[i])
            i += 1
            
        return new_idx
    
    
    def _recommend_books_from_user_id(self, user_id, n_books=100):
        """
        Recommend books for a user in our database.
        If he's not in our database we're recommanding typical best rated books
        params:
            user_id -> int
            n_books -> int; n_books >= 1
        """
        
        # get ratings of user
        df_user = self.df_r[self.df_r["user_id"] == user_id]
        
        # size of list we wanna create; we're gonna return n_books value randomly inside
        book_list_size = n_books * 3
        
        # if user didn't read any book return typical best rated books
        if len(df_user) == 0:
            book_list = self.popularity_recommender(book_list_size)
            # shuffle
            random.shuffle(book_list)
            return book_list[:n_books]
        
        # sort books rated by user by rating (descending)
        df_user = df_user.sort_values(by="rating", ascending=False, ignore_index=True)
        
        # get list of id of books rated by user
        user_read_books = df_user["book_id"].values.tolist()
        
        # count how many books the user liked (liked means: rating >= average_all_ratings)
        liked_books = df_user[df_user["rating"] >= self.rating_mean].count()
    
        # for each book liked, we're recommanding others books the user didn't read already
        # these books are the most related with books the user liked the most
        stars_count = []
        for i in range(0, 5):
            stars_count.append(len(df_user[df_user["rating"] == i + 1]))

        min_accepted = int(np.ceil(self.rating_mean))
        
        i = 0
        while min_accepted + i <= 5:
            if stars_count[min_accepted + i - 1] > 0:
                min_rated_in_accepted_ratings = min_accepted + i
                break
            i += 1
            
        denominator = 0
        i = 0
        while min_accepted + i <= 5:
            denominator += stars_count[min_accepted + i - 1] * np.power(2, i)
            i += 1
            
        book_list = []
        i = 0
        while i < len(df_user.index):
            if df_user["rating"][df_user.index[i]] < self.rating_mean:
                break
                
            nominator = np.power(2, df_user["rating"][df_user.index[i]] - min_rated_in_accepted_ratings)
            wanted_n_books = int(np.ceil(nominator * (book_list_size / denominator)))
            
            book_list += self.related_books(book=df_user["book_id"][df_user.index[i]],
                                            n_books=wanted_n_books, unwanted_id=book_list+user_read_books)
            i += 1
                
        if len(book_list) < book_list_size:
            book_list += self.popularity_recommender(n_books=book_list_size-len(book_list), unwanted_id=book_list+user_read_books)
            
        # shuffle
        random.shuffle(book_list)

        return book_list[:n_books]
    
    
    def popularity_recommender(self, n_books=50, unwanted_id=[]):
        # goal: create a dataframe of weighted ratings for each book, and return the 
        # indexes of the n_books best rated books.
        # (v*R + m*C) / (v+m)
        # v: number of votes for the book
        # m: minimum number of votes required to appear in the list
        # R: average rating of the book
        # C: mean value of all the votes
        # create a pandas Dataframe with book_id and the number of ratings for the book
        df_wr = self.df_r[["book_id", "rating"]].groupby("book_id", as_index=False).count().rename(columns={"rating": "v"})
        # add the mean score for each book
        df_wr["R"] = self.df_r[["book_id", "rating"]].groupby("book_id", as_index=False).mean()["rating"]
        # let m be the median of the numbers of votes (248)
        m = df_wr["v"].quantile(0.90)
        # print(m)
        C = self.df_r["rating"].mean()
        # compute the weighted ratings for each books
        df_wr["wr"] = df_wr.apply(lambda row: (row["v"]*row["R"] + m*C) / (row["v"] + m), axis=1)
        # sort the books by their weighted ratings
        df_wr = df_wr.sort_values(by="wr", ascending=False, ignore_index=True).reset_index(drop=True)
        book_list = []
        i = 0
        while len(book_list) < n_books and i < len(df_wr):
            if df_wr["book_id"][i] not in unwanted_id:
                book_list.append(df_wr["book_id"][i])
            i += 1
        return book_list
            
        
            
    def generate_recommendation(self, user_id, n_books=10, best=False, new_horizon=True):

        if user_id in self.new_user_id or user_id not in self.df_r["user_id"].values.tolist() or new_horizon == False:
            return self._recommend_books_from_user_id(user_id, n_books)
        
        book_ids = list(self.df_b['book_id'].values)
        unwanted_id = self.df_r[self.df_r["user_id"] == user_id]["book_id"].values.tolist()

        random.shuffle(book_ids)

        book_list = {"book_id": [], "rating": []}

        for book_id in book_ids:
            if book_id in unwanted_id:
                continue
            rating = self.svd.predict(uid=user_id, iid=book_id).est
            if rating >= self.rating_mean:
                book_list["book_id"].append(book_id)
                book_list["rating"].append(rating)
            if best == False and len(book_list["rating"]) >= n_books:
                break
        
        book_list = pd.DataFrame(data=book_list).sort_values(by="rating", ascending=False)["book_id"][:n_books]
        return book_list


    def show_books_from_user_id(self, user_id):
        df_user = self.df_r[self.df_r["user_id"] == user_id]
        df_user = df_user.sort_values(by="rating", ascending=False, ignore_index=True)
        for i in df_user.index:
            print(self.df_b[self.df_b["book_id"] == df_user["book_id"][i]]["title"].values[0], df_user["rating"][i])

    
    def add_ratings(self, user_id, book_id, rating):
        """
         if user doesn't exist -> create user
        """
        if len(book_id) == 0 or len(book_id) != len(rating):
            # print("wrong params")
            return
        
        if self.df_r[self.df_r["user_id"] == user_id].shape[0] > 0:
            # if user already exist
            # drop doublons
            all_books = self.df_r[self.df_r["user_id"] == user_id]["book_id"].values.tolist()
            known_books = []
            for i in all_books:
                if i in book_id:
                    known_books.append(i)
            i = 0
            while i < len(book_id):
                if book_id[i] in known_books:
                    book_id.pop(i)
                    rating.pop(i)
                    i -= 1
                i += 1
            if len(book_id) == 0 or len(book_id) != len(rating):
                return
        
        _dict = {"user_id": [user_id]*len(book_id), "book_id": book_id, "rating": rating}
        df = pd.DataFrame(data=_dict)
        self.df_r = self.df_r.append(df, ignore_index=True)
        self.new_user_id.append(user_id)
        
        
    def del_user(self, user_id):
        self.df_r = self.df_r.drop(self.df_r[self.df_r["user_id"] == user_id].index)
        self.df_r.reset_index(drop=True)


def show_books(start=0, end=10):
    print("\n", str(" "+str(start)+" ").center(50, "-"))
    book_recommendation.show_books(start, end)
    print(str(" "+str(end)+" ").center(50, "-"), "\n")
    
    
def show_user(user_id):
    book_recommendation.show_books_from_user_id(user_id)

    
def recommend_user(user_id, n_books=10, new_horizon=True):
    print("\n", " recommendations ".center(50, "-"))
    book_list = book_recommendation.generate_recommendation(user_id, n_books=n_books, new_horizon=new_horizon)
    print("nb_books", len(book_list))
    for i in book_list:
        book_recommendation.show_book_title_from_id(i)
    print("\n", " end ".center(50, "-"))

    
def add_ratings(user_id=1000000, book_id=[1], rating=[5]):
    book_recommendation.add_ratings(user_id, book_id, rating)
    # test
    
    
def del_user(user_id=1000000):
    book_recommendation.del_user(user_id)


if __name__ == "__main__":

    book_recommendation = BookRecommendation(verbose=1)

    #show_books(0, 10)
    #show_related_books(1, n_books=10)
    add_ratings(user_id=1000000, book_id=[1, 2, 3, 4, 5], rating=[3, 5, 5, 4, 5])
    show_user(user_id=1000000)
    recommend_user(1000000, n_books=10, new_horizon=False)
    recommend_user(1000000, n_books=10, new_horizon=True)
    del_user(user_id=1000000)


