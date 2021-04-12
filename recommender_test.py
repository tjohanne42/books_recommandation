import pandas as pd
import numpy as np
import sklearn
from sklearn.decomposition import TruncatedSVD
import time

class ContentRecommendation(object):

    def __init__(self, verbose=0, n_components=10):
        init_timer = time.time()
        self.df_r, self.df_b = self._load_df(verbose=verbose)
        self.corr = self._init_corr(verbose=verbose, n_components=n_components)
        self.title_series = self.df_b["title"]
        if verbose > 0:
            print("Time to init:", time.time() - init_timer, "sec")


    def _load_df(self, verbose=0):
        if verbose > 0:
            print("Loadind ratings.csv ...")
        df_r = pd.read_csv('ratings.csv')
        df_r.sort_values(by="user_id", inplace=True)
        df_r = df_r.reset_index()
        if verbose > 0:
            print("Done")
            print("Loadind books.csv ...")
        df_b = pd.read_csv('books.csv')
        if verbose > 0:
            print("Done")
        return df_r, df_b


    def _init_corr(self, verbose=0, n_components=20):
        if verbose > 0:
            print("Loading matrix ...")
        df_r_pivot = self.df_r.pivot(index="user_id", columns ="book_id", values="rating")
        df_r_pivot = df_r_pivot.fillna(0)
        X = df_r_pivot.values.T
        if verbose > 0:
            print("Done")
            print("Fiting SVD ...")
        # SVD = TruncatedSVD(n_components=n_components, random_state=42)
        # matrix = SVD.fit_transform(X)
        if verbose > 0:
            print("Done")
            print("Loading corr ...")
        #corr = np.corrcoef(matrix)
        corr = np.corrcoef(X)
        if verbose > 0:
            print("Done")
        return corr
        

    def show_book_title_from_id(self, book_id):
        book_title = self.df_b.loc[self.df_b["book_id"] == book_id, "title"].values[0]
        print(book_id, book_title)


    def show_books(self, start, end):
        while start < end and start < len(self.title_series):
            print("book_id", start+1, "title", self.title_series[start])
            start += 1


    def related_books(self, book_id=False, book_title=False, n_books=10):
        idx = []
        if book_id:
            book_corr = self.corr[book_id - 1]
            idx = (-book_corr).argsort()[:n_books]

        elif book_title:
            book_id = self.df_b.loc[self.df_b["title"] == book_title, "book_id"].values[0]
            book_corr = self.corr[book_id - 1]
            idx = (-book_corr).argsort()[:n_books]

        i = 0
        while i < len(idx):
            idx[i] += 1
            i += 1

        return idx


    def show_books_from_user_id(self, user_id):
        print("User:", user_id)
        df_user = self.df_r[self.df_r["user_id"] == user_id]
        for i in range(df_user.shape[0]):
            print(self.df_b[self.df_b["book_id"] == df_user["book_id"][i]]["original_title"], df_user["rating"][i])


    #def recommend_books_from_user_id(self, user_id, n_books=10):



if __name__ == "__main__":

    content_recommendation = ContentRecommendation(verbose=1)

    while True:

        text = "\nEnter corresponding keys.\n"
        text += "1. Show list of n books. ('1 0 10' will show ten first books)\n"
        text += "2. Show related books. ('2 1 10' will show ten first related books with book_id==1)\n"
        text += "3. Show user. ('3 1' will show user with user_id==1)\n"
        text += "5. Exit\n"
        ret = input(text)

        if ret[0] == "1":
            ret = ret.split()
            try:
                start = int(ret[1])
                end = int(ret[2])
                print("\n", str(" "+str(start)+" ").center(50, "-"))
                content_recommendation.show_books(start, end)
                print(str(" "+str(end)+" ").center(50, "-"), "\n")
            except:
                print("Unvalid input.")

        elif ret[0] == "2":
            ret = ret.split()
            try:
                book_id = int(ret[1])
                n_books = int(ret[2])
                content_recommendation.show_book_title_from_id(book_id)
                book_list = content_recommendation.related_books(book_id=book_id, n_books=n_books)
                print("\n", " 0 ".center(50, "-"))
                for i in book_list:
                    content_recommendation.show_book_title_from_id(i)
                print("\n", str(" "+str(n_books)+" ").center(50, "-"))
            except:
                print("Unvalid input.")

        elif ret[0] == "3":
            ret = ret.split()
            user_id = int(ret[1])
            content_recommendation.show_books_from_user_id(user_id)
            try:
                user_id = int(ret[1])
                n_books = int(ret[2])
                content_recommendation.show_books_from_user_id(user_id)
                # book_list = content_recommendation.recommend_books_from_user_id(user_id, n_books)
                # print("\n", " 0 ".center(50, "-"))
                # for i in book_list:
                #   content_recommendation.show_book_title_from_id(i)
                # print("\n", str(" "+str(n_books)+" ").center(50, "-"))
            except:
                print("Unvalid input.")

        elif ret[0] == "5":
            break

        else:
            print("Unvalid input.")
