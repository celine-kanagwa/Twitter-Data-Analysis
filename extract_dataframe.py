import json 
import pandas as pd 
from textblob import textBlob 

def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
     """
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))
    
    
    return len(tweets_data), tweets_data

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self)->list:
        column = []
        for data in self.tweets_list:
            column.append(data['user']['statuses_count'])
        statuses_count = column
        return statuses_count
        
        
    def find_full_text(self)->list:
        column = []
        for data in self.tweets_list:
            column.append(data['text'])
        text = column
        return text

    
    def find_sentiments(self, text)->list:
        polarity = []
        subjectivity = []
        for data in self.tweets_list:
            try:
                polarity.append(data['polarity'])
            except KeyError:
                polarity.append(None)
            try:
                subjectivity.append(data['subjectivity'])
            except KeyError:
                subjectivity.append(None)
        return polarity, subjectivity
    

    def find_created_time(self)->list:
        created_at = []
        for data in self.tweets_list:
            created_at.append(data['created_at'])
        return created_at

    def find_source(self)->list:
        source = []
        for data in self.tweets_list:
            source.append(data['source'])
        return source

    def find_screen_name(self)->list:
        column = []
        for data in self.tweets_list:
            try:
                column.append(data['entities']['user_mentions'][0]['screen_name'])
            except IndexError:
                column.append(None)
        screen_name = column
        return screen_name


    def find_followers_count(self)->list:
        column = []
        for data in self.tweets_list:
            column.append(data['user']['followers_count'])
        followers_count = column
        return followers_count

    def find_friends_count(self)->list:
        column = []

        for data in self.tweets_list:
            column.append(data['user']['friends_count'])
        friends_count = column
        return friends_count

    def is_sensitive(self)->list:
        column = []
        for data in self.tweets_list:
            try:
                column.append(data['possibly_sensitive'])
            except KeyError:
                    column.append(None)
        
        is_sensitive = column
        return is_sensitive


    def find_favourite_count(self)->list:
        column = []
        for data in self.tweets_list:
            column.append(data['user']['favourites_count'])
        favourites_count = column
        return favourites_count
    
    def find_retweet_count(self)->list:
        column = []
        for data in self.tweets_list:
            column.append(data['retweet_count'])
        retweet_count = column
        return retweet_count

    def find_hashtags(self)->list:
        column = []
        for data in self.tweets_list:
            column.append(data['entities']['hashtags'])
        hashtags = column
        return hashtags

    def find_mentions(self)->list:
        column = []
        for data in self.tweets_list:
            column.append(data['entities']['user_mentions'])
        mentions = column
        return mentions


    def find_location(self)->list:
        column = []
        for data in self.tweets_list:
            column.append(data['user']['location'])
        location = column
            
        return location

    def find_lang(self)->list:
        column = []
        for data in self.tweets_list:
            column.append(data['user']['lang'])
        lang = column
        return lang
    
        
        
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
            'original_author', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()

        print(len(created_at),len(source),len(text),len(polarity),len(subjectivity),len(lang),len(fav_count),len(retweet_count)
            ,len(screen_name),len(follower_count),len(friends_count),len(sensitivity),len(hashtags),len(mentions),len(location))
        data = (
            created_at, 
            source, 
            text, 
            polarity, 
            subjectivity, 
            lang, 
            fav_count, 
            retweet_count, 
            screen_name, 
            follower_count, 
            friends_count, 
            sensitivity, 
            hashtags, 
            mentions, 
            location)
        
     
        print(len(follower_count))
        df = pd.DataFrame({
                "created_at": created_at,
                "source": source,
                "text": text,
                "polarity": polarity,
                "subjectivity": subjectivity,
                "lang": lang,
                "fav_count": fav_count,
                "retweet_count": retweet_count,
                "screen_name": screen_name,
                "follower_count": follower_count,
                "friends_count": friends_count,
                "sensitivity": sensitivity,
                "hashtags": hashtags,
                "mentions": mentions,
                "location": location
            })

        if save:
            df.to_csv('./data/processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'original_text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    _, tweet_list = read_json("./data/Economic_Twitter_data.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df(save=True) 

    # use all defined functions to generate a dataframe with the specified columns above

      
