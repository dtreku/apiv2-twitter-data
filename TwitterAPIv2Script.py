#pip install csvkit
#pip install jsonlines
import requests  # For sending GET requests from the API
import os  # For saving access tokens and for file management when creating and adding to the dataset
import json # For dealing with json responses we receive from the API
#import jsonlines
import pandas as pd #using pandas dataframe for data handling
import csv # For saving the response data in CSV format
import datetime # For parsing the dates received from twitter in readable formats
import dateutil.parser
import unicodedata
import time #To add wait time between requests
from time import gmtime, strftime, localtime # For current time
from config.pathDefinitions import ROOT_DIR #for handling relative paths

#from dataAppendFormats.appendCSV import append_to_csv
#from dataAppendFormats import appendJSON
#from main_program import endpoint2_main
#from main_program import create_headers

#APIpath ='C:/Users/niina/OneDrive - The University of Texas-Rio Grande Valley/Twitter Data Collection Workshop Folder/TwitterDataCollectionWorkshop/' #'./gdrive/My Drive/'
#APIpath = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', , 'mydata.json')))
#print("working directory: " "{}".format(os.getcwd()))

APIpath = os.path.join(ROOT_DIR, 'APICredentials.json')
APICredentials = json.loads(open(APIpath).read())  
bearer_token = APICredentials['bearer_token']
endpoint2 = APICredentials[input("Enter endpoint name: ")]
print("endpointURL_In_Use: ""{}".format(endpoint2))
print("APICredentials: " "{}".format(APIpath))


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results = 10):
    search_url = endpoint2 

    #change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id', 
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,entities,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)

  
def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        time.sleep(15)
        try:
            return connect_to_endpoint(url, headers, params, next_token = next_token)
        except Exception:
            raise Exception(response.status_code, response.text)
    return response.json()


def append_to_csv(json_response, fileName):

    #A counter variable
    counter = 0

    #Open OR create the target CSV file
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)


    #for user in json_response['data']:
        #follower_count = user['public_metrics']
        #print(follower_count)
      

    #Loop through each tweet
    for tweet in json_response['data']:
        
        # We will create a variable for each since some of the keys might not exist for some tweets
        # So we will account for that

        # 1. Author ID
        try: #if('author_id' in tweet):
            author_id = tweet['author_id']
        except KeyError: #else:
            author_id = " "
        
        # 2. Time created
        try: #if ('created_at' in tweet):
            created_at = dateutil.parser.parse(tweet['created_at'])
        except: #else: 
            created_at = " "

        # 3. Geolocation
        try:   
            geo = tweet['geo']['place_id']
            #geo = tweet['geo']['coordinates']['coordinates']
        except KeyError:
            geo = " "

        # 4. Tweet ID
        try: #if('id' in tweet):
            tweet_id = tweet['id']
        except KeyError: #else:
            tweet_id = " "

        # 5. Language
        try: #if('lang' in tweet):
            lang = tweet['lang']
        except KeyError: #else: 
            lang = " "

        # 6. Tweet metrics
        #impression_count = tweet['non_public_metrics']['impression_count']
        #user_profile_clicks= ['non_public_metrics']['user_profile_clicks']

        if('public_metrics' in tweet) and 'retweet_count' in (tweet['public_metrics']):
            retweet_count = tweet['public_metrics']['retweet_count']
        else: retweet_count = " "

        #organic_retweet_count = tweet['organic_metrics']['retweet_count']

        if('public_metrics' in tweet) and 'reply_count' in (tweet['public_metrics']):
            reply_count = tweet['public_metrics']['reply_count']
        else: reply_count = " "


        #organic_reply_count = tweet['organic_metrics']['reply_count']


        if('public_metrics' in tweet) and 'like_count' in (tweet['public_metrics']):
            like_count = tweet['public_metrics']['like_count']
        else: like_count = " "


        #organic_like_count = tweet['organic_metrics']['like_count']


        if('public_metrics' in tweet) and 'quote_count' in (tweet['public_metrics']):
            quote_count = tweet['public_metrics']['quote_count']
        quote_count = " "

        
        if ('entities' in tweet) and 'hashtags' in (tweet['entities']):
            hashtags = tweet['entities']['hashtags'][0]['tag']
        else:
            hashtags = " "


        if ('entities' in tweet) and 'cashtags' in (tweet['entities']):
            cashtags = tweet['entities']['cashtags'][0]['tag']
        else:
            cashtags = " "

        #print(cashtags)

        #PM1 = tweet['author_id'][0]
        #PM2 = tweet['author_id'][1]
        #PM3 = tweet['author_id'][2]
        #PM4 = tweet['author_id'][3]
        #PM5 = tweet['author_id'][4]
        #PM6 = tweet['author_id'][5]
        #PM7 = tweet['author_id'][6]

        # 7. source
        try: #if('source' in tweet):
            source = tweet['source']
        except: #else: 
            source = " "
        

        # 8. Tweet text
        try: #if('text' in tweet):
            text = tweet['text']
        except: #else: 
            text = " "

        #
        try: #if('conversation_id' in tweet):
            conversation_id = tweet['conversation_id']
        except: #else: 
            conversation_id = " "
            

        # Assemble all data in a list
        #res = [author_id, created_at, geo, tweet_id, lang, like_count, organic_like_count, quote_count, reply_count, organic_reply_count, retweet_count, organic_retweet_count, source, text, conversation_id]
        res = [author_id, created_at, geo, tweet_id, lang, like_count, quote_count, reply_count, retweet_count, source, text, conversation_id, hashtags, cashtags]

        # Append the result to the CSV file
        csvWriter.writerow(res)
        counter += 1

    # When done, close the CSV file
    csvFile.close()

    # Print the number of tweets for this iteration
    print("# of Tweets added from this response: ", counter) 

def endpoint2_main():
    try:
        #Inputs for the request
        #bearer_token = auth()
        headers = create_headers(bearer_token)
        keyword = "(from:bitcoin OR Bitcoins OR $BTC OR XBT OR #XBT OR $XBT OR BTCTN OR #bitcoin OR #BTC OR (Bitcoin cryptocurrency) OR satoshi) lang:en -is:retweet"
        #keyword = "(from:ethereum OR #ethereum OR #Ethereum OR Ethereum OR #ETH OR (Ethereum cryptocurrency)) lang:en -is:retweet"
        #start_time = "2015-01-01T00:00:00.000Z"
        #end_time = "2015-01-31T00:00:00.000Z"

        start_list =[#'2022-01-01T00:00:00.000Z',
                    #'2022-02-01T00:00:00.000Z',
                    '2022-03-01T00:00:00.000Z',
                    #'2022-04-01T00:00:00.000Z',
                    #'2022-05-01T00:00:00.000Z',
                    #'2022-06-01T00:00:00.000Z',
                    #'2022-07-01T00:00:00.000Z',
                    #'2022-08-01T00:00:00.000Z',
                    #'2022-09-01T00:00:00.000Z',
                    #'2022-10-01T00:00:00.000Z',
                    #2022-11-01T00:00:00.000Z',
                    #2022-12-01T00:00:00.000Z'
                    ]

        end_list =  [#'2022-01-31T23:59:59.000Z',
                    #'2022-02-28T23:59:59.000Z',
                    '2022-03-24T23:59:59.000Z',
                    #'2022-04-30T23:59:59.000Z',
                    #'2022-05-31T23:59:59.000Z',
                    #'2022-06-30T23:59:59.000Z',
                    #'2022-07-31T23:59:59.000Z',
                    #'2022-08-31T23:59:59.000Z',
                    #'2022-09-30T23:59:59.000Z',
                    #'2022-10-31T23:59:59.000Z',
                    #'2022-11-30T23:59:59.000Z',
                    #'2022-12-31T23:59:59.000Z'
                    ] 

        max_results = 500

        #Total number of tweets we collected from the loop
        total_tweets = 0
        
        DataPath = "dataExploring2022.csv" 
        # Create file
        csvFile = open(DataPath, "a", newline="", encoding='utf-8')
        csvWriter = csv.writer(csvFile)

        #Create headers for the data you want to save, in this example, we only want save these columns in our dataset
        csvWriter.writerow(['author_id', 'created_at', 'geo', 'tweet_id','lang', 'like_count', 'quote_count', 'reply_count','retweet_count', 'source','tweet','conversation_id', 'hashtags', 'cashtags'])
        csvFile.close()

        #next_token = 'b26v89c19zqg8o3fpyqo6rnt3xrw7lcl9lts8v4trj5a5'
        next_token = None

        for i in range(0,len(start_list)):

            # Inputs
            count = 0 # Counting tweets per time period
            max_count = 50000000 #Max tweets per time period
            flag = True

            # Check if flag is true
            while flag:
                # Check if max_count reached
                if count >= max_count:
                    break
                print("-------------------")
                print("Token: ", next_token)
                url = create_url(keyword, start_list[i],end_list[i], max_results)
                json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
                result_count = json_response['meta']['result_count']

                if 'next_token' in json_response['meta']:
                    # Save the token to use for next call
                    next_token = json_response['meta']['next_token']
                    print("Next Token: ", next_token)
                    if result_count is not None and result_count > 0 and next_token is not None:
                        print("Start Date: ", start_list[i])
                        print("End Date: ", end_list[i])
                        append_to_csv(json_response, DataPath)
                        count += result_count
                        total_tweets += result_count
                        print("Total # of Tweets added: ", total_tweets)
                        #print(strftime("%Y-%m-%d %H:%M:%S", gmtime()))   
                        print(strftime("%Y-%m-%d %H:%M:%S", localtime()))                                                                                    
                        print("-------------------")
                        time.sleep(5)                
                # If no next token exists
                else:
                    if result_count is not None and result_count > 0:
                        print("-------------------")
                        print("Start Date: ", start_list[i])
                        append_to_csv(json_response, DataPath)
                        count += result_count
                        total_tweets += result_count
                        print("Total # of Tweets added: ", total_tweets)
                        print("No Next token exists-------------------")
                        time.sleep(5)
                    
                    #Since this is the final request, turn flag to false to move to the next time period.
                    flag = False
                    next_token = None
                time.sleep(5)
        print("Total number of results: ", total_tweets)
        
        pass

    finally:

        #Saving to Google Drive #Method 2 
        #df = pd.read_csv('data.csv',engine='python', error_bad_lines=False)
        #df = df[0].str.split(',', expand=True)
        #df.to_csv('data_Complete.csv')
        pass


if __name__ == '__main__':
    endpoint2_main()
