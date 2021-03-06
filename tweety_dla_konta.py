# SKRYPT KTORY DAJE TWEETY KONTA NA PODSTAWIE JEGO NAZWY
import tweepy
import pandas as pd
import re
from textblob import TextBlob
import nltk
from nltk.tokenize import word_tokenize
from nltk import pos_tag
from nltk.corpus import stopwords
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
from sklearn import cluster
from datetime import datetime, timedelta
import pyodbc
import gensim.downloader
import numpy as np
from scipy.spatial import distance

# Połączenie z bazą danych
server = 'ipzazuresql.database.windows.net'
database = 'ipz2db'
username = 'osuch'
password = 'IPZ_2022_sem_letni'
conn = pyodbc.connect(
'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = conn.cursor()

# Pobranie danych o prodycentach
car_makers = pd.read_sql('SELECT * FROM Manufacturers', conn)

api_key = 'JZsFN9WxBmW2SFAk8EtF2BGG8'
api_key_secret = '2w5dlu6slQQTYK1ESKY4YU1uEGkVoIA8YaGO7ROKnNoApicOAK'

access_token = '1501132479540998144-iqS5ppPFTPR0AT9bb2GWkm7GHXQCX5'
access_token_secret = '8ZPMhuCPeB2cXHLayy02rCaJWr9s7yavihJgTvTVriBjh'

# authentication
auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

# user tweets
limit = 150
# create DataFrame
columns = ['manu_id', 'date', 'likes', 'tweet']
data = []

# wyznaczenie daty (dane pobierane z ostatniego tygodna wiec sprawdzamy date na tydzien wstecz)
since_when = datetime.now() - timedelta(days=7)

for i, tw_name in enumerate(car_makers["tw_name"]):
    print(tw_name)
    if tw_name == '':
        continue
    user = tw_name
    tweets = api.user_timeline(screen_name=user, count=limit, tweet_mode="extended")

    for tweet in tweets:
        data.append([car_makers['manu_id'][i], tweet.created_at, tweet.favorite_count, tweet.full_text])

df = pd.DataFrame(data, columns=columns)
df['date'] = pd.to_datetime(df.date).dt.tz_localize(None)
df = df[df.date > since_when]
# print(df)

# jak tweet zaczyna sie od RT to jest to retweet
# print(df.to_string())

# czesc 2

mydata = df.drop('user',1, errors='ignore')

def clean(x):
    x = re.sub("@[A-Za-z0-9_]+","", x)
    x = re.sub("#[A-Za-z0-9_]+","", x)
    x = x.lower().strip()
    # romove urls
    url = re.compile(r'https?://\S+|www\.\S+')
    x = url.sub(r'',x)
    # remove html tags
    html = re.compile(r'<.*?>')
    x = html.sub(r'',x)
    x = re.sub('[^A-Za-z]+', ' ', x)
    return x

mydata['Cleaned Reviews'] = mydata['tweet'].apply(clean)

# czesc 3
nltk.download('omw-1.4')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')
nltk.download('wordnet')

# POS tagger
pos_dict = {'J': wordnet.ADJ, 'V': wordnet.VERB, 'N': wordnet.NOUN, 'R': wordnet.ADV}

def token_stop_pos(text):
    tags = pos_tag(word_tokenize(text))
    newlist = []
    for word, tag in tags:
        if word.lower() not in set(stopwords.words('english')):
            newlist.append(tuple([word, pos_dict.get(tag[0])]))
    return newlist

mydata['POS tagged'] = mydata['Cleaned Reviews'].apply(token_stop_pos)
mydata.head()

# czesc 4

wordnet_lemmatizer = WordNetLemmatizer()

def lemmatize(pos_data):
    lemma_rew = " "
    for word, pos in pos_data:
        if not pos:
            lemma = word
            lemma_rew = lemma_rew + " " + lemma
        else:
            lemma = wordnet_lemmatizer.lemmatize(word, pos=pos)
            lemma_rew = lemma_rew + " " + lemma
    return lemma_rew

mydata['Lemma'] = mydata['POS tagged'].apply(lemmatize)
mydata.head()

# czesc 5

# subjectivity
def getSubjectivity(review):
    return TextBlob(review).sentiment.subjectivity

# polarity
def getPolarity(review):
    return TextBlob(review).sentiment.polarity

# analyze
def analysis(score):
    if score < 0:
        return 'Negative'
    elif score == 0:
        return 'Neutral'
    else:
        return 'Positive'

def analysis_val(score):
    if score < 0:
        return -1
    elif score == 0:
        return 0
    else:
        return 1

fin_data = pd.DataFrame(mydata[['manu_id', 'date', 'tweet', 'Lemma']])
fin_data['Subjectivity'] = fin_data['Lemma'].apply(getSubjectivity)
fin_data['Polarity'] = fin_data['Lemma'].apply(getPolarity)
fin_data['Analysis'] = fin_data['Polarity'].apply(analysis)
fin_data['Analysis_val'] = fin_data['Polarity'].apply(analysis_val)
fin_data.head()

sentences = list(fin_data['Lemma'])

for i in range(len(sentences)):
    sentences[i] = sentences[i].split()

# downloading KeyedVectors
glove_vectors = gensim.downloader.load('glove-twitter-25')

sentences_vectors = []

for sentence in sentences:
    matchingwords = []
    for word in sentence:
        if word in glove_vectors.key_to_index:
            matchingwords.append(glove_vectors[word])
    matchingwords = np.array(matchingwords)
    sentences_vectors.append(np.mean(matchingwords, axis=0))

NUM_CLUSTERS = 6
kmeans = cluster.KMeans(n_clusters=NUM_CLUSTERS)
kmeans.fit(sentences_vectors)

labels = kmeans.labels_
centroids = kmeans.cluster_centers_

center = np.mean(sentences_vectors, axis=0)

distances = []
for i in range(len(centroids)):
    center_to_centroid_dist = distance.cosine(center, centroids[i])
    distances.append(center_to_centroid_dist)
distances = np.array(distances)
removed_cluster_idx = np.argmax(distances)
fin_data = fin_data[labels != removed_cluster_idx]

for index, row in fin_data.iterrows():
    try:
        tweet = str(row.tweet)
        if len(tweet) > 240:
            tweet = tweet[0:240] + '...'
        date = row.date.strftime('%Y-%m-%d')
        cursor.execute(
            "INSERT INTO Tweets (manu_id, date_, tweet, subjectivity, polarity, analysis, analysis_val) values(?,?,?,?,?,?,?)",
            int(row.manu_id), date, tweet, row.Subjectivity, row.Polarity, row.Analysis, row.Analysis_val)
    except ValueError:
        print(row)
        continue
    except pyodbc.ProgrammingError:
        continue
conn.commit()
