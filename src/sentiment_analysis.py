import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, WordNetLemmatizer
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re
import emoji
import mysql.connector
host = "feenix-mariadb.swin.edu.au"
user = "s104489467"
password = "101100"
database = "s104489467_db"
connection = mysql.connector.connect(host=host,user=user,password=password,database=database)
cursor = connection.cursor()
sql_query = "select * from energy_data"
df = pd.read_sql(sql_query, connection)
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
stop_words = set(stopwords.words('english'))
stemmer= PorterStemmer()
lemmatizer = WordNetLemmatizer()
def tokenization(text):
    return nltk.word_tokenize(text)
def to_lowecase(tokens):
    return [word.lower() for word in tokens]
def remove_stopwords(tokens):
    return [word for word in tokens if word not in stop_words]
def stem_tokens(tokens):
    return [stemmer.stem(word) for word in tokens]
def lemmatize_tokens(tokens):
    return [lemmatizer.lemmatize(word) for word in tokens]
def remove_special_characters(tokens):
    return [re.sub(r'[^\w\s]', '', token) for token in tokens if re.sub(r'[^\w\s]', '', token)]
def handle_emojis(tokens):
    return [emoji.demojize(token) for token in tokens]
def correct_spelling(tokens):
    corrected_tokens = []
    for token in tokens:
        corrected_token = str(TextBlob(token).correct())
        corrected_tokens.append(corrected_token)
    return corrected_tokens
def preprocess_text(text):
    tokens = tokenization(text)
    tokens = to_lowecase(tokens)
    tokens = remove_stopwords(tokens)
    tokens = stem_tokens(tokens)
    tokens = lemmatize_tokens(tokens)
    tokens = remove_special_characters(tokens)
    tokens = handle_emojis(tokens)
    tokens = correct_spelling(tokens)
    return ' '.join(tokens)
df['cleaned_comment'] = df['comment'].apply(preprocess_text)
nltk.download('vader_lexicon')
analyzer = SentimentIntensityAnalyzer()
def analyze_sentiment(text):
    if not text:
        return {'compound': 0.0, 'neg': 0.0, 'neu': 0.0, 'pos': 0.0}
    return analyzer.polarity_scores(text)
df = df[df['cleaned_comment'].notnull()]
df['sentiment'] = df['cleaned_comment'].apply(analyze_sentiment)
df['compound'] = df['sentiment'].apply(lambda x: x['compound'])
def rescale_score(compound_score):
    # Rescale from [-1, 1] to [1, 5]
    return 1 + (compound_score + 1) * 2
df['rescaled_compound'] = df['compound'].apply(rescale_score)
