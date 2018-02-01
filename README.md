# Twitter-Live-Data-Analysis-using-Apache-Storm

Colorado State University. Course : CS535 - Programming  Assignment 2


Detecting the Most Popular Topics with Sentiments from Live Twitter Message Streams using the Lossy Counting Algorithm with Apache Storm

The goal of this system is to detect the most frequently
occurring hash tags and named-entities from the live Twitter data stream and
calculate sentiment values in real-time.

A hashtag is a type of label or metadata tag used in social networks that makes it easier
for users to find messages with a specific theme or content.

Named entities are a real-world objects such as persons, locations, organizations,
products, etc. 

Finding popular and trendy topics (via hashtags and named entities) in real-time
marketing implies that we include something topical in our social media posts to help
increase the overall reach. Also, associating sentiment information with topics will allow
us to use the media more precisely. 

To implement this system, I have used the Apache Storm Framework. To get the live Tweets from Twitter, I have used the Twitter4J package.
Twitter4J is a Java library for the Twitter API. We can use pure HTTP GET to retrieve the messages.

To perform sentiment analysis, I have used the Stanford Core NLP Package. Stanford CoreNLP 3.8.0 is available at its official website:
http://stanfordnlp.github.io/CoreNLP

The system generates the Top 100 Hashtags and the Top 100 Named-entities for every 10 seconds using the Lossy Counting Algorithm.
