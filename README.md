# Twitter-Sentiment-Analysis

Project Description:

The framework performs SENTIMENT analysis of particular hash tags in twitter data in real-time. But if you want to implement it with Python, Kafka is required. 

Sentiment Analysis is the process of determining whether a piece of writing is positive, negative or neutral. It’s also known as opinion mining.
For example,
“President Donald Trump approaches his first big test this week from a position of unusual weakness.” - has positive sentiment.  
“Trump has the lowest standing in public opinion of any new president in modern history.” - has neutral sentiment.  
“Trump has displayed little interest in the policy itself, casting it as a thankless chore to be done before getting to tax-cut legislation he values more.” - has negative sentiment.  
You can use any third party sentiment analyzer like Stanford CoreNLP (java/scala), nltk(python) for sentiment analyzing.  
In Spark Streaming, create a Kafka consumer (for python, shown in the class for streaming) and periodically collect filtered tweets (required for both scala and python) from producer. For each hash tag, perform sentiment analysis using Sentiment Analyzing tool (discussed above). Then for each hash tag, save the output with twitter itself.
