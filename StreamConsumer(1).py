from kafka import KafkaConsumer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import json

sid = SentimentIntensityAnalyzer()

def detectSentiment(sentence):
    compondScore = sid.polarity_scores(sentence)["compound"]
    sentiment = ""
    if compondScore == 0:
        sentiment = "Neutral"
    elif compondScore < 0:
        sentiment = "Negative"
    elif compondScore > 0:
        sentiment = "Positive"

    return sentiment


def main():
    consumer = KafkaConsumer('twitter')
    for msg in consumer:
        jsonMsg = json.loads(msg.value)
        if 'extended_tweet' in jsonMsg:
            fullText = jsonMsg['extended_tweet']['full_text']
        else:
            fullText = ""
        if fullText != "":
            sentiment = detectSentiment(fullText)
            output = "\nTweet Content: " + fullText + " \nSentiment: " + sentiment + " \n\n"
            f = open("output.txt", "a+")
            f.write(output.encode("utf-8"))
            f.close()
            # print(output)


if __name__ == '__main__':
    main()
