/*
   Author : Nikhila Chireddy
   Date : 10-24-2017
*/


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import twitter4j.Status;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import java.util.List;
import java.util.Properties;
import org.apache.storm.tuple.Fields;

import org.apache.storm.tuple.Values;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

public class SentimentBolt extends BaseRichBolt {
	private OutputCollector collector;
	private FileWriter fileWriter;
	private BufferedWriter bw;

	public void execute(Tuple tuple) {

		Status tweet = (Status) tuple.getValueByField("tweets");

		int sentiment = 0;
		Properties properties = new Properties();
		properties.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(properties);

		Annotation document = new Annotation(tweet.getText());
		pipeline.annotate(document);
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		/*
		 * try { fileWriter = new
		 * FileWriter("/s/chopin/a/grad/cnreddy/twitter/Sentiments.txt", true);
		 * bw = new BufferedWriter(fileWriter);
		 * bw.write("\nBefore sentiment tweet: " + tweet.getText()); bw.flush();
		 * } catch (Exception e) { e.printStackTrace(); }
		 */

		for (CoreMap sentence : sentences) {
			String sentimentWord = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
			if (sentimentWord.contentEquals("Neutral")) {
				sentiment += 0;
			} else if (sentimentWord.contentEquals("Positive")) {
				sentiment += 1;
			} else if (sentimentWord.contentEquals("Negative")) {
				sentiment += -1;
			} else if (sentimentWord.contentEquals("Very Negative")) {
				sentiment += -2;
			} else if (sentimentWord.contentEquals("Very Positive")) {
				sentiment += 2;
			}
		}
		if(sentiment >=2)
			sentiment = 2;
		else if(sentiment <= -2)
			sentiment = -2;
		
		collector.emit(new Values(tweet, sentiment));
		try {
			fileWriter = new FileWriter("/s/chopin/a/grad/cnreddy/twitter/Sentiments.txt", true);
			bw = new BufferedWriter(fileWriter);
			bw.write(tweet.getText() + "\t" + sentiment + "\n");
			bw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}

		//System.out.println("Sentiment score: " + sentiment + "Tweet" + tweet);

	}

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {

		this.collector = collector;
		// this.tweet = new ArrayList<Object>();

		try {
			fileWriter = new FileWriter("/s/chopin/a/grad/cnreddy/twitter/Sentiments.txt", true);
			bw = new BufferedWriter(fileWriter);
		} catch (Exception e) {
			System.out.println("Error in writing to file");
			e.printStackTrace();
		}

		// this.tweetSentiment = new HashMap<Status, Integer>();

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet", "sentiment"));
	}
}
