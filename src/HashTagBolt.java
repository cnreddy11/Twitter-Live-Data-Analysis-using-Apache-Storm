/*
   Author : Nikhila Chireddy
   Date : 10-24-2017
*/

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class HashTagBolt extends BaseRichBolt {

	private OutputCollector collector;
	// private List<Object> tweets = null;
	private FileWriter fileWriter;
	private BufferedWriter bw;
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {

		this.collector = collector;
		// this.tweets = new ArrayList<Object>();

		try {
			fileWriter = new FileWriter("/s/chopin/a/grad/cnreddy/twitter/HashTagLog.txt",true);
			bw = new BufferedWriter(fileWriter);
		} catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILE :: 1 ");
			e.printStackTrace();
		}

	}

	public void execute(Tuple tuple) {
		Status tweetsFromBolt = (Status) tuple.getValueByField("tweet");
		int sentiment = (Integer) tuple.getValueByField("sentiment");

		collector.ack(tuple);

		// Status tweetsFromBolt = tweets_from_bolt;

		for (HashtagEntity ht : tweetsFromBolt.getHashtagEntities()) {

			String hashTag = ht.getText().toLowerCase();

			if (!hashTag.isEmpty()) {
				try{
					fileWriter = new FileWriter("/s/chopin/a/grad/cnreddy/twitter/HashTagLog.txt",true);
					bw = new BufferedWriter(fileWriter);
					bw.write(hashTag + "\t" + sentiment+"\n");
					bw.flush();
				}catch(Exception e){
					e.printStackTrace();
				}

				this.collector.emit(new Values(hashTag, sentiment, "ht"));
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("hashTag", "sentiment","type"));

	}

}
