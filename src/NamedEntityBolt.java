############################################
##  
##  Author : Nikhila Chireddy
##
############################################

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

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;

import twitter4j.Status;

public class NamedEntityBolt extends BaseRichBolt {

	private OutputCollector collector;

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {

		this.collector = collector;
	}

	public List<String> getNamedEntities(Status tweetsFromBolt) {

		List<String> namedEntities = new ArrayList<String>();
		String serializedClassifier = "edu/stanford/nlp/models/ner/english.muc.7class.distsim.crf.ser.gz";
		AbstractSequenceClassifier<CoreLabel> classifier = null;
		try {
			classifier = CRFClassifier.getClassifier(serializedClassifier);
		} catch (Exception e) {
			e.printStackTrace();
		}

		List<List<CoreLabel>> out = classifier.classify(tweetsFromBolt.getText());
		for (List<CoreLabel> sentence : out) {
			//String s = "";
			//String prevLabel = null;
			for (CoreLabel word : sentence) {
	
				String label = word.get(CoreAnnotations.AnswerAnnotation.class);
				if (!label.equals("O"))
					namedEntities.add(word.toString());
			}
			

		}
		return namedEntities;
	}

	public void execute(Tuple tuple) {
		Status tweetsFromBolt = (Status) tuple.getValueByField("tweet");
		int sentiment = (Integer) tuple.getValueByField("sentiment");

		collector.ack(tuple);

		// Status tweetsFromBolt = tweets_from_bolt;

		for (String entities : getNamedEntities(tweetsFromBolt)) {
			if (!entities.isEmpty()) {
				String entity = entities.toLowerCase();

				this.collector.emit(new Values(entity.trim(), sentiment, "entity"));
				try {
					File file = new File("/s/chopin/a/grad/cnreddy/twitter/EntityBolt.txt");
					if (!file.exists()) {
						file.createNewFile();
					}
					FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write(entity.trim() + "\t" + sentiment + "\n");
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("entity", "sentiment", "type"));

	}

}
