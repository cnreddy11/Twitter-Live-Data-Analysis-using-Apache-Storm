/*
   Author : Nikhila Chireddy
   Date : 10-24-2017
*/


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class TwitterTopologyParallel {
	public static double e;
	public static double threshold;
	public static void main(String[] args) throws Exception {

		Config conf = new Config();

		conf.setDebug(true);
		//conf.setNumWorkers(4);

		TopologyBuilder builder = new TopologyBuilder();
		if(args.length > 2){
			threshold = Double.parseDouble(args[2]);
		}
		e = Double.parseDouble(args[1]);
		builder.setSpout("twitter-spout", new TwitterSpout());
		builder.setBolt("sentiment-bolt", new SentimentBolt()).shuffleGrouping("twitter-spout");
		builder.setBolt("Hashtag-bolt", new HashTagBolt()).shuffleGrouping("sentiment-bolt");
		builder.setBolt("Entity-bolt", new NamedEntityBolt()).shuffleGrouping("sentiment-bolt");
		LossyCountingParallel ls1 = new LossyCountingParallel(e, threshold);
		LossyCountingParallel ls2 = new LossyCountingParallel(e, threshold);
		builder.setBolt("HashTag-Lossy", ls1,4).setNumTasks(8).fieldsGrouping("Hashtag-bolt", new Fields("hashTag"));
		builder.setBolt("Entity-Lossy", ls2,4).setNumTasks(8).fieldsGrouping("Entity-bolt", new Fields("entity"));
		builder.setBolt("Report-HashTag-Bolt", new OutputBoltParallel()).globalGrouping("HashTag-Lossy");
		builder.setBolt("Report-Entity-Bolt", new OutputBoltParallel()).globalGrouping("Entity-Lossy");

		StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

		
		Utils.sleep(10000);
	}
}
