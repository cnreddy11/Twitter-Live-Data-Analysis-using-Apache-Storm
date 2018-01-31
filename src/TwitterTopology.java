############################################
##  
##  Author : Nikhila Chireddy
##
############################################


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class TwitterTopology {
	public static double e;
	public static String filename = null;
	public static Double threshold = -1.0;
	public static void main(String[] args) throws Exception {

		Config conf = new Config();

		conf.setDebug(true);
		// conf.setNumWorkers(4);

		TopologyBuilder builder = new TopologyBuilder();
		e = Double.parseDouble(args[1]);
		if(args.length > 2){
			threshold = Double.parseDouble(args[2]);
		}
		builder.setSpout("twitter-spout", new TwitterSpout());
		builder.setBolt("sentiment-bolt", new SentimentBolt()).shuffleGrouping("twitter-spout");
		builder.setBolt("Hashtag-bolt", new HashTagBolt()).shuffleGrouping("sentiment-bolt");
		builder.setBolt("Entity-bolt", new NamedEntityBolt()).shuffleGrouping("sentiment-bolt");

		LossyCounting lc1 = new LossyCounting(e, threshold);
		LossyCounting lc2 = new LossyCounting(e, threshold);
		builder.setBolt("HashTag-Lossy", lc1).shuffleGrouping("Hashtag-bolt");
		builder.setBolt("Entity-Lossy", lc2).shuffleGrouping("Entity-bolt");
		builder.setBolt("Report-HashTag-Bolt", new OutputBolt()).globalGrouping("HashTag-Lossy");
		builder.setBolt("Report-Entity-Bolt", new OutputBolt()).globalGrouping("Entity-Lossy");

		StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

		// LocalCluster cluster = new LocalCluster();

		// cluster.submitTopology("test", conf, builder.createTopology());

		Utils.sleep(10000);
	}
}
