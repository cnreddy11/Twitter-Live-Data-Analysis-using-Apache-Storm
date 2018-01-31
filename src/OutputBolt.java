############################################
##  
##  Author : Nikhila Chireddy
##
############################################


import java.util.Date;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.FileWriter;

import java.text.SimpleDateFormat;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class OutputBolt extends BaseRichBolt {

	private FileWriter fileWriter1;
	private BufferedWriter bw1;
	private FileWriter fileWriter2;
	private BufferedWriter bw2;
	private long time;
	static String type = null;
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {

		try {
			fileWriter1 = new FileWriter("/s/chopin/a/grad/cnreddy/twitter/HashTags.txt", true);
			fileWriter2 = new FileWriter("/s/chopin/a/grad/cnreddy/twitter/NamedEntities.txt", true);
			bw1 = new BufferedWriter(fileWriter1);
			bw2 = new BufferedWriter(fileWriter2);

		} catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILE :: 1 ");
			e.printStackTrace();
		}

	}

	public void execute(Tuple tuple) {

		type = tuple.getStringByField("type");
		String list = tuple.getStringByField("list");
		time = tuple.getLongByField("time");
		//displayOutput(list);
		Date resultdate = new Date(time);
		try {
			if (type.equalsIgnoreCase("entity")) {
				bw2.write("<" + dateFormat.format(resultdate) + ">" + list + "\n");
				bw2.flush();
			} else {
				bw1.write("<" + dateFormat.format(resultdate) + ">" + list + "\n");
				bw1.flush();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * public void displayOutput(String list)
	 * 
	 * {
	 * 
	 * Date resultdate = new Date(time); try { if
	 * (type.equalsIgnoreCase("entity")) { bw2.write("<" +
	 * dateFormat.format(resultdate) + ">" + list + "\n"); bw2.flush(); } else {
	 * bw1.write("<" + dateFormat.format(resultdate) + ">" + list + "\n");
	 * bw1.flush(); } } catch (Exception e) { e.printStackTrace(); } }
	 */

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public void cleanup() {
		try {
			bw1.close();
			bw2.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
