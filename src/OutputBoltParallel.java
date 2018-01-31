/*
   Author : Nikhila Chireddy
   Date : 10-24-2017
*/


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.FileWriter;

import java.text.SimpleDateFormat;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class OutputBoltParallel extends BaseRichBolt {

	private FileWriter fileWriter1;
	private BufferedWriter bw1;
	private FileWriter fileWriter2;
	private BufferedWriter bw2;
	private int freq;
	static String type = null;
	long startTime = System.currentTimeMillis();
	HashMap<String, Integer> hm = new HashMap<String, Integer>();
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {

		try {
			fileWriter1 = new FileWriter("/s/chopin/a/grad/cnreddy/twitterParallel/HashTags.txt", true);
			fileWriter2 = new FileWriter("/s/chopin/a/grad/cnreddy/twitterParallel/NamedEntities.txt", true);
			bw1 = new BufferedWriter(fileWriter1);
			bw2 = new BufferedWriter(fileWriter2);

		} catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILE :: 1 ");
			e.printStackTrace();
		}
		startTime = System.currentTimeMillis();

	}

	public void execute(Tuple tuple) {

		type = tuple.getStringByField("type");
		String list = tuple.getStringByField("list");
		freq = tuple.getIntegerByField("freq");
		//long time = tuple.getLongByField("time");
		//long currentTime = System.currentTimeMillis();
		displayOutput(list,freq);
		/*if(time < startTime + 10000){
			if(!hm.containsKey(list))
				hm.put(list, freq);
		}*/
	}
	
	public void displayOutput(String list, int freq){
		long currentTime = System.currentTimeMillis();
		if(currentTime >= startTime + 10000 && !hm.isEmpty()){
			LinkedHashMap<String, Integer> lhm = sortHashMap(hm);
			Collection<String> str;
			if(lhm.size()>100){
				str = Collections.list(Collections.enumeration(lhm.keySet())).subList(0, 100);
			}
			str = (Collection<String>) lhm.keySet();
			Date resultdate = new Date(currentTime);
			try {
				if (type.equalsIgnoreCase("entity")) {
					bw2.write("<" + dateFormat.format(resultdate) + ">" + str.toString() + "\n");
					bw2.flush();
				} else {
					bw1.write("<" + dateFormat.format(resultdate) + ">" + str.toString() + "\n");
					bw1.flush();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			hm.clear();
			startTime = currentTime;
		}
		else{
			if(!hm.containsKey(list))
				hm.put(list, freq);
		}
	}
	
	
	public LinkedHashMap<String, Integer> sortHashMap(HashMap<String, Integer> passedMap) {
		List<String> mapKeys = new ArrayList<String>(passedMap.keySet());
		List<Integer> mapValues = new ArrayList<Integer>(passedMap.values());
		Collections.sort(mapValues, Collections.reverseOrder());
		Collections.sort(mapKeys, Collections.reverseOrder());

		LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();

		Iterator<Integer> valueIt = mapValues.iterator();
		while (valueIt.hasNext()) {
			Integer val = valueIt.next();
			Iterator<String> keyIt = mapKeys.iterator();

			while (keyIt.hasNext()) {
				String key = keyIt.next();
				Integer comp1 = passedMap.get(key);
				Integer comp2 = val;

				if (comp1.equals(comp2)) {
					keyIt.remove();
					sortedMap.put(key, val);
					break;
				}
			}
		}
		return sortedMap;
	}

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
