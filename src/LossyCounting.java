############################################
##  
##  Author : Nikhila Chireddy
##
############################################


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class LossyCounting extends BaseRichBolt {

	static int sentiment;
	static String type = null;
	private OutputCollector collector;
	private int numElements = 0;
	private Map<String, Objects> window = new ConcurrentHashMap<String, Objects>();
	private double epsilon;
	private double thresh;

	private int currBucket = 1;
	private int bucketSize = (int) Math.ceil(1 / epsilon);
	private static long startTime;

	public LossyCounting(double e, double threshold){
		epsilon = e;
		thresh = threshold;
	}
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {

		this.collector = collector;
		startTime = System.currentTimeMillis();
	}

	public void execute(Tuple tuple) {
		type = tuple.getStringByField("type");
		
		String word;
		if (type.equalsIgnoreCase("entity")) {
			word = tuple.getStringByField("entity");
			sentiment = tuple.getIntegerByField("sentiment");
		} else {
			word = tuple.getStringByField("hashTag");
			sentiment = tuple.getIntegerByField("sentiment");
		}
		lossyCounting(word);
	}

	public void lossyCounting(String word) {
		if (numElements < bucketSize) {
			if (!window.containsKey(word)) {
				Objects d = new Objects();
				d.delta = currBucket - 1;
				d.freq = 1;
				d.element = word;
				d.svalue = sentiment;
				window.put(word, d);
			} else {
				Objects d = window.get(word);
				d.freq += 1;
				d.svalue = d.svalue + sentiment;
				window.put(word, d);
			}
			numElements += 1;
		}
		
		long currentTime = System.currentTimeMillis();
		if (currentTime >= startTime + 10000) {
			if (!window.isEmpty()) {
				HashMap<String, Integer> emittingList = new HashMap<String, Integer>();
				for (String str_loss : window.keySet()) {
					boolean b = check(str_loss);
					if (b) {
						Objects d = window.get(str_loss);
						float finalSent = (float)d.svalue / d.freq;
						d.finalSent = finalSent;
						emittingList.put(str_loss, d.freq);
					}
				}
				if (!emittingList.isEmpty()) {
					/*try {
						
							bw1.write("<" + emittingList.toString() + ">\n");
							bw1.flush();
						
					} catch (Exception e) {
						e.printStackTrace();
					}*/
					//System.out.println(emittingList.toString());
					LinkedHashMap<String, Integer> lhm = sortHashMap(emittingList);
					Collection<String> str;
					if(lhm.size()>100){
						str = Collections.list(Collections.enumeration(lhm.keySet())).subList(0, 100);
					}
					str = (Collection<String>) lhm.keySet();
					LinkedHashMap<String, Integer> finalEmit = new LinkedHashMap<String, Integer>();
					for(String s: str){
						finalEmit.put("<"+s+":"+window.get(s).finalSent+">", window.get(s).freq);
					}
					collector.emit(new Values(finalEmit.keySet().toString(), type, currentTime));
					//System.out.println("str:" + str.toString());	
				}
			}
			startTime = currentTime;

		}
		if (bucketSize == numElements) {
			Delete();
			numElements = 0;
			currBucket += 1;
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

	public boolean check(String lossyWord) {
		if(thresh == -1.0){
			return true;
		}
		else{
			Objects d= window.get(lossyWord);
			double a = (thresh - epsilon) * numElements;
			if(d.freq >= a)
				return true;
			else 
				return false;
		}
	}

	public void Delete() {
		for (String word : window.keySet()) {
			Objects d = window.get(word);
			double sum = d.freq + d.delta;
			if (sum <= currBucket) {
				window.remove(word);
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("list", "type","time"));
	}

}
