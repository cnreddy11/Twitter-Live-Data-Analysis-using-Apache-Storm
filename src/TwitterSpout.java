############################################
##  
##  Author : Nikhila Chireddy
##
############################################


import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;

import twitter4j.*;
import twitter4j.auth.AccessToken;

public class TwitterSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream twitterStream;
	PrintWriter _log;

	String consumerKey = "8BAlVNjhdXQMVe8SNoldOUYOF";
	String consumerSecret = "YbfWgk8LyZUltiBqMGLuDYynmvBlewbsd2gpcGf2rnhbKXURrD";
	String accessToken = "925746575682531328-3rxhBjr9Dmv8pjZdTpYXAYbFZDdciRc";
	String accessTokenSecret = "vmi9bSwHNxJwMvTctII68shaGKerwdsVYtHbO4zpZqD9T";

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;
		try {
			_log = new PrintWriter(new File("/s/chopin/a/grad/cnreddy/tweets.txt"));
		} catch (Exception e) {
			System.out.println("Error in writing to file");
			e.printStackTrace();
		}

		StatusListener listener = new StatusListener() {

			public void onStatus(Status status) {
				try {
					List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
					LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
							.withProfiles(languageProfiles).build();
					TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
					TextObject textObject = textObjectFactory.forText(status.getText());
					com.google.common.base.Optional<LdLocale> lang = languageDetector.detect(textObject);
					FileWriter fileWriter = new FileWriter(
							"/s/chopin/a/grad/cnreddy/twitter/Spout.txt", true);
					BufferedWriter bw = new BufferedWriter(fileWriter);
					if (lang.isPresent()) {
						if (lang.get().getLanguage().toLowerCase().trim().equalsIgnoreCase("en")) {
							queue.put(status);
							bw.write(status.getText());
							bw.write("\n");

							bw.flush();
						}

						// System.out.println(status.getText());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

			}

			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			public void onTrackLimitationNotice(int i) {
			}

			public void onScrubGeo(long l, long l1) {
			}

			public void onException(Exception ex) {
			}

			public void onStallWarning(StallWarning arg0) {

			}
		};

		ConfigurationBuilder cb = new ConfigurationBuilder();

		cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
				.setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);

		twitterStream = new TwitterStreamFactory(new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		twitterStream.addListener(listener);
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);
		twitterStream.sample();

	}

	public void nextTuple() {
		Status ret = queue.poll();

		if (ret == null) {
			Utils.sleep(50);
		} else {
			_collector.emit(new Values(ret));
			_log.write(ret.getText() + "\n");
			_log.flush();
		}
	}

	public void close() {
		twitterStream.shutdown();
	}

	public Map<String, java.lang.Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	public void ack(Object id) {
	}

	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweets"));
	}

}