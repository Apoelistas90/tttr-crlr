package com.andy.id.twittercrawler;


import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

//import org.apache.commons.lang3.StringUtils;
//import db.MySqlConn;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;


public class KafkaProducer {
	
	private final static Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);
	private final static ConfigurationBuilder cb = new ConfigurationBuilder();
	private final static String twitter_cfg_path = System.getProperty("user.home") + File.separator + "twitter_credentials.cfg";
	private final static Properties twitter_props = new Properties();
	private static InputStream input = null;
	
/*
 * to test run a local consumer:
 * ./kafka-console-consumer.sh ec2-52-212-186-139.eu-west-1.compute.amazonaws.com:9092 --topic footy_tweets --from-beginning --zookeeper ec2-52-212-186-139.eu-west-1.compute.amazonaws.com:2181
 */
	
	 public static void main(String[] args) {
		
		try{
			input = new FileInputStream(twitter_cfg_path);
			// load a properties file
			twitter_props.load(input);
			cb.setDebugEnabled(true);
			cb.setOAuthConsumerKey(twitter_props.getProperty("ConsumerKey"));
			cb.setOAuthConsumerSecret(twitter_props.getProperty("ConsumerSecret"));
			cb.setOAuthAccessToken(twitter_props.getProperty("AccessToken"));
			cb.setOAuthAccessTokenSecret(twitter_props.getProperty("AccessTokenSecret"));
		}
		catch(Exception e){
			
		}
		
		 
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		 
		 /** Producer properties **/
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
			
		ProducerConfig config = new ProducerConfig(props);
			
		final Producer<String, String> producer = new Producer<String, String>(config);
		 
		final String all_hashtags[] = {"#arsenal","#arsenalfc", "#afc", "#gunners", "#thegunners","#gooners","#comeongunners", "#comeongooners" ,"#comeonyougunners", "#coyg", "#comeonarsenal","#arsenalfamily", "#afcfamily",
				 "#astonvillafc", "#villans", "#avfc", "#astonvilla", "#govilla", "#comeonastonvilla", "#comeonvilla", "#comeonyouvillans", "#villafamily", "#avfcfamily",
				 "#cardiff","#cardiffcity" , "#ccfc" , "#comeoncardiff" , "#cardifffc","#cardiffcityfamily","#cccfcfamily","#comeoncardiff","#comeonyoucardiff",
				 "#chelsea","#chelseafc","#cfc","#ktbffh", "#gochelsea", "#cfcfamily", "#letsgochelsea", "#comeonchelsea" , "#comeoncfc", "#cfcfamily", "#chelseafcfamily","#cfcfamily",
				 "#crystalpalace","#crystalpalacefc" , "#cpfc","#crystalpalacefamily","#cpfcfamily","#comeoncrystalpalace",
				 "#everton", "#evertonfc", "#efc", "#evertonfamily", "#schoolofscience", "#toffees", "#toffeemen","#coyb", "#comeoneverton","#comeontoffees", "#goeverton","#efcfamily",
				 "#fulham","#fulhamfc", "#cottagers", "#ffc", "#gofulham","#fulhamfamily", "#comeonfulham", "#comeonyouwhites", "#coyws","#ffcfamily",
				 "#hull","#hullfc", "#hullcityfc","#hcafc", "#hullcity", "#hcfc", "#comeonhull", "#hullcityfamily","#comeonyouhull",
				 "#liverpool", "#liverpoolfc","#lfc","#goliverpool","#youwillneverwalkalone","#ynwa","#neverwalkalone","#comeonlfc", "#comeonliverpool", "#redordead", "#lfcfamily",
				 "#manchestercity", "#manchestercity", "#manchestercityfc","#mancity", "#mancityfc" ,"#citizens" ,"#mcfc","#gomcfc" ,"#gomancity" ,"#comeonmancity","#comeoncitizens","#mancityfamily","#mcfcfamily",
				 "#manchesterunited", "#manchesterunitedfc", "#manutd", "#manunited", "#manunitedfc", "#reddevils" ,"#mufc", "#gomanutd", "#gomanunited", "#foreverunited" ,"#gomufc", "#gloryglorymanunited", "#ggmu","#manutdfamily","#mcfcfamily",
				 "#newcastle","#newcastleunited","#nufc", "newcastlefc", "#nufcroundup","#comeonnewcastle","#toonarmy","#nufcfamily",
				 "#norwichfc", "#norwichcity", "#norwichcityfc", "#comeonyoucanaries", "#ncfc", "#comeonnorwich","#ncfcfamily",
				 "#southampton","#southamptonfc", "#saints", "#saintsfc", "#upthesaints", "#comeonsouthampton","southamptonfcfamily",
				 "#stoke","#stokecity", "#stokecityfc", "#scfc", "#comeonstoke", "#comeonyoustoke", "#comeonpotters", "#comeonyoupotters","#scfcfamily",
				 "#sunderland", "#sunderlandfc", "#safc", "#mackems", "#comeonblackcats", "#comeonsunderland", "#comeonyoumackems", "#comeonyousunderland", "#comeonyoublackcats", "#gosunderland", "#goblackcats","#safcfamily",
				 "#swansea","#swanseacity","#swanseacityfc","#swanseafc", "#swans", "#swansfc", "#jackarmy", "#comeonswansea", "#twitterjacks","#comeonyoujacks",
				 "#tottenham", "#tottenhamfc", "#tottenhamhotspur", "#tottenhamhotspurfc", "#yids", "#yidarmy", "#comeonyouspurs", "#coys", "#thfc", "#hotspurs", "#spursfamily","#thfcfamily",
				 "#westbromwich","#westbromwichalbionfc", "#westbromwichfc", "#westbromfc", "#westbrom", "#baggies", "#wba", "#wbafc", "#wbfc", "#comeonbaggies", "#comeonyoubaggies", "#gowba", "#gobaggies",
				 "#westham","#westhamunited", "#westhamunitedfc", "#whu", "#whufc","#westhamfamily", "#whtid", "#comeonwestham", "#comeonyouirons", "#coyi", "#comeonwestham", "#gowestham","#whufcfamily"};
		 
			 
		 StatusListener listener = new StatusListener() {
			
			String createdAt,userScreenName,tweet,coord,kafka_message;
			int userFollowers,retweetCount,favoriteCount;			
			String regex = "\\'";
			String replacement = "";			
			
			public void onStatus(Status status) {
						
				if (status.getLang().equals("en")){
					createdAt  = status.getCreatedAt().toString();						
					userScreenName = status.getUser().getScreenName().toString();	
					tweet = status.getText().toString();
					
					if(status.getGeoLocation()!=null){
						double lat = status.getGeoLocation().getLatitude();
						double lon = status.getGeoLocation().getLongitude();
						coord = lat+","+lon;
					}else{
						coord = "null";
					}
					
					userFollowers = status.getUser().getFollowersCount();											
					retweetCount = status.getRetweetCount();
					favoriteCount = status.getFavoriteCount();
					
					//System.out.println("Tweet:" + tweet);
										
					tweet = tweet.replaceAll(regex,replacement);
					
					kafka_message="\""+tweet+"\","
							+createdAt+","
							+userScreenName+","
							+coord+","
							+userFollowers+","
							+retweetCount+","
							+favoriteCount+","
							;				
					
					KeyedMessage<String, String> data = 
							new KeyedMessage<String, String>("footy_tweets", kafka_message);//DataObjectFactory.getRawJSON(status));
					producer.send(data);
					
					//check if tweet is here
					//*****need to think about retweets count

				}
				
			}
			
			public void onException(Exception arg0) {
				// TODO Auto-generated method stub				
			}

			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub				
			}
			
			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub
				
			}

			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}

			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}
			 
		 };
		 
		 FilterQuery fq = new FilterQuery();
		 fq.track(all_hashtags);
		 twitterStream.addListener(listener);
		 LOG.info("Listener Added!!");
		 twitterStream.filter(fq); 
	     LOG.info("Filters set!");
		 
	 }
}