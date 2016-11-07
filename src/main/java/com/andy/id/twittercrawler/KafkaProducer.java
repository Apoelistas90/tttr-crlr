package com.andy.id.twittercrawler;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
	
	
	 public static void main(String[] args) throws TwitterException {
		
	
		final String teams[] = new String[20];
		teams[0] = "arsenal";
		teams[1] = "astonvilla";
		teams[2] = "cardiff";
		teams[3] = "chelsea";
		teams[4] = "crystalpalace";
		teams[5] = "everton";
		teams[6] = "fulham";
		teams[7] = "hull";
		teams[8] = "liverpool";
		teams[9] = "mcity";
		teams[10] = "munited";
		teams[11] = "newcastle";
		teams[12] = "norwich";
		teams[13] = "southampton";
		teams[14] = "stoke";
		teams[15] = "sunderland";
		teams[16] = "swansea";
		teams[17] = "tottenham";
		teams[18] = "westbrom";
		teams[19] = "westham";
		
		//final MySqlConn conn = new MySqlConn(db, user, psw, hostname, port);
		
		 
		 ConfigurationBuilder cb = new ConfigurationBuilder();
		 cb.setDebugEnabled(true);
		 cb.setOAuthConsumerKey("L0LJ9GQb1Q2UdIPY96w3yw");
		 cb.setOAuthConsumerSecret("r2s6hS709x7gNaIOHzO7mOxaECuRIhapDU0zNo84UTg");
		 cb.setOAuthAccessToken("199872881-cuNNuDTolVYNlVF4Cv49Bhm4EqC63dXD8S3E8IWD");
		 cb.setOAuthAccessTokenSecret("0pkRLD9Dg5qPlpAT7PufdGcCQUJyIChbRmLmooNX6BRzk");
		 
		 TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		 
		 /** Producer properties **/
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
			
		ProducerConfig config = new ProducerConfig(props);
			
		final Producer<String, String> producer = new Producer<String, String>(config);
		 
		 String arsenal[] = {"#arsenal","#arsenalfc", "#afc", "#gunners", "#thegunners","#gooners","#comeongunners", "#comeongooners" ,"#comeonyougunners", "#coyg", "#comeonarsenal","#arsenalfamily", "#afcfamily"};
		 String astonvilla[] = { "#astonvillafc", "#villans", "#avfc", "#astonvilla", "#govilla", "#comeonastonvilla", "#comeonvilla", "#comeonyouvillans", "#villafamily", "#avfcfamily"};
		 String cardiff[] = { "#cardiff","#cardiffcity" , "#ccfc" , "#comeoncardiff" , "#cardifffc","#cardiffcityfamily","#cccfcfamily","#comeoncardiff","#comeonyoucardiff"};
		 String chelsea[] = {"#chelsea","#chelseafc","#cfc","#ktbffh", "#gochelsea", "#cfcfamily", "#letsgochelsea", "#comeonchelsea" , "#comeoncfc", "#cfcfamily", "#chelseafcfamily","#cfcfamily"};
		 String crystalpalace[] = {"#crystalpalace","#crystalpalacefc" , "#cpfc","#crystalpalacefamily","#cpfcfamily","#comeoncrystalpalace"};
		 String everton[] = {"#everton", "#evertonfc", "#efc", "#evertonfamily", "#schoolofscience", "#toffees", "#toffeemen","#coyb", "#comeoneverton","#comeontoffees", "#goeverton","#efcfamily"};
		 String fulham[] = { "#fulham","#fulhamfc", "#cottagers", "#ffc", "#gofulham","#fulhamfamily", "#comeonfulham", "#comeonyouwhites", "#coyws","#ffcfamily"};
		 String hull[] = {"#hull","#hullfc", "#hullcityfc","#hcafc", "#hullcity", "#hcfc", "#comeonhull", "#hullcityfamily","#comeonyouhull"};
		 String liverpool[] = {"#liverpool", "#liverpoolfc","#lfc","#goliverpool","#youwillneverwalkalone","#ynwa","#neverwalkalone","#comeonlfc", "#comeonliverpool", "#redordead", "#lfcfamily"};
		 String mcity[] = {"#manchestercity", "#manchestercity", "#manchestercityfc","#mancity", "#mancityfc" ,"#citizens" ,"#mcfc","#gomcfc" ,"#gomancity" ,"#comeonmancity","#comeoncitizens","#mancityfamily","#mcfcfamily"};
		 String munited[] = {"#manchesterunited", "#manchesterunitedfc", "#manutd", "#manunited", "#manunitedfc", "#reddevils" ,"#mufc", "#gomanutd", "#gomanunited", "#foreverunited" ,"#gomufc", "#gloryglorymanunited", "#ggmu","#manutdfamily","#mcfcfamily"};
		 String newcastle[] = {"#newcastle","#newcastleunited","#nufc", "newcastlefc", "#nufcroundup","#comeonnewcastle","#toonarmy","#nufcfamily"};
		 String norwich[] = {"#norwichfc", "#norwichcity", "#norwichcityfc", "#comeonyoucanaries", "#ncfc", "#comeonnorwich","#ncfcfamily"};
		 String southampton[] = {"#southampton","#southamptonfc", "#saints", "#saintsfc", "#upthesaints", "#comeonsouthampton","southamptonfcfamily"};
		 String stoke[] = {"#stoke","#stokecity", "#stokecityfc", "#scfc", "#comeonstoke", "#comeonyoustoke", "#comeonpotters", "#comeonyoupotters","#scfcfamily"};
		 String sunderland[] = {"#sunderland", "#sunderlandfc", "#safc", "#mackems", "#comeonblackcats", "#comeonsunderland", "#comeonyoumackems", "#comeonyousunderland", "#comeonyoublackcats", "#gosunderland", "#goblackcats","#safcfamily"};
		 String swansea[] = {"#swansea","#swanseacity","#swanseacityfc","#swanseafc", "#swans", "#swansfc", "#jackarmy", "#comeonswansea", "#twitterjacks","#comeonyoujacks"};
		 String tottenham[] = {"#tottenham", "#tottenhamfc", "#tottenhamhotspur", "#tottenhamhotspurfc", "#yids", "#yidarmy", "#comeonyouspurs", "#coys", "#thfc", "#hotspurs", "#spursfamily","#thfcfamily"};
		 String westbrom[] = {"#westbromwich","#westbromwichalbionfc", "#westbromwichfc", "#westbromfc", "#westbrom", "#baggies", "#wba", "#wbafc", "#wbfc", "#comeonbaggies", "#comeonyoubaggies", "#gowba", "#gobaggies"};
		 String westham[] = {"#westham","#westhamunited", "#westhamunitedfc", "#whu", "#whufc","#westhamfamily", "#whtid", "#comeonwestham", "#comeonyouirons", "#coyi", "#comeonwestham", "#gowestham","#whufcfamily"};
		 
		 String all[] = {"#arsenal","#arsenalfc", "#afc", "#gunners", "#thegunners","#gooners","#comeongunners", "#comeongooners" ,"#comeonyougunners", "#coyg", "#comeonarsenal","#arsenalfamily", "#afcfamily",
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
		 
		 final List<String[]> hashtags = new ArrayList<String[]>();
		 hashtags.add(arsenal);hashtags.add(astonvilla); hashtags.add(cardiff);hashtags.add(chelsea);
		 hashtags.add(crystalpalace);hashtags.add(everton); hashtags.add(fulham);hashtags.add(hull);
		 hashtags.add(liverpool);hashtags.add(mcity); hashtags.add(munited);hashtags.add(newcastle);
		 hashtags.add(norwich);hashtags.add(southampton); hashtags.add(stoke);hashtags.add(sunderland);
		 hashtags.add(swansea);hashtags.add(tottenham); hashtags.add(westbrom);hashtags.add(westham);
		 
		 StatusListener listener = new StatusListener() {
			
			int totalTweets = 0;
			String createdAt,userScreenName,tweet,coord;
			int userFollowers,retweetCount,favoriteCount;
			
			String regex = "\\'";
			String replacement = "";
			
			String kafka_message;
			
			@Override
			public void onException(Exception arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onStatus(Status status) {
						
				if (status.getLang().equals("en")){
			     	totalTweets++;
			    	
					createdAt  = status.getCreatedAt().toString();						
					userScreenName = status.getUser().getScreenName().toString();	
					tweet = status.getText().toString();
					if(String.valueOf(totalTweets).matches("([1][0]*|[5][0]*)")){
			    		System.out.println(createdAt + "  :  "+totalTweets +" tweets retieved so far!");
			    		System.out.println("The " +totalTweets+"th retrieved tweet is: " + tweet);
			    	};
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
					//need to remove apostrophes
					
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
							new KeyedMessage<String, String>("kafka.topic", kafka_message);//DataObjectFactory.getRawJSON(status));
					producer.send(data);
					
					//check if tweet is here
					//*****need to think about retweets count
					/*int i = 0;
					for(String[] array : hashtags){							
						for(String hashtag:array){							
							if(StringUtils.containsIgnoreCase(tweet, hashtag)){
							//if (tweet.contains(hashtag)){
							
							KeyedMessage<String, String> data = 
									new KeyedMessage<String, String>("kafka.topic", kafka_message);//DataObjectFactory.getRawJSON(status));
							producer.send(data);
							//(previously was adding new tweets to SQL table)
							}
							
						}
						i++;
					}*/

				}				
			}

			@Override
			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}
			 
		 };
		 
		 FilterQuery fq = new FilterQuery();
		 
		 fq.track(all);
		 
		 twitterStream.addListener(listener);
		 System.out.println("Listener added!");
		 twitterStream.filter(fq); 
		 System.out.println("Filters set!");		 
		 
	 }
}