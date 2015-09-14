package com.data.preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.bson.Document;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Preprocess {
	/**
	 * MongoDB Twitter���ݽ���Ԥ����
	 * @param args
	 * @throws Exception 
	 */
	
	public static  void exportCSVfile() throws Exception{
		
		MongoClient mongoClient = new MongoClient();
		MongoDatabase database = mongoClient.getDatabase("pakdd");
		
		MongoCollection<Document> twitterdata = database.getCollection("twitter");
		
		FindIterable<Document> curs;
		curs = twitterdata.find();
		
		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//twitter.csv"));
		for(Document object : curs){
			
			DateFormat df = new SimpleDateFormat("MM-dd-yyyy-HH-mm-ss");//?????
			String reportDate = df.format(object.get("timestamp"));
			
			bw.write((String) object.get("userid") + ","); // userid
			bw.write(reportDate + ","); // date
			bw.write(String.valueOf(object.get("tweetId")) + ","); // tweetID
			String tweet = (String) object.get("tweet");
			tweet = tweet.replaceAll("\n", "");
			tweet = tweet.replaceAll("\"", "");
			tweet = tweet.replaceAll("\r", "");tweet = tweet.replaceAll("\t", "");
			bw.write(tweet + "\n"); // tweet
		}

		bw.flush();bw.close();
		
	}
	
	public static  void countUsers() throws Exception{
		
		/**
		 * ��ͳ���û���
		 */
		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter.csv"));
		Set<String> userids; //contain user ids
		userids = new LinkedHashSet<String>();
		
		String line = null;
		int count = 0;
		
		List<String> lines = new ArrayList<String>();
		while((line = br.readLine()) != null){

			String[] data = line.split(",");
//			
			userids.add(data[0]);
			
		}
		
		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//userids.txt"));
		for(String user: userids){
			bw.write(user + "\n");
		}
		bw.flush();bw.close();
	}

	public static void saveEnglishTweets() throws Exception{
		/**
		 * tweet����Щ����Ӣ�ģ�Ӧ��ɾ��
		 */
		
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter.csv"));
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//newtwitter.csv"));
		
//		String line = null;
//		int ct = 0;
//		while((line = br.readLine()) != null){
//			//�е�line����һ��������document �������ڴ����ݿ⵼���������ļ�\n���ַ�ԭ������Ӧɾ��
//			
//			String[] fields = line.split(",");
//			if(fields.length >= 4){
//				bw.write(line + "\n");bw.flush();
//				ct ++;
////				System.out.println(line);
//			}
//			
//		}
//		bw.close();
//		
//		System.out.println("there are " + ct + "tweets left"); //6600669
//		
		
		////////////////////////////////////////////////////////////////////////////////////////////
//		//ɾ���ظ���tweet������tweetid��
//		BufferedReader br = null; BufferedWriter bw = null;
//		br = new BufferedReader(new FileReader("pakdd//newtwitter.csv"));
//		bw = new BufferedWriter(new FileWriter("pakdd//twitter2.csv"));
//		String line = null;
//		int count = 0;//tweet��Ŀ
//		Map<String, Integer> tweets = new LinkedHashMap<String, Integer>();
//		
//		while((line = br.readLine()) != null){
//			
//			String[] fields = line.split(","); //fields[2]�� tweetId
//			if(tweets.containsKey(fields[2]) == false){//��tweet
//				tweets.put(fields[2], count);
//				count ++;
//				bw.write(line + "\n");bw.flush();
//				
//			}
//		}
//		bw.close();
//		System.out.println("there are " + count + " tweets left"); //6390236
		
		/////////////////////////////////////////////////////////////////////////////////////////////////

		//ͳ�ƺ���url��tweet��Ŀ����һ�����������￴��������raw data������English tweet��NonEnglish tweet
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter2.csv"));
//		String line = null;
//		int count = 0;
//		
//		while((line = br.readLine()) != null){
//			
//			if(line.toLowerCase().contains("http") == true)
//				count ++;
//			
//		}
//		System.out.println("����" + count + "��tweet����url"); //����3342330��tweet����url
		
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		
//		//ɾ����ַ, ��Ϊ��Щurl���淶��������һ������english tweetʱ���ֳ���
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter2.csv"));
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//twitter3.csv"));
//		String line = null;
//		int count = 0;
//		
//		while((line = br.readLine()) != null){
////			line = line.replaceAll("https?://\\S+\\s?", ""); //�����ɾ�������https��  https:/�� �ȵȶ�������, ʹ���������ʽcount3922536
//			line = line.replaceAll("http\\S+", ""); //���Լ�д������ʽ
//			bw.write(line + "\n");bw.flush();
//			count ++;
//		}
//		bw.close();
//		System.out.println(count); //6390236
		
		////////////////////////////////////////////////////////////////////////////////////////////////////
		//ɾ����Ӣ��tweet
		
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter3.csv"));
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//twitter4.csv"));
//		
//		String line = null;
//		int count = 0;
//		
//		while((line = br.readLine()) != null){
//			boolean b = line.matches("^[\u0000-\u0080]+$");
//			if(b == true){
//				bw.write(line + "\n"); bw.flush();
//				count ++;
//			}
//		}
//		bw.close();
//		System.out.println(count); //5002842
		
		
		
		/////////////////////////////////////////////////////////////////////////////////////////////
		//ͳ��Ӣ��������@����Ŀ
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter4.csv"));
//		String line = null;
//		int count = 0;
//		
//		while(( line = br.readLine()) != null ){
//			if(line.contains("@") == true)
//				count ++;
//		}
//		System.out.println("����" + count + "����@����"); //������3206028����@����
		
		/////////////////////////////////////////////////////////////////////////////////////////////////
		//ͳ��Ӣ��������hashtag��Ŀ
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter4.csv"));
//		String line = null;
//		int count = 0;
//		
//		while(( line = br.readLine()) != null ){
//			if(line.contains("#") == true)
//				count ++;
//		}
//		System.out.println("����" + count + "����# hashtag"); //����1497635����# hashtag
		
		
		//	/////////////////////////////////////////////////////////////////////////////////////////////////
		
		//ɾ�� �������ַ�@screename, ȫ��תΪСдtoLowerCase()
		
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter4.csv"));
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//twitter5.csv"));
//		String line = null;
//		int count = 0;
//		
//		while(( line = br.readLine()) != null){
//			
//			line = line.replaceAll("@\\S+", "");
//			line = line.toLowerCase();
//			bw.write(line + "\n"); bw.flush();
//			
//		}
//		bw.close();
		
		/////////////////////////////////////////////////////////////////////////////////////////////////
		
		// ����hashtag��ô���� ��һ�ִ���ʽɾ��#���ţ��� hashtag������ͨword����, ͬʱ��Щ�в�����tweet content,ɾ����
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter5.csv"));
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//twitter6.csv"));
//		String line = null;
//		int count = 0;
//		
//		while(( line = br.readLine()) != null){
//			line = line.replaceAll("#", "");
//			String[] attributes = line.split(",");
//			if(attributes.length >= 4){
//				bw.write(line + "\n"); bw.flush();
//			}
//			
//		}
//		bw.close();
		
		///////////////////////////////////////////////////////////////////////////////////////////////
		//���hashtag �۲�����
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter4.csv"));
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//hashtag&frequency.txt"));
//		Map<String, Integer> hashtag2id = new TreeMap<String, Integer>();
//		String line = null;
//		int count = 0;
//		
//		while(( line = br.readLine()) != null){
//			
//			String[] attributes = line.split(","); //attributes[3]��tweet content
//			if(attributes.length >= 4){
//				String[] words = attributes[3].split(" ");
//				for(int i=0;i<words.length;i++){
//					if(words[i].startsWith("#") == true){
//						if(hashtag2id.containsKey(words[i]) == true){
//							int times = hashtag2id.get(words[i]);
//							times ++;
//							hashtag2id.put(words[i], times);
//							
//						}else
//							hashtag2id.put(words[i],1);
//					}
////						hashtag2id.put(words[i], 1);
//				}
//			}
//		}//while
//		Map<String, Integer> sortedmap = Preprocess.sortByValue(hashtag2id);
//		for(Map.Entry<String, Integer> entry: sortedmap.entrySet()){
//			bw.write(entry.getKey() + "   " + entry.getValue()+"\n");bw.flush();
//		}
//		
//		bw.close();
//		
		
		
		///////////////////////////////////////////////////////////////////////////////////////////
	
		//delete stop words
		//��https://sites.google.com/site/kevinbouge/stopwords-lists ���ص�stop words
//		BufferedReader brr = new BufferedReader(new FileReader("pakdd//stopwords_en.txt"));
//		List<String> stopwords = new ArrayList<String>();
//		String line = null;
//		
//		while((line = brr.readLine()) != null){
//			stopwords.add(line);
//		}
//		brr.close();
//		
//		
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter6.csv"));
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//twitter7.csv"));
//		int ct = 0;
//		while(( line = br.readLine()) != null){
//			
//			String[] data = line.split(",");
//			int l1 = data[0].length();
//			int l2 = data[1].length();
//			int l3 = data[2].length();
//			
//			line = line.substring(l1+l2+l3+3, line.length());
//			
//			List<String> linewords = new ArrayList<String>();
//			Scanner sc = new Scanner(line);
//			sc.useDelimiter("[^A-Za-z0-9]+"); //word format/////////////////����Ҳ����Ҫ,���Ǳ������ְ�,���������ֻ���ɾ����!!!!!
//			while(sc.hasNext()){
//				
//				String word = sc.next(); //�����Ѿ���word�˰ɣ�����Ҫ������ж���
//				boolean tag = true;
//				for(int i=0; i<word.length();i++){
//					char c = word.charAt(i);
//					if(Character.isLetter(c)==false){//if a word
//						tag = false; // not a word, pass
//						break;
//					}
//				}
//				if(tag == true){//is a word
//					//�ж��Ƿ���stop word
//					boolean st = false;
//					for(String sw: stopwords){
//						if(sw.contentEquals(word) == true){
//							st = true;
//							break;
//						}
//					}
//					if(st == false)
//						linewords.add(word);
//				}
//			}//end while
//			
//			//�����һ�е�tweet������Ŀ>=2 �ű�������tweet
//			if(linewords.size() >=2){
//				ct ++;
//				bw.write(data[0] + "," + data[1] + "," + data[2] + ",");
//				for(int ii=0;ii<linewords.size()-1;ii++){
//					bw.write(linewords.get(ii) + " ");
//				}
//				bw.write(linewords.get(linewords.size()-1) + "\n"); bw.flush();
//
//			}
//		}
//		bw.close();
//		System.out.println("there are " + ct + " tweets left"); /////there are 4590923 tweets left
//		
		
		
		/////////////////////////////////////////////////////////////////////////////////////////////
		//����2015���tweet
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter7.csv"));
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//twitter8.csv"));
//		String line = null;
//		while(( line = br.readLine()) != null){
//			String[] attributes = line.split(",");
//			String year = attributes[1].substring(6, 10);
//			if(year.contentEquals("2015") == true){
//				bw.write(line + "\n");
//				bw.flush();
//			}
//		}//while
//		bw.close();
		
	//////////////////////////////////////////////////////////////////////////////////////////////////////
		//ͳ��ÿ��user��tweet��Ŀ
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter8.csv"));
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//2015user&tweetscounts.txt"));
//		String line = null;
//		Map<String, Integer> user2counts = new HashMap<String, Integer>();
//		
//		while((line = br.readLine()) != null){
//			String[] attributes = line.split(",");
//			if(user2counts.containsKey(attributes[0]) == true){
//				int times = user2counts.get(attributes[0]);
//				times ++;
//				user2counts.put(attributes[0], times);
//			}
//			else
//				user2counts.put(attributes[0], 1);
//		}
//		
//		for(Map.Entry<String, Integer> entry : user2counts.entrySet()){
//			bw.write(entry.getKey() + "," + entry.getValue() + "\n");
//		}
//		bw.flush();bw.close();
		
		////////////////////////////////////////////////////////////////////////////////////////////////
		//ͳ�ƴʱ�ʹ�Ƶ
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter8.csv"));
//		Map<String, Integer> word2fre = new TreeMap<String, Integer>(); //word and its frequency
//		String line = null;
//		while((line = br.readLine()) != null){
//			String[] attributes = line.split(","); //attributes[3] is tweet's content
//			String[] words = attributes[3].split(" ");
//			for(String word: words){
//				if(word2fre.containsKey(word) == true){
//					int times = word2fre.get(word);
//					times ++;
//					word2fre.put(word, times);
//				}
//				else
//					word2fre.put(word, 1);
//			}
//		}//while
//		
//		Map<String, Integer> sortedmap = Preprocess.sortByValue(word2fre);
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//word2fre.txt"));
//		for(Map.Entry<String, Integer> entry: sortedmap.entrySet()){
//			bw.write(entry.getKey() +  "," + entry.getValue() + "\n");
//			bw.flush();
//		}
//		
//		bw.close();
//		
//		
//		
		
		
		/////////////////////////////////////////////////////////////////////////////////////////////
		//��tweet��word ���д��λ�ԭ
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//word2stem.txt"));
//		Map<String, String> word2stem = new HashMap<String, String>(); //word and its stemming format
//		String line = null;
//		
//		while((line = br.readLine()) != null){
//			String[] attributes = line.split(",");
//			word2stem.put(attributes[0], attributes[1]);
//		}
//		br.close();
//		
//		br = new BufferedReader(new FileReader("pakdd//twitter8.csv"));
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//twitter9.csv"));
//		int ct = 0;
//		while((line = br.readLine()) != null){
//			String[] attributes = line.split(",");
//			String[] words = attributes[3].split(" ");
//			List<String> stemwords = new ArrayList<String>();
//			for(String word : words){
//				if(word2stem.containsKey(word) == true)
//					stemwords.add(word2stem.get(word));
//			}
//			if(stemwords.size() >=2){
//				ct ++;
//				bw.write(attributes[0] + ",");
//				bw.write(attributes[1] + ","); //no output TweetID
//				for(String word : stemwords)
//					bw.write(word + " ");
//				bw.write("\n");
//			}
//			
//			
//		}
//		bw.flush();bw.close();
//		System.out.println("there are " + ct + " tweets left"); //there are 2389060 tweets left 
//		
//		
//		
		
		//////////////////////////////////////////////////////////////////////////////////////////////
		//ͳ��ÿ��user��tweets
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter9.csv"));
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//2015user&tweetscounts.txt"));
//		String line = null;
//		Map<String, Integer> user2counts = new HashMap<String, Integer>();
//		
//		while((line = br.readLine()) != null){
//			String[] attributes = line.split(",");
//			if(user2counts.containsKey(attributes[0]) == true){
//				int times = user2counts.get(attributes[0]);
//				times ++;
//				user2counts.put(attributes[0], times);
//			}
//			else
//				user2counts.put(attributes[0], 1);
//		}
//		
//		for(Map.Entry<String, Integer> entry : user2counts.entrySet()){
//			bw.write(entry.getKey() + "," + entry.getValue() + "\n");
//		}
//		bw.flush();bw.close();
		
	
		
		
		/////////////////////////////////////////////////////////////////////////////////////////////////
		
		
		//������Щtweet��Ŀ>=300��user
//		BufferedReader brr = new BufferedReader(new FileReader("pakdd//2015user&tweetscounts.txt"));
//		Map<String, Integer> user2counts = new HashMap<String, Integer>();
//		String line = null;
//		while((line = brr.readLine()) != null){
//			String[] attributes = line.split(",");
//			user2counts.put(attributes[0], Integer.valueOf(attributes[1]));
//			
//		}
//		brr.close();
//				
//		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter9.csv"));
//		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//twitter10.csv"));
//		int ct = 0;
//		while((line = br.readLine()) != null){
//			String[] attributes = line.split(",");
//			if(user2counts.get(attributes[0]) >= 300){
//				ct ++;
//				bw.write(line + "\n");
//			}
//		}
//		bw.flush();bw.close();
//		System.out.println("there are " + ct + " tweets left"); //there are 1989467 tweets left
//		
		
		////////////////////////////////////////////////////////////////////////////////////
		//��tweetת��bog of word��ʽ
		getWord2id();
		
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		//��дʱ���ʽ, ������ʱ������
		
		

		
	}
	public static void getWord2id() throws IOException{
		
		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter10.csv"));
		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//word2id.txt"));
		Map<String, Integer> vocabulary = new TreeMap<String, Integer>();
		String line = null;
		
		while((line = br.readLine()) != null){
			String[] attributes = line.split(",");
			String[] words = attributes[2].split(" ");
			for(String word : words){
				if(vocabulary.containsKey(word) == false)
					vocabulary.put(word, 1);
			}
		}//while
		
		Map<String, Integer> word2id = new LinkedHashMap<String, Integer>();
		int ct = 0;
		for(Map.Entry<String, Integer> entry: vocabulary.entrySet()){
			word2id.put(entry.getKey(), ct);
			ct ++;
		}
		
		for(Map.Entry<String, Integer> entry : word2id.entrySet()){
			bw.write(entry.getKey() + "," + entry.getValue() + "\n");
		}
		bw.flush();bw.close();
		
		br = new BufferedReader(new FileReader("pakdd//twitter10.csv"));
		bw = new BufferedWriter(new FileWriter("pakdd//twitter11.csv"));
		while((line = br.readLine()) != null){
			String[] attributes = line.split(",");
			String[] words = attributes[2].split(" ");
			List<Integer> tmp = new ArrayList<Integer>();
			for(String word : words){
				tmp.add(word2id.get(word));
			}
			bw.write(attributes[0] + ",");
			bw.write(attributes[1] + ",");
			for(Integer i: tmp)
				bw.write(i + " ");
			bw.write("\n");
			
		}
		bw.flush();bw.close();
		
		
		
	}
	public static void sortByTime() throws Exception{
		//��дʱ���ʽ, ����ʱ������
		BufferedReader br = new BufferedReader(new FileReader("pakdd//twitter11.csv"));
		BufferedWriter bw = new BufferedWriter(new FileWriter("pakdd//twitter12.csv"));
		String line = null;
		
		while((line = br.readLine()) != null){
			String[] attributes = line.split(",");
			
		}
	}

	
	

	public static <K, V extends Comparable<? super V>> Map<K, V> 
    sortByValue( Map<K, V> map )
    {
		List<Map.Entry<K, V>> list =
				new LinkedList<>( map.entrySet() );
		Collections.sort( list, new Comparator<Map.Entry<K, V>>(){
			@Override
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 ){
				return (o2.getValue()).compareTo( o1.getValue() );
			}
		} );

		Map<K, V> result = new LinkedHashMap<>();
		for (Map.Entry<K, V> entry : list){
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
    }
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
//			exportCSVfile();
//			countUsers();
			saveEnglishTweets();
//			new Preprocess().getWord2id();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	



}

