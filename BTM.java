package ml.tm.shortext;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.commons.math3.special.Beta;

import com.data.preprocess.Preprocess;

import ml.lda.LDAarguments;
import ml.tm.tot.TOT;

/**
 * 
 * @author hehe
 * @WWW'13 BTM implement in java by lijia
 */
public class BTM {
	
	private int K; //the number of topics K
	private int V; // the number of words
	private double alpha; // hyperparameter of Dirichlet Distribution, THETA
	private double beta; // hyperpaparmeter of Dirichlet Distribution, PHI
	private List<Pair> B; // biterm set
	private int window; //�������ڴ�С
	int[] z; // the topic of biterm set
	int[] Nz; // the number of biterms assigned to topic z ��LDA�е�Nk
	int[][] Nwz; // each word assigned to each topic number ��LDA��Nkt
	int[] sumNwz; //the number of words assigned to topic z
	int iterations; //Gibbs sampling iterations
	int savestep;

	double[] theta;
	double[][] phi;
	long startTime=System.currentTimeMillis();
	
	
	class Pair{
		private int wi;
		private int wj;
		Pair(int i, int j){
			wi = min(i,j);
			wj = max(i,j);
//			System.out.println("i:" + i + " " + "j:" + j);
//			System.out.println("wi:" + wi + " wj:" + wj);
		}
		public int getWi(){
			return wi;
		}
		public int getWj(){
			return wj;
		}
	}

	BTM(){
		setDefaultValues(); // call set default method
	}
	
	public void setDefaultValues(){
		
		
		V = 0;
		K = 0;
		alpha = 50.0 / K;
		beta = 0.01;
		window = 3; ///////////////////////////////////////////////////////////////////////////
		B = new ArrayList<Pair>();
		z = null;
		Nz = null;
		Nwz = null;
		sumNwz = null;
		
		iterations = 500;
		savestep = 50;
		theta = null;
		phi = null;
	}
	protected boolean init(LDAarguments option){ // init() -> readFile(filename) -> train()
		if(option == null)
			return false;
		
		K = option.K;
		V = option.V;
		alpha = option.alpha;
		beta = option.beta;
		
		Nz = new int[K];
		Nwz = new int[V][K];
		sumNwz = new int[K];
		iterations = option.iterations;
		savestep = option.savestep;
		
		theta = new double[K];
		phi = new double[K][V];
		return true;
		
	}


	public void readFile(){
		/**
		 * 
		 */
		
		try {
			BufferedReader br = new BufferedReader(new FileReader("experiments//train.txt"));
			String line = null;
			try {
				while((line = br.readLine()) != null){
					String[] d = line.split(",");
					String[] data = d[1].split(" ");
					if(data.length >= 2){
						for(int i=0;i<data.length-1;i++){
							for(int j=i+1;j<min(i+window, data.length);j++){
								B.add(new Pair(Integer.valueOf(data[i]), Integer.valueOf(data[j])));
							}
						}
					}	
					
				}//while
				
				//assign random topics to biterm
				z = new int[B.size()];
				Random rand = new Random();
				for(int i=0;i<z.length;i++){
					int tmpz = rand.nextInt(K);
					
					z[i] = tmpz;
					Nz[tmpz] ++;
					Nwz[B.get(i).getWi()][tmpz] ++;
					Nwz[B.get(i).getWj()][tmpz] ++;
					sumNwz[tmpz] += 2;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("file open error");
			e.printStackTrace();	
		}
		
	}
	public int min(int i, int j){
		if(i <= j)
			return i;
		else
			return j;
	}

	public int max(int i, int j){
		if(i <= j)
			return j;
		else 
			return i;
	}
	public void estimation(){
		
		for(int iteration = 0; iteration < iterations; iteration ++){ // for each iteration
			
			System.out.println("iteration" + iteration);
			if(iteration % 100 ==0){
				computePhi();
				computeTheta();
				saveModel();
			}
			for(int b = 0; b < z.length; b ++){ // for each biterm
				
//				Nz[b] --;
				int tmpz = gibbsampling(b);
//				System.out.println("tmpz:" + tmpz);
				if(tmpz >= 50)
					System.out.println(tmpz);
				z[b] = tmpz; //这里之前漏了！！！!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				Nz[tmpz] ++;
				Nwz[B.get(b).getWi()][tmpz] ++;
				Nwz[B.get(b).getWj()][tmpz] ++;
				sumNwz[tmpz] += 2;
				
				
			}
		}
	}
	
	public int gibbsampling(int b){
		
		int currentopic = z[b];
		int wordi = B.get(b).getWi();
		int wordj = B.get(b).getWj();
		Nz[currentopic] --; 
		Nwz[wordi][currentopic] --;
		Nwz[wordj][currentopic] --;
		sumNwz[currentopic] -= 2; // sumNwz = 2 * Nz
		
//		if(Nwz[wordi][currentopic] < 0){
//			System.out.println("wordi:" + wordi + "currentopic:" + currentopic+  " " + Nwz[wordi][currentopic]);
//		}
//		if(Nwz[wordj][currentopic] < 0){
//			System.out.println("wordj:" + wordj + "currentopic:" + currentopic+  " " + Nwz[wordj][currentopic]);
//		}
//		
		
		
//		if(Nwz[wordi][currentopic] < 0) //
//			Nwz[wordi][currentopic] = 0;
//		if(Nwz[wordj][currentopic] < 0)
//			Nwz[wordj][currentopic] = 0;

		int tmpz = 0;
		double[] proba = new double[K];
		Beta betafunction = null;
		for(int i=0;i<K;i++){
			
			proba[i] = (Nz[i] + alpha) * (Nwz[wordi][i] + beta) * (Nwz[wordj][i] + beta) /(sumNwz[i] + V*beta)/(sumNwz[i] + 1 + V*beta);
		
			if(proba[i] < 0){
//				System.out.println(proba[i]);
//				System.out.println("Nwz" + Nwz[wordi][i]);
//				System.out.println("Nwz" + Nwz[wordj][i]);
			}
		}

		for(int i=1;i<K;i++)
			proba[i] += proba[i-1];

		
		double u = Math.random() * proba[K-1]; //random u
//		if(u<0) //算出的u有的小于0   
//		System.out.println("u:" + u);
		for(tmpz=0;tmpz<K;tmpz++){
			if(u <= proba[tmpz])
				break;
		}
//		System.out.println("proba[tmpz]:" + proba[tmpz]);
		return tmpz;
		
	}
	
	public void computePhi(){
		/**
		 * compute phi[][]
		 */
		for(int k =0; k< K;k ++){
			for(int t=0;t<V;t++)
				phi[k][t] = (Nwz[t][k] + beta)/(sumNwz[k] + V*beta);
		}

	}
	public void computeTheta(){

		/**
		 * compute theta[]
		 */
		for(int k=0;k<K;k++){
			theta[k] = (Nz[k] + alpha)/(B.size() + K*alpha);
		}

	}
	
	public void getTopNWords(int N)  {
		/**
		 * 模型训练完，得到每个topic的前N个最大概率word, 将phi读入Map<Id,proba>,然后根据value排序,再从id2word 中得到前K个word
		 */// phi = new double[K][V]   
		
		List<HashMap<Integer, Double>> topwords = new ArrayList<HashMap<Integer, Double>>();
		for(int k=0;k<K;k++){
			
			HashMap<Integer, Double > currentTopicPhi = new HashMap<Integer, Double>();
			for(int v=0;v<V;v++){
				currentTopicPhi.put(v, phi[k][v]);
			}
			topwords.add(currentTopicPhi);
			
		}//for
		
		//对每一个topic下的proba 排序, 即对Map<Integer, Double> sort by value
		List<LinkedHashMap<Integer, Double>> sortedTopWords = new ArrayList<LinkedHashMap<Integer, Double>>();
		
		for(int k=0;k<K;k++){
			LinkedHashMap<Integer, Double> sortedmap = (LinkedHashMap<Integer, Double>) Preprocess.sortByValue(topwords.get(k));
			sortedTopWords.add(sortedmap);
		}
		
		
		BufferedReader br;
		Map<Integer, String> id2word = null;
		try {
			br = new BufferedReader(new FileReader("experiments//word2id.txt"));
//			br = new BufferedReader(new FileReader("testdata//word2id.txt"));
			String line = null;
			 id2word = new HashMap<Integer, String>();
			
			try {
				while(( line = br.readLine()) != null){
					String[] data = line.split(" ");
					id2word.put(Integer.valueOf(data[1]), data[0]);
				}
				br.close();
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//将每个topic 的前N个词汇output
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter("experiments//topNwords.txt"));
//			BufferedWriter bw = new BufferedWriter(new FileWriter("testdata//200topNwords.txt"));
			for(int k=0;k<K;k++){
				int n = 0;
				System.out.println("topic" + k +": ");
				bw.write(k + "\n");
				for(Map.Entry<Integer, Double> entry: sortedTopWords.get(k).entrySet()){
					if(n < N){
//						System.out.print();
						bw.write(id2word.get(entry.getKey()) + ",");
					}else
						break;
					n ++;
				}//for
//				System.out.println();
				bw.write("\n");
			}//for
			bw.flush();bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void inferenceNewDocument(String filename){
		/**
		 * inference new documents's topid distribution
		 */
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = null;
			List<Integer> TOPICS = new ArrayList<Integer>(); //记录每个document概率最大的topic
			
			while((line = br.readLine()) != null){
				String[] d = line.split(",");
				String[] data = d[1].split(" ");
				
				Map<Pair, Integer> documentB = new HashMap<Pair, Integer>(); //当前文档的词对及其出现次数
				List<Pair> document = new ArrayList<Pair>();
				if(data.length >= 2){
					int Nd = 0;
					for(int i=0;i<data.length-1;i++){
						for(int j=i+1;j<min(i+window, data.length);j++){
							Nd ++;
							int minimum = min(Integer.valueOf(data[i]), Integer.valueOf(data[j]));
							int maximum = max(Integer.valueOf(data[i]), Integer.valueOf(data[j]));
							Pair pair = new Pair(minimum, maximum);
							document.add(pair);
							if(documentB.containsKey(pair) == false){
								documentB.put(pair, 1);
								
							}else{
								int fre = documentB.get(pair);
								fre ++;
								documentB.put(pair, fre);
								
							}
								
//							documentB.add(new Pair(minimum, maximum));
						}
					}//for
					
					
					double[] denominator = new double[Nd];
					for(int i=0;i<Nd;i++){
						for(int k=0;k<K;k++){
							denominator[i] += theta[k] * phi[k][document.get(i).getWi()] * phi[k][document.get(i).getWj()];
						}
					}//for
					
					
					double[] proba = new double[K];
					int TOPIC = 0; //当前document 概率最大的topic
					double maxproba = proba[0];
					for(int k=0;k<K;k++){
						
						for(int i=0;i<Nd;i++){
							proba[k] += theta[k] * phi[k][document.get(i).getWi()] * phi[k][document.get(i).getWj()] / denominator[i] * documentB.get(document.get(i))/Nd;
						}
						if(proba[k] > maxproba){
							maxproba = proba[k];
							TOPIC = k;
						}
					}
					
					TOPICS.add(TOPIC);

					
					
					
				}//fi
				
			}//while
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(filename + "TOPIC_DISTRIBUTION")); //output each document's topic - distribution
			for(Integer inte: TOPICS)
				bw.write(inte + "\n");
			bw.flush();
			bw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	public void saveModel(){
		
		String prefix = "BTM" + String.valueOf(iterations) + "iterations" + "_K" + K + "_alpha" + alpha+"_beta" + beta;
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(prefix + "B.txt"));
			for(int b=0;b<B.size();b++){
				bw.write(B.get(b).getWi() + " " + B.get(b).getWj());
				bw.write("\n");
			}
			bw.flush();bw.close();
			
			bw = new BufferedWriter(new FileWriter(prefix + "z.txt"));
			for(int b=0;b<B.size();b++){
//				for(int w=0;w<z[m].length;w++)
					bw.write(z[b]);
				bw.write("\n");
			}
			bw.flush();bw.close();

			
			bw = new BufferedWriter(new FileWriter(prefix + "Nz.txt"));

			for(int k=0;k<K;k++){
				
				bw.write(Nz[k]);
				bw.write("\n");
			}
			bw.flush();bw.close();

			
			
			bw = new BufferedWriter(new FileWriter(prefix + "Nwz.txt"));
			for(int v=0;v<V;v++){
				for(int k=0;k<K-1;k++){
					bw.write(Nwz[v][k] + " ");
				}
				bw.write(Nwz[v][K-1] + "\n");
			
			}
			bw.flush();bw.close();


			bw = new BufferedWriter(new FileWriter(prefix + "sumNwz.txt"));
			for(int k=0;k<K;k++)
				bw.write(sumNwz[k] + "\n");
			bw.flush();bw.close();



			bw = new BufferedWriter(new FileWriter(prefix + "phi.txt"));
			for(int k=0;k<K;k++){
				for(int v=0;v<V;v++)
					bw.write(phi[k][v] + " ");
				bw.write("\n");
			}
			bw.flush();bw.close();

			bw = new BufferedWriter(new FileWriter(prefix + "theta.txt"));
			for(int k=0;k<K;k++){
				
				bw.write(theta[k] + "\n");
			}
			bw.flush();bw.close();
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		LDAarguments arg = new LDAarguments();
		arg.Setalpha(0.1);arg.Setbeta(0.01);
		arg.Setiterations(10);arg.SetK(25);
		arg.SetV(25943); //word is sparse
		
		
		BTM lda = new BTM();
		lda.init(arg);
		lda.readFile();
		lda.estimation();
		lda.inferenceNewDocument("experiments//test.txt");
		lda.getTopNWords(20);
	
		
		

	}

}
