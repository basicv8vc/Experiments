package ml.lda;

import com.data.preprocess.*;

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

public class LDA {
	
	int iterations; //Gibbs sampling iterations
	int savestep;
	double alpha; //hyperparameter
	double beta; //hyperparameter
	int K; // the number of topics
	int V; // the number of words
	int M;//the number of documents
	int[] vocabulary; // the vocabulary
	int[][] Nmk; // each document's topic number
	int[][] Nkt; // each word assigned to each topic number
	int[] Nm; //the number of words in document d
	int[] Nk; // the number of words assigned to topic k
	int[][] z; 
	int[][] corpus; //
	double[][] theta;
	double[][] phi;
	long startTime=System.currentTimeMillis();
	
	public LDA(){
		setDefaultValues(); // call set default method
	}
	
	public void setDefaultValues(){
		
		M = 0;
		V = 0;
		K = 0;
		alpha = 50.0 / K;
		beta = 0.01;
		iterations = 500;
		savestep = 50;
		
		vocabulary = null;
		Nmk = null;
		Nkt = null;
		Nm = null;
		Nk = null;
		z = null;
		corpus = null;
		theta = null;
		phi = null;
	}

	public void readFile(String filename){
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = null;
			int count = 0;
			try {
//				br.readLine();br.readLine();
				while((line = br.readLine()) != null){

					String[] d = line.split(",");
					String[] data = d[1].split(" ");
					z[count] = new int[data.length];
					corpus[count] = new int[data.length];
					Random rand = new Random();
					for(int i=0;i<data.length;i++){

						int tmpz = rand.nextInt(K);//random initialize topic
						corpus[count][i] = Integer.valueOf(data[i]);//store input data into corpus
						z[count][i] = tmpz;
						Nmk[count][tmpz] ++;
						Nkt[Integer.valueOf(data[i])][tmpz] ++;
						Nm[count] ++;
						Nk[tmpz] ++;

					}
					count ++;
				}



			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	
	public void saveModel(){
		
		String prefix = String.valueOf(iterations) + "iterations" + "_K" + K + "_alpha" + alpha+"_beta" + beta;
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(prefix + "Nmk.txt"));
			for(int m=0;m<M;m++){
				for(int k=0;k<K;k++){
					bw.write(Nmk[m][k] + " ");
				}
				bw.write("\n");
			}
			bw.flush();bw.close();

			bw = new BufferedWriter(new FileWriter(prefix + "Nkt.txt"));

			for(int v=0;v<V;v++){
				for(int k=0;k<K;k++){
					bw.write(Nkt[v][k] + " ");
				}
				bw.write("\n");
			}
			bw.flush();bw.close();

			bw = new BufferedWriter(new FileWriter(prefix + "Nm.txt"));
			for(int m=0;m<M;m++)
				bw.write(Nm[m] + "\n");
			bw.flush();bw.close();


			bw = new BufferedWriter(new FileWriter(prefix + "Nk.txt"));
			for(int k=0;k<K;k++)
				bw.write(Nk[k] + "\n");
			bw.flush();bw.close();

			bw = new BufferedWriter(new FileWriter(prefix + "z.txt"));
			for(int m=0;m<M;m++){
				for(int w=0;w<z[m].length;w++)
					bw.write(z[m][w] + " ");
				bw.write("\n");
			}
			bw.flush();bw.close();

			bw = new BufferedWriter(new FileWriter(prefix + "phi.txt"));
			for(int k=0;k<K;k++){
				for(int v=0;v<V;v++)
					bw.write(phi[k][v] + " ");
				bw.write("\n");
			}
			bw.flush();bw.close();

			bw = new BufferedWriter(new FileWriter(prefix + "theta.txt"));
			for(int m=0;m<M;m++){
				for(int k=0;k<K;k++)
					bw.write(theta[m][k] + " ");
				bw.write("\n");
			}
			bw.flush();bw.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	protected boolean init(LDAarguments option){ // init() -> readFile(filename) -> train()
		if(option == null)
			return false;
		
		alpha = option.alpha;
		beta = option.beta;
		iterations = option.iterations;
		K = option.K;
		V = option.V;
		M = option.M;
		savestep = option.savestep;
		
		vocabulary = new int[V];
		Nmk = new int[M][K];
		Nkt = new int[V][K];
		Nm = new int[M];
		Nk = new int[K];
		z = new int[M][];
		corpus = new int[M][];
		theta = new double[M][K];
		phi = new double[K][V];
		
		return true;
		
	}
	
	
	public void Estimation(){
		
		/**
		 *
		 */

		for(int iteration=1;iteration<iterations;iteration++){

			//sampling

			System.out.println("iteration: " + iteration);
			if(iteration %10 ==0){
				
				computePhi();
				computeTheta();
//				lda.OutputModel();
				computeLogLikelihood();
			}
			for(int document=0;document<M;document++){
				for(int i=0;i<z[document].length;i++){
					int word = corpus[document][i];
					int tmpz = gibbsampling(document, i);//gibbs sampling
					Nmk[document][tmpz] ++;
					Nkt[word][tmpz] ++;
					Nm[document] ++;
					Nk[tmpz] ++;
					z[document][i] = tmpz; //


				}
			}

		}
		
	}
	
	public int gibbsampling(int document, int n){

		Nmk[document][z[document][n]] --; // Nmk --
		Nkt[corpus[document][n]][z[document][n]] --; // Nkt --
		Nm[document]--;
		Nk[z[document][n]] --;


		int tmpz = 0;
		double[] proba = new double[K];
		for(int i=0;i<K;i++){
			proba[i] = (Nkt[corpus[document][n]][i] + beta)/(Nk[i] + V*beta) * (Nmk[document][i] + alpha);///(Nm[document]+ K*alpha);

		}

		for(int i=1;i<K;i++)
			proba[i] += proba[i-1];

		double u = Math.random() * proba[K-1]; //random u
		for(tmpz=0;tmpz<K;tmpz++){
			if(u <= proba[tmpz])
				break;
		}

		return tmpz;


	}

	public void computePhi(){
		/**
		 * compute phi[][]
		 */
		for(int k =0; k< K;k ++){
			for(int t=0;t<V;t++)
				phi[k][t] = (Nkt[t][k] + beta)/(Nk[k] + V*beta);
		}

	}
	public void computeTheta(){

		/**
		 * compute theta[][]
		 */
		for(int m=0;m<M;m++){
			for(int k=0;k<K;k++)
				theta[m][k] = (Nmk[m][k] + alpha)/(Nm[m] + K*alpha);
		}

	}
	
	public void computeLogLikelihood(){
		/**
		 * compute loglikelihood
		 */
		double logllhood = 0.0;
		double sum = 0.0;
		for(int m=0;m<M;m++){
			for(int n=0;n<z[m].length;n++){
				sum = 0.0;
				for(int k=0;k<K;k++){
					sum += theta[m][k] * phi[k][corpus[m][n]];
//					System.out.println(sum);
				}
				logllhood += Math.log(sum);
			}
		}
		long endTime=System.currentTimeMillis(); 
		System.out.println((endTime-startTime)+"ms");
		String prefix = "_K" + K + "_alpha" + alpha+"_beta" + beta;
		System.out.println(prefix + "," + logllhood);
	}

	public void readModel(String iterations, String K, String alpha, String beta){
		/**
		 *有的时候迭代了500次，可是不得不停止，这时候保存变量，下一次肯定想接着500次再迭代, readModel 
		 */
//		String prefix = iterations + "iterations" + "_K" + K + "_alpha" + alpha + "_beta" + beta;
//		try {
//			BufferedReader br = new BufferedReader(new FileReader(prefix + "Nmk.txt"));
//			for(int m=0;m<M;m++){
//				for(int k=0;k<K;k++){
//					
//				}
//			}
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
		
		
		
	}
	
	public void InferenceNewDocument(String filename, int newM){
		
		/**
		 * 计算filename 中每条tweet的topic distributionn
		 */
		
		//init new file .
		LDAarguments arg = new LDAarguments();
		arg.Setalpha(0.1);arg.Setbeta(0.01);
		arg.Setiterations(200);arg.SetK(30);
		arg.SetM(newM);arg.SetV(V);
		
		LDA newModel = new LDA();
		newModel.init(arg);
		newModel.readFile(filename);
		
		
		//inference
		for(int iteration = 0; iteration < newModel.iterations; iteration ++){
			
			System.out.println("iteration:" + iteration);
			for(int m = 0; m < newModel.M; m ++){
				for(int i = 0; i < newModel.z[m].length; i ++){
					
					int currenttopic = newModel.z[m][i];
					int currentword = newModel.corpus[m][i];
					
					//gibbs sampling
					newModel.Nmk[m][currenttopic] --; // Nmk --
					newModel.Nkt[currentword][currenttopic] --; // Nkt --
					newModel.Nm[m]--;
					newModel.Nk[currenttopic] --;

					
					int tmpz = 0;
					double[] proba = new double[newModel.K];
					for(int k=0;k<newModel.K;k++){
						proba[k] = (Nkt[currentword][k] + newModel.Nkt[currentword][k] + newModel.beta)/(Nk[k] + newModel.Nk[k] +  V*newModel.beta) * (newModel.Nmk[m][k] + newModel.alpha)/(newModel.Nm[m] + K * newModel.alpha);///(Nm[document]+ K*alpha);

					}

					for(int k=1;k<newModel.K;k++)
						proba[k] += proba[k-1];

					double u = Math.random() * proba[newModel.K-1]; //random u
					for(tmpz=0;tmpz<newModel.K;tmpz++){
						if(u <= proba[tmpz])
							break;
					}

					newModel.Nmk[m][tmpz] --; // Nmk --
					newModel.Nkt[currentword][tmpz] --; // Nkt --
					newModel.Nm[m]--;
					newModel.Nk[tmpz] --;
					
					newModel.z[m][i] = tmpz;
					
					
				}//一篇文档sampling完毕
			}//测试集一次sampling
			
			
		}//samping
		
		
		//计算测试集每篇文档的theta
		
		for(int m = 0; m < newModel.M; m ++){
			for(int k = 0; k < newModel.K; k ++)
				newModel.theta[m][k] = (newModel.Nmk[m][k] + newModel.alpha) / (newModel.Nm[m] + newModel.K * newModel.alpha);
		}
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter("experiments//inferenceTestTheta.txt"));
			for(int m = 0; m < newModel.M; m ++){
				for(int k=0;k<newModel.K-1;k++){
					bw.write(newModel.theta[m][k] + " ");
				}
				bw.write(newModel.theta[m][newModel.K-1] + "\n");bw.flush();
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		LDAarguments arg = new LDAarguments();
		arg.Setalpha(0.1);arg.Setbeta(0.01);
		arg.Setiterations(400);arg.SetK(30);
//		arg.SetM(2727204);arg.SetV(113438);
		arg.SetM(260000);arg.SetV(25943);
//		arg.SetM(17087);arg.SetV(20974);
		
		
		LDA lda = new LDA();
		lda.init(arg);
		lda.readFile("experiments//train.txt");
//		lda.readFile("testdata//finaldata.txt");
		lda.Estimation();
		lda.InferenceNewDocument("experiments//test.txt", 5519);
		lda.getTopNWords(20);

	}

}
