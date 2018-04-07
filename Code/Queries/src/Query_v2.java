import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lemurproject.indri.DocumentVector;
import lemurproject.indri.ParsedDocument;
import lemurproject.indri.QueryEnvironment;
import lemurproject.indri.ScoredExtentResult;


public class Query_v2 {

	private static Double alpha_1 = new Double(0.6);
	private static Double alpha_2 = new Double(0.2);
	private static Double alpha_3 = new Double(0.2);
	
	public static void main(String args[]) throws Exception {
		//System.out.println(System.getProperty("java.library.path"));
		//C:\Users\Purusharth\Desktop\Ranking
		String articleIndex = "PATH TO DOCUMENT INDEX";
		String passageIndex = "PATH TO PASSAGE INDEX";
		
		QueryEnvironment passage_env = new QueryEnvironment(); //open passage index
		QueryEnvironment article_env = new QueryEnvironment(); //open article index
		QueryEnvironment baseline_article_env = new QueryEnvironment(); 
		
		article_env.addIndex(articleIndex);
		baseline_article_env.addIndex(articleIndex);
		passage_env.addIndex(passageIndex);
		
		String path_stopwords = "PATH TO STOPWORDS";
		
		HashMap<String,Integer> stop_words = Load_stopwords(path_stopwords);
		if(stop_words==null) {
			System.out.println("Unable to load stopwords! Exitting..");
			return;	
		}
		System.out.println("Loaded stopwords!");
		
		//Process the queries
		List<String> query_list = new ArrayList<>();
		//FileWriter fw1 = new FileWriter("C:/Users/Purusharth/Desktop/ibm/Queries_main/Processed/processed_q3.txt");
		//String query_path = "C:\\Users\\Purusharth\\Desktop\\ibm\\Queries_main\\q_v3.txt";
		FileWriter fw1 = new FileWriter("WRITE PATH FOR PROCESSED QUERIES");
		String query_path = "PATH TO QUERIES";
				
		try (BufferedReader br = new BufferedReader(new FileReader(query_path))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       //System.out.println(line.trim());
		       line = line.trim();
		       query_list.add(line);
		    }
		    
		}catch(Exception e) {
			System.out.println("Unable to read queries!");
			e.printStackTrace();
		}
		
		List<String> indri_queryList=new ArrayList<>();
		List<String> processed_queryList=new ArrayList<>();
		List<String> baseline_processed_queryList=new ArrayList<>();
		List<String> original_query = new ArrayList<>();
		
		
		//Execute the queries
		for(int i=0;i<query_list.size();i++){
			try{
			String[] triple = query_list.get(i).split("\\*\\*");
			String entity_1 = triple[0].replaceAll("\\.|'|:|,", "");
			String entity_2 = triple[2].replaceAll("\\.|'|:|,", "");;
			String relations[] = triple[1].split("&");
			List<String> processed_relations =  processRelations(relations,stop_words, passage_env);
			String[] res = processed_relations.toArray(new String[0]);
			
			String _relations = String.join(" ", processed_relations.toArray(new String[0]));
			String query_string = entity_1+" "+_relations+" "+entity_2;
			String baseline = entity_1+" "+res[0]+" "+entity_2;
			String original_query_string = triple[0]+" "+triple[1].split("&")[0]+" "+triple[2];
			original_query.add(original_query_string);
			
			//original
			indri_queryList.add("#band("+entity_1+" "+"#syn("+_relations+") "+entity_2+")");
			
			
			processed_queryList.add(query_string);
			baseline_processed_queryList.add(baseline);
			
			}catch(Exception e){
				continue;
			}
		}
		
		
		BufferedWriter bw1 = new BufferedWriter(fw1);
		PrintWriter out1 = new PrintWriter(bw1);
		
		for(int i=0;i<processed_queryList.size();i++){
			out1.println(processed_queryList.get(i)+"\t"+baseline_processed_queryList.get(i)+"\t"+original_query.get(i));
		}
		out1.close();
		
		Thread baseline_run = new Thread(new Runnable() {
	    	 public void run() {
	    		 try{
	    	        for(int i=0;i<baseline_processed_queryList.size();i++){
	    	    		BaseLine.PerformBaseline(baseline_article_env,baseline_processed_queryList.get(i));
	    	        }
	    	        baseline_article_env.close();
	    		 }catch(Exception e){
	    			 System.out.println("Problem in baseline method");
 	        		e.printStackTrace();
	    		 }
	    	}
	    });
		
		
		Thread non_baseline_run = new Thread(new Runnable() {
	    	 public void run() {
	    		 try{
	    	        for(int i=0;i<processed_queryList.size();i++){
	    	        	try{
	    	        		System.out.println("Q:"+i+"/"+processed_queryList.size());
	    	        		non_BaseLineQuery(passage_env, article_env,processed_queryList.get(i), indri_queryList.get(i));
	    	        	
	    	        	}catch(Exception e){
	    	        		System.out.println("Problem in non_baseline method");
	    	        		e.printStackTrace();
	    	        	}
	    	        }
	    	        article_env.close();
	    	        passage_env.close();
	    	        
	    		 }catch(Exception e){
	    			System.out.println("Problem in non_baseline method");
 	        		e.printStackTrace();
	    		 }      
	    	  }
	    });
		
		
		
	   baseline_run.start();
	   non_baseline_run.start();
	   baseline_run.join();
	   non_baseline_run.join();
	   
	}
	
	private static void non_BaseLineQuery(QueryEnvironment passage_env, QueryEnvironment article_env, String query, String indri_query) throws Exception{
				
				ArrayList<String> query_terms = new ArrayList<String>(Arrays.asList(query.split(" ")));
				//String myQuery = "#band(George H W Bush #syn(medical condition disease disability disorder health problem ailment illness paralympic) Parkinson disease)";
				
				String myQuery = indri_query;
				
				ScoredExtentResult[] result = null;
				result = passage_env.runQuery(myQuery, 100000);
				System.out.println("Results retrieved. Length:"+result.length);
				if(result.length>5000){
					return;
				}
				//System.out.println("Results retrieved. Length:"+result.length);
				
				//Get all unique doc_ids
				int[] docNums = new int[result.length];
				for(int i=0; i<result.length; i++) {
					docNums[i] = (result[i].document); //make a set of all docnumbers
				}
				System.out.println("Unique docs:"+docNums.length);
				DocumentVector[] docVec = passage_env.documentVectors(docNums);
				System.out.println("Doc_vector size:"+docVec.length);
				
				//This data-structure holds the doc_id and their respective term_counts
				ArrayList<Entity_v2> passage_docs = new ArrayList<>();//----------IMP
				HashMap<String,Long> term_map;
				System.out.println("Calculating stats from the documents vectors..!");
				for(int i=0;i<docVec.length;i++) {
					term_map  = new HashMap<>();
					int[] positions = docVec[i].positions;
					String[] stems = docVec[i].stems;
					for(int j=0;j<positions.length;j++) {
						String word = stems[positions[j]];
						if(term_map.containsKey(word)) {
							Long val = term_map.get(word);
							term_map.put(word, val+1);
						}else {
							term_map.put(word, new Long(1));
						}
					}
					Entity_v2 obj = new Entity_v2();
					obj.setTermCounts(term_map);
					obj.setDocId(docNums[i]);
					Long docLength = (long) passage_env.documentLength(docNums[i]);
					obj.setDocLength(docLength);
					passage_docs.add(obj);
				}
				
				System.out.println("Started Scoring!!");
				
				
				
				passage_docs = populateParentCounts(passage_env, article_env,passage_docs,docNums);
				
				if(passage_docs == null){
					System.out.print("Problem occured in populateParentCounts function!!");
					return;
				}
				
				System.out.println("Computing Ranking score!!");
				//Retrieve ranked set of documents
				ArrayList<RankedDocumentSet> ranked_set = ComputeRankingScores(passage_docs,query_terms,passage_env, article_env);
				
				
				
				if(ranked_set==null){
					System.out.println("Ranked set is null!!");
					return;
				}
				
				Collections.sort(ranked_set, new Comparator<RankedDocumentSet>() {
				    @Override
				    public int compare(RankedDocumentSet o1,RankedDocumentSet o2) {
				    	Double s1 = new Double(o1.getDocument_score());
				    	Double s2 = new Double(o2.getDocument_score());
				    	return s2.compareTo(s1);
				    }
				});
				
				System.out.println("First document:"+ranked_set.get(0).document_score);
				System.out.println("Last document:"+ranked_set.get(ranked_set.size()-1).document_score);
				
				Map<Integer, Integer> map  = new HashMap<Integer, Integer>();
				List<RankedDocumentSet> final_set = new ArrayList<>();
				
				for(int i=0;i<ranked_set.size();i++){
					int id = ranked_set.get(i).getDocId();
					if(!map.containsKey(id+1) && !map.containsKey(id+2) &&  !map.containsKey(id-1) && !map.containsKey(id-2)){
						map.put(id, 1);
						final_set.add(ranked_set.get(i));
					}
					map.put(id, 1);
				}
				
				//Write ranked set to file
				FileWriter fw = new FileWriter("PATH TO OUTPUT OF PROPOSED/"+query.replaceAll(" ","_")+".txt");
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw);
				
				for(int i=0;i<final_set.size();i++){
					String text = final_set.get(i).getPassage();
					int id = final_set.get(i).getDocId();
					out.println(text+"\n");
				}
				
				out.close();
	}
	
	
	
	
	private static long countTerm(QueryEnvironment env, String word) throws Exception{
		long term_count = env.termCount(word);
		return term_count;
	}
	
	
	private static List<String> processRelations(String[] relations,HashMap<String,Integer> stopwords, QueryEnvironment env){
		
		HashMap<String,Integer> duplicates = new HashMap<>();
		List<String> processed_relations = new ArrayList<>();
		
		for(int i=0;i<relations.length;i++){
			String relation = relations[i];
			relation = relation.replaceAll("(?s)\\((.*?)\\)","$1");
			
			
			String[] relation_words = relation.split(" ");
			List<String> processed_relation_words = new ArrayList<>();
			for(int j=0;j<relation_words.length;j++){
				String word = relation_words[j];
				word = word.replaceAll("\\.|'|:|,", "");
				String stem_word = env.stemTerm(word.trim());
				if(!stopwords.containsKey(word.toLowerCase()) && !duplicates.containsKey(stem_word.toLowerCase()) && !word.trim().equals(word.trim().toUpperCase())){
					processed_relation_words.add(word);
					duplicates.put(stem_word.toLowerCase(), 1);
				}
			}
			if(!processed_relation_words.isEmpty()){
				String str = String.join(" ", processed_relation_words.toArray(new String[0]));
				processed_relations.add(str);
			}
		}
		return processed_relations;
	}
	
	
	private static ArrayList<RankedDocumentSet> ComputeRankingScores(ArrayList<Entity_v2> passage_docs, ArrayList<String> query_terms, QueryEnvironment env, QueryEnvironment doc_env) {
		
		double vocab_size = 14708255;
		double collection_size = 96686802;
		
		double doc_vocab_size = 29159624;
		
		try{
			System.out.println("new size:"+passage_docs.size());
			ArrayList<RankedDocumentSet> ranked_set = new ArrayList<>();
			for(int i=0;i<passage_docs.size();i++){
				
				if(i%50==0)
					System.out.println("Computing score for :"+i);
				
				
				HashMap<String,Long> passage_term_counts = passage_docs.get(i).getTermCounts();
				HashMap<String,Long> article_term_counts = passage_docs.get(i).getParent_term_counts();	
				
				long article_length = passage_docs.get(i).getParent_docLength();
				long passage_length = passage_docs.get(i).getDocLength();
				
				double document_score = 0;
				
				for(int j=0;j<query_terms.size();j++){
					
					String query_entity = query_terms.get(j);
					String stem_query_entity = env.stemTerm(query_entity);
					
					long passage_count = 0;
					long article_count = 0;
					
					long collection_count = countTerm(doc_env,query_entity);
					
					if(passage_term_counts.containsKey(stem_query_entity)){
						passage_count = passage_term_counts.get(stem_query_entity);
					}else{
						//System.out.println("Term not in sentence_index!!");
					}
					if(article_term_counts.containsKey(stem_query_entity)){
						article_count = article_term_counts.get(stem_query_entity);
					}else{
						//System.out.println("Term not in document!!");
					}
					
					
					
					double doc_score;
					doc_score = (double)(alpha_1*(passage_count+1))/(vocab_size+passage_length);
					doc_score += (double)(alpha_2*(article_count+1))/(vocab_size+article_length);
					doc_score += (double)(alpha_3*(collection_count))/(collection_size);
					
					/*
					System.out.println("Passage count:"+passage_count);
					System.out.println("Document count:"+article_count);
					System.out.println("collection_count:"+collection_count);
					System.out.println("Passage_Length:"+passage_length);
					System.out.println("Article Length:"+article_length);
					*/
					
					document_score += Math.log(doc_score);				
				}
				//System.out.println("-------------------------------");
				RankedDocumentSet obj = new RankedDocumentSet();
				obj.setDocId(passage_docs.get(i).getDocId());
				obj.setPassage(passage_docs.get(i).getPassage_text());
				obj.setDocument_score(document_score);
				ranked_set.add(obj);
			}
			
			
			return ranked_set;
			
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("Problem is ComputeScores function");
			return null;
		}
	}

	private static ArrayList<Entity_v2> populateParentCounts(QueryEnvironment passage_env, QueryEnvironment env, ArrayList<Entity_v2> passage_docs, int[] docIds){
		try{
			ArrayList<Entity_v2> clean_passage_docs = new ArrayList<>();
			
			System.out.println("Started populatedParentCounts function!!");
			
			ParsedDocument[] passages = passage_env.documents(docIds);
			String document_content = null;
			String regex = "<DOCID>(.*)</DOCID>";
			Pattern r = Pattern.compile(regex);
			
			for(int i=0;i<passages.length;i++) {
				//Retrieve passage text 
				document_content = passages[i].content;
				//System.out.println("-----------Start-----------");
				//System.out.println("ID: "+docIds[i]+"\n"+document_content);
				//System.in.read();
				
				//Retrieve external id
				Matcher m = r.matcher(document_content);
				m.find();
				//System.out.println("Found: "+ m.group(1));
				Integer ext_article_docid = Integer.parseInt(m.group(1).split("\\.",2)[0]);
				//System.out.println("Extracted: "+ext_article_docid );
				//System.in.read();
				
				
				//Search using external id
				//String field_query = "#combine[docid](" + ext_article_docid + ")" ;
				String field_query = "#band("+ext_article_docid+").docid";
				ScoredExtentResult[] query_results = env.runQuery(field_query,10);
				//System.out.println("Got the main document: "+i);
				//System.out.println("Total: "+query_results.length);
				if(query_results!=null && query_results.length!=0){
					
					Integer int_article_docid = query_results[0].document;
					//System.out.println("ArticleFound: "+int_article_docid);
					//System.out.println("-----------END-----------");
					//System.in.read();
					
					
					//Retrieve the article document's DocumentVector
					int[] articleDoc = new int[]{int_article_docid};
					DocumentVector[] article_docVec = env.documentVectors(articleDoc);
					//System.out.println("Article_Doc_vector size:"+article_docVec.length);
					
					
					//Create map of words, found in the Article Document
					HashMap<String,Long> article_term_map;
					for(int j=0;j<article_docVec.length;j++) {
						article_term_map  = new HashMap<>();
						int[] positions = article_docVec[j].positions;
						String[] stems = article_docVec[j].stems;
						for(int k=0;k<positions.length;k++) {
							String word = stems[positions[k]];
							if(article_term_map.containsKey(word)) {
								Long val = article_term_map.get(word);
								article_term_map.put(word, val+1);
							}else {
								article_term_map.put(word, new Long(1));
							}
						}
						passage_docs.get(i).setPassage_text(document_content);
						passage_docs.get(i).setParent_term_counts(article_term_map);
						passage_docs.get(i).setParent_docId(articleDoc[j]);
						//Long parent_docLength = (long) env.documentLength(articleDoc[j]);
						//System.out.println("length:" + parent_docLength);
						passage_docs.get(i).setParent_docLength((long)positions.length);
						clean_passage_docs.add(passage_docs.get(i));
					}
					//System.out.println("Computed doc stats: "+i);
					
				}
			}
			//System.out.println("old size:"+passage_docs.size());
			//System.out.println("new_old size:"+clean_passage_docs.size());
			System.out.println("Ended populatedParentCounts function!!");
			
			return clean_passage_docs;
			
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}

	private static HashMap<String, Integer> Load_stopwords(String path) {
		HashMap<String,Integer> stop_words = new HashMap<>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       //System.out.println(line.trim());
		       stop_words.put(line.trim().toLowerCase(), 1);
		    }
		    return stop_words;
		}catch(Exception e) {
			e.printStackTrace();
			return null;
		}
		
	}
}
