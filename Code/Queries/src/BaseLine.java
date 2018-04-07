
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;

import lemurproject.indri.ParsedDocument;
import lemurproject.indri.QueryEnvironment;
import lemurproject.indri.ScoredExtentResult;


public class BaseLine {

	public static ScoredExtentResult[] PerformBaseline(QueryEnvironment env, String query) throws Exception{
		//Perform query
		String field_query = "#combine[passage600:300](" + query + ")" ;
		System.out.println(field_query);

		ScoredExtentResult[] query_results = env.runQuery(field_query,100);
		int[] docNums = new int[query_results.length];
		
		//Print results
		for(int i=0;i<query_results.length;i++){
			docNums[i] = query_results[i].document;
			//System.out.println("Begin:"+query_results[i].begin+" End:"+query_results[i].end+" id:"+query_results[i].score);
		}
		
		//Retrieve Documents
		//Write ranked set to file
		String name = query.replace(" ", "_");
		FileWriter fw = new FileWriter("PATH TO BASELINE OUTPUT/"+name+".txt");
		BufferedWriter bw = new BufferedWriter(fw);
		PrintWriter out = new PrintWriter(bw);
		
		ParsedDocument[] passages = env.documents(docNums);
		for(int i=0;i<passages.length;i++) {
			String document_content = passages[i].content;
			int begin_index = query_results[i].begin;
			int end_index = query_results[i].end;
			begin_index = getCompleteIndex(document_content, begin_index);
			end_index = getCompleteIndex(document_content, end_index);
			String passage_doc = document_content.substring(begin_index, end_index);
			passage_doc = passage_doc.replaceAll("\n"," ");
			//System.out.println(passage_doc);
			out.println(passage_doc+"\n");
		}
		out.close();
		return query_results;
	}
	
	private static int getCompleteIndex(String doc, int id){
		int curr = id;
		int back_id = 0;
		while(curr-1>=0){
			if(doc.charAt(curr-1)=='.' && doc.charAt(curr)==' '){
				break;
			}else{
				back_id+=1;
			}
			curr-=1;
		}
		curr=id;
		int front_id = 0;
		while(curr+1 < doc.length()){
			
			if(doc.charAt(curr+1)==' ' && doc.charAt(curr)=='.'){
				front_id+=1;
				break;
			}else{
				front_id+=1;
			}
			curr+=1;
			
		}
		
		if(back_id < front_id){
			id = id - back_id;
		}else{
			id = id + front_id;
		}
		return id;
	}
	
	public static void performQuery(QueryEnvironment article_env, String query){
		try{
			//PerformBaseline(article_env, query);
		}catch(Exception e){
			System.out.println("Problem in baseline!!");
			e.printStackTrace();
		}
	}	
}