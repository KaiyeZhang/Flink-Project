package ml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;

public class task3 {
	public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		HashMap<String,Integer> allSupport = new HashMap<String, Integer>();
//		int minsupport=(int) Math.round(minsupportrate*numtransactions);

        // Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String geneDir = params.getRequired("gene-dir");
        if(geneDir.charAt(geneDir.length() - 1) != '/') {
            geneDir = geneDir + '/';
        }

        // Read in the ratings file.
        DataSet<String> input = env.readTextFile(geneDir);

        // Count how many ratings each movie has received, outputting:
        // (patient, cancer)
        DataSet<Tuple2<String, Integer>> allsetsupport =
        		input
                .flatMap((line, out) -> {
                	String[] values = line.split("\t");
                	Map<String, Integer> genemap = new HashMap<String,Integer>();
                	
                	
                	int length = values.length;

                	for(int i=1;i<length;i++){
                		String temp="";
                		String[] sin=values[i].split(",");
                		for(int j=0;j<sin.length;j++){
                			temp+=sin[j].trim()+" ";
                		}
                		genemap.put(temp.trim(),prasetoint(values[0]));
                	}                         
                	for(Map.Entry<String, Integer> entry : genemap.entrySet()){
               		 out.collect(new Tuple2<String, Integer>(entry.getKey(), entry.getValue()));
                	}  	
                	
                })
                ;
       List<Tuple2<String,Integer>> collected = allsetsupport.collect();
		for (Tuple2<String, Integer> tuple : collected) {
			allSupport.put(tuple.f0, tuple.f1);
		}
        
        DataSet<Tuple1<String>> patientnum =
        		allsetsupport
                .flatMap((line, out) -> {
                    String[] values = line.f0.split(" ");
                    int length = values.length;

                    if(length >1) {
                    	ArrayList<Integer> allgene = new ArrayList<Integer>();
                    	ArrayList<String> subset = new ArrayList<String>();
                    	for(int i=0;i<length;i++){
                    		allgene.add(prasetoint(values[i].trim()));
                    	}
                    	HashSet<ArrayList<Integer>> temp=getSubsets(allgene);
                    	String result="";
                    	for(ArrayList<Integer> te:temp){
                    		Collections.sort(te);
                    		String tempkk="";
                    		te=(ArrayList<Integer>) te.stream().distinct().collect(Collectors.toList());
                    		for(int j=0;j<te.size();j++){
                    			result+=te.get(j)+" ";
                    		}
                    		tempkk=distinct(result);
                    		subset.add(tempkk.trim());
                    	}
                    	
                    	ArrayList<String> templist = new ArrayList<String>();
                    	
                    	for(String pp:subset){
                    		int len=pp.split(" ").length;
                    		if(!pp.isEmpty()&&len>1){
                    			templist.add(pp.trim());
                    		}
                    	}
//                    	for(String k:templist){
//                    		out.collect(new Tuple1<String>(k));
//                    	}
                    	String  outformat="";
                    	ArrayList<String> finalout = new ArrayList<String>();
                    	for(String k:templist){
                    		String tp=distinct(k);
                    		int lentp=tp.split(" ").length;
                    		int lenf0=line.f0.split(" ").length;
 //                   		if(lentp!=lenf0){
                        		//int suppR=allSupport.get(tp.trim());
                        		//double confi=((double)line.f1/(double)suppR);
                        		//outformat=tp+" "+"("+line.f0+"-"+tp+")"+"\t"+confi;
                    			outformat=tp;
                        		finalout.add(outformat);
//                    		}
                    	}
                    	for(String k:finalout){
                    		out.collect(new Tuple1<String>(k));
                    	}
                    	

                    }
                })
                ;
        
        DataSet<Tuple1<String>> confidence=
        		patientnum
                .reduceGroup((tuples, out) -> {
                List<String> DisList = new ArrayList<>();
      			for(Tuple1<String> tuple:tuples){
      				DisList.add(tuple.f0);
      			}
      			DisList=DisList.stream().distinct().collect(Collectors.toList());
      			String  outformat="";
      			ArrayList<String> finalout = new ArrayList<String>();
      			double confi;
      			for(String k:DisList){
      				for(Map.Entry<String, Integer> entry : allSupport.entrySet()){
               		 if((entry.getKey().trim()!=k.trim())&&(entry.getKey().contains(k))){	 
               			 confi=((double)entry.getValue()/(double)allSupport.get(k.trim()));
               			double temp=(double)Math.round(confi * 100d) / 100d;
               			 if(confi>0.6){
               				outformat=k+" "+"("+entry.getKey()+"-"+k+")"+"\t"+confi;
               				finalout.add(outformat);
               			 }	 
               		 }
                	} 
      				
      			}
             	for(String hh:finalout){
             		out.collect(new Tuple1<String>(hh));
             	}
              	
            });
//        		.flatMap((line,out)->{
//        			String value=line.f0;
//        			String  outformat="";
//        			ArrayList<String> finalout = new ArrayList<String>();
//        			double confi;
//                	for(Map.Entry<String, Integer> entry : allSupport.entrySet()){
//                  		 if((entry.getKey().trim()!=value.trim())&&(entry.getKey().contains(value))){	 
//                  			 confi=((double)entry.getValue()/(double)allSupport.get(value.trim()));
//                  			double temp=(double)Math.round(confi * 100d) / 100d;
//                  			 if(confi>0.6){
//                  				outformat=line.f0+" "+"("+entry.getKey()+"-"+line.f0+")"+"\t"+confi;
//                  				finalout.add(outformat);
//                  			 }	 
//                  		 }
//                   	} 
//                	for(String k:finalout){
//                		out.collect(new Tuple1<String>(k));
//                	}
//        			
//        		});





        // End the program by writing the output!
        if(params.has("output")) {
        	confidence.writeAsCsv(params.get("output"));
            env.execute();
        } else {
            // Always limit direct printing
            // as it requires pooling all resources into the driver.
            System.err.println("No output location specified; printing first 100.");
            confidence.first(100).print();
        }
	}
	
	protected static int prasetoint(String gene){
		int val = 0;
		try{
			val=Integer.parseInt(gene);
		}catch(Exception e){
			System.out.println("gene id");
			e.printStackTrace(); //not doing anything
		}
		return val;
		
	}
	protected static int findsupport(String exp,HashMap<String,Integer> allSupport){
		int val = 0;
		for (Map.Entry<String, Integer> entry : allSupport.entrySet()) {
			String entryvalues=entry.getKey();
                if(entryvalues.trim().equals(exp)){                
                val=entry.getValue();
		}
	  }

		return val;
	}
	public static HashSet<ArrayList<Integer>> getSubsets(ArrayList<Integer> subList) {
		  HashSet<ArrayList<Integer>> allsubsets = new HashSet<ArrayList<Integer>>();
		  int max = 1 << subList.size();
		  for(int loop = 0; loop < max; loop++) {
		   int index = 0;
		   int temp = loop;
		   ArrayList<Integer> currentCharList = new ArrayList<Integer>();
		   while(temp > 0) {
		    if((temp & 1) > 0) {
		     currentCharList.add(subList.get(index));
		    }
		    temp>>=1;
		    index++;
		   }    
		   allsubsets.add(currentCharList);  
		   }
		  return allsubsets;
	}
	protected static String distinct(String temp) {
		List<Integer> DisList = new ArrayList<>();
		String result="";
		String[] values=temp.split(" ");
		for(int i=0;i<values.length;i++){
			DisList.add(prasetoint(values[i].trim()));
		}
		Collections.sort(DisList);
		DisList=DisList.stream().distinct().collect(Collectors.toList());
		for(int j=0;j<DisList.size();j++){
			result+=DisList.get(j)+" ";
		}
		return result;
	}
	
	
		

}
