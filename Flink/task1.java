package ml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;

public class task1 {
	public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String geneDir = params.getRequired("gene-dir");
        if(geneDir.charAt(geneDir.length() - 1) != '/') {
            geneDir = geneDir + '/';
        }

        // Read in the ratings file.
        DataSet<String> patientRaw = env.readTextFile(geneDir + "PatientMetaData.txt");

        // Count how many ratings each movie has received, outputting:
        // (patient, cancer)
        DataSet<Tuple2<String, String>> cancer =
        		patientRaw
                .flatMap((line, out) -> {
                	String[] values = line.split(",");
                	int length = values.length;
                	if (length==6){
                		ArrayList<String> canlist = new ArrayList<String>();  
                		String patientid="";
                		String[] disease=values[4].split(" ");
                		String cnames[] = {"breast-cancer", "prostate-cancer", "pancreatic-cancer", "leukemia", "lymphoma"};
                		for(int i=0;i<disease.length;i++){
                			for(int j=0;j<cnames.length;j++){
                				if(disease[i].contains(cnames[j])){
                					patientid=values[0];
                    				canlist.add(disease[i]);
                    			}
                			}
                			
                		}
                		if(patientid!=null){
                			for(String can : canlist) {
                                out.collect(new Tuple2<String, String>(patientid, can));
                            }               			
               		}
                        
                		
                	}
                	
                })
                ;
        // Read in the movies CSV file, whose initial format is:
        // (movieId, name, genreList)
        //
        // We'll flatMap the genre list:
        // (patient, 1)
        DataSet<Tuple2<String, Integer>> patientnum =
            env.readTextFile(geneDir + "GEO.txt")
                .flatMap((line, out) -> {
                    String[] values = line.split(",");
                    int length = values.length;

                    if(length == 3) {
                        String patientid = values[0].trim();
                        int geneid=prasetoint(values[1].trim());
                        float expre=prasetofloat(values[2].trim());
                        float threshold=1250000;

                        if((geneid==42)&&(expre > threshold)) { // Movie title contains commas?
                        	out.collect(new Tuple2<String, Integer>(patientid, 1));
                        }

                    }
                });

        // Join the two datasets to find the rating of each movie:
        // (movieId, ratingCount), (movieId, name, genre)
        //
        // ... to:
        // (cancer,1)
        DataSet<Tuple2<String,Integer>> cancerpeople =
        		cancer
                .join(patientnum)
                .where(0)
                .equalTo(0)
                .projectFirst(1)
                .projectSecond(1);

        // Map the dataset to:
        // (genre, MovieRatingCount)
        DataSet<Tuple2<String, Integer>> sumpatient =
        		cancerpeople
        		.groupBy(0)
        		.sortGroup(0, Order.ASCENDING)
                .reduceGroup((tuples, out) -> {
                    String cancertype = "";
                    int sum=0;

                    for(Tuple2<String, Integer> tuple : tuples) {
                        cancertype = tuple.f0;
                        sum += tuple.f1;
                    }

                    out.collect(new Tuple2<String, Integer>(cancertype, sum));
                });

        // Group movies for each genre and sort based on rating count, going from:
        // (genre, MovieRatingCount)
        //
        // ... to:
        // (genre, [MovieRatingCount1, ..., MovieRatingCount5])
        DataSet<Tuple2<String, Integer>> sortedsum =
        		sumpatient
        		.partitionByRange(0)
        		.sortPartition(1, Order.DESCENDING)
                ;


        // End the program by writing the output!
        if(params.has("output")) {
    		sortedsum
    		.writeAsFormattedText(params.get("output"),
    			    new TextFormatter<Tuple2<String, Integer>>() {
    			        public String format (Tuple2<String, Integer> value) {
    			            return value.f0 + "\t" + value.f1;
    			        }
    			    });
        	

            env.execute();
        } else {
            // Always limit direct printing
            // as it requires pooling all resources into the driver.
            System.err.println("No output location specified; printing first 100.");
            sortedsum.first(100).print();
        }
	}
	protected static float prasetofloat(String exp){
		float val = 0;
		try{
			val=Float.parseFloat(exp);
		}catch(Exception e){
			System.out.println("expression_value");
			e.printStackTrace(); //not doing anything
		}
		return val;
		
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
}