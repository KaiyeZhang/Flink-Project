package ml;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 *
 * Apriori algorithm implemented in Flink.
 *
 */
public class task2 {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

		String inputDir = params.getRequired("gene-dir");
        if(inputDir.charAt(inputDir.length() - 1) != '/') {
            inputDir = inputDir + '/';
        }

		// Read in the gene file.
        DataSet<String> geneRaw = env.readTextFile(inputDir + "GEO.txt");

        // Extract patientId and their expressed geneId, outputting:
        // (patientId, geneId)
        DataSet<Tuple2<Integer, Integer>> patientIdGeneId =
            geneRaw
                .map(line ->{
                		String[] dataArray = line.split(",");
                		if(dataArray.length == 3) {
							 try {
								 if(Double.parseDouble(dataArray[2]) > 1250000) {
									 int patientId = Integer.parseInt(dataArray[0].replaceAll("patient", ""));
									 return new Tuple2<Integer, Integer>(patientId, Integer.parseInt(dataArray[1]));
								 }
							 } catch (Exception ex) {
								 ex.printStackTrace();
								 return null;
							 }
						 }
						return null;
					})
                .filter(line -> line != null);

        // Read in the patients CSV file 
        // FlatMap the disease list. If has cancer, output:
        // (patentId, count)
        DataSet<Tuple2<Integer, Integer>> cPatientIdCount =
            env.readTextFile(inputDir + "PatientMetaData.txt")
                .map(line -> {
                		String[] dataArray = line.split(",");
						if(dataArray.length == 6) {
						    String[] diseases = dataArray[4].split(" ");
						String[] cancerList = {"breast-cancer","prostate-cancer","pancreatic-cancer","leukemia","lymphoma"};
						    for(String disease : diseases) {
						    	if(Arrays.asList(cancerList).contains(disease)) {
						    		int patientId = Integer.parseInt(dataArray[0].replaceAll("patient", ""));
						    		return new Tuple2<Integer, Integer>(patientId, 1);
						    	}
						    }
						 } 
						return null;
					})
                .filter(line -> line != null);

        // Join the two data sets to extract the cancer patients with gene expressed:
        // (patientId, geneId), (patientId, cancerType)
        //
        // ... to:
        // (patientId, geneId)
        DataSet<Tuple2<Integer, Integer>> cPatientIdGeneId =
        	cPatientIdCount
                .join(patientIdGeneId)
                .where(0)
                .equalTo(0)
                .projectSecond(0,1);

     // get minimum support
        final double minSupportRatio = params.getDouble("min-support", 0.3);
        final long numOfPatients = cPatientIdCount.count();
        final long minSupport = (long) (minSupportRatio * numOfPatients);
        
        // get maximum size
        final int maxSize = params.getInt("max-size", 5);

		DataSet<Tuple2<Integer, ArrayList<Integer>>> transactions = cPatientIdGeneId
				.groupBy(0)
				.reduceGroup(new gptransactions());

		// compute frequent itemsets for itemset_size = 1
		DataSet<geneset> itemSet1 = cPatientIdGeneId
				// map item to 1
				.map(new selfInputMapFunction())
				// group by hashCode of the ItemSet
				.groupBy(new genePosition())
				// sum the number of transactions containing the ItemSet
				.reduce(new genesetReducer())
				// remove ItemSets with frequency under the support threshold
				.filter(new FrequencyFilterFunction(minSupport));

		// start of the loop
		// itemset_size = 2
		IterativeDataSet<geneset> initial = itemSet1.iterate(maxSize - 1);

		// create the candidate itemset for the next iteration
		DataSet<geneset> candidates = initial.cross(itemSet1)
				.with(new allgenesetcross())
				.distinct(new genePosition());

		// calculate actual numberOfTransactions
		DataSet<geneset> selected = candidates
				.map(new genesetoccurences()).withBroadcastSet(transactions, "transactions")
				.filter(new FrequencyFilterFunction(minSupport));

		// end of the loop
		// stop when we run out of iterations or candidates is empty
		DataSet<geneset> output = initial.closeWith(selected, selected);
		

		if (params.has("output")) {
			// write the final solution to file
		DataSet<Tuple2<Integer, ArrayList<Integer>>> tmp = 
				output
					.map(new OutputMapFunction());
			
		DataSet<Tuple2<Integer, String>> results = tmp
		        	.groupBy(0)
		        	.reduceGroup(new geneGroupReduceFunction());
		results
			.partitionCustom(new selfPartitioner(), 0)
			.sortPartition(0, Order.DESCENDING)
			.map(tuple -> tuple.f0+"\t"+tuple.f1)
			.writeAsText(params.get("output"));
			
			env.execute("Flink Apriori");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			output.print();
		}
	}

}
