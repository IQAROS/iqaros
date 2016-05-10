package edu.aueb.RDfox;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;
import org.oxford.comlab.perfectref.parser.DLliteParser;
import org.oxford.comlab.perfectref.rewriter.Clause;
import org.oxford.comlab.perfectref.rewriter.PI;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import edu.aueb.queries.ClauseParser;
import edu.ntua.image.incremental.Incremental;
import uk.ac.ox.cs.JRDFox.JRDFStoreException;


public class RDFoxSingleExecution {

	private static final DLliteParser parser = new DLliteParser();


	static String originalPath = "/Users/avenet/Academia/Ntua/Ontologies/";

	static String uri = "";

	public static void main(String[] args) throws Exception{
		PropertyConfigurator.configure("./logger.properties");

		String ontologyFile, queryPath, optPath, path, dbPath, dataFiles;
		RDFoxQueryEvaluator evaluator = null;

		boolean RDFoxOpt = true;

//		/**
//		 * LUBM Tests
//		 */
//		path = originalPath + "LUBM/";
//		ontologyFile = "file:" + path + "univ-bench_DL-Lite_mine.owl";
//		queryPath = path + "QueriesWithConstants/";
//		uri = "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#";
//		/*
//		 * LUBM DB
//		 */
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/LUBM";
//		optPath = path + "OptimizationClausesRDFox/optimizationClauses.txt";
//		dataFiles = "/Users/avenet/Academia/Ntua/JavaWorkspace/uba1.7/Ontos1/";
//		System.out.println("**************************");
//		System.out.println("**\tLUBM\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		System.exit(0);
//		/*
//		 * LUBM10 DB
//		 */
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/LUBM10"; //This database contains data from 15 universities
//		optPath = path + "OptimizationClausesRDFox/optimizationClauses10.txt";
//		dataFiles = "/Users/avenet/Academia/Ntua/JavaWorkspace/uba1.7/Ontos10/";
//		System.out.println("**************************");
//		System.out.println("**\tLUBM10\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		evaluator.shutDown();
//		/*
//		 * LUBM30 DB
//		 */
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/LUBM30"; //This database contains data from 30 universities
//		optPath = path + "OptimizationClausesRDFox/optimizationClauses30.txt";
//		dataFiles = "/Users/avenet/Academia/Ntua/JavaWorkspace/uba1.7/Ontos30/";
//		System.out.println("**************************");
//		System.out.println("**\tLUBM30\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		evaluator.shutDown();
//		/**
//		 * UOBM Tests
//		 */
//		path = originalPath + "UOBM/";
//		ontologyFile = "file:" + path + "univ-bench-dl-Zhou_DL-Lite_mine.owl";
//		queryPath = path + "QueriesWithConstants/";
//		optPath = path + "OptimizationClausesRDFox/optimizationClauses.txt";
//		uri = "http://semantics.crl.ibm.com/univ-bench-dl.owl#";
//		dataFiles = path + "UOBMGenerator/preload_generated_uobm50_dbcreation/";
//		/*
//		 * UOBM DB
//		 */
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/UOBM";
//		System.out.println("**************************");
//		System.out.println("**\tUOBM\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		evaluator.shutDown();
//		/*
//		 * UOBM10 DB
//		 */
//		optPath = path + "OptimizationClausesRDFox/optimizationClauses10.txt";
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/UOBM10"; //This database contains data from 15 universities
//		System.out.println("**************************");
//		System.out.println("**\tUOBM10\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		evaluator.shutDown();
////		System.exit(0);
//		/*
//		 * UOBM30 DB
//		 */
//		optPath = path + "OptimizationClausesRDFox/optimizationClauses30.txt";
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/UOBM30"; //This database contains data from 30 universities
//		System.out.println("**************************");
//		System.out.println("**\tUOBM30\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		evaluator.shutDown();
//		/**
//		 * Semintec Tests
//		 */
//		path = originalPath + "Semintec/";
//		ontologyFile = "file:" + path + "bigFile_DL-Lite_mine.owl";
//		queryPath = path + "Queries/";
//		optPath = path + "OptimizationClausesRDFox/optimizationClauses.txt";
//		uri = "http://www.owl-ontologies.com/unnamed.owl#";
//		/*
//		 * Semintex DB
//		 */
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/Semintec";
//		dataFiles = path;
//		System.out.println("**************************");
//		System.out.println("**\tSemintec\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		evaluator.shutDown();
//		/**
//		 * Vicodi Tests
//		 */
//		path = originalPath + "Vicodi/";
//		ontologyFile = "file:" + path + "vicodi_all_DL-Lite_mine.owl";
//		queryPath = path + "Queries/";
//		optPath = path + "OptimizationClausesRDFox/optimizationClauses.txt";
//		uri = "http://vicodi.org/ontology#";
//		/*
//		 * Vicodi DB
//		 */
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/Vicodi";
//		dataFiles = path;
//		System.out.println("**************************");
//		System.out.println("**\tVicodi\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		evaluator.shutDown();
		/**
		 * Reactome tests
		 */
		path =  originalPath + "reactome/";
		ontologyFile = "file:" + path + "biopax-level3-processed_DL-Lite_mine.owl";
		queryPath = path + "Queries/";
		optPath = path + "OptimizationClausesRDFox/optimizationClauses.txt";
		uri = "http://www.biopax.org/release/biopax-level3.owl#";
		/*
		 * Reactome DB
		 */
		dbPath = "jdbc:postgresql://127.0.0.1:5432/reactome";
		dataFiles = path;
		System.out.println("**************************");
		System.out.println("**\tReactome\t\t**");
		System.out.println("**************************");
		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		evaluator.shutDown();
		System.exit(0);

	}

	private static RDFoxQueryEvaluator createDataSetsAndEvaluator(String dataFiles, String dbPath, String ontologyFile) throws FileNotFoundException, OWLOntologyCreationException, URISyntaxException, JRDFStoreException {

		String[] datasetFiles = null;
		RDFoxQueryEvaluator evaluator = new RDFoxQueryEvaluator();
		
		System.out.println( "dataFiles = " + dataFiles  );
		if ( dataFiles.contains("uba1.7") ){
			System.out.println("LUBM" + dataFiles);
			datasetFiles = createDataSetFilesLUBM(dataFiles);
		}
		else if (dbPath.contains("UOBM10") ) {
			System.out.println("UOBM10");
			datasetFiles = createDataSetFilesUOBM(dataFiles,10);
		}
		else if (dbPath.contains("UOBM30") ) {
			System.out.println("UOBM30");
			datasetFiles = createDataSetFilesUOBM(dataFiles,30);
		}
		else if (dbPath.contains("UOBM") ) {
			System.out.println("UOBM");
			datasetFiles = createDataSetFilesUOBM(dataFiles,1);
		}
		else if (dataFiles.contains("Semintec") ) {
			System.out.println("Semintec");
			datasetFiles = createDataSetFilesSemintec(dataFiles);
		}
		else if (dataFiles.contains("Vicodi") ) {
			System.out.println("Vicodi");
			datasetFiles = createDataSetFilesVicodi(dataFiles);
		}
		else if (dataFiles.contains("reactome") ) {
			System.out.println("reactome");
			datasetFiles = createDataSetFilesReactome(dataFiles);
		}
		evaluator.loadInputToSystem(ontologyFile, datasetFiles);
    	return evaluator;
	}
	

	private static void runTest(String ontologyFile, String queryPath, String optPath, RDFoxQueryEvaluator evaluator, boolean print, boolean RDFoxOpt) throws Exception {
		long start=0,totalTime = 0;
		ArrayList<PI> tBoxAxioms = parser.getAxioms(ontologyFile);
		ArrayList<Clause> rewriting = new ArrayList<Clause>();

		File queriesDir = new File( queryPath );
		File[] queries = queriesDir.listFiles();

//		System.out.println( queriesDir );
		for( int i=0 ; i<queries.length ; i++ ) {
			if( queries[i] == null )
				// Either dir does not exist or is not a directory
				return;
	        else if( !queries[i].toString().contains(".svn") && !queries[i].toString().contains(".DS_Store") ){
	        	
	        	if ( !queries[i].toString().contains("01") )
	        		continue;
//	        	if ( ontologyFile.contains("UOBM") && queries[i].toString().contains("Query_11") )
//	        		continue;
//	        		&& !queries[i].toString().contains("Query_11.txt")) {
//	        	if (print)
//	        		System.out.println(queries[i] + "\n" + parser.getQuery(queries[i].toString()));
	        	Clause query = new ClauseParser().parseClause((new BufferedReader(new FileReader(queries[i]))).readLine());
        		System.out.println(i + ": " + queries[i] + "\n" + new ClauseParser().parseClause((new BufferedReader(new FileReader(queries[i]))).readLine()));
//	        	start = System.currentTimeMillis();
	        	//use optimization to cut off queries with no answers
//        		Incremental incremental = new Incremental(optPath,RDFoxOpt);
//	        	rewriting = incremental.computeUCQRewriting(tBoxAxioms,new ClauseParser().parseClause((new BufferedReader(new FileReader(queries[i]))).readLine()), print);
	        	//no optimization
	        	rewriting = new Incremental().computeUCQRewriting(tBoxAxioms,new ClauseParser().parseClause((new BufferedReader(new FileReader(queries[i]))).readLine()));
	        	/** OR, in order to run the evaluation using non-restricted subsumption */
//	    		Configuration c = new Configuration();
//	    		c.redundancyElimination=RedundancyEliminationStrategy.Full_Subsumption;
//	    		Incremental incremental = new Incremental( c );
//	    		incremental.computeUCQRewriting(tBoxAxioms,parser.getQuery(queries[i].toString()));

//	        	for (Clause cl: rewriting )
//	        		System.out.println(cl + "\t");

//	        	System.out.println( "Original rew size = " + rewriting.size() );
	        	Set<Clause> RDFoxRew = new HashSet<Clause>();
	        	for ( Clause c: rewriting )
	        		if (c.getToBeSentToRDFox()) {
	        			RDFoxRew.add(c);
	        			System.out.println(c);
	        		}
	        	System.out.println( "RFDox rew size = " + RDFoxRew.size() );
	        	
//	        	System.out.println(query);
//	        	RDFoxRew.add(query);
	        	/*
	        	 * RDFox evaluation
	        	 */
	        	if ( RDFoxRew.size() != 0 ) {
	        		long startTime = System.currentTimeMillis();
	        		long ans = evaluator.evaluateQuery(RDFoxRew, uri);
	        		long end = System.currentTimeMillis() - startTime;
	        		System.out.println( "RDFox Answers: " + ans + " in " + end + "ms");
	        	}
//	        	else if (  )
        		System.out.println("\n\n");

//				totalTime+= (System.currentTimeMillis()-start);
//	        	System.out.println("\n\n\n");
	        }
		}
//		System.out.println( "Finished rewriting " + queries.length + " in " + totalTime + " ms" );
//		System.out.println( "Rew time = " + totalTime );

	}

	public static String[] createDataSetFilesLUBM(String dataFilesPath) {

		File dataDir = new File( dataFilesPath );
		File[] dataFiles = dataDir.listFiles();

		int k = 0;

		for( int i=0 ; i<dataFiles.length ; i++ ) {
//			if( dataFiles[i] == null )
//				// Either dir does not exist or is not a directory
//				return;
//			else
			if( !dataFiles[i].toString().contains(".svn") && !dataFiles[i].toString().contains(".DS_Store") ){
				k++;
			}
		}

//		System.out.println( "k = " + k);

		String[] result = new String[k];
		int globalCounter = 0;
		for ( int j = 0 ; globalCounter < k ; j++) {
//			System.out.println(globalCounter);

			int limit = 0;
			if (j==0)
				limit = 14;
			else if (j==1)
				limit = 18;
			else if (j==2)
				limit = 15;
			else if (j==3)
				limit = 20;
			else if (j==4)
				limit = 21;
			else if (j==5)
				limit = 14;
			else if (j==6)
				limit = 23;
			else if (j==7)
				limit = 16;
			else if (j==8)
				limit = 17;
			else if (j==9)
				limit = 21;
			else if (j==10)
				limit = 19;
			else if (j==11)
				limit = 23;
			else if (j==12)
				limit = 24;
			else if (j==13)
				limit = 18;
			else if (j==14)
				limit = 15;
			else if (j==15)
				limit = 20;
			else if (j==16)
				limit = 18;
			else if (j==17)
				limit = 22;
			else if (j==18)
				limit = 20;
			else if (j==19)
				limit = 24;
			else if (j==20)
				limit = 14;
			else if (j==21)
				limit = 18;
			else if (j==22)
				limit = 16;
			else if (j==23)
				limit = 20;
			else if (j==24)
				limit = 21;
			else if (j==25)
				limit = 15;
			else if (j==26)
				limit = 23;
			else if (j==27)
				limit = 17;
			else if (j==28)
				limit = 17;
			else if (j==29)
				limit = 22;
			for ( int i = 0 ; i <= limit ; i++) {
				result[globalCounter++] = dataFilesPath + "University"+ j + "_" + i + ".owl";
//				System.out.println(result[globalCounter]);
			}

		}
		return result;
	}

	public static String[] createDataSetFilesUOBM(String dataFilesPath, int limit) {
		String[] result = new String[limit];

		for ( int j = 0 ; j < limit ; j++) {
			result[j] = dataFilesPath + "univ"+ j + ".owl";
		}

		return result;
	}

	public static String[] createDataSetFilesVicodi(String dataFilesPath) {
		String[] result = new String[1];

		result[0] = dataFilesPath + "vicodi_all.owl";

		return result;
	}

	public static String[] createDataSetFilesSemintec(String dataFilesPath) {
		String[] result = new String[1];

		result[0] = dataFilesPath + "bigFile.owl";

		return result;
	}

	public static String[] createDataSetFilesReactome(String dataFilesPath) {
		String[] datasetFiles = new String[1];
		datasetFiles[0] = dataFilesPath + "sample_10.ttl";

		return datasetFiles;
	}

}