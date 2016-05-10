package edu.aueb.NPD;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.log4j.PropertyConfigurator;
import org.oxford.comlab.perfectref.parser.DLliteParser;
import org.oxford.comlab.perfectref.rewriter.Clause;
import org.oxford.comlab.perfectref.rewriter.PI;

import edu.aueb.queries.ClauseParser;
import edu.aueb.queries.Evaluator;
import edu.aueb.queries.QueryOptimization;
import edu.ntua.image.incremental.Incremental;


public class IncEvalOntologies {

	private static final DLliteParser parser = new DLliteParser();

	static String originalPath = "/Users/avenet/Academia/Ntua/Ontologies/";
	static String excelFile = "/Users/avenet/Academia/Aueb/Research/IncrementalQueryAnswering/EvaluationResults/NPD/IQAROS/DB/DB_OLDMAPPINGS_NewPreprocessingFile.xlsx";
	static String mappings;
	static String uri = "";

	static String addon = "";

	public static void main(String[] args) throws Exception{
		PropertyConfigurator.configure("./logger.properties");

		String ontologyFile, queryPath, optPath, path, dbPath;
		/**
		 * NPD Tests
		 */
		path = originalPath + "npd-benchmark-master/";
//		ontologyFile = "file:" + path + "ontology/npd-v2-ql.owl";
		ontologyFile = "file:" + path + "ontology/npd-v2-ql_checkExistentials.owl";
		queryPath = path + "avenet_queriesWithURIs/";
		mappings = path + "mappings/mysql/npd-v2-ql-mysql_chekcExistentials.obda";
//		optPath = "";
		optPath = path + "OptimizationClauses/optimizationClauses_npd-v2-ql-mysql_gstoil_avenet.obdav2.txt";
		uri = "http://semantics.crl.ibm.com/univ-bench-dl.owl#";
		/*
		 * NPD DB
		 */
		dbPath = "jdbc:mysql://localhost:3306/npd";
		System.out.println("**************************");
		System.out.println("**\tNPD\t\t**");
		System.out.println("**************************");
		runTest(ontologyFile, queryPath, null, dbPath, true, 1);
		
	}

	private static void runTest(String ontologyFile, String queryPath, String optPath, String dbPath, boolean print) throws Exception {
		runTest(ontologyFile, queryPath, optPath, dbPath, true, 0);
	}

	private static void runTest(String ontologyFile, String queryPath, String optPath, String dbPath, boolean print, int limit) throws Exception {
		runTest(ontologyFile, queryPath, optPath, dbPath, true, 0, false);
	}

	private static void runTest(String ontologyFile, String queryPath, String optPath, String dbPath, boolean print, int limit, boolean printToExcel) throws Exception {
		long start=0,totalTime = 0;

		ArrayList<PI> tBoxAxioms = parser.getAxiomsWithURI(ontologyFile);
		ArrayList<Clause> rewriting = new ArrayList<Clause>();

//		System.out.println(tBoxAxioms.size());
//		System.exit(0);
		
		QueryOptimization qOpt = null;

		File queriesDir = new File( queryPath );
		File[] queries = queriesDir.listFiles();
		if (!print)
			System.out.println("...Warming up...");

		/**
		 * Emptiness optimization
		 */
		if (optPath != null && optPath != "") {
			qOpt = new QueryOptimization(optPath);
		}

		int queryIndex=1;
    	Evaluator ev = new Evaluator(dbPath,mappings);
		for( int i=0 ; i<queries.length ; i++ ) {
			if( queries[i] == null ) {
				System.out.println(i + " " + queries[i]);
				// Either dir does not exist or is not a directory
				return;
			}
	        else if( !queries[i].toString().contains(".svn") && !queries[i].toString().contains(".DS_Store") && !queries[i].isDirectory()){
	        	System.out.println( queryIndex++ + ": " + queries[i]);
        		System.out.println(new ClauseParser().parseClause((new BufferedReader(new FileReader(queries[i]))).readLine()));
//	        	start = System.currentTimeMillis();
	        	//use optimization to cut off queries with no answers
        		Incremental incremental = new Incremental(qOpt,false);
	        	rewriting = incremental.computeUCQRewriting(tBoxAxioms,new ClauseParser().parseClause((new BufferedReader(new FileReader(queries[i]))).readLine()), print);
	        	//no optimization
//        		Incremental incremental = new Incremental();
//	        	rewriting = incremental.computeUCQRewriting(tBoxAxioms,new ClauseParser().parseClause((new BufferedReader(new FileReader(queries[i]))).readLine()));
	        	/** OR, in order to run the evaluation using non-restricted subsumption */
//	    		Configuration c = new Configuration();
//	    		c.redundancyElimination=RedundancyEliminationStrategy.Full_Subsumption;
//	    		Incremental incremental = new Incremental( c );
//	    		incremental.computeUCQRewriting(tBoxAxioms,parser.getQuery(queries[i].toString()));

	        	System.out.println( "Original rew size = " + rewriting.size() );
	        	for (Clause cl: rewriting ) {
//	        		System.out.println(cl.toString());
	        		System.out.println(cl);
//	        		System.out.println("\n\n");
	        	}

//	        	/*
//	        	 * DB evaluation
//	        	 */
	        	if ( rewriting.size() != 0 )
	        	{
	        		System.out.println("Evaluating rewriting...");
	        		ev.evaluateSQL( null, ev.getSQLWRTMappings( rewriting ), true);
	        	}

	        	System.out.println("\n\n\n");
	        }
	        else {
	        	System.out.println(i + " " + queries[i]);
	        }
		}
		ev.closeConn();

//		System.out.println( "Finished rewriting " + queries.length + " in " + totalTime + " ms" );
//		System.out.println( "Rew time = " + totalTime );

	}

}