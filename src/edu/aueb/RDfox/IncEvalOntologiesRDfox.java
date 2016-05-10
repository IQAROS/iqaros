package edu.aueb.RDfox;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
import edu.aueb.queries.QueryOptimization;
import edu.ntua.image.incremental.Incremental;
import uk.ac.ox.cs.JRDFox.JRDFStoreException;


public class IncEvalOntologiesRDfox {

	private static final DLliteParser parser = new DLliteParser();

	private int orderedQueryIndex=0;

	static String originalPath = "/Users/avenet/Academia/Ntua/Ontologies/";

	static String excelFile = "";
	static boolean print2Excel = true;

	static String addon = "";
	static String dbPath = "";


	public static void main(String[] args) throws Exception{
		PropertyConfigurator.configure("./logger.properties");

		String ontologyFile, queryPath, optPath, path, dataFiles;
		RDFoxQueryEvaluator evaluator = null;

		boolean RDFoxOpt = true;

//		/**
//		 * LUBM Tests
//		 */
//		path = originalPath + "LUBM/";
//		ontologyFile = "file:" + path + "univ-bench_DL-Lite_mine.owl";
////		ontologyFile = "file:" + path + "univ-bench-gstoil.owl";
//		queryPath = path + "QueriesWithConstantsRDFox/";
//		/*
//		 * LUBM DB
//		 */
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/LUBM";
//		optPath = path + "OptimizationClausesRDFox/optimizationClausesLUBM.txt";
//		dataFiles = "/Users/avenet/Academia/Ntua/JavaWorkspace/uba1.7/Ontos1/";
//		System.out.println("**************************");
//		System.out.println("**\tLUBM\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, null, evaluator, true, RDFoxOpt);
//		/*
//		 * LUBM10 DB
//		 */
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/LUBM10"; //This database contains data from 15 universities
//		optPath = path + "OptimizationClausesRDFox/optimizationClausesLUBM10.txt";
//		dataFiles = "/Users/avenet/Academia/Ntua/JavaWorkspace/uba1.7/Ontos10/";
//		System.out.println("**************************");
//		System.out.println("**\tLUBM10\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		runTest(ontologyFile, queryPath, null, evaluator, true, RDFoxOpt);
//		/*
//		 * LUBM30 DB
//		 */
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/LUBM30"; //This database contains data from 30 universities
//		optPath = path + "OptimizationClausesRDFox/optimizationClausesLUBM30.txt";
//		dataFiles = "/Users/avenet/Academia/Ntua/JavaWorkspace/uba1.7/Ontos30/";
//		System.out.println("**************************");
//		System.out.println("**\tLUBM30\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		
//		excelFile = "/Users/avenet/Academia/Aueb/Research/IncrementalQueryAnswering/EvaluationResults/LUBM/IQAROS/RDFox/RDFox_newRLRules.xlsx";
//		/**
//		 * LUBM-ex Tests
//		 */
//		path = originalPath + "LUBM-ex/";
//		ontologyFile = "file:" + path + "EUGen/ontologies/lubm-ex-20.owl";
//		queryPath = path + "EUGen/QueriesRDFox/";
//		uri = "";
//		/*
//		 * LUBM-20ex DB
//		 */
//		dataFiles = "/Users/avenet/Academia/Ntua/JavaWorkspace/uba1.7/lubmex20-1Univ/";
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/LUBM_20ex_1";
//		optPath = path + "OptimizationClausesRDFox/optimizationClauses20ex.txt";
//		System.out.println("**************************");
//		System.out.println("**\tLUBM-ex\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		/**
//		 * UOBM Tests
//		 */
//		path = originalPath + "UOBM/";
//		ontologyFile = "file:" + path + "univ-bench-dl-Zhou_DL-Lite_mine.owl";
//		queryPath = path + "QueriesWithConstantsRDFox/";
//		optPath = path + "OptimizationClausesRDFox/optimizationClausesUOBM.txt";
//		dataFiles = path + "UOBMGenerator/preload_generated_uobm50_dbcreation/";
//		/*
//		 * UOBM DB
//		 */
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/UOBM";
//		System.out.println("**************************");
//		System.out.println("**\tUOBM\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		print2Excel = false;
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		print2Excel = true;
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		/*
//		 * UOBM10 DB
//		 */
//		optPath = path + "OptimizationClausesRDFox/optimizationClausesUOBM10.txt";
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/UOBM10"; //This database contains data from 15 universities
//		System.out.println("**************************");
//		System.out.println("**\tUOBM10\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
////		print2Excel = false;
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		print2Excel = true;
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		/*
////		 * UOBM30 DB
////		 */
//		optPath = path + "OptimizationClausesRDFox/optimizationClausesUOBM30.txt";
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/UOBM30"; //This database contains data from 30 universities
//		System.out.println("**************************");
//		System.out.println("**\tUOBM30\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
////		print2Excel = false;
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		print2Excel = true;
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		excelFile = "/Users/avenet/Academia/Aueb/Research/IncrementalQueryAnswering/EvaluationResults/UOBM/IQAROS/RDFox/RDFoxNewRules.xlsx";

////		/**
////		 * Semintec Tests
////		 */
////		path = originalPath + "Semintec/";
////		ontologyFile = "file:" + path + "bigFile_DL-Lite_mine.owl";
////		queryPath = path + "QueriesRDFox/";
////		optPath = path + "OptimizationClausesRDFox/optimizationClauses.txt";
////		uri = "http://www.owl-ontologies.com/unnamed.owl#";
////		/*
////		 * Semintex DB
////		 */
////		dbPath = "jdbc:postgresql://127.0.0.1:5432/Semintec";
////		dataFiles = path;
////		System.out.println("**************************");
////		System.out.println("**\tSemintec\t\t**");
////		System.out.println("**************************");
////		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		evaluator.shutDown();
////		/**
////		 * Vicodi Tests
////		 */
////		path = originalPath + "Vicodi/";
////		ontologyFile = "file:" + path + "vicodi_all_DL-Lite_mine.owl";
////		queryPath = path + "QueriesRDFox/";
////		optPath = path + "OptimizationClausesRDFox/optimizationClauses.txt";
////		uri = "http://vicodi.org/ontology#";
////		/*
////		 * Vicodi DB
////		 */
////		dbPath = "jdbc:postgresql://127.0.0.1:5432/Vicodi";
////		dataFiles = path;
////		System.out.println("**************************");
////		System.out.println("**\tVicodi\t\t**");
////		System.out.println("**************************");
////		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		evaluator.shutDown();
//		/**
//		 * Reactome tests
//		 */
//		path =  originalPath + "reactome/";
//		ontologyFile = "file:" + path + "biopax-level3-processed.owl";
//		queryPath = path + "QueriesRDFox/";
//		optPath = path + "OptimizationClausesRDFox/optimizationClauses.txt";
//		/*
//		 * Reactome DB
//		 */
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/reactome";
//		dataFiles = path;
//		System.out.println("**************************");
//		System.out.println("**\tReactome\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
////		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//		excelFile = "/Users/avenet/Academia/Aueb/Research/IncrementalQueryAnswering/EvaluationResults/reactome/IQAROS/RDFox/RDFox_newRules.xlsx";
//
//		/**
//		 * Fly tests
//		 */
//		path =  originalPath + "Fly/";
////		String onto = "";
////		String onto = "-new";
//		String onto = "-new-noTrans";
//		ontologyFile = "file:" + path + "fly_anatomy_XP_with_GJ_FC_individuals_owl-tBox-AssNorm" + onto + ".owl";
////		ontologyFile = "file:" + path + "Fly_DL_Lite.owl";
//		queryPath = path + "QueriesRDFox/";
//		optPath = path + "OptimizationClausesRDFox/OptimizationClauses"+ onto + ".txt";
////		optPath = path + "OptimizationClausesRDFox/OptimizationClauses"+ onto + "_noCombinations.txt";
//		dataFiles = path;
////		excelFile = "/Users/avenet/Academia/Aueb/Research/IncrementalQueryAnswering/EvaluationResults/Fly/IQAROS/RDFox/Fly" + onto + "NAIVE.xlsx";
////		excelFile = "/Users/avenet/Academia/Aueb/Research/IncrementalQueryAnswering/EvaluationResults/Fly/IQAROS/RDFox/Fly" + onto + ".xlsx";
//		print2Excel = true;
//		excelFile = "/Users/avenet/Academia/Aueb/Research/IncrementalQueryAnswering/EvaluationResults/Fly/IQAROS/RDFox/Fly" + onto + "_newRules.xlsx";
//		/*
//		 * Fly DB
//		 */
//		dbPath = "jdbc:postgresql://127.0.0.1:5432/Fly";
//		System.out.println("**************************");
//		System.out.println("**\tFly\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
////		print2Excel=false;
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, true);

		/**
		 * NPD tests
		 */
//		path =  originalPath + "npd_original/";
		path =  originalPath + "npd-benchmark-master/";
		ontologyFile = "file:" + path + "ontology/npd-v2-ql.owl";
//		ontologyFile = "file:" + path + "npd.owl";
		queryPath = path + "avenet_queriesWithURIs/";
//		optPath = path + "";
		optPath = path + "OptimizationClausesRDFox/OptimizationClauses_npd-v2-ql-mysql_gstoil_avenet.obda_v2.txt";
		dataFiles = path;
		/*
		 * NPD DB
		 */
		dbPath = "jdbc:mysql://127.0.0.1:3306/npd";
		System.out.println("**************************");
		System.out.println("**\tNPD\t\t**");
		System.out.println("**************************");
		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);

		excelFile = "/Users/avenet/Academia/Aueb/Research/IncrementalQueryAnswering/EvaluationResults/NPD/IQAROS/RDFox/RDFox_DiorthomenaQueriesv2.xlsx";


//		/**
//		 * Chembl tests
//		 */
//		path =  originalPath + "Chembl/";
//		ontologyFile = "file:" + path + "cco-noDPR_rdfxml.owl";
//		queryPath = path + "Queries/";
////		optPath = path + "";
//		optPath = path + "OptimizationClausesRDFox/OptimizationClauses.txt";
//		dataFiles = path;
//		/*
//		 * NPD DB
//		 */
//		dbPath = "jdbc:mysql://127.0.0.1:3306/Chembl";
//		System.out.println("**************************");
//		System.out.println("**\tChembl\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//
//		excelFile =  "/Users/avenet/Academia/Aueb/Research/IncrementalQueryAnswering/EvaluationResults/Chembl/IQAROS/RDFox/Chembl_newRules.xlsx";
//
//		/**
//		 * Uniprot tests
//		 */
//		path =  originalPath + "Uniprot/";
//		ontologyFile = "file:" + path + "core-sat-processed-cardNorm.owl";
//		queryPath = path + "Queries_URIs/";
////		optPath = path + "";
//		optPath = path + "OptimizationClausesRDFox/OptimizationClauses.txt";
//		dataFiles = path;
//		/*
//		 * NPD DB
//		 */
//		dbPath = "jdbc:mysql://127.0.0.1:3306/Uniprot";
//		System.out.println("**************************");
//		System.out.println("**\tUniprot\t\t**");
//		System.out.println("**************************");
//		evaluator = createDataSetsAndEvaluator(dataFiles, dbPath, ontologyFile);
//		runTest(ontologyFile, queryPath, optPath, evaluator, true, RDFoxOpt);
//
//		excelFile =  "/Users/avenet/Academia/Aueb/Research/IncrementalQueryAnswering/EvaluationResults/Uniprot/IQAROS/RDFox/Uniprot_newRules.xlsx";

		print2Excel=false;
	}

	private static void runTest(String ontologyFile, String queryPath, String optPath, RDFoxQueryEvaluator evaluator, boolean print, boolean RDFoxOpt) throws Exception {
		long start=0,totalTime = 0;
		ArrayList<PI> tBoxAxioms = parser.getAxiomsWithURI(ontologyFile);
//		ArrayList<PI> tBoxAxioms = parser.getAxioms(ontologyFile);
		ArrayList<Clause> rewriting = new ArrayList<Clause>();

//		for (PI pi: tBoxAxioms) {
//			System.out.println(pi.m_left + "\t" + pi.m_type + "\t" + pi.m_right);
//		}
//
//		System.exit(0);

		File queriesDir = new File( queryPath );
		File[] queries = queriesDir.listFiles();

		QueryOptimization qOpt = null;

		/**
		 * Emptiness optimization
		 */
		if (optPath != null && optPath != "") {
			qOpt = new QueryOptimization(optPath);
		}

		System.out.println();
//		System.out.println( queriesDir );
		for( int i=0 ; i<queries.length ; i++ ) {
			if( queries[i] == null )
				// Either dir does not exist or is not a directory
				return;
	        else if( !queries[i].toString().contains(".svn") && !queries[i].toString().contains(".DS_Store") && !queries[i].isDirectory() ){
//	        	if ( ontologyFile.contains("UOBM") && queries[i].toString().contains("Query_11") )
	        	if (!queries[i].toString().contains("07"))
	        		continue;
//	        		&& !queries[i].toString().contains("Query_11.txt")) {
//	        	if (print)
//	        		System.out.println(queries[i] + "\n" + parser.getQuery(queries[i].toString()));
	        	BufferedReader readQueries = new BufferedReader(new FileReader(queries[i]));
	        	String query = readQueries.readLine();
	        	readQueries.close();
        		System.out.println((i+1) + ": " + queries[i] + "\n" + new ClauseParser().parseClause(query));
//	        	start = System.currentTimeMillis();
	        	//use optimization to cut off queries with no answers
        		Incremental incremental = new Incremental(qOpt,RDFoxOpt);
	        	rewriting = incremental.computeUCQRewriting(tBoxAxioms,new ClauseParser().parseClause(query), print);
	        	//no optimization
//	        	rewriting = new Incremental().computeUCQRewriting(tBoxAxioms,new ClauseParser().parseClause((new BufferedReader(new FileReader(queries[i]))).readLine()));
	        	/** OR, in order to run the evaluation using non-restricted subsumption */
//	    		Configuration c = new Configuration();
//	    		c.redundancyElimination=RedundancyEliminationStrategy.Full_Subsumption;
//	    		Incremental incremental = new Incremental( c );
//	    		incremental.computeUCQRewriting(tBoxAxioms,parser.getQuery(queries[i].toString()));

//	        	for (Clause cl: rewriting )
//	        		System.out.println(cl + "\t");

	        	System.out.println( "Original rew size = " + rewriting.size() );
	        	Set<Clause> rdfoxRew = new HashSet<Clause>();
	        	for ( Clause c: rewriting )
	        		if (c.getToBeSentToRDFox()) {
	        			rdfoxRew.add(c);
	        			System.out.println(c);
	        		}
	        	System.out.println( "RFDox rew size = " + rdfoxRew.size() );

	        	/*
	        	 * RDFox evaluation
	        	 */
	        	long end = 0;
	        	long ans = 0;
	        	if ( rdfoxRew.size() != 0 ) {
	        		long startTime = System.currentTimeMillis();
	        		ans = evaluator.evaluateQuery(rdfoxRew);
	        		end = System.currentTimeMillis() - startTime;
	        		System.out.println( "RDFox Answers: " + ans + " in " + end + "ms");
	        	}

	        	System.out.println("\n\n");

//				totalTime+= (System.currentTimeMillis()-start);
//	        	System.out.println("\n\n\n");
	        }
		}
//		System.out.println( "Finished rewriting " + queries.length + " in " + totalTime + " ms" );
//		System.out.println( "Rew time = " + totalTime );

	}

	private static RDFoxQueryEvaluator createDataSetsAndEvaluator(String dataFiles, String dbPath, String ontologyFile) throws FileNotFoundException,URISyntaxException, JRDFStoreException, Exception  {

		String[] datasetFiles = null;
		RDFoxQueryEvaluator evaluator = new RDFoxQueryEvaluator();
		long t = System.currentTimeMillis();
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
		else if (dataFiles.contains("Fly") ) {
			System.out.println("Fly");
			datasetFiles = createDataSetFilesFly(dataFiles);
		}
		else if (dataFiles.contains("npd")  ) {
			System.out.println("NPD");
			datasetFiles = createDataSetFilesNPD(dataFiles);
		}
		else if (dataFiles.contains("Chembl")  ) {
			System.out.println("Chembl");
			datasetFiles = createDataSetFilesChembl(dataFiles);
		}
		else if (dataFiles.contains("Uniprot")  ) {
			System.out.println("Uniprot");
			datasetFiles = createDataSetFilesUniprot(dataFiles);
		}
		evaluator.loadInputToSystem(ontologyFile, datasetFiles);
		System.out.println("loading took: " + (System.currentTimeMillis()-t) + "msec\n");
    	return evaluator;
	}

	public static String[] createDataSetFilesUniprot(String dataFilesPath) {
		String[] result = new String[1];

		result[0] = dataFilesPath + "dataset/sample_1.nt";

		return result;
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
			if( !dataFiles[i].toString().contains(".svn") && !dataFiles[i].toString().contains(".DS_Store") && dataFiles[i].toString().contains(".ttl") ){
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
				result[globalCounter++] = dataFilesPath + "University"+ j + "_" + i + ".ttl";
//				System.out.println(result[globalCounter]);
			}

		}
		return result;
	}


	public static String[] createDataSetFilesUOBM(String dataFilesPath, int limit) {
		String[] result = new String[limit];

		for ( int j = 0 ; j < limit ; j++) {
			result[j] = dataFilesPath + "univ"+ j + ".ttl";
		}

		return result;
	}

	public static String[] createDataSetFilesVicodi(String dataFilesPath) {
		String[] result = new String[1];

		result[0] = dataFilesPath + "vicodi_all.ttl";

		return result;
	}

	public static String[] createDataSetFilesSemintec(String dataFilesPath) {
		String[] result = new String[1];

		result[0] = dataFilesPath + "bigFile.ttl";

		return result;
	}

	public static String[] createDataSetFilesReactome(String dataFilesPath) {
		String[] datasetFiles = new String[1];
		datasetFiles[0] = dataFilesPath + "sample_10.ttl";
		return datasetFiles;
	}

	public static String[] createDataSetFilesFly(String dataFilesPath) {
		String[] result = new String[1];

		result[0] = dataFilesPath + "fly_anatomy_XP_with_GJ_FC_individuals_owl-aBox-AssNorm.ttl";

		return result;
	}

	public static String[] createDataSetFilesNPD(String dataFilesPath) {
		String[] result = new String[1];

		result[0] = dataFilesPath + "ontology/npd-v2-ql-abox_gstoil_avenet.ttl";

		return result;
	}

	public static String[] createDataSetFilesChembl(String dataFilesPath) {
		String[] result = new String[1];

		result[0] = dataFilesPath + "dataset/sample_1.nt";

		return result;
	}
}