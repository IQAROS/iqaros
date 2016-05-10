import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import org.oxford.comlab.perfectref.parser.DLliteParser;
import org.oxford.comlab.perfectref.rewriter.Clause;
import org.oxford.comlab.perfectref.rewriter.PI;

import edu.aueb.queries.ClauseParser;
import edu.aueb.queries.Evaluator;
import edu.aueb.queries.QueryOptimization;
import edu.ntua.image.incremental.Incremental;

public class IncEval {

	private static final DLliteParser parser = new DLliteParser();

	public static void main(String[] args) throws Exception{

		String path = System.getProperty("user.dir").replace("\\", "/") + "/Dataset/";
		
		
		String ontologyFile = "file:" + path + "UOBM/univ-bench-dl-Zhou_DL-Lite.owl";
		String queryPath = path + "UOBM/CQ/";
		//enter the optimization file computed using PreProcessingDB_v1 or PreProcessingDB_v2
		String optPath = "";

		runTest(ontologyFile, queryPath, optPath);
	}
	
	private static void runTest(String ontologyFile, String queryPath, String optPath) throws Exception {
		long start=0,totalTime = 0;
		/**
		 * getAxiomsWithURI is used when queries use uris for their predicates (Uniprot, NPD). 
		 * Moreover, tables in the database (in all except for NPD) should use the 
		 * URIs as names.
		 * 
		 * In case the queries use URIs and the PIs don't (or vise versa) the produced
		 * rewriting always has a size of 1, and contains only the original query.
		 */
//		ArrayList<PI> tBoxAxioms = parser.getAxiomsWithURI(ontologyFile);
		ArrayList<PI> tBoxAxioms = parser.getAxioms(ontologyFile);
		ArrayList<Clause> rewriting = new ArrayList<Clause>();
		
		QueryOptimization qOpt = null;
		
		/**
		 * Emptiness optimization
		 */
		if (optPath != null && optPath != "") {
			long startOpt = System.currentTimeMillis();
			qOpt = new QueryOptimization(optPath);
            System.out.println("Optimization Took " + (System.currentTimeMillis() - startOpt) + "ms");
		}

		File queriesDir = new File( queryPath );
		File[] queries = queriesDir.listFiles();
//		System.out.println( queriesDir );
		for( int i=0 ; i<queries.length ; i++ ) {
			if( queries[i] == null )
				// Either dir does not exist or is not a directory
				return;
	        else if( !queries[i].toString().contains(".svn") && !queries[i].toString().contains(".DS_Store") ){ 
        		System.out.println(queries[i] + "\n" + new ClauseParser().parseClause((new BufferedReader(new FileReader(queries[i]))).readLine()));
	        	//use optimization to cut off queries with no answers
	        	rewriting = new Incremental(qOpt).computeUCQRewriting(tBoxAxioms,new ClauseParser().parseClause((new BufferedReader(new FileReader(queries[i]))).readLine()), true);
	        	//no optimization
//	        	rewriting = new Incremental().computeUCQRewriting(tBoxAxioms,new ClauseParser().parseClause((new BufferedReader(new FileReader(queries[i]))).readLine()));
	        	/** OR, in order to run the evaluation using non-restricted subsumption */
//	    		Configuration c = new Configuration();
//	    		c.redundancyElimination=RedundancyEliminationStrategy.Full_Subsumption;
//	    		Incremental incremental = new Incremental( c );
//	    		incremental.computeUCQRewriting(tBoxAxioms,parser.getQuery(queries[i].toString()));

	        	for (Clause cl: rewriting )
	        		System.out.println(cl);

	        	if ( rewriting.size() != 0 )
	        	{
	        		Evaluator ev = new Evaluator();
	        		ev.evaluateSQL( null, ev.getSQL( rewriting ), true);
	        		ev.closeConn();
	        	}
//				totalTime+= (System.currentTimeMillis()-start);
//	        	System.out.println("\n\n\n");
	        }
		}
//		System.out.println( "Finished rewriting " + queries.length + " in " + totalTime + " ms" );
//		System.out.println( "Rew time = " + totalTime );
		
	}
}