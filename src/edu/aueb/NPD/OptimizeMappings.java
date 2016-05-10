/*
 * #%L
 * 
 *
 * $Id$
 * $HeadURL$
 * %%
 * Copyright (C) 2016 by the Web Information Management Lab, AUEB
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


package edu.aueb.NPD;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;
import org.oxford.comlab.perfectref.parser.DLliteParser;

import edu.aueb.queries.Evaluator;

/**
 * The functionality of this class has been integrated in the Evaluator.java class
 * 
 * @author avenet
 *
 */
public class OptimizeMappings {

	public void optimizeMappings( String dbPath, String mappingFile ) throws Exception {

		long start = System.currentTimeMillis();
       	Evaluator ev = new Evaluator(dbPath,mappingFile);
       	Map<String, String> atomsToSPJs = ev.mappingManager.getAtomsToSPJ();
       	Map<String, Set<String>> atomsToNonRedundantSQLQueries = new HashMap<String, Set<String>>();
       	Map<String, Set<String>> atomToSQL = new HashMap<>();
       	int counter = 0;
       	String[] result;
       	for ( String str : atomsToSPJs.keySet() )
       	{
       		counter++;
           	Set<Map<String,Set<String>>> sqlToAnswers = new HashSet<Map<String, Set<String>>>();
       		ArrayList<Set<String>> answers = new ArrayList<Set<String>>();
//       		System.out.println(str);
       		Set<String> nonRedundantSQLs = new HashSet<String>();
       		result = atomsToSPJs.get(str).split(" UNION ");
       		for (int x=0; x<result.length; x++) {
           		String sqlQueries = result[x];
           		nonRedundantSQLs.add(sqlQueries);
//                System.out.println("\t" + sqlQueries);
           		Set<String> ans = ev.evaluateSQLReturnResults(null, sqlQueries);
//                if (ans.size()>0)
                answers.add(ans);
               	Map<String,Set<String>> sqlToAnswer = new HashMap<String, Set<String>>();
                sqlToAnswer.put(sqlQueries, ans);
                sqlToAnswers.add(sqlToAnswer);
//                answersToSQL.put(ans, sqlQueries);
           	}
       		System.out.println(str + "\t" + answers.size());

       		Set<String> redundantSQLs = new HashSet<String>();
       		for ( Map<String,Set<String>> sql2ans1: sqlToAnswers ) {
       			Set<String> sqls1 = sql2ans1.keySet();
       			for (String sql1: sqls1) {
       				if ( redundantSQLs.contains(sql1) )
       					continue;
       				
       				Set<String> sql2answers1 = sql2ans1.get(sql1);

       	       		for ( Map<String,Set<String>> sql2ans2: sqlToAnswers ) {
       	       			Set<String> sqls2 = sql2ans2.keySet();

       	       			for (String sql2: sqls2) {
       	       				if (!sql1.equals(sql2)) {
	       	       				Set<String> sql2answers2 = sql2ans2.get(sql2);

	       	       				if (containsAnswers(sql2answers1,sql2answers2)) {
	       	       					nonRedundantSQLs.remove(sql2);
	       	       					redundantSQLs.add(sql2);
	       	       					System.out.println("\tSQL " + sql2answers1.size() + "\n\tsubsumes" + sql2answers2.size() );
	       	       				}
       	       				}
       	       			}
       	       		}
       			}
       		}
       		atomsToNonRedundantSQLQueries.put(str, nonRedundantSQLs);


//       		for (int i = 0 ; i < answers.size() ; i++) {
//       			Set<String> ansi = answers.get(i);
//       			for (int j = 0; j < answers.size() ; j++ ) {
//       				Set<String> ansj = answers.get(j);
//       				if ( i != j && ansi.size() != 0 && ansj.size() != 0  )
//       				{
////       	       			System.out.println("\tset2 answersize = " + answers.get(j).size());
//       					if (containsAnswers(ansi,ansj))
//       					{
////       						System.out.println(answers.get(i));
////       						System.out.println(answers.get(j));
//       						System.out.println("\tSet " + i + " (" + answers.get(i).size() + ") contains Set " + j + " ("+ answers.get(j).size() + ")" );
//       						if (Objects.equals(answers.get(i),answers.get(j))) {
//       							System.out.println( "Objects are equal: " + answers.get(i).equals(answers.get(j)));
//       							System.out.println("--");
//       						}
////       						System.out.println(answersToSQL.keySet().size());
//
//       					}
//       				}
//       			}
//       		}
//           	atomToSQL.put(str, set);
       	}

//		String existingSQL = atomsToSPJ.get(atom);
//		if (existingSQL!=null)
//			atomsToSPJ.put(atom, existingSQL+" UNION " + select);
//		else
//			atomsToSPJ.put(atom, select);
       	Map<String, String> atomsToNonRedundantSPJ = new HashMap<String,String>();
       	for ( String atom: atomsToNonRedundantSQLQueries.keySet() ) {
       		System.out.println(atom + ": " + atomsToNonRedundantSQLQueries.get(atom).size() );
       		Set<String> sqls = atomsToNonRedundantSQLQueries.get(atom);
       		for (String sql: sqls) {
       			String existingSQL = atomsToNonRedundantSPJ.get(atom);
       			if (existingSQL!=null)
       				atomsToNonRedundantSPJ.put(atom, existingSQL+" UNION " + sql);
       			else
       				atomsToNonRedundantSPJ.put(atom, sql);

       		}
       	}
       	ev.mappingManager.setAtomsToSPJ(atomsToNonRedundantSPJ);

       	System.out.println("\nPerformed in " + ((System.currentTimeMillis() - start)) + "ms");
       	System.out.println(counter);
		ev.closeConn();
	}

	private static boolean containsAnswers(Set<String> set1, Set<String> set2) {

		if (set1 ==null || set2 == null)
			System.out.println("null");
		if ( set2.size() == 0 )
			return true;

		for (String set2Ans: set2) {
			if ( !set1.contains(set2Ans) )
			{
				return false;
			}
		}

		return true;
	}


}