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

package edu.aueb.queries;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.oxford.comlab.perfectref.rewriter.Clause;
import org.oxford.comlab.perfectref.rewriter.Constant;
import org.oxford.comlab.perfectref.rewriter.Term;
import org.oxford.comlab.perfectref.rewriter.Variable;

import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;

import edu.aueb.NPD.OBDAParser;

public class Evaluator {

	public static final String ANSWERSEPARATOR = "----IQAROS----";

	String connStr;
	int resSize;
	long evalTime;
	Connection conn;
	
	public OBDAParser mappingManager = null;
//	public OBDAParser_Clauses mappingManager = null;
	
	String database;
	
	public int getNumOfAns() {
		return resSize;
	}

	public long getEvalTime() {
		return evalTime;
	}

	public int getMaxAtoms() {
		return maxAtoms;
	}

	int maxAtoms;
	
	public void closeConn() {
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void createConnection() throws SQLException {
    	if ( connStr.startsWith("jdbc:postgresql")) {
			try {
				DriverManager.registerDriver(new org.postgresql.Driver());
				conn = DriverManager.getConnection(connStr + "?user=postgres"+"&password=0000");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
    	}
		else if (connStr.startsWith("jdbc:mysql")) {
			try {
				DriverManager.registerDriver(new com.mysql.jdbc.Driver());
				conn = DriverManager.getConnection(connStr + "?user=root"+"&password=");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		conn.setAutoCommit(false);
		
//		System.out.println("Connection Created");
	}
	
	
	public Evaluator(String dbUsed) throws SQLException {
		maxAtoms = 0;
		connStr = dbUsed;
		createConnection();
	}
	
	public Evaluator(String dbUsed,String mappings) throws Exception {
		this(dbUsed, mappings, false);
	}

	public Evaluator(String dbUsed,String mappings, boolean optimizeMappings) throws Exception {
		maxAtoms = 0;
		connStr = dbUsed;
		createConnection();

		mappingManager = new OBDAParser(mappings);
//		mappingManager = new OBDAParser_Clauses(mappings,null);
		
		if (optimizeMappings)
			optimizeMappings();
	}
	
	public Evaluator() throws SQLException {
		maxAtoms = 0;
		
//		connStr = "jdbc:postgresql://127.0.0.1:5432/TestOnto";
//		connStr = "jdbc:postgresql://127.0.0.1:5432/reactome";
//		connStr = "jdbc:postgresql://127.0.0.1:5432/reactome2";
//		connStr = "jdbc:postgresql://127.0.0.1:5432/Fly";
//		connStr = "jdbc:postgresql://127.0.0.1:5432/Vicodi";
//		connStr = "jdbc:postgresql://127.0.0.1:5432/Semintec";
//		connStr = "jdbc:postgresql://127.0.0.1:5432/UOBM";
//		connStr = "jdbc:postgresql://127.0.0.1:5432/UOBM5";
//		connStr = "jdbc:postgresql://127.0.0.1:5432/UOBM10";
		connStr = "jdbc:postgresql://127.0.0.1:5432/UOBM30";
//		connStr = "jdbc:postgresql://127.0.0.1:5432/UOBM50";
//		connStr = "jdbc:postgresql://127.0.0.1:5432/LUBM";
//		connStr = "jdbc:postgresql://127.0.0.1:5432/LUBM-ex";
//		connStr = "jdbc:postgresql://127.0.0.1:5432/LUBM5"; //This database contains data from 15 universities
//		connStr = "jdbc:postgresql://127.0.0.1:5432/LUBM10"; //This database contains data from 15 universities
//		connStr = "jdbc:postgresql://127.0.0.1:5432/LUBMLarge"; //This database contains data from 15 universities
//		connStr = "jdbc:postgresql://127.0.0.1:5432/LUBM30"; //This database contains data from 30 universities
		
		createConnection();

	}


	public void runAllRewQueriesAtOnce(ArrayList<Clause> rewriting) {
		String sqlQuery;
		try {
		    if ( rewriting.size() > 0 ) {
				sqlQuery = getSQL(rewriting);
//				System.out.println(rewriting);
//				System.out.println(sqlQuery);
				System.out.println( "Evaluating rew size = " + rewriting.size() );
//		    	long begin = System.currentTimeMillis();
		    	evaluateSQL(null, sqlQuery);
//		    	long end = System.currentTimeMillis();
//		    	System.out.println("Eval Time = " + (end - begin) + "ms" );
	    	}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void runAllRewQueriesOneByOne(ArrayList<Clause> rewriting) {
		String sqlQuery;
		ArrayList<Clause> rewWithNonEmptyClauses = new ArrayList<Clause>();

		try {
			if (rewriting.size() > 0 ) {
				for ( Clause clause : rewriting ) {
					if ( clause.getHead().getName().toString().equals( "Q" ) ) {
						sqlQuery = getSQLavenet(clause).concat(";");
						int clauseAnsSize = evaluateSQLandReturnNumOfAnws(null,sqlQuery);
						//avenet - me ayto petame ta queries pou exoun to headOf epeidi oi apantiseiw tous einai redundant
						//LUBM
//						if ( clauseAnsSize > 0 && !( queryContainsBodyAtom(clause, "headOf") || queryContainsBodyAtom(clause, "ResearchAssistant") ) ) {
						//UOBM
//						if ( clauseAnsSize > 0 && !(queryContainsBodyAtom(clause, "ResearchAssistant") ) ) {
//						Semintec
//						if ( clauseAnsSize > 0 && !queryContainsBodyAtom(clause, "hasCreditCard") ) {
//						Vicodi
//						if ( clauseAnsSize > 0 && !queryContainsBodyAtom(clause, "") ) {
						if ( clauseAnsSize > 0 ) {
							System.out.println( "Clause = " + clause );
							System.out.println( "SQL = " + sqlQuery );
							System.out.println( clauseAnsSize );
							rewWithNonEmptyClauses.add(clause);
						}
					}
					else
						System.err.println("\t\t" + clause);
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println( "Size of rew that all queries return answers = " + rewWithNonEmptyClauses.size() );

		runAllRewQueriesAtOnce(rewWithNonEmptyClauses);
	}

	private boolean queryContainsBodyAtom(Clause clause, String string) {

		for ( Term atom: clause.getBody())
			if (atom.getName().toString().equals(string)) {
//				System.out.println( "\t\t\t" + clause );
				return true;
			}

		return false;
	}

	private ArrayList<Clause> pruneEmptyPredicates(ArrayList<Clause> clauses, HashMap<String,String> mappings){

		ArrayList<Clause> result = new ArrayList<Clause>();

		boolean toBePruned;

		for(Clause c: clauses){
			toBePruned = false;
			for(Term bodyAtom : c.getBody()){
				if(!mappings.containsKey(bodyAtom.getName())){
					toBePruned = true;
					break;
				}
			}
			if(!toBePruned){
				result.add(c);
			}
		}

		return result;
	}

//	public String getSQL(ArrayList<Clause> rewriting, HashMap<String,String> mappings) throws Exception {
//
//		rewriting = this.pruneEmptyPredicates(rewriting, mappings);
//
//		if(rewriting.size() > 0){
//			String result = "";
//			Clause clause;
//
//			for(int i=0; i<rewriting.size(); i++){
//				clause = rewriting.get(i);
//
//				if(clause.getHead().getPredicate().equals("Q")){
//					if(i!=0)
//						result += " UNION \n";
//					result += getSQL(rewriting.get(i), mappings);
//				}
//				else{
//					return null;
//
//				}
//			}
//
//			result += ";";
//			return result;
//		}
//		return "";
//	}

	public String getSQL(ArrayList<Clause> rewriting) throws Exception {

//		rewriting = this.pruneEmptyPredicates(rewriting, mappings);

		if(rewriting.size() > 0){
			String result = "";
			Clause clause;

			int j = 0;

			for(int i=0; i<rewriting.size(); i++){
				clause = rewriting.get(i);

//				System.out.println(clause.getHead().getPredicate().toString().equals("Q"));

				if(clause.getHead().getName().toString().equals("Q")){
					if(j!=0)
						result += " UNION \n";
					result += getSQLavenet(rewriting.get(i));
					j++;
				}
//				else{
//					return null;
//				}
			}

			result += ";";
//			System.out.println(result);
			return result;
		}
		return "";
	}
	
	public String getSQLWRTMappings(ArrayList<Clause> rewriting) throws Exception {

//		rewriting = this.pruneEmptyPredicates(rewriting, mappings);
		
		if(rewriting.size() > 0){
			String result = "";
			Clause clause;

			
			int j = 0;
			for(int i=0; i<rewriting.size(); i++){
				clause = rewriting.get(i);

//				System.out.println(clause.getHead().getPredicate().toString().equals("Q"));

				if(clause.getHead().getName().toString().equals("Q")){
//					System.out.println("query as clause: " + clause);
					String cqInUCQ = getSQLavenet(clause);
					
//					System.out.println("SQL before mapping translation: " + cqInUCQ);
//					String myTesting = mappingManager.buildSQLFromClauses(clause);
////					System.out.println("myTesting: " + myTesting);
//					if (!myTesting.isEmpty()) {
//						System.out.println("myTesting: " + myTesting);
////						myTesting =myTesting.replace(", WHERE" , " WHERE");
//						result += myTesting + " UNION ";
//					}
//					
					if(j!=0)
						result += " UNION \n";
					cqInUCQ = cqInUCQ.replaceAll("http://sws.ifi.uio.no/vocab/npd-v2#", "npdv:").replaceAll("http://sws.ifi.uio.no/vocab/npd-v2-ptl#", "ptl:");
					result += mappingManager.replaceFrom(cqInUCQ);

					j++;
				}
//				else{
//					return null;
//				}
			}
			result += "ENDEEEED";
			result = result.replace(" UNION ENDEEEED", "");
			result = result.replace("ENDEEEED", "");
			if (result.isEmpty())
				return "";
			result += ";";
			System.out.println("res: " + result);
			return result;
		}
		return "";
	}

	public String getSQLWRTMappings(Clause clause) throws Exception {

		String result = "";
		if(clause.getHead().getName().toString().equals("Q")){
//			System.out.println("query as clause: " + clause);
			String cqInUCQ = getSQLavenet(clause);
			result = mappingManager.replaceFrom(cqInUCQ.replaceAll("http://sws.ifi.uio.no/vocab/npd-v2#", "npdv:").replaceAll("http://sws.ifi.uio.no/vocab/npd-v2-ptl#", "ptl:"));
		}
		result += ";";
		return result;
	}

//	private String getSQL(Clause clause, HashMap<String,String> mappings){
//		String result = "";
//
//		ArrayList<String> selectParts = new ArrayList<String>();
//		ArrayList<String> fromParts = new ArrayList<String>();
//		ArrayList<String> whereParts = new ArrayList<String>();
//
//
//		HashMap<String,String> mappingVariables = new HashMap<String,String>();
//
//		for(int i = 0; i < clause.getBody().size(); i ++){
//			Atom bodyAtom = clause.getBody().get(i);
//
////			fromParts.add("(" + mappings.get(bodyAtom.getPredicate()) + ") AS T" + i);
//
//			for(int j=0; j<bodyAtom.getTerms().size(); j++){
//				//variable
//				if(bodyAtom.getArgument(j) instanceof Variable){
//					Variable var = (Variable) bodyAtom.getArgument(j);
//					if(!mappingVariables.containsKey(var.getName())){
//						mappingVariables.put(var.getName(), "T" + i + ".C" + j);
//					}
//					//Shared variable
//					else{
//						whereParts.add(mappingVariables.get(var.getName()) + " = " + "T" + i + ".C" + j);
//						mappingVariables.put(var.getName(), "T" + i + ".C" + j);
//					}
//				}
//				//constant
//				if(bodyAtom.getArgument(j) instanceof FunctionalTerm){
//					FunctionalTerm cons = (FunctionalTerm) bodyAtom.getArgument(j);
//					whereParts.add("T" + i + ".C" + j + " = '" + cons.getName().substring(1, cons.getName().length()-1) + "'");
//				}
//			}
//		}
//
//		for(Term vc: clause.getHead().getArguments()){
//			//variable
//			if(vc instanceof Variable){
//				selectParts.add(mappingVariables.get(vc.getName()));
//			}
//			//constant
//			if(vc instanceof FunctionalTerm){
//				selectParts.add("'" + vc.getName() + "'");
//			}
//		}
//
//		result = "SELECT ";
//		for(int s=0; s<selectParts.size(); s++){
//			if(s!=0)
//				result += ", ";
//			result += selectParts.get(s);
//		}
//
//		result += " FROM ";
//
//		for(int f=0; f<fromParts.size(); f++){
//			if(f!=0)
//				result += ", ";
//			result += fromParts.get(f);
//		}
//
//		if(whereParts.size() > 0){
//			result += " WHERE ";
//
//			for(int w=0; w<whereParts.size(); w++){
//				if(w!=0)
//					result += " AND ";
//				result += whereParts.get(w);
//			}
//		}
//
//		return result;
//	}


//	public String getSQL(Clause clause){
//		String result = "";
//
//		ArrayList<String> selectParts = new ArrayList<String>();
//		ArrayList<String> fromParts = new ArrayList<String>();
//		ArrayList<String> whereParts = new ArrayList<String>();
//
//
//		HashMap<String,String> mappingVariables = new HashMap<String,String>();
//
//		for(int i = 0; i < clause.getBody().size(); i ++){
//			Atom bodyAtom = clause.getBody().get(i);
//
////			fromParts.add("(" + bodyAtom.getName() + ") AS T" + i);
//			fromParts.add(bodyAtom.getPredicate() + " AS T" + i);
//
//			for(int j=0; j<bodyAtom.getTerms().size(); j++){
//				//variable
//				if(bodyAtom.getArgument(j) instanceof Variable){
//					Variable var = (Variable) bodyAtom.getArgument(j);
//					if(!mappingVariables.containsKey(var.getName())){
//						mappingVariables.put(var.getName(), "T" + i + ".C" + j);
//					}
//					//Shared variable
//					else{
//						whereParts.add(mappingVariables.get(var.getName()) + " = " + "T" + i + ".C" + j);
//						mappingVariables.put(var.getName(), "T" + i + ".C" + j);
//					}
//				}
//				//constant
//				if(bodyAtom.getArgument(j) instanceof FunctionalTerm){
//					FunctionalTerm cons = (FunctionalTerm) bodyAtom.getArgument(j);
//					whereParts.add("T" + i + ".C" + j + " = '" + cons.getName().substring(1, cons.getName().length()-1) + "'");
//				}
//			}
//		}
//
//		for(Term vc: clause.getHead().getArguments()){
//			//variable
//			if(vc instanceof Variable){
//				selectParts.add(mappingVariables.get(vc.getName()));
//			}
//			//constant
//			if(vc instanceof FunctionalTerm){
//				selectParts.add("'" + vc.getName() + "'");
//			}
//		}
//
//		result = "SELECT ";
//		for(int s=0; s<selectParts.size(); s++){
//			if(s!=0)
//				result += ", ";
//			result += selectParts.get(s);
//		}
//
//		result += " FROM ";
//
//		for(int f=0; f<fromParts.size(); f++){
//			if(f!=0)
//				result += ", ";
//			result += fromParts.get(f);
//		}
//
//		if(whereParts.size() > 0){
//			result += " WHERE ";
//
//			for(int w=0; w<whereParts.size(); w++){
//				if(w!=0)
//					result += " AND ";
//				result += whereParts.get(w);
//			}
//		}
//
//		return result;
//	}


	public String getSQLavenet(Clause clause){
		String result = "";

		ArrayList<String> selectParts = new ArrayList<String>();
		ArrayList<String> fromParts = new ArrayList<String>();
		ArrayList<String> whereParts = new ArrayList<String>();

//		System.out.println("Clause = " + clause);

		HashMap<String,String> mappingVariables = new HashMap<String,String>();

		if ( maxAtoms < clause.getBody().length )
			maxAtoms = clause.getBody().length;
		
		for(int i = 0; i < clause.getBody().length; i ++){
			Term bodyAtom = clause.getBody()[i];

			fromParts.add("\"" + bodyAtom.getName() + "\" AS T" + i);
//			fromParts.add("(" + bodyAtom.getName() + ") AS T" + i);
//			fromParts.add("\"" + bodyAtom.getName().replace("/", "") + "\" AS T" + i);

			for(int j=0; j<bodyAtom.getArguments().length; j++){
				//variable
				if(bodyAtom.getArgument(j) instanceof Variable){
					Variable var = (Variable) bodyAtom.getArgument(j);
					if(!mappingVariables.containsKey(var.getName())){
						if (bodyAtom.getArguments().length == 1)
							mappingVariables.put(var.getName(), "T" + i + ".individual");
						else
							if (j == 0)
								mappingVariables.put(var.getName(), "T" + i + ".subject");
							else
								mappingVariables.put(var.getName(), "T" + i + ".obj");
					}
					//Shared variable
					else{
						if (bodyAtom.getArguments().length == 1) {
							whereParts.add(mappingVariables.get(var.getName()) + " = " + "T" + i + ".individual");
							mappingVariables.put(var.getName(), "T" + i + ".individual");
						}else
							if (j == 0)
							{
								whereParts.add(mappingVariables.get(var.getName()) + " = " + "T" + i + ".subject");
								mappingVariables.put(var.getName(), "T" + i + ".subject");
							}
							else
							{
								whereParts.add(mappingVariables.get(var.getName()) + " = " + "T" + i + ".obj");
								mappingVariables.put(var.getName(), "T" + i + ".obj");
							}
					}
				}
				//constant
				if(bodyAtom.getArgument(j) instanceof Constant){
					Constant cons = (Constant) bodyAtom.getArgument(j);
					if (bodyAtom.getArguments().length == 1)
						whereParts.add("T" + i + ".individual = '" + cons.getName().substring(0, cons.getFunctionalPrefix().length()) + "'");
					else
						if (j == 0)
							whereParts.add("T" + i + ".subject = '" + cons.getName().substring(0, cons.getName().length()) + "'");
						else
							whereParts.add("T" + i + ".obj = '" + cons.getName().substring(0, cons.getName().length()) + "'");
				}
			}
		}

		Term headAtom = clause.getHead();

		for ( int j = 0; j < headAtom.getArguments().length ; j++) {
			if ( headAtom.getArgument(j) instanceof Variable ) {
				Variable var = (Variable) headAtom.getArgument(j);
				selectParts.add(mappingVariables.get(var.getName()));
			}
			//constant
			if(headAtom.getArgument(j) instanceof Constant){
				Constant con = (Constant) headAtom.getArgument(j);
				selectParts.add("'" + con.getName() + "'");
			}
		}
		//select count(1) 28, select no distinct 29, select count( ,,, ) 26, select distinct 43, select * 29, 
//		result = "SELECT * ";
		result = "SELECT distinct ";
		for(int s=0; s<selectParts.size(); s++){
			if(s!=0)
				result += ", ";
			result += selectParts.get(s);
		}

		result += " FROM ";

		for(int f=0; f<fromParts.size(); f++){
			if(f!=0)
				result += ", ";
			result += fromParts.get(f);
		}

		if(whereParts.size() > 0){
			result += " WHERE ";

			for(int w=0; w<whereParts.size(); w++){
				if(w!=0)
					result += " AND ";
				result += whereParts.get(w);
			}
		}
//		result = "SELECT EXISTS( " + result + ")";
		
//		System.out.println("SQL Query" + result);
//		result=mappingManager.replaceFrom(result);
		return result;
	}

	public String getSQLFromAtom(Term bodyAtom){
		String result = "";

		ArrayList<String> selectParts = new ArrayList<String>();
		ArrayList<String> fromParts = new ArrayList<String>();
		ArrayList<String> whereParts = new ArrayList<String>();

//		System.out.println("Clause = " + clause);

		HashMap<String,String> mappingVariables = new HashMap<String,String>();

//		for(int i = 0; i < clause.getBody().size(); i ++){
		int i = 0;
//			Atom bodyAtom = clause.getBody().get(i);

//			fromParts.add("(" + bodyAtom.getName() + ") AS T" + i);
			fromParts.add("\"" + bodyAtom.getName() + "\" AS T" + i);

			for(int j=0; j<bodyAtom.getArguments().length; j++){
				//variable
				if(bodyAtom.getArgument(j) instanceof Variable){
					Variable var = (Variable) bodyAtom.getArgument(j);
					if(!mappingVariables.containsKey(var.getName())){
						if (bodyAtom.getArguments().length == 1)
							mappingVariables.put(var.getName(), "T" + i + ".individual");
						else
							if (j == 0)
								mappingVariables.put(var.getName(), "T" + i + ".subject");
							else
								mappingVariables.put(var.getName(), "T" + i + ".obj");
					}
					//Shared variable
					else{
						if (bodyAtom.getArguments().length == 1) {
							whereParts.add(mappingVariables.get(var.getName()) + " = " + "T" + i + ".individual");
							mappingVariables.put(var.getName(), "T" + i + ".individual");
						}else
							if (j == 0)
							{
								whereParts.add(mappingVariables.get(var.getName()) + " = " + "T" + i + ".subject");
								mappingVariables.put(var.getName(), "T" + i + ".subject");
							}
							else
							{
								whereParts.add(mappingVariables.get(var.getName()) + " = " + "T" + i + ".obj");
								mappingVariables.put(var.getName(), "T" + i + ".obj");
							}
					}
				}
				//constant
				if(bodyAtom.getArgument(j) instanceof Constant){
					Constant cons = (Constant) bodyAtom.getArgument(j);
					if (bodyAtom.getArguments().length == 1)
						whereParts.add("T" + i + ".individual = '" + cons.getName().substring(0, cons.getFunctionalPrefix().length()) + "'");
					else
						if (j == 0)
							whereParts.add("T" + i + ".subject = '" + cons.getName().substring(0, cons.getName().length()) + "'");
						else
							whereParts.add("T" + i + ".obj = '" + cons.getName().substring(0, cons.getName().length()) + "'");
				}
			}
//		}

//		Atom headAtom = clause.getHead();

		for ( int j = 0; j < bodyAtom.getArguments().length ; j++) {
			if ( bodyAtom.getArgument(j) instanceof Variable ) {
				Variable var = (Variable) bodyAtom.getArgument(j);
				selectParts.add(mappingVariables.get(var.getName()));
			}
			//constant
//			if(bodyAtom.getArgument(j) instanceof Constant){
//				Constant con = (Constant) bodyAtom.getArgument(j);
//				selectParts.add("'" + con.getName() + "'");
//			}
		}

		result = "SELECT distinct ";
		for(int s=0; s<selectParts.size(); s++){
			if(s!=0)
				result += ", ";
			result += selectParts.get(s);
		}

		result += " FROM ";

		for(int f=0; f<fromParts.size(); f++){
			if(f!=0)
				result += ", ";
			result += fromParts.get(f);
		}

		if(whereParts.size() > 0){
			result += " WHERE ";

			for(int w=0; w<whereParts.size(); w++){
				if(w!=0)
					result += " AND ";
				result += whereParts.get(w);
			}
		}

		return result;
	}

	
	//	public boolean evaluateSQL(String[] connectionData, String query, JTextArea textArea) throws Exception{
//        Statement stmt = null;
//	    ResultSet rs = null;
//	    ResultSetMetaData rsmd;
//	    try {
//	    	long begin = System.currentTimeMillis();
//			DriverManager.registerDriver(new com.mysql.jdbc.Driver());
//			DriverManager.registerDriver(new org.postgresql.Driver());
//
//	        conn = DriverManager.getConnection(connectionData[0]+connectionData[1]+"?user="+connectionData[2]+"&password="+connectionData[3]);
//	        conn.setAutoCommit(false);
//	        stmt = conn.createStatement();//java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
//	        stmt.setFetchSize(50);
//
//	        rs = stmt.executeQuery(query);
//
//	        rsmd = rs.getMetaData();
//	        int numCols = rsmd.getColumnCount();
//	        int numRows = 0;
//	        while (rs.next()){
//		         for (int i = 1; i <= numCols; i++){
//		        	 //System.out.print(rs.getString(i)+"\t");
//		        	 textArea.append(rs.getString(i)+"\t");
//		         }
//		         //System.out.println();
//		         textArea.append("\n");
//		         numRows++;
//	         }
//	        long end = System.currentTimeMillis();
//	        textArea.append("-------------------------------\n");
//	        if(numRows == 1){
//	        	textArea.append("1 row retrieved in " + Long.toString(end - begin) + " millisec.");
//	        }
//	        else{
//	        	textArea.append(numRows + " rows retrieved in " + Long.toString(end - begin) + " millisec.");
//	        }
//
//		}
//		finally {
//		        if (rs != null) {
//		            try {
//		                rs.close();
//		            } catch (SQLException sqlEx) { } // ignore
//		            rs = null;
//		        }
//		        if (stmt != null) {
//		            try {
//		                stmt.close();
//		            } catch (SQLException sqlEx) { } // ignore
//		            stmt = null;
//		        }
//		    }
//		return true;
//	}

	public boolean evaluateSQL(String[] connectionData, String query) throws Exception{
		return evaluateSQL(connectionData, query, true);
	}
	
	public boolean evaluateSQL(String[] connectionData, String query, boolean print) {
        Statement stmt = null;
	    ResultSet rs = null;
	    ResultSetMetaData rsmd;
//	    PreparedStatement stmt = null; 
	    try {
			stmt = conn.createStatement();
//			stmt.setFetchSize(500);
	    	
//	    	stmt = conn.prepareStatement(query);
	    	
//			System.out.println("Query to pose " + query);

	    	long begin = System.currentTimeMillis();

	        rs = stmt.executeQuery(query);

//	        rsmd = rs.getMetaData();

	    	long end = System.currentTimeMillis();

	        resSize = 0;
	    	while (rs.next()) {
//	    		System.out.println(rs.getString(1));
	    		resSize++;
	    	}
	    	
	    	evalTime = (end - begin);
			System.out.println("AnswersSize = " + resSize);
		    System.out.println("Eval Time = " + (end - begin) + "ms" );	        
        } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}//java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
	        
		finally {
		        if (rs != null) {
		            try {
		                rs.close();
		            } catch (SQLException sqlEx) { } // ignore
//		            rs = null;
		        }
//		        if (stmt != null) {
		            try {
		                stmt.close();
		            } catch (SQLException sqlEx) { } // ignore
//		            stmt = null;
//		        }
////		        conn.close();
		    }
		return true;
	}

	
	public Set<String> evaluateSQLReturnResults(String[] connectionData, String query) throws Exception {
        Statement stmt = null;
	    ResultSet rs = null;
//	    ResultSetMetaData rsmd;
	    Set<String> result = new HashSet<String>();
//	    try {
//	    	if ( connStr.startsWith("jdbc:postgresql")) {
//				DriverManager.registerDriver(new org.postgresql.Driver());
//				conn = DriverManager.getConnection(connStr + "?user=postgres"+"&password=0000");
//	    	}
//			else if (connStr.startsWith("jdbc:mysql")) {
//				DriverManager.registerDriver(new com.mysql.jdbc.Driver());
//				conn = DriverManager.getConnection(connStr + "?user=root"+"&password=");
//			}
//
//
//			conn.setAutoCommit(false);
    	long begin = System.currentTimeMillis();

//    	try 
    	{
//        	System.err.println("query: " + query);

			stmt = conn.createStatement();
	        try {
	        	rs = stmt.executeQuery(query);
	        } catch(MySQLSyntaxErrorException e){
//	        	System.err.println("query: " + query);
//	        	e.printStackTrace();
	        	
//	        	if (!e.getMessage().contains("Table") || !e.getMessage().contains("doesn't exist")) {
//	        		e.printStackTrace();
//	        		System.err.println("query: " + query);
//	        		System.exit(0);
//	        	}
	        	
	        	return new HashSet<String>();
	        }

//	        resSize = 0;
//	        System.out.println(query + " TTT: " + rs.getMetaData().getColumnCount());
	        while (rs.next()) {
	        	if (rs.getMetaData().getColumnCount() == 1)
	        		result.add(rs.getString(1));
	        	else
	        		result.add(rs.getString(1)+ANSWERSEPARATOR+rs.getString(2));
//	    		resSize++;
	    	}
	    	
//	    	System.out.println("numTuples = " + resSize);

		} 
//    	catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}//java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
//	        stmt.setFetchSize(50);


//	        rsmd = rs.getMetaData();


	        
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		finally {
//		        if (rs != null) {
//		            try {
//		                rs.close();
//		            } catch (SQLException sqlEx) { } // ignore
//		            rs = null;
//		        }
//		        if (stmt != null) {
//		            try {
//		                stmt.close();
//		            } catch (SQLException sqlEx) { } // ignore
//		            stmt = null;
//		        }
//		        conn.close();
//		    }
		return result;
	}
	
	
	public Set<String> evaluateSQLReturnResultsInverse(String[] connectionData, String query) throws Exception {
        Statement stmt = null;
	    ResultSet rs = null;
	    Set<String> result = new HashSet<String>();
    	{
			stmt = conn.createStatement();
	        try {
	        	rs = stmt.executeQuery(query);
	        } catch(MySQLSyntaxErrorException e){
	        	if (!e.getMessage().contains("Table") || !e.getMessage().contains("doesn't exist")) {
//	        		e.printStackTrace();
//	        		System.err.println("query: " + query);
//	        		System.exit(0);
	        	}
	        	return new HashSet<String>();
	        }
	        while (rs.next()) {
	        	if (rs.getMetaData().getColumnCount() == 1)
	        		result.add(rs.getString(1));
	        	else
	        		result.add(rs.getString(2)+ANSWERSEPARATOR+rs.getString(1));
	    	}
		} 
		return result;
	}
	
	
	public int evaluateSQLandReturnNumOfAnws(String[] connectionData, String query) throws Exception{
        Statement stmt = null;
	    ResultSet rs = null;
	    ResultSetMetaData rsmd;
	    int resSize = 0;
//	    try 
	    {

	    	long begin = System.currentTimeMillis();

	        stmt = conn.createStatement();//java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
//	        stmt.setFetchSize(50);
//	        System.out.println(query);
	        try {
	        	rs = stmt.executeQuery(query);
	        } catch(MySQLSyntaxErrorException e){
//        		System.err.println(query);
	        	if (!e.getMessage().contains("Table") || !e.getMessage().contains("doesn't exist")) {
//	        		e.printStackTrace();
//	        		System.out.println(query);
	        	}
	        	
	        	return 0;
	        }

	        rsmd = rs.getMetaData();

	        resSize = 0;
	    	while (rs.next())
	    		resSize++;

//	        System.out.println("numOfTuples = " + resSize);


		}
//		finally {
//		        if (rs != null) {
//		            try {
//		                rs.close();
//		            } catch (SQLException sqlEx) { } // ignore
//		            rs = null;
//		        }
//		        if (stmt != null) {
//		            try {
//		                stmt.close();
//		            } catch (SQLException sqlEx) { } // ignore
//		            stmt = null;
//		        }
//		        conn.close();
//		    }
		return resSize;
	}
	
	public String getSQLConceptConceptgstoil(Clause clause) {
		String result=null;
		result = "SELECT individual FROM " + clause.getBody()[0].getName() + " INTERSECT SELECT individual FROM " + clause.getBody()[1].getName(); 
		result=mappingManager.replaceFrom(result);
		return result;
	}
	
	public String getSQLConceptRolegstoil(Clause clause) {
		String result=null;
		if (clause.getBody()[0].getArguments().length==1)
			result = "SELECT individual FROM " + clause.getBody()[0].getName() + " INTERSECT SELECT subject, obj FROM " + clause.getBody()[1].getName(); 
		else
			result = "SELECT subject, obj FROM " + clause.getBody()[0].getName() + " INTERSECT SELECT individual FROM " + clause.getBody()[1].getName();
		
		result=mappingManager.replaceFrom(result);
		return result;
	}

	public String getSQLgstoil(Clause clause) {
		
		String result = "SELECT subject, obj FROM " + clause.getBody()[0].getName() + " INTERSECT SELECT subject, obj FROM " + clause.getBody()[1].getName(); 
		result=mappingManager.replaceFrom(result);
		return result;
	}
	
	public void seeForeignKeys() throws SQLException {
		System.out.println("Got Connection.");
//	    Statement st = conn.createStatement();
//	    st.executeUpdate("drop table survey;");
//	    st.executeUpdate("create table survey (id int,name varchar(30));");
//	    st.executeUpdate("insert into survey (id,name ) values (1,'nameValue')");

	    DatabaseMetaData meta = conn.getMetaData();
	    ResultSet res = meta.getTables(conn.getCatalog(), null, null, new String[] {"TABLE"});

	    ArrayList<String> tableNames = new ArrayList<String>();
	    
	    while (res.next() ) {
//	        System.out.println(
//	                "   "+res.getString("TABLE_CAT") 
//	               + ", "+res.getString("TABLE_SCHEM")
//	               + ", "+res.getString("TABLE_NAME")
//	               + ", "+res.getString("TABLE_TYPE")
//	               + ", "+res.getString("REMARKS")); 
	        tableNames.add(res.getString("TABLE_NAME"));
	    }
	    
	    for (int i = 0; i < tableNames.size(); i++ ) {
	    	 System.out.println("\n\n\n\ntable = " + tableNames.get(i));
	    	 ResultSet rs = null;
	    	 
	    	 /**
	    	  * Get primary key
	    	  */
	    	 rs = meta.getPrimaryKeys(conn.getCatalog(), null, tableNames.get(i));
	    	 while (rs.next()) {
	    		 String columnName = rs.getString("COLUMN_NAME");
	    		 System.out.println("Primary Key: " + columnName);
	    	 }
	    	 
	    	 /**
	    	  * Get foreign keys
	    	  */
	    	 rs = meta.getExportedKeys(conn.getCatalog(), null, tableNames.get(i));
		     while (rs.next()) {
		       String fkTableName = rs.getString("FKTABLE_NAME");
		       String fkColumnName = rs.getString("FKCOLUMN_NAME");
		       int fkSequence = rs.getInt("KEY_SEQ");
		       System.out.println("getExportedKeys(): fkTableName="+fkTableName);
		       System.out.println("getExportedKeys(): fkColumnName="+fkColumnName);
		       System.out.println("getExportedKeys(): fkSequence="+fkSequence);
		     }
	    }
	}
	
	public void optimizeMappings() throws Exception {

       	Map<String, String> atomsToSPJs = mappingManager.getAtomsToSPJ();
       	Map<String, Set<String>> atomsToNonRedundantSQLQueries = new HashMap<String, Set<String>>();

       	String[] result;

       	System.out.println("Mapping Optimization STARTED");
       	long start = System.currentTimeMillis();
       	for ( String str : atomsToSPJs.keySet() )
       	{
           	Set<Map<String,Set<String>>> sqlToAnswers = new HashSet<Map<String, Set<String>>>();
       		ArrayList<Set<String>> answers = new ArrayList<Set<String>>();
//       		System.out.println(str);
       		Set<String> nonRedundantSQLs = new HashSet<String>();
       		result = atomsToSPJs.get(str).split(" UNION ");
       		for (int x=0; x<result.length; x++) {
           		String sqlQueries = result[x];
           		nonRedundantSQLs.add(sqlQueries);
           		Set<String> ans = evaluateSQLReturnResults(null, sqlQueries);
                answers.add(ans);
               	Map<String,Set<String>> sqlToAnswer = new HashMap<String, Set<String>>();
                sqlToAnswer.put(sqlQueries, ans);
                sqlToAnswers.add(sqlToAnswer);
           	}
//       		System.out.println(str + "\t" + answers.size());

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
//	       	       					System.out.println("\tSQL " + sql2answers1.size() + "\n\tsubsumes" + sql2answers2.size() );
	       	       				}
       	       				}
       	       			}
       	       		}
       			}
       		}
       		atomsToNonRedundantSQLQueries.put(str, nonRedundantSQLs);

/**
 * Non optimized method - Naive method that works well but does not allow
 * storing of the mappings
 */
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

       	Map<String, String> atomsToNonRedundantSPJ = new HashMap<String,String>();
       	for ( String atom: atomsToNonRedundantSQLQueries.keySet() ) {
//       		System.out.println(atom + ": " + atomsToNonRedundantSQLQueries.get(atom).size() );
       		Set<String> sqls = atomsToNonRedundantSQLQueries.get(atom);
       		for (String sql: sqls) {
       			String existingSQL = atomsToNonRedundantSPJ.get(atom);
       			if (existingSQL!=null)
       				atomsToNonRedundantSPJ.put(atom, existingSQL+" UNION " + sql);
       			else
       				atomsToNonRedundantSPJ.put(atom, sql);

       		}
       	}
       	mappingManager.setAtomsToSPJ(atomsToNonRedundantSPJ);

       	System.out.println("Mapping Optimization Performed in " + ((System.currentTimeMillis() - start)) + "ms\n");
//		ev.closeConn();
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
