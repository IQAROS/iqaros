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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.oxford.comlab.perfectref.rewriter.Clause;
import org.oxford.comlab.perfectref.rewriter.Term;
import org.oxford.comlab.perfectref.rewriter.Variable;

import edu.aueb.queries.ClauseParser;

public class OBDAParser_Clauses {

	Map<String,String> atomsToSPJ = new HashMap<String,String>();
	Map<String,Set<String>> atomsToDDRules = new HashMap<String,Set<String>>();
	Set<String> spjs = new HashSet<String>();
	Connection conn = null;

	public Map<String,String> getAtomsToSPJ () {
		return atomsToSPJ;
	}
	
	public Map<String,Set<String>> getAtomsToClauses () {
		return atomsToDDRules;
	}

	public void setAtomsToSPJ(Map<String,String> atomsToSPJ) {
		this.atomsToSPJ = atomsToSPJ;
	}

	public Set<String> getSpjs() {
		return spjs;
	}

	public OBDAParser_Clauses(String obdaSpecificationFile, Connection conn) {
		this.conn=conn;
		try {
			BufferedReader bf = new BufferedReader(new FileReader(obdaSpecificationFile));
			Map<String,String> prefixesToUris = new HashMap<String,String>();
			String line;
			int lineNumber=0;
			line = bf.readLine();
			lineNumber++;
			while (line!=null) {
				/** Getting URI prefixed */
				if (line.contains("[PrefixDeclaration]")) {
					line = bf.readLine();
					lineNumber++;
					while (!line.contains("[") && !line.contains("]") && line.trim().length()!=0) {
						StringTokenizer tok = new StringTokenizer(line, ":");
						if (tok.countTokens()==2) {
							String prefix = tok.nextToken().trim();
							prefixesToUris.put("base", prefix + tok.nextToken().trim());
						}
						else if (tok.countTokens()==3) {
							String prefix = tok.nextToken();
							String uri = tok.nextToken().trim() + ":";
							while (tok.hasMoreTokens()) {
								uri += tok.nextToken().trim();
							}
							prefixesToUris.put(prefix, uri.trim());
						}
						line = bf.readLine();
						lineNumber++;
					}
//					for (String str: prefixesToUris.keySet() )
//						System.out.println("\t" + str + "\t" + prefixesToUris.get(str));
				}
				else if (line.contains("[MappingDeclaration]")) {
					line = bf.readLine();
					lineNumber++;
					while (!line.contains("]]")) {
						while (!line.contains("mappingId")) {
							line = bf.readLine();
							lineNumber++;
						}
						String target = bf.readLine().replace("target", " ").trim();
						String select = bf.readLine().replace("source", " ").trim();
						lineNumber+=2;
						String[] lineElements = new String[3];
						StringTokenizer strTok = new StringTokenizer(target," ");
						lineElements[0] = strTok.nextToken();
						lineElements[1] = strTok.nextToken();
						lineElements[2] = strTok.nextToken();
						String subject = "";
						/** We first construct the head of the mapping */
						try { //The first element is always an individual
							subject = constructIndividualName(lineElements[0].length(),lineElements[0].toCharArray(),lineNumber);
						} catch (ArrayIndexOutOfBoundsException e) {
							System.out.println("No closing \"}\" in line " + lineNumber);
							bf.close();
							System.exit(1);
						}
						String object = "";
						String atom=null;
						//if the second element is the keyword "a" then the last is the name of a concept.
						if (lineElements[1].equals("a")) {
							atom=lineElements[2];
						}
						//Otherwise the last element is also an individual/constant and the middle element is the name of a role.
						else {
							atom=lineElements[1];
							if (lineElements[2].contains("{")) {
								if (lineElements[2].contains("^^")) { //DATATYPE
									char[] elementInCharArray = lineElements[2].toCharArray();
									int i=1;
									while (elementInCharArray[i]!='}')
										object +=elementInCharArray[i++];
								}
								else { //INDIVIDUAL
									try {
										object = constructIndividualName(lineElements[2].length(),lineElements[2].toCharArray(),lineNumber);
									} catch (ArrayIndexOutOfBoundsException e) {
										System.out.println("No closing \"}\" in line " + lineNumber);
										bf.close();
										System.exit(1);
									}
								}
							}
							else  if (lineElements[2].contains("^^")) { //BOOLEAN DATATYPE
								if (lineElements[2].contains("true") || lineElements[2].contains("false"))
									object=lineElements[2].contains("true")?"true":"false";//DO NOTHING THERE IS NO VAR TO ADD IN THE SELECT PART
							}
							else {	//CONSTANT
								object=lineElements[2];
							}
						}
						for (String key : prefixesToUris.keySet()) {
							if (subject.contains(key + ":"))
								subject = subject.replace(key + ":", prefixesToUris.get(key));
							if (object.contains(key + ":"))
								object = object.replace(key + ":", prefixesToUris.get(key));
						}
						String mappingAsDatalogRule = null;
						/** We now construct the body of the mapping */
						String argumentsInRawString = select.substring(select.indexOf("SELECT")+7, select.indexOf(" FROM"));
//						String[] selectArguments = argumentsInRawString.split(",");
						//if the last element is a constant, a boolean, or it is a concept atom mapping replace everything in the select with the subject agument
						if (lineElements[1].equals("a")) {
							select=select.replace( "SELECT " + argumentsInRawString, "SELECT " + subject + " AS individual");
							
							/** 
							 * DOING CLAUSE STAFF 
							 */
							mappingAsDatalogRule = buildClauseForRole(lineElements[2], subject, null, select);
						}
						//an to 2o orisma tou rolou einai ths morfhs {seaSourcePressure}^^something
						else if (lineElements[2].contains("^^") &&  !lineElements[2].contains("true") && !lineElements[2].contains("false")) {
							object=object.split("}")[0];
							object=object.replace("{", "");
							select=select.replace( "SELECT " + argumentsInRawString, "SELECT " + subject + " AS subject, " + object + " AS obj");

							/** 
							 * DOING CLAUSE STAFF 
							 */
							mappingAsDatalogRule = buildClauseForRole(lineElements[1], subject, object, select);
						}
						//An to 2o orisma einai karfwto url ths morfhs 'http://dbpedia.org/resource/Norway'
						else if (lineElements[2].contains("http://") ) {
//							select=select.replace( "SELECT " + argumentsInRawString, "SELECT " + subject + " AS subject");
							//avenet 2016-01-14
							select=select.replace( "SELECT " + argumentsInRawString, "SELECT " + subject + " AS subject, " + lineElements[2] + " AS obj");
						}
						//An to 2o orisma tou rolou einai ths morfhs "true"^^rdfs:Literal h "false"^^rdfs:Literal
						else if (lineElements[2].contains("^^") && (lineElements[2].contains("true") || lineElements[2].contains("false"))) {
							//avenet 2016-01-14
							if ( lineElements[2].contains("true") )
								object = "true";
							else
								object = "false";
							select=select.replace( "SELECT " + argumentsInRawString, "SELECT " + subject + " AS subject, " + object + " AS obj");
						}
						//An aplws einai individual pou kataskeuazetai me CONCAT h oxi.
						else {
							//replace the first n-1th with subject agument and nth with the object agument
//							String firstN_1Atoms = "";
//							String newSelectClause="";
//							for (int j=0 ; j<selectArguments.length-1; j++)
//								firstN_1Atoms += selectArguments[j] + ",";
//							select=select.replace(firstN_1Atoms, subject+",");
//							select=select.replace(selectArguments[selectArguments.length-1], object);
							select=select.replace( "SELECT " + argumentsInRawString, "SELECT " + subject + " AS subject," + object + " AS obj");

							/** 
							 * DOING CLAUSE STAFF 
							 */
							mappingAsDatalogRule = buildClauseForRole(lineElements[1], subject, object, select);
						}

						String existingSQL = atomsToSPJ.get(atom);
						if (existingSQL!=null)
							atomsToSPJ.put(atom, existingSQL+" UNION " + select);
						else
							atomsToSPJ.put(atom, select);
						
						if (mappingAsDatalogRule!=null) {
							Set<String> ddRules = atomsToDDRules.get(atom.replaceAll("http://sws.ifi.uio.no/vocab/npd-v2#", "npdv:").replaceAll("http://sws.ifi.uio.no/vocab/npd-v2-ptl#", "ptl:"));
							if (ddRules!=null) {
								ddRules.add(mappingAsDatalogRule);
								atomsToDDRules.put(atom.replaceAll("http://sws.ifi.uio.no/vocab/npd-v2#", "npdv:").replaceAll("http://sws.ifi.uio.no/vocab/npd-v2-ptl#", "ptl:"), ddRules);
//								atomsToDDRules.put(atom, ddRules);
							}
							else
								atomsToDDRules.put(atom.replaceAll("http://sws.ifi.uio.no/vocab/npd-v2#", "npdv:").replaceAll("http://sws.ifi.uio.no/vocab/npd-v2-ptl#", "ptl:"), new HashSet<String>(Collections.singleton(mappingAsDatalogRule)));
						}
						
						//avenet - added it
						spjs.add(select);
						line = bf.readLine();
						lineNumber++;
					}
				}
				line = bf.readLine();
				lineNumber++;
			}
			bf.close();
//			for (String key : atomsToSPJ.keySet())
//				System.out.println(key + " @ '" + atomsToSPJ.get(key) + "'");
//			for (String key : atomsToDDRules.keySet())
//				System.out.println(key + " @ '" + atomsToDDRules.get(key) + "'");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
//	private void introspectTable(String tableName) {
//		if (conn == null )
//			return;
//		DatabaseMetaData metaData;
//		try {
//			metaData = conn.getMetaData();
////			System.out.println( "tableNAme: '" + tableName.trim() + "'");
//			ResultSet resultSet = metaData.getColumns(null, null, tableName, null);
//			 while (resultSet.next()) {
//				 
//				 String name = resultSet.getString("COLUMN_NAME");
////				 System.out.print( name + " ");
//
//			 }
////			 System.out.println();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
	private String buildClauseForRole(String lineElements, String subject, String object, String select) {
		String queryAsClauseString;
		if (object!=null) {
			if (!object.contains("{"))
				object = "{" + object + "}";
			queryAsClauseString = subject + "@@" + object + " <- ";
		}
		else
			queryAsClauseString = subject + " <- ";
//		System.out.println(queryAsClauseString + " ==== " + select);
		String isItQuantified = queryAsClauseString.substring(queryAsClauseString.indexOf("{"), queryAsClauseString.indexOf("}"));
		String fromPart = "";
		String originalFromPart = "";
		if (select.contains("WHERE")) {
			fromPart = select.substring(select.indexOf("FROM "), select.indexOf(" WHERE"));
			fromPart=fromPart.replace("FROM", "");
			String[] allTables =  fromPart.split(",");
			originalFromPart = new String(fromPart);
			if (!isItQuantified.contains("."))
				queryAsClauseString=queryAsClauseString.replace("{", fromPart +".");
			else
				queryAsClauseString=queryAsClauseString.replace("{","");
			queryAsClauseString=queryAsClauseString.replace("}", "");
			for (int i=0 ; i<allTables.length; i++) {
				queryAsClauseString+= allTables[i] + "]]]";
				if (i!=allTables.length-1)
					queryAsClauseString+= ",";
			}
		}
		else if (select.contains("INNER JOIN")) {
			fromPart = select.substring(select.indexOf("FROM "), select.indexOf(" INNER JOIN"));
			fromPart=fromPart.replace("FROM", "");
			originalFromPart = new String(fromPart);
			if (!isItQuantified.contains("."))
				queryAsClauseString=queryAsClauseString.replace("{", fromPart +".");
			else
				queryAsClauseString=queryAsClauseString.replace("{","");
			queryAsClauseString=queryAsClauseString.replace("}", "");
			queryAsClauseString+= fromPart + "]]], ";
//			introspectTable(fromPart);
			fromPart = select.substring(select.indexOf("INNER JOIN "), select.indexOf(" ON"));
			fromPart=fromPart.replace("INNER JOIN", "");
			queryAsClauseString+= fromPart + "]]]";
			//And do something with the variables
			queryAsClauseString+= ", " + select.split(" ON ")[1];
		}
		else {
			fromPart = select.split("FROM")[1];
			originalFromPart = new String(fromPart);
			if (!isItQuantified.contains("."))
				queryAsClauseString=queryAsClauseString.replace("{", fromPart +".");
			else
				queryAsClauseString=queryAsClauseString.replace("{","");
			queryAsClauseString=queryAsClauseString.replace("}", "");
			queryAsClauseString+= fromPart + "]]]";
		}
		
		String wherePart ="";
		if (select.contains("WHERE")) {
			wherePart = select.split("WHERE")[1];
			if (wherePart.contains("<>")) {
				String whereIndvParts = wherePart.split(" <> ")[0];			//WARNING: Strong assumption here
//				whereIndvParts = originalFromPart + "." + whereIndvParts;
//				queryAsClauseString+= ", internalNOTEQUAL(?1,something)";
				wherePart=wherePart.replace(whereIndvParts, originalFromPart + "." + whereIndvParts.trim());
				queryAsClauseString+= ", " + wherePart;
			}
			else if (wherePart.contains("="))
				queryAsClauseString+= ", " + wherePart;
//			queryAsClauseString+= ", internalEQUAL(?1,something)";
			else 
				System.err.println("Unhandled case: " + wherePart);
		}
		
//		System.out.println(queryAsClauseString);
//		try {
//			Clause cl =  new ClauseParser().parseClause(queryAsClauseString);
//			System.out.println(cl);
//		} catch(NullPointerException e) {
//			System.out.println(queryAsClauseString);
//		}
//		System.out.println(cl);
		return queryAsClauseString;
		
	}

	public static void main(String[] args) {
		String originalPath = System.getProperty("user.dir");
		String file = originalPath+"/Ontologies/NPD/npd-v2-ql-mysql_gstoil_avenet.obda";
		OBDAParser_Clauses obdaParser = new OBDAParser_Clauses(file,null);
		String  sqlStatement = "SELECT TO.obj, T1.subj FROM \"InjectionWellbore\" AS T0, \"fieldOperator\" AS T1 WHERE T0.subj=T1.obj";
		
		String clause = "Q(?0,?1,?2)  <-  dateLicenseeValidFrom(?44,?2), licenseeForLicence(?44,?0), licenseeInterest(?44,?1)";
		
		clause = "Q(?0,?1,?2)  <-  dateOperatorValidFrom(?44,?2), licenceOperatorCompany(?44,?45), name(?45,?1), operatorForLicence(?44,?0)";
		
		Clause queryAsClause = new ClauseParser().parseClause(clause);
//		System.out.println( obdaParser.replaceFrom(sqlStatement) );
		System.out.println(obdaParser.buildSQLFromClauses(queryAsClause));
	}

	public String replaceFrom(String sqlStatement) {
//		System.out.println("sql " + sqlStatement);
		String oldFromClause;
		if (sqlStatement.contains("WHERE"))
			oldFromClause = sqlStatement.substring(sqlStatement.indexOf("FROM"), sqlStatement.indexOf("WHERE"));
		else
			oldFromClause = sqlStatement.split("FROM")[1];

//		System.out.println("oldFromClause " + oldFromClause);

		String newFromClause = new String(oldFromClause);
		while (newFromClause.contains("\"")) {
			String tableInFromClause = newFromClause.substring(newFromClause.indexOf("\""), newFromClause.indexOf("\" AS")+1);
			String toReplace = atomsToSPJ.get("npdv:" + tableInFromClause.replace("\"", ""));
//			System.out.println( "atomsToSPJ " + atomsToSPJ.get("npdv:" + tableInFromClause.replace("\"", "")));
			//try alternatives. this is of course hard coded bull shit.
			if (toReplace== null)
				toReplace = atomsToSPJ.get("ptl:" + tableInFromClause.replace("\"", ""));

//			System.out.println( "atomsToSPJ " + atomsToSPJ.get("ptl:" + tableInFromClause.replace("\"", "")));

			if (toReplace!= null)
				newFromClause=newFromClause.replace(tableInFromClause, "(" + toReplace +")");
			else {
//				System.out.println("tableInFromClause: " + tableInFromClause);
				newFromClause=newFromClause.replace( tableInFromClause, tableInFromClause.replace("\"", ""));

			}

//			System.out.println("newFrom " + newFromClause);
		}
		return sqlStatement.replace(oldFromClause, newFromClause);
	}

	private String constructIndividualName(int lineLength,char[] firstArgumentInCharArray, int lineNumber) throws ArrayIndexOutOfBoundsException {
		String firstArgument = "CONCAT( '";
		int i = 0;
		while (i<lineLength) {
			if (firstArgumentInCharArray[i]!='{')
				firstArgument +=firstArgumentInCharArray[i++];
			else {
				//found opening "{". We will iterate until we find a closing one "}"
				firstArgument += "', {";
				i++;
				while (firstArgumentInCharArray[i]!='}')
					firstArgument +=firstArgumentInCharArray[i++];
				firstArgument += "}, '";
				i++;
			}
		}
		if (firstArgument.endsWith(", '")) {
			firstArgument = firstArgument + ")";
			firstArgument=firstArgument.replace(", ')", " )");
		}
		else
			firstArgument=firstArgument+ "')";

		if (firstArgument.contains("CONCAT( '',")) {
			firstArgument=firstArgument.replace("CONCAT( '',", "");
			firstArgument=firstArgument.replace(" )", "");
		}
		return firstArgument;
	}

	public String buildSQLFromClauses(Clause clause) {
		long start = System.currentTimeMillis();

		Set<String> allSQLAsDatalog = new HashSet<String>( Collections.singleton(" <- "));
		
		Set<Variable> boundVars = new HashSet<Variable>();
		for (Variable v : clause.getVariables()) {
			if (clause.isBound(v) && !clause.getDistinguishedVariables().contains(v))
				boundVars.add(v);
		}
		ArrayList<Variable> ansVars = clause.getDistinguishedVariables();

		for (Term term : clause.getBody()) {
			Set<String> mappingsForAtom = atomsToDDRules.get(term.getName().replaceAll("http://sws.ifi.uio.no/vocab/npd-v2#", "npdv:").replaceAll("http://sws.ifi.uio.no/vocab/npd-v2-ptl#", "ptl:"));
//			System.out.println(term.getName().replaceAll("http://sws.ifi.uio.no/vocab/npd-v2#", "npdv:").replaceAll("http://sws.ifi.uio.no/vocab/npd-v2-ptl#", "ptl:") + " has mappings " + mappingsForAtom);

//			Set<String> mappingsForAtom = atomsToDDRules.get("npdv:" + term.getName());
//			if (mappingsForAtom==null)
//				mappingsForAtom = atomsToDDRules.get("ptl:" + term.getName());

			
			if (mappingsForAtom==null)
				System.out.println("No mapping found for atom: " +  term.getName());
			else {

				Set<String> newSQLAsData = new HashSet<String>(allSQLAsDatalog);
				allSQLAsDatalog.clear();
				for (String sqlAsDat : newSQLAsData) {
					for (String str : mappingsForAtom) {
//						System.out.println("mfat " + str);
						String[] splitted =  str.split("<-");
						String head = splitted[0];
						String[] variables = head.split("@@");
//						System.out.println(variables[0].trim() + " ####\n" + variables[1].trim());
//								System.out.println(term.getArgument(i));

						String newSQLAsDataTemp = sqlAsDat + splitted[1] +",";
						
						for(int i =0 ; i< term.getArguments().length ; i++ ) {
							if (boundVars.contains((Variable) term.getArgument(i)))
								newSQLAsDataTemp = ((Variable) term.getArgument(i)) + "@@" + variables[i] + "++" + ((Variable) term.getArgument(i)) + newSQLAsDataTemp;
							if (ansVars.contains((Variable) term.getArgument(i)))
								newSQLAsDataTemp = "{{{" + variables[i] + "}}}" + newSQLAsDataTemp;
						}
						allSQLAsDatalog.add(newSQLAsDataTemp);
					}
				}
			}
		}
		int i=1;
		String unionOfSQLs = "START";
		
		Set<String> newSQLAsDataOnlyBody = new HashSet<String>(allSQLAsDatalog);
		for (String sqlAsData : allSQLAsDatalog) {
			if ( sqlAsData.equals(" <- "))
				continue;
			String[] splitted =  sqlAsData.split(" <- ");
			String headContraints = splitted[0];
			Map<Variable,Set<String>> boundVarsToContraints = new HashMap<Variable,Set<String>>();
			for (Variable var : boundVars)
				boundVarsToContraints.put(var, new HashSet<String>());
				
			for (Variable var : boundVars) {
				while (headContraints.contains(var+"@@")) {
					Set<String> varConstraints = boundVarsToContraints.get(var); 
//					System.out.println("1--" + headContraints);
					String constraint = headContraints.substring(headContraints.indexOf(var+"@@"), headContraints.indexOf("++"+var));
					varConstraints.add(constraint.replace(var+"@@", ""));
					boundVarsToContraints.put(var,varConstraints);
					headContraints = headContraints.replace(constraint + "++"+var, "");
				}
			}
			boolean validQuery = true;
			for (Variable var : boundVarsToContraints.keySet()) {
//				System.out.println( var + " " + boundVarsToContraints.get(var));
				if (!canBeJoined(boundVarsToContraints.get(var))) {
					validQuery=false;
					break;
				}
			}
//			System.out.println(validQuery);
			if (validQuery) {
				String sql = "SELECT ";
				String wherePart = " WHERE ";
				while (headContraints.contains("{{{")) {
					String selectIndv = headContraints.substring(headContraints.indexOf("{{{"), headContraints.indexOf("}}}"));
//					System.out.println("2--" + headContraints + " " + selectIndv);
					headContraints=headContraints.replace(selectIndv+"{{{", "");
					headContraints=headContraints.replace(selectIndv+"}}}", "");
					selectIndv=selectIndv.replace("{{{", "");
					sql += selectIndv + ", ";
					
				}
				sql += "GGGGGGGG";
				sql = sql.replace(", GGGGGGGG", "");
//				System.out.println( sql + "\n" + splitted[1] + "\n");
				String[] bodyAtoms = splitted[1].split(", ");
				Set<String> bodyAtomsInSet = new HashSet<String>();
				sql += " FROM ";
				for (String atom : bodyAtoms) {
//					System.out.println(atom);
					if (atom.contains("=") || atom.contains(" <> "))
						wherePart += atom.replace(",", "") + " AND ";
					else if (!sql.contains(atom))
						sql+= atom + ", ";
				}
				sql=sql.replace("]]]", "");
				sql += "GGGGGGGG";
				sql = sql.replace(", GGGGGGGG", "");
				wherePart += "GGGGGGGG";
				wherePart = wherePart.replace(" AND GGGGGGGG", "");
				wherePart = wherePart.replace(" WHERE GGGGGGGG", "");
				sql += wherePart;
				unionOfSQLs+= " UNION " + sql;
//				System.out.println(sql);				
			}
			
		}
		System.out.println(" translated in : " + (System.currentTimeMillis()-start));
		unionOfSQLs=unionOfSQLs.replace("START UNION ", "");
		unionOfSQLs=unionOfSQLs.replace("START", "");
		unionOfSQLs=unionOfSQLs.replace(", UNION ", " UNION ");
		unionOfSQLs=unionOfSQLs + " EEEEEEEND";
		unionOfSQLs=unionOfSQLs.replace(", EEEEEEEND", "");
		unionOfSQLs=unionOfSQLs.replace(" EEEEEEEND", "");
		return unionOfSQLs;
	}

	private boolean canBeJoined(Set<String> set) {
		String firstContraint = set.iterator().next();
		String instantiatedConstraint = instantiateLine(firstContraint);
//		System.out.println(instantiatedConstraint + "\n" + firstContraint);
		set.remove(firstContraint);
		for (String constrs : set) {
			if (!instantiatedConstraint.equals(instantiateLine(constrs)))
				return false;
		}
		return true;
	}

	private String instantiateLine(String firstContraint) {
		String instConstraint = "";
		char[] contraintAsChar = firstContraint.toCharArray();
		int index=0;
		boolean quoteOpenned = false;
		while (index<firstContraint.length()) {
			if (contraintAsChar[index] == '\'' ) {
				++index;
				quoteOpenned = true;
			}
			if (quoteOpenned) {
				while (contraintAsChar[index] != '\'') {
					instConstraint += contraintAsChar[index++];
				}
				quoteOpenned = false;
				index++;
			}
			else {
//				instConstraint += "@";
				index++;
			}
		}
		return instConstraint;
	}
}
