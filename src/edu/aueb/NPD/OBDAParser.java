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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class OBDAParser {

	Map<String,String> atomsToSPJ = new HashMap<String,String>();
	Set<String> spjs = new HashSet<String>();

	public Map<String,String> getAtomsToSPJ () {
		return atomsToSPJ;
	}

	public void setAtomsToSPJ(Map<String,String> atomsToSPJ) {
		this.atomsToSPJ = atomsToSPJ;
	}

	public Set<String> getSpjs() {
		return spjs;
	}

	public OBDAParser(String obdaSpecificationFile) {
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
						try {
							subject = constructIndividualName(lineElements[0].length(),lineElements[0].toCharArray(),lineNumber);
						} catch (ArrayIndexOutOfBoundsException e) {
							System.out.println("No closing \"}\" in line " + lineNumber);
							bf.close();
							System.exit(1);
						}
						String object = "";
						String atom=null;
						if (lineElements[1].equals("a")) {
							atom=lineElements[2];
						}
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

						String argumentsInRawString = select.substring(select.indexOf("SELECT")+7, select.indexOf(" FROM"));
//						String[] selectArguments = argumentsInRawString.split(",");
						//if the last element is a constant, a boolean, or it is a concept atom mapping replace everything in the select with the subject agument
						if (lineElements[1].equals("a")) {
							select=select.replace( "SELECT " + argumentsInRawString, "SELECT " + subject + " AS individual");
						}
						else if (lineElements[2].contains("^^") &&  !lineElements[2].contains("true") && !lineElements[2].contains("false")) {
							object=object.split("}")[0];
							object=object.replace("{", "");
							select=select.replace( "SELECT " + argumentsInRawString, "SELECT " + subject + " AS subject, " + object + " AS obj");
						}
						else if (lineElements[2].contains("<http://") ) {
//							select=select.replace( "SELECT " + argumentsInRawString, "SELECT " + subject + " AS subject");
							//avenet 2016-01-14
							select=select.replace( "SELECT " + argumentsInRawString, "SELECT " + subject + " AS subject, " + lineElements[2] + " AS obj");
						}
						else if (lineElements[2].contains("^^") && (lineElements[2].contains("true") || lineElements[2].contains("false"))) {
							//avenet 2016-01-14
							if ( lineElements[2].contains("true") )
								object = "true";
							else
								object = "false";
							select=select.replace( "SELECT " + argumentsInRawString, "SELECT " + subject + " AS subject, " + object + " AS obj");
						}
						else {
							//replace the first n-1th with subject agument and nth with the object agument
//							String firstN_1Atoms = "";
//							String newSelectClause="";
//							for (int j=0 ; j<selectArguments.length-1; j++)
//								firstN_1Atoms += selectArguments[j] + ",";
//							select=select.replace(firstN_1Atoms, subject+",");
//							select=select.replace(selectArguments[selectArguments.length-1], object);
							select=select.replace( "SELECT " + argumentsInRawString, "SELECT " + subject + " AS subject," + object + " AS obj");
						}

						String existingSQL = atomsToSPJ.get(atom);
						if (existingSQL!=null)
							atomsToSPJ.put(atom, existingSQL+" UNION " + select);
						else
							atomsToSPJ.put(atom, select);
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
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		String originalPath = "/Users/avenet/Academia/Ntua/Ontologies/";
		String file = originalPath+"npd-benchmark-master/mappings/mysql/npd-v2-ql-mysql.obda";
		OBDAParser obdaParser = new OBDAParser(file);
		String  sqlStatement = "SELECT TO.obj, T1.subj FROM \"npdv:InjectionWellbore\" AS T0, \"npdv:fieldOperator\" AS T1 WHERE T0.subj=T1.obj";
		System.out.println( obdaParser.replaceFrom(sqlStatement) );
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
			
//			System.out.println("\t" + tableInFromClause);
			
			String toReplace = atomsToSPJ.get(tableInFromClause.replace("\"", ""));
//			String toReplace = atomsToSPJ.get("npdv:" + tableInFromClause.replace("\"", ""));

			
//			System.out.println( "atomsToSPJ " + tableInFromClause.replace("\"", "") + "\t\t" + atomsToSPJ.get(tableInFromClause.replace("\"", "")));

			//try alternatives. this is of course hard coded bull shit.
			if (toReplace== null)
				toReplace = atomsToSPJ.get(tableInFromClause.replace("\"", ""));
//				toReplace = atomsToSPJ.get("ptl:" + tableInFromClause.replace("\"", ""));

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
				firstArgument += "', ";
				i++;
				while (firstArgumentInCharArray[i]!='}')
					firstArgument +=firstArgumentInCharArray[i++];
				firstArgument += ", '";
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
}
