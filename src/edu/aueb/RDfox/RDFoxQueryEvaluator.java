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

package edu.aueb.RDfox;
import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.oxford.comlab.perfectref.rewriter.Clause;
import org.oxford.comlab.perfectref.rewriter.Term;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import uk.ac.ox.cs.JRDFox.JRDFStoreException;
import uk.ac.ox.cs.JRDFox.Prefixes;
import uk.ac.ox.cs.JRDFox.model.GroundTerm;
import uk.ac.ox.cs.JRDFox.store.DataStore;
import uk.ac.ox.cs.JRDFox.store.Parameters;
import uk.ac.ox.cs.JRDFox.store.TupleIterator;

public class RDFoxQueryEvaluator {

	DataStore store = null;
	Prefixes prefixes;
	
	public static final String ANSWERSEPARATOR = "----IQAROS----";

	public void loadInputToSystem(String ontologyFile, String[] datasetFiles) throws FileNotFoundException, OWLOntologyCreationException, URISyntaxException, JRDFStoreException {

		prefixes = Prefixes.DEFAULT_IMMUTABLE_INSTANCE;

		System.out.println("Ontology..." + IRI.create((ontologyFile)));
		store = new DataStore(DataStore.StoreType.ParallelSimpleNN, true);
		System.out.println("Setting the number of threads...");
		store.setNumberOfThreads(2);

		System.out.println("Adding the ontology to the store...");
		long addOnto = System.currentTimeMillis();
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(IRI.create(ontologyFile));
		store.importOntology(ontology);
		manager.removeOntology(ontology);
		System.out.println("in " + (System.currentTimeMillis() - addOnto) + "ms");

		System.out.println("Import triples for incremental reasoning");
		long importTriples = System.currentTimeMillis();
		File[] aBoxesAsJavaFiles = new File[datasetFiles.length];
		int i=0;
		for (String datasetFile : datasetFiles) {
//			System.out.println(datasetFile);
			aBoxesAsJavaFiles[i++] = new File(datasetFile);
		}

		store.importFiles(aBoxesAsJavaFiles);
		System.out.println("Number of tuples after import: " + store.getTriplesCount() + " in "+ (System.currentTimeMillis() - importTriples) + "ms");
		long startReason = System.currentTimeMillis();
		store.applyReasoning();
		System.out.println("Number of tuples after materialization: " + store.getTriplesCount() + " in "+ (System.currentTimeMillis() - startReason) + "ms");
		
//		//added avenet to see if it saves time -- seems it does not work...
//		store.clearRulesAndMakeFactsExplicit();

	}

//	public long evaluateQuery(Clause conjunctiveQueryAsClause) throws JRDFStoreException {
//		return evaluateQuery(clauseCQ2RDFoxQL(conjunctiveQueryAsClause));
//	}
//
//	public long evaluateQuery(Clause conjunctiveQueryAsClause, String uri) throws JRDFStoreException {
//		return evaluateQuery(clauseCQ2RDFoxQL(conjunctiveQueryAsClause, uri));
//	}

	public long evaluateQuery(Set<Clause> queriesCreatedByShrinkingOnly, String uri) throws JRDFStoreException {
		return evaluateQuery(ucqInClauses2SeRQL(queriesCreatedByShrinkingOnly, uri));
	}


	public long evaluateQuery(Set<Clause> queriesCreatedByShrinkingOnly) throws JRDFStoreException {
		return evaluateQuery(ucqInClauses2SeRQL(queriesCreatedByShrinkingOnly));
	}

//	public String ucqInClauses2SeRQL(Set<Clause> ucq) {
//		String queryString="";
//		for (Clause clauseInRewriting : ucq)
//			queryString+=clauseCQ2RDFoxQL(clauseInRewriting) +"\nUNION\n";
//
//		queryString+="END";
//		queryString=queryString.replace("UNION\nEND", "");
//
//		return queryString;
//	}

//	public String ucqInClauses2SeRQL(Set<Clause> ucq, String uri) {
//		String queryString="";
//		for (Clause clauseInRewriting : ucq)
//			queryString+=clauseCQ2RDFoxQL(clauseInRewriting, uri) +"\nUNION\n";
//
//		queryString+="END";
//		queryString=queryString.replace("UNION\nEND", "");
//
//		return queryString;
//	}

//	private String clauseCQ2RDFoxQL(Clause conjunctiveQueryAsClause) {
//		String serqlQuery="SELECT DISTINCT ";
//		for (int i=0 ; i<conjunctiveQueryAsClause.getHead().getArguments().length ; i++) {
//			String distVariable = conjunctiveQueryAsClause.getHead().getArgument(i).toString();
//			distVariable=distVariable.replace("?", "");
//			serqlQuery+="?X"+distVariable+",";
//		}
//
//		serqlQuery+=" WHERE ";
//		serqlQuery=serqlQuery.replace(", WHERE ", " WHERE ");
// 		serqlQuery+=clauseCQ2RDFoxQL(conjunctiveQueryAsClause.getBody());
//// 		System.out.println(serqlQuery);
//		return serqlQuery;
//	}
//
//	private String clauseCQ2RDFoxQL(Clause conjunctiveQueryAsClause, String uri) {
//		String serqlQuery="SELECT DISTINCT ";
//		for (int i=0 ; i<conjunctiveQueryAsClause.getHead().getArguments().length ; i++) {
//			String distVariable = conjunctiveQueryAsClause.getHead().getArgument(i).toString();
//			distVariable=distVariable.replace("?", "");
//			serqlQuery+="?X"+distVariable+" ";
//		}
//
//		serqlQuery+=" WHERE ";
//		serqlQuery=serqlQuery.replace(", WHERE ", " WHERE ");
// 		serqlQuery+=clauseCQ2RDFoxQL(conjunctiveQueryAsClause.getBody(),uri);
//// 		System.out.println(serqlQuery);
//		return serqlQuery;
//	}

	//	TupleIterator tupleIterator = store.compileQuery("select distinct ?y where { ?x ?y ?z } ", prefixes, parameters, true);

	private static String clauseCQ2RDFoxQL(Term[] atomsOfConjunctiveQuery) {
		String serqlQuery="{ ";
		for (int i=0 ; i<atomsOfConjunctiveQuery.length ; i++) {
			Term at = atomsOfConjunctiveQuery[i];
			if (at.getArguments().length==1)
				serqlQuery+=handleArgument(at.getArgument(0))+" rdf:type <"+at.getName()+ ">, ";
			else
				serqlQuery+=""+handleArgument(at.getArgument(0))+" <"+at.getName()+ "> "+handleArgument(at.getArgument(1))+", ";
		}
		serqlQuery+="END";
		serqlQuery=serqlQuery.replace(", END", " }");
//		serqlQuery=serqlQuery.replace("?", "");

		return serqlQuery;
	}

	private static String clauseCQ2RDFoxQL(Term[] atomsOfConjunctiveQuery, String uri) {
		String serqlQuery="{ ";
		for (int i=0 ; i<atomsOfConjunctiveQuery.length ; i++) {
			Term at = atomsOfConjunctiveQuery[i];
			if (at.getArguments().length==1)
				serqlQuery+=handleArgument(at.getArgument(0))+" rdf:type <"+uri+at.getName()+ ">. ";
			else
				serqlQuery+=""+handleArgument(at.getArgument(0))+" <"+uri+at.getName()+ "> "+handleArgument(at.getArgument(1))+". ";
		}
		serqlQuery+="END";
		serqlQuery=serqlQuery.replace(". END", " }");
//		serqlQuery=serqlQuery.replace("?", "");

		return serqlQuery;
	}

	private static String handleArgument(Term argument) {
		String tempArgument=argument.toString();
		tempArgument=tempArgument.replace("?", "");
		if (argument.isVariable())
			return "?X"+tempArgument;
		//avenet 14-4-2016
//		else if (argument.isConstant() && argument.toString().startsWith("\"") && argument.toString().endsWith("\"")) {
//			System.out.println("Tassos: " + tempArgument.replace("\"", "\'"));
//			return tempArgument.replace("\"", "");
//		}
		else if (argument.isConstant() && !argument.toString().contains("^^") && !argument.toString().startsWith("\"") && !argument.toString().endsWith("\"") ) {
			return "<"+tempArgument+">";
		}
		else
			return tempArgument;
	}

	private long evaluateQuery(String queryAsString) throws JRDFStoreException {
//		System.out.println("Query\n" + queryAsString);
		TupleIterator tupleIterator = store.compileQuery(queryAsString, prefixes, new Parameters() );
		// tupleIterator.getMultiplicity(); mhpws auto epistrefei to numberOfRows?
		int numberOfRows = 0;
		try {
			int arity = tupleIterator.getArity();
			for (long multiplicity = tupleIterator.open(); multiplicity != 0; multiplicity = tupleIterator.getNext()) {
				for (int termIndex = 0; termIndex < arity; ++termIndex) {
//					if (termIndex != 0)
//						System.out.print("  ");
					GroundTerm groundTerm = tupleIterator.getGroundTerm(termIndex);
//					System.out.print(groundTerm.toString(prefixes));
				}
//				System.out.print(" * ");
//				System.out.print(multiplicity);
//				System.out.println();
				++numberOfRows;
//				if (numberOfRows > 0)
//					return numberOfRows;
			}
			tupleIterator.dispose();
		}
		finally {
			// Make sure that the iterator frees all its allocated resources!
//			System.out.println("Memory cleaned " + numberOfRows);
//			tupleIterator.dispose();
		}
		return numberOfRows;
	}

	private Set<String> evaluateQueryWithAnswers(String queryAsString) throws JRDFStoreException {

		Set<String> result = new HashSet<String>();

		TupleIterator tupleIterator = store.compileQuery(queryAsString, prefixes, new Parameters() );
		// tupleIterator.getMultiplicity(); mhpws auto epistrefei to numberOfRows?
		int numberOfRows = 0;
		try {
			int arity = tupleIterator.getArity();
			for (long multiplicity = tupleIterator.open(); multiplicity != 0; multiplicity = tupleIterator.getNext()) {
				String answer = "";
				for (int termIndex = 0; termIndex < arity; ++termIndex) {
					GroundTerm groundTerm = tupleIterator.getGroundTerm(termIndex);
					if (termIndex != 0)
						answer = answer + ANSWERSEPARATOR + groundTerm.toString(prefixes);
					else
						answer = groundTerm.toString(prefixes);
//					System.out.print();
				}
				result.add(answer);
//				System.out.print(" * ");
//				System.out.print(multiplicity);
//				System.out.println();
//				++numberOfRows;
			}
			tupleIterator.dispose();
		}
		finally {
			// Make sure that the iterator frees all its allocated resources!
//			tupleIterator.dispose();
		}
		return result;
	}

	
	private Set<String> evaluateQueryWithAnswersInverse(String queryAsString) throws JRDFStoreException {

		Set<String> result = new HashSet<String>();

		TupleIterator tupleIterator = store.compileQuery(queryAsString, prefixes, new Parameters() );
		// tupleIterator.getMultiplicity(); mhpws auto epistrefei to numberOfRows?
		int numberOfRows = 0;
		try {
			int arity = tupleIterator.getArity();
			for (long multiplicity = tupleIterator.open(); multiplicity != 0; multiplicity = tupleIterator.getNext()) {
				String answer = "";
				for (int termIndex = 0; termIndex < arity; ++termIndex) {
					GroundTerm groundTerm = tupleIterator.getGroundTerm(termIndex);
					if (termIndex != 1)
						answer = groundTerm.toString(prefixes);
					else
						answer = groundTerm.toString(prefixes) + ANSWERSEPARATOR + answer;
//					System.out.println();
				}
				result.add(answer);
//				System.out.print(" * ");
//				System.out.println(tupleIterator.getGroundTerm(1) + "\t" + tupleIterator.getGroundTerm(0));
//				System.out.println(answer);
//				System.out.println();
//				++numberOfRows;
			}
			tupleIterator.dispose();
		}
		finally {
			// Make sure that the iterator frees all its allocated resources!
			tupleIterator.dispose();
		}
		return result;
	}

	public String ucqInClauses2SeRQL(Set<Clause> ucq) {
		Clause conjunctiveQueryAsClause = ucq.iterator().next();
		String queryString="SELECT DISTINCT ";
		for (int i=0 ; i<conjunctiveQueryAsClause.getHead().getArguments().length ; i++) {
			String distVariable = conjunctiveQueryAsClause.getHead().getArgument(i).toString();
			distVariable=distVariable.replace("?", "");
			queryString+="?X"+distVariable+" ";
		}

		queryString+=" WHERE {";
		queryString=queryString.replace(", WHERE ", " WHERE ");

		for (Clause clauseInRewriting : ucq)
			queryString+=clauseCQ2RDFoxQL(clauseInRewriting) +"\nUNION\n";

		queryString+="END";
		queryString=queryString.replace("UNION\nEND", "");

// 		System.out.println(queryString);
		return queryString + "}";
	}

	public String ucqInClauses2SeRQL(Set<Clause> ucq, String uri) {
		Clause conjunctiveQueryAsClause = ucq.iterator().next();
		String queryString="SELECT DISTINCT ";
		for (int i=0 ; i<conjunctiveQueryAsClause.getHead().getArguments().length ; i++) {
			String distVariable = conjunctiveQueryAsClause.getHead().getArgument(i).toString();
			distVariable=distVariable.replace("?", "");
			queryString+="?X"+distVariable+" ";
		}

		queryString+=" WHERE {";
		queryString=queryString.replace(", WHERE ", " WHERE ");

		for (Clause clauseInRewriting : ucq)
			queryString+=clauseCQ2RDFoxQL(clauseInRewriting, uri) +"\nUNION\n";

		queryString+="END";
		queryString=queryString.replace("UNION\nEND", "");

// 		System.out.println(queryString);
		return queryString + "}";
	}

	private String clauseCQ2RDFoxQL(Clause conjunctiveQueryAsClause) {
		String serqlQuery="{";
		Term[] atomsOfConjunctiveQuery = conjunctiveQueryAsClause.getBody();
		for (int i=0 ; i<atomsOfConjunctiveQuery.length ; i++) {
			Term at = atomsOfConjunctiveQuery[i];
			if (at.getArguments().length==1)
				serqlQuery+=handleArgument(at.getArgument(0))+" rdf:type <"+at.getName()+ ">. ";
			else
				serqlQuery+=""+handleArgument(at.getArgument(0))+" <"+at.getName()+ "> "+handleArgument(at.getArgument(1))+". ";
		}
		serqlQuery+="END";
		serqlQuery=serqlQuery.replace(". END", " ");

		return serqlQuery + " }";
	}

	private String clauseCQ2RDFoxQL(Clause conjunctiveQueryAsClause, String uri) {
		String serqlQuery="{";
		Term[] atomsOfConjunctiveQuery = conjunctiveQueryAsClause.getBody();
		for (int i=0 ; i<atomsOfConjunctiveQuery.length ; i++) {
			Term at = atomsOfConjunctiveQuery[i];
			if (at.getArguments().length==1)
				serqlQuery+=handleArgument(at.getArgument(0))+" rdf:type <"+uri+at.getName()+ ">. ";
			else
				serqlQuery+=""+handleArgument(at.getArgument(0))+" <"+uri+at.getName()+ "> "+handleArgument(at.getArgument(1))+". ";
		}
		serqlQuery+="END";
		serqlQuery=serqlQuery.replace(". END", " ");

		return serqlQuery + " }";
	}


	public long evaluateQuery(Clause conjunctiveQueryAsClause, String uri) throws JRDFStoreException {
		return evaluateQuery(ucqInClauses2SeRQL(Collections.singleton(conjunctiveQueryAsClause),uri));
	}

	public long evaluateQuery(Clause conjunctiveQueryAsClause) throws JRDFStoreException {
		return evaluateQuery(ucqInClauses2SeRQL(Collections.singleton(conjunctiveQueryAsClause)));
	}

	public Set<String> evaluateQueryWithAnswers(Clause conjunctiveQueryAsClause) throws JRDFStoreException {
		return evaluateQueryWithAnswers(ucqInClauses2SeRQL(Collections.singleton(conjunctiveQueryAsClause)));
	}
	
	public Set<String> evaluateQueryWithAnswersInverse(Clause conjunctiveQueryAsClause) throws JRDFStoreException {
		return evaluateQueryWithAnswersInverse(ucqInClauses2SeRQL(Collections.singleton(conjunctiveQueryAsClause)));
	}
}

