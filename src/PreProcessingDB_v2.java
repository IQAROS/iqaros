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


import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.oxford.comlab.perfectref.rewriter.Clause;
import org.oxford.comlab.perfectref.rewriter.Term;
import org.oxford.comlab.perfectref.rewriter.TermFactory;
import org.oxford.comlab.perfectref.rewriter.Variable;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLEntity;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLProperty;

import edu.aueb.queries.Evaluator;


/**
 * Create clauses that consist of one or two conjuncts and have no answers
 *
 * These will be later used to cut off queries from the UCQ rewriting
 *
 */

public class PreProcessingDB_v2 {

	private static TermFactory m_termFactory = new TermFactory();
	private static boolean add;

	public static void main(String[] args) throws Exception {
		String path = System.getProperty("user.dir").replace("\\", "/") + "/Dataset/";
		
		String ontologyFile = "file:" + path + "UOBM/univ-bench-dl-Zhou_DL-Lite.owl";
		String queryPath = path + "UOBM/CQ/";
		//enter the optimization file computed using PreProcessingDB_v1 or PreProcessingDB_v2
		String optimizationPath = "";


		HashMap<String,Set<String>> conceptsAndAnswers = new HashMap<String,Set<String>>();
		HashMap<String,Set<String>> rolesAndAnswers = new HashMap<String,Set<String>>();
		HashMap<String,Set<String>> invRolesAndAnswers = new HashMap<String,Set<String>>();


		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		IRI physicalURI = IRI.create(ontologyFile);
        OWLOntology ontology = manager.loadOntology(physicalURI);

        Evaluator ev = new Evaluator("jdbc:postgresql://127.0.0.1:5432/UniprotNoURIs");
//        Evaluator ev = new Evaluator("jdbc:mysql://127.0.0.1:3306/npdfactpages");
//		Evaluator ev = new Evaluator("jdbc:mysql://127.0.0.1:3306/npd",path + "mappings/mysql/npd-v2-ql-mysql_gstoil_avenet.obda", true);

		Set<Term> conceptTermsWithAnswers = new HashSet<Term>();
		Set<Term> roleTermsWithAnswers = new HashSet<Term>();
//		Set<Term> invRoleTermsWithAnswers = new HashSet<Term>();

		Set<Clause> clausesWithNoAnswers = new HashSet<Clause>();

		Variable var1 = m_termFactory.getVariable(0);
		Variable var2 = m_termFactory.getVariable(1);

		int clausesWithAnswers = 0;
		long start = System.currentTimeMillis();

		Set<OWLEntity> signature = ontology.getSignature();
		Set<OWLClass> concepts = new HashSet<OWLClass>();
		Set<OWLProperty> props = new HashSet<OWLProperty>();
		/*
		 * Identify all entities of the ontology
		 * Entities are:	classes
		 * 					properties (object + data properties)
		 */
		for (OWLEntity entity: signature)
		{
			if ( entity instanceof OWLClass )
				concepts.add((OWLClass) entity);
			if ( entity instanceof OWLObjectProperty || entity instanceof OWLDataProperty )
				props.add((OWLProperty) entity);
		}

//		for ( OWLOntology on: ontology.getImports() )
//			for (OWLEntity entity: on.getSignature())
//			{
//				if ( entity instanceof OWLClass )
//					concepts.add((OWLClass) entity);
//				if ( entity instanceof OWLObjectProperty || entity instanceof OWLDataProperty )
//					props.add((OWLProperty) entity);
//			}

		System.out.println("IDENTIFIEED _ in answercomparison");
		System.out.println("Concepts = " + concepts.size() );
		System.out.println("Properties = " + props.size() );
		System.out.println("in " + (System.currentTimeMillis() - start) + " ms");

		/*
		 * Evaluate all classes on the DB and store
		 */

		System.out.println("\nEvaluating all concepts: there are " + concepts.size());
		long startConcepts = System.currentTimeMillis();
		int processed=0;
		double progress=0;
		double lastprogress=0;
		int allWork=concepts.size();
		for ( OWLClass cl: concepts )
		{
			if ( cl.toString().contains("owl:Thing") )
				continue;
			processed++;
			progress=processed;
			progress=progress/allWork;
//			System.out.println(progress);
			if (progress>lastprogress+0.05) {
				System.out.print((int)(progress*100) + "%, ");
				lastprogress=progress;
			}
//			String clName = cl.getIRI().toString();
			//getFragment does not work well when the string starts with a number
			String clName = cl.getIRI().getFragment();
			if (clName==null)
				clName = cl.getIRI().toString().substring(cl.getIRI().toString().lastIndexOf("/")+1);
//			System.out.println(cl.getIRI().toString() + " " + clName + " " + var1);
			Term clTerm = m_termFactory.getFunctionalTerm(clName, var1);
			Term[] body = new Term[1];
			body[0] = clTerm;
			Clause clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
//			System.out.println("\n" + clause + "\t" + ev.getSQLWRTMappings(clause));
			Set<String> ans;
			if (path.contains("npd"))
				ans = ev.evaluateSQLReturnResults(null, ev.getSQLWRTMappings(clause));
			else
				ans = ev.evaluateSQLReturnResults(null, ev.getSQLavenet(clause));
			if (ans.size() == 0) {
//				entitiesWithNoAnswersMap.put(clName, ans);
//				conceptTermsWithNoAnswers.add(clTerm);
				clausesWithNoAnswers.add(clause);
			}
			else {
				conceptTermsWithAnswers.add(clTerm);
				conceptsAndAnswers.put(clName, ans);
			}
		}
		System.out.println("\nClauses With Answers " + conceptTermsWithAnswers.size());
		System.out.println("ClausesWithNoAnswersSize = " + clausesWithNoAnswers.size() + " in " + (System.currentTimeMillis() - startConcepts) + "ms");



		/*
		 * Evaluate all properties on the DB and store
		 * 		entitiesWithNoAnswer
		 * 		roleTermsTermWithNoAnswer
		 */
		allWork=props.size();
		System.out.println("\nEvaluating all properties. there are: " + allWork);
		long startProperties = System.currentTimeMillis();
		processed=0;
		progress=0;
		lastprogress=0;
		for ( OWLProperty prop: props )
		{
			processed++;
			progress=processed;
			progress=progress/allWork;
//			System.out.println(progress);
			if (progress>lastprogress+0.05) {
				System.out.print((int)(progress*100) + "%, ");
				lastprogress=progress;
			}
//			String propName = prop.getIRI().toString();
			String propName = prop.getIRI().getFragment();
			if (propName==null)
				propName=prop.getIRI().toString().substring(prop.getIRI().toString().lastIndexOf("/")+1);
			Term propTerm = m_termFactory.getFunctionalTerm(propName, var1, var2);
			Term propTermInv = m_termFactory.getFunctionalTerm(propName, var2, var1);
			Term[] body = new Term[1];
			body[0] = propTerm;
			Clause clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1, var2));
			Set<String> ans;
			String queries;
			if (path.contains("npd"))
				queries = ev.getSQLWRTMappings(clause);
			else
				queries = ev.getSQLavenet(clause);
			ans = ev.evaluateSQLReturnResults(null, queries);
			if (ans.size() == 0) {
				clausesWithNoAnswers.add(clause);
				//Create the clause with the inverse role and add it also to the list
				body = new Term[1];
				body[0] = propTermInv;
				clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
				clausesWithNoAnswers.add(clause);
			}
			else {
				roleTermsWithAnswers.add(propTerm);
//				invRoleTermsWithAnswers.add(propTermInv);

				rolesAndAnswers.put(propName, ans);
//				invRolesAndAnswers.put(propName, ev.evaluateSQLReturnResultsInverse(null, queries));
			}
		}
		System.out.println("\nClauses With Answers " + (roleTermsWithAnswers.size() + invRolesAndAnswers.size() ) );
		System.out.println("ClausesWithNoAnswersSize = " + clausesWithNoAnswers.size() + " in " + (System.currentTimeMillis() - startProperties) + "ms");


		/**
		 * Evaluate COMBINATIONS of CONCEPTS
		 * For example for concepts C, D create clause
		 *
		 * Q(x) <- C(x), D(x)
		 *
		 * and identify if there exist answers
		 */
		allWork=conceptTermsWithAnswers.size()* (conceptTermsWithAnswers.size() -1);
		System.out.println("\n\nEvaluating combination of concepts");
		System.out.println("Possible combinations: " + allWork);
		long startConceptCombination = System.currentTimeMillis();
		processed=0;
		progress=0;
		lastprogress=0;
		HashSet<String> checkedConcepts = new HashSet<String>();
		for (Term conceptTerm1 : conceptTermsWithAnswers ) {
			for (Term conceptTerm2 : conceptTermsWithAnswers ) {
				if ( checkedConcepts.contains(conceptTerm2.toString()))
					continue;
				processed++;
				progress=processed;
				progress=progress/allWork;
//				System.out.println(progress);
				if (progress>lastprogress+0.05) {
					System.out.print((int)(progress*100) + "%, ");
					lastprogress=progress;
				}
				if ( !conceptTerm1.equals(conceptTerm2) ) {

					Set<String> term1Ans = conceptsAndAnswers.get(conceptTerm1.getName());
					Set<String> term2Ans = conceptsAndAnswers.get(conceptTerm2.getName());

					Term[] body = new Term[2];
					body[0] = conceptTerm1;
					body[1] = conceptTerm2;
					Clause clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
					if ( !haveCommonAnswers(term1Ans,term2Ans) ) {
						clausesWithNoAnswers.add(clause);
					}
					else {
//						System.out.print("|");
						clausesWithAnswers++;
					}
				}
			}
			checkedConcepts.add(conceptTerm1.toString());
		}
		System.out.println("\nCombinations of concepts with answers " + clausesWithAnswers);
		System.out.println("ClausesWithNoAnswersSize = " + clausesWithNoAnswers.size() + " in " + (System.currentTimeMillis() - startConceptCombination) + "ms");

		clausesWithAnswers=0;

//		System.exit(0);

		/**
		 * Evaluate COMBINATIONS of CONCEPTS and ROLES
		 * For example for concept C and role R create clause
		 *
		 * Q(x) <- C(x), R(x,y)
		 *
		 * and identify if there exist answers
		 */
		allWork=conceptTermsWithAnswers.size()*roleTermsWithAnswers.size();
		System.out.println("\n\nEvaluating combinations of concepts and properties");
		System.out.println("Possible Combinations: " + allWork);
		long startConceptRoleCombination = System.currentTimeMillis();
		processed=0;
		progress=0;
		lastprogress=0;
		for (Term conceptTerm : conceptTermsWithAnswers ) {
			for (Term roleTerm : roleTermsWithAnswers ) {
				processed++;
				progress=processed;
				progress=progress/allWork;
//				System.out.println(progress);
				if (progress>lastprogress+0.05) {
					System.out.print((int)(progress*100) + "%, ");
					lastprogress=progress;
				}

				Set<String> conceptAns = conceptsAndAnswers.get(conceptTerm.getName());
				Set<String> propsAns = getFirstVariablesAns(rolesAndAnswers.get(roleTerm.getName()));

				if ( !haveCommonAnswersWithFirstVariable(conceptAns,rolesAndAnswers.get(roleTerm.getName())) ) {
					Term[] body = new Term[2];
					body[0] = conceptTerm;
					body[1] = roleTerm;
					Clause clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
					clausesWithNoAnswers.add(clause);
				}
				else {
//					System.out.print("|");
					clausesWithAnswers++;
				}

				/**
				 * C(x), R(y,x)
				 */
				if ( !haveCommonAnswersWithSecondVariable(conceptAns,rolesAndAnswers.get(roleTerm.getName())) ) {
					Term propTermInv = m_termFactory.getFunctionalTerm(roleTerm.getName(), var2, var1);
					Term[] invBody = new Term[2];
					invBody[0] = conceptTerm;
					invBody[1] = propTermInv;
//					System.out.println(invBody[1] + " \t" + invBody[0]);
					Clause invClause = new Clause(invBody, m_termFactory.getFunctionalTerm("Q", var1, var2));						
					clausesWithNoAnswers.add(invClause);
				}
				else {
//					System.out.print("|");
					clausesWithAnswers++;
				}

			}
		}
		System.out.println("\nCombinations of concepts with roles with answers " + clausesWithAnswers);
		System.out.println("ClausesWithNoAnswersSize = " + clausesWithNoAnswers.size() + " in " + (System.currentTimeMillis() - startConceptRoleCombination) + "ms");

		clausesWithAnswers = 0;


//		/**
//		 * Evaluate COMBINATIONS of CONCEPTS and ROLES
//		 * For example for concept C and role R create clause
//		 *
//		 * Q(x) <- C(x), R(y,x)
//		 *
//		 * and identify if there exist answers
//		 */
//		allWork=conceptTermsWithAnswers.size()*roleTermsWithAnswers.size();
//		System.out.println("\n\nEvaluating combinations of concepts and inverse properties");
//		System.out.println("Possible Combinations: " + allWork);
//		long startConceptInvRoleCombination = System.currentTimeMillis();
//		processed=0;
//		progress=0;
//		lastprogress=0;
//		for (Term conceptTerm : conceptTermsWithAnswers ) {
//			for (Term roleTerm : invRoleTermsWithAnswers ) {
//				processed++;
//				progress=processed;
//				progress=progress/allWork;
////				System.out.println(progress);
//				if (progress>lastprogress+0.05) {
//					System.out.print((int)(progress*100) + "%, ");
//					lastprogress=progress;
//				}
//				Term[] body = new Term[2];
//				body[0] = conceptTerm;
//				body[1] = roleTerm;
//				Clause clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
//
//				Set<String> conceptAns = conceptsAndAnswers.get(conceptTerm.getName());
//				Set<String> propsAns = getSecondVariablesAns(rolesAndAnswers.get(roleTerm.getName()));
//
//				if ( !haveCommonAnswers(conceptAns,propsAns) ) {
//					clausesWithNoAnswers.add(clause);
//				}
//				else {
////					System.out.print("|");
//					clausesWithAnswers++;
//				}
//			}
//		}
//		System.out.println("\nCombinations of concepts with inverse roles with answers " + clausesWithAnswers);
//		System.out.println("ClausesWithNoAnswersSize = " + clausesWithNoAnswers.size() + " in " + (System.currentTimeMillis() - startConceptInvRoleCombination) + "ms");

		clausesWithAnswers = 0;
//
		/**
		 * Evaluate COMBINATIONS of ROLES
		 * For example for roles S and role R create clause
		 *
		 * Q(x) <- S(x,y), R(x,y)
		 *
		 * and identify if there exist answers
		 */
		allWork=roleTermsWithAnswers.size()*roleTermsWithAnswers.size();
		System.out.println("\n\nEvaluating combinations of properties");
		System.out.println("Possible Combinations: " + allWork);
		long startRoleRoleCombination = System.currentTimeMillis();
		processed=0;
		progress=0;
		lastprogress=0;
		Set<String> checkedRoles = new HashSet<String>();
		for (Term roleTerm1 : roleTermsWithAnswers ) {
			for (Term roleTerm2 : roleTermsWithAnswers ) {
				if ( checkedRoles.contains(roleTerm2.toString()) )
					continue;
				processed++;
				progress=processed;
				progress=progress/allWork;
//				System.out.println(progress);
				if (progress>lastprogress+0.05) {
					System.out.print((int)(progress*100) + "%, ");
					lastprogress=progress;
				}
				if (!roleTerm1.equals(roleTerm2))
				{
					Term[] body = new Term[2];
					body[0] = roleTerm1;
					body[1] = roleTerm2;
					Clause clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));

					Set<String> role1Ans = rolesAndAnswers.get(roleTerm1.getName());
					Set<String> role2Ans = rolesAndAnswers.get(roleTerm2.getName());

					if ( !haveCommonAnswers(role1Ans,role2Ans) ) {
						clausesWithNoAnswers.add(clause);
					}
					else
						clausesWithAnswers++;
					if ( !haveCommonAnswersInverse(role1Ans,role2Ans) ) {
						Term propTermInv = m_termFactory.getFunctionalTerm(roleTerm2.getName(), var2, var1);
						Term[] invBody = new Term[2];
						invBody[0] = roleTerm1;
						invBody[1] = propTermInv;
						Clause invClause = new Clause(invBody, m_termFactory.getFunctionalTerm("Q", var1, var2));						
						clausesWithNoAnswers.add(invClause);
					}
					else {
						clausesWithAnswers++;
					}

				}
			}
			checkedRoles.add(roleTerm1.toString());
		}
		System.out.println("\nCombinations of roles with answers " + clausesWithAnswers);
		System.out.println("ClausesWithNoAnswersSize = " + clausesWithNoAnswers.size() + " in " + (System.currentTimeMillis() - startRoleRoleCombination) + "ms");

//		clausesWithAnswers = 0;
//		/**
//		 * Evaluate COMBINATIONS of ROLES
//		 * For example for roles S and role R create clause
//		 *
//		 * Q(x) <- S(x,y), R(y,x)
//		 *
//		 * and identify if there exist answers
//		 */
//		allWork=roleTermsWithAnswers.size()*invRoleTermsWithAnswers.size();
//		System.out.println("\n\nEvaluating combinations of properties with inv");
//		System.out.println("Possible Combinations: " + allWork);
//		startRoleRoleCombination = System.currentTimeMillis();
//		processed=0;
//		progress=0;
//		lastprogress=0;
//		checkedRoles = new HashSet<String>();
//		for (Term roleTerm1 : roleTermsWithAnswers ) {
//			for (Term roleTerm2 : invRoleTermsWithAnswers ) {
////				if ( checkedRoles.contains(roleTerm2.toString()) ) {
////					System.out.println("ds");
////					continue;
////				}
//				processed++;
//				progress=processed;
//				progress=progress/allWork;
////				System.out.println(progress);
//				if (progress>lastprogress+0.05) {
//					System.out.print((int)(progress*100) + "%, ");
//					lastprogress=progress;
//				}
//				if (!roleTerm1.equals(roleTerm2))
//				{
//					Term[] body = new Term[2];
//					body[0] = roleTerm1;
//					body[1] = roleTerm2;
//					Clause clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
//
//					Set<String> role1Ans = rolesAndAnswers.get(roleTerm1.getName());
//					Set<String> role2Ans = invRolesAndAnswers.get(roleTerm2.getName());
//
//					if ( !haveCommonAnswers(role1Ans,role2Ans) ) {
////						System.out.println(processed);
//						clausesWithNoAnswers.add(clause);
//					}
//					else {
//						clausesWithAnswers++;
//					}
//				}
//			}
////			checkedRoles.add(roleTerm1.toString());
//		}
//		System.out.println("\nCombinations of roles with answers " + clausesWithAnswers);
//		System.out.println("ClausesWithNoAnswersSize = " + clausesWithNoAnswers.size() + " in " + (System.currentTimeMillis() - startRoleRoleCombination) + "ms");


		System.out.println("\n\nOverall Time = " + (System.currentTimeMillis() - start));
		/**
		 * Store this info in a file
		 */
		printResultToFile(optimizationPath, clausesWithNoAnswers);
		ev.closeConn();
	}

	private static Set<String> getFirstVariablesAns(Set<String> set) {

		Set<String> result = new HashSet<String>();

		for ( String str: set ) {
//			System.out.println("1: " + str);
//			System.out.println("2: " + str.substring(0, str.indexOf("----IQAROS---")));
			result.add(str.substring(0, str.indexOf("----IQAROS----")));
		}
		return result;
	}

	private static Set<String> getSecondVariablesAns(Set<String> set) {

		Set<String> result = new HashSet<String>();

		for ( String str: set ) {
//			System.out.println("1: " + str);
//			System.out.println("2: " + str.substring((str.indexOf("----IQAROS---")+13), str.length()));
			result.add(str.substring((str.indexOf("----IQAROS----")+14), str.length()));
		}
		return result;
	}


	private static boolean haveCommonAnswers(Set<String> term1Ans, Set<String> term2Ans) {

		for ( String s1: term1Ans )
			if ( term2Ans.contains(s1) )
				return true;

		return false;
	}

	private static boolean haveCommonAnswersInverse(Set<String> term1Ans, Set<String> term2Ans) {

//		for ( String s1: term1Ans )
//			for (String s2 : term2Ans)
//				if (Objects.equals(s1,s2))
//					return true;
//		return false;
//
		for ( String s1: term1Ans ) {
			if ( term2Ans.contains(s1.substring((s1.indexOf("----IQAROS----")+14), s1.length())+Evaluator.ANSWERSEPARATOR+s1.substring(0, s1.indexOf("----IQAROS----"))) )
//				String[] splitString = s1.split(Evaluator.ANSWERSEPARATOR);
//				if ( term2Ans.contains(splitString[1]+Evaluator.ANSWERSEPARATOR+splitString[0]) )
				return true;
		}
		return false;
	}

	private static void printResultToFile(String outputFilestr, Set<Clause> clauses) throws Exception{
		File outputFile = new File(outputFilestr);
        FileWriter out = new FileWriter(outputFile);

        for(Clause c: clauses){
			out.write( c.m_canonicalRepresentation + "\n");
		}

        out.close();
	}
	private static boolean haveCommonAnswersWithFirstVariable(Set<String> conceptAns, Set<String> propsAns) {
		for ( String s1: propsAns ) {
			if ( conceptAns.contains(s1.substring(0, s1.indexOf("----IQAROS----"))) )
//				if ( conceptAns.contains(s1.split(Evaluator.ANSWERSEPARATOR)[0]) )
				return true;
		}
		return false;
	}

	private static boolean haveCommonAnswersWithSecondVariable(Set<String> conceptAns, Set<String> propsAns) {
		for ( String s1: propsAns ) {
			
			if ( conceptAns.contains(s1.substring((s1.indexOf("----IQAROS----")+14), s1.length()) ) ) 
//				if ( conceptAns.contains(s1.split(Evaluator.ANSWERSEPARATOR)[1]) )
				return true;
		}
		return false;
	}

}
