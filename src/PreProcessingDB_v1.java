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

public class PreProcessingDB_v1 {

	private static TermFactory m_termFactory = new TermFactory();

	public static void main(String[] args) throws Exception {

		System.out.println("EVALUATION");

		String path = System.getProperty("user.dir").replace("\\", "/") + "/Dataset/";
		
		String ontologyFile = "file:" + path + "UOBM/univ-bench-dl-Zhou_DL-Lite.owl";
		String queryPath = path + "UOBM/CQ/";
		//enter the optimization file computed using PreProcessingDB_v1 or PreProcessingDB_v2
		String optimizationPath = "";

		
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		IRI physicalURI = IRI.create(ontologyFile);
        OWLOntology ontology = manager.loadOntology(physicalURI);

        //use for other ontologies
        Evaluator ev = new Evaluator("jdbc:postgresql://127.0.0.1:5432/UOBM30");
        //use for NPD
//		Evaluator ev = new Evaluator("jdbc:mysql://127.0.0.1:3306/npd",path + "mappings/mysql/npd-v2-ql-mysql_gstoil_avenet.obda", true);

		Set<OWLEntity> signature = ontology.getSignature();
		Set<OWLClass> concepts = new HashSet<OWLClass>();
		Set<OWLProperty> props = new HashSet<OWLProperty>();

		Set<Term> conceptTermsWithAnswers = new HashSet<Term>();
		Set<Term> roleTermsWithAnswers = new HashSet<Term>();

		Set<Clause> clausesWithNoAnswers = new HashSet<Clause>();

		Variable var1 = m_termFactory.getVariable(0);
		Variable var2 = m_termFactory.getVariable(1);

		int clausesWithAnswers = 0;
		long start = System.currentTimeMillis();

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
			String clName = cl.getIRI().getFragment();
//			if (clName==null)
//				clName=cl.getIRI().toString().substring(cl.getIRI().toString().lastIndexOf("/")+1);
//			String clName = cl.getIRI().toString();
//			System.out.println(cl + " " + cl.getIRI() + " " + clName + " " + var1);

			Term clTerm = m_termFactory.getFunctionalTerm(clName, var1);
			Term[] body = new Term[1];
			body[0] = clTerm;
			Clause clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
//			System.out.println("\n" + clause + "\t" + ev.getSQLWRTMappings(clause));
			int ans = 0;
			if (path.contains("npd"))
				ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLWRTMappings(clause));
			else
				ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLavenet(clause));
			if (ans == 0) {
//				entitiesWithNoAnswersMap.put(clName, ans);
//				conceptTermsWithNoAnswers.add(clTerm);
				clausesWithNoAnswers.add(clause);
			}
			else
				conceptTermsWithAnswers.add(clTerm);
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
			String propName = prop.getIRI().getFragment();
//			if (propName==null)
//				propName=prop.getIRI().toString().substring(prop.getIRI().toString().lastIndexOf("/")+1);
//			String propName = prop.getIRI().toString();
			Term propTerm = m_termFactory.getFunctionalTerm(propName, var1, var2);
			Term propTermInv = m_termFactory.getFunctionalTerm(propName, var2, var1);
			Term[] body = new Term[1];
			body[0] = propTerm;
			Clause clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
			int ans = 0;
			if (path.contains("npd"))
				ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLWRTMappings(clause));
			else
				ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLavenet(clause));
//			int ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLFromAtom(propTerm));
			if (ans == 0) {
				clausesWithNoAnswers.add(clause);
				//Create the clause with the inverse role and add it also to the list
				body = new Term[1];
				body[0] = propTermInv;
				clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
				clausesWithNoAnswers.add(clause);
			}
			else {
				roleTermsWithAnswers.add(propTerm);
//				roleTermsWithAnswers.add(propTermInv);
			}
		}
		System.out.println("\nClauses With Answers " + roleTermsWithAnswers.size());
		System.out.println("ClausesWithNoAnswersSize = " + clausesWithNoAnswers.size() + " in " + (System.currentTimeMillis() - startProperties) + "ms");


		/**
		 * Evaluate COMBINATIONS of CONCEPTS
		 * For example for concepts C, D create clause
		 *
		 * Q(x) <- C(x), D(x)
		 *
		 * and identify if there exist answers
		 */
		allWork=conceptTermsWithAnswers.size()*conceptTermsWithAnswers.size();
		System.out.println("\n\nEvaluating combination of concepts");
		System.out.println("Possible combinations: " + allWork);
		long startConceptCombination = System.currentTimeMillis();
		processed=0;
		progress=0;
		lastprogress=0;
		Set<String> processedTerms = new HashSet<String>();
		for (Term conceptTerm1 : conceptTermsWithAnswers ) {
			for (Term conceptTerm2 : conceptTermsWithAnswers ) {
				if ( processedTerms.contains(conceptTerm2.toString()))
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
					Term[] body = new Term[2];
					body[0] = conceptTerm1;
					body[1] = conceptTerm2;
					Clause clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
					int ans = 0;
					if (path.contains("npd"))
						ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLWRTMappings(clause));
					else
						ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLavenet(clause));
//					int ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLConceptConceptgstoil(clause));

					if (ans == 0) {
						clausesWithNoAnswers.add(clause);
//						System.out.println("clause = " + clause + "\t\t" + ans);
					}
					else
						clausesWithAnswers++;
				}
			}
			processedTerms.add(conceptTerm1.toString());
		}
		System.out.println("\nCombinations of concepts with answers " + clausesWithAnswers);
		System.out.println("ClausesWithNoAnswersSize = " + clausesWithNoAnswers.size() + " in " + (System.currentTimeMillis() - startConceptCombination) + "ms");

		clausesWithAnswers=0;

//		System.exit(0);

		/**
		 * Evaluate COMBINATIONS of CONCEPTS and ROLES
		 * For example for concept C and role R create clause
		 *
		 * Q(x) <- C(x), R(x,y) and Q(x) <- C(x), R(y,x)
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
				Term[] body = new Term[2];
				body[0] = conceptTerm;
				body[1] = roleTerm;
				Clause clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
				int ans = 0;
				if (path.contains("npd"))
					ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLWRTMappings(clause));
				else
					ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLavenet(clause));
				if (ans == 0) {
					clausesWithNoAnswers.add(clause);
//					System.out.println("clause = " + clause + "\t\t" + ans);
				}
				else
					clausesWithAnswers++;

				body[0] = conceptTerm;
				body[1] = m_termFactory.getFunctionalTerm(roleTerm.toString().replace("(X0,X1)", ""), var2, var1);
				clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
				ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLavenet(clause));
				if (ans == 0) {
					clausesWithNoAnswers.add(clause);
//					System.out.println("clause = " + clause + "\t\t" + ans);
				}
				else {
					clausesWithAnswers++;
//					System.out.print("|");
				}
			}
		}
		System.out.println("\nCombinations of concepts with roles with answers " + clausesWithAnswers);
		System.out.println("ClausesWithNoAnswersSize = " + clausesWithNoAnswers.size() + " in " + (System.currentTimeMillis() - startConceptRoleCombination) + "ms");

		clausesWithAnswers = 0;

//		/**
//		 * Evaluate COMBINATIONS of ROLES
//		 * For example for roles S and role R create clause
//		 *
//		 * Q(x) <- S(x,y), R(x,y) and Q(x) <- S(x,y), R(y,x)
//		 *
//		 * and identify if there exist answers
//		 */
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
					int ans = 0;
					if (path.contains("npd"))
						ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLWRTMappings(clause));
					else
						ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLavenet(clause));
//					int ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLgstoil(clause));
//					System.out.println(clause + "\t\t" + ans);
					if (ans == 0) {
						clausesWithNoAnswers.add(clause);
//						System.out.println("clause = " + clause + "\t\t" + ans);
					}
					else
						clausesWithAnswers++;

					/**
					 * R(x,y), P(y,x)
					 */
					body[0] = roleTerm1;
					body[1] = m_termFactory.getFunctionalTerm(roleTerm2.toString().replace("(X0,X1)", ""), var2, var1);
					clause = new Clause(body, m_termFactory.getFunctionalTerm("Q", var1));
					ans = ev.evaluateSQLandReturnNumOfAnws(null, ev.getSQLavenet(clause));
					if (ans == 0) {
						clausesWithNoAnswers.add(clause);
//						System.out.println("clause = " + clause + "\t\t" + ans);
					}
					else {
						clausesWithAnswers++;
//						System.out.print("|");
					}

				}
			}
			checkedRoles.add(roleTerm1.toString());
		}
		System.out.println("\nCombinations of roles with answers " + clausesWithAnswers);
		System.out.println("ClausesWithNoAnswersSize = " + clausesWithNoAnswers.size() + " in " + (System.currentTimeMillis() - startRoleRoleCombination) + "ms");


		System.out.println("\n\nOverall Time = " + (System.currentTimeMillis() - start));
		/**
		 * Store this info in a file
		 */
		printResultToFile(optimizationPath, clausesWithNoAnswers);
		ev.closeConn();
	}

	private static void printResultToFile(String outputFilestr, Set<Clause> clauses) throws Exception{
		File outputFile = new File(outputFilestr);
        FileWriter out = new FileWriter(outputFile);

        for(Clause c: clauses){
			out.write( c.m_canonicalRepresentation + "\n");
		}

        out.close();
	}

}
