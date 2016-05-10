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

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.oxford.comlab.perfectref.rewriter.Clause;
import org.oxford.comlab.perfectref.rewriter.Term;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAnnotationAssertionAxiom;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLDataPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;


public class OptimizeRew {

	OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
	OWLDataFactory factory = manager.getOWLDataFactory();

	Set<String> classesWithInstances = new HashSet<String>();
	Set<String> propsWithInstances = new HashSet<String>();
	Set<String> classesNOInstances = new HashSet<String>();
	Set<String> propsNOInstances = new HashSet<String>();
	Set<String> atomsWithNOAnswers = new HashSet<String>();


	public OptimizeRew() {
		classesWithInstances = new HashSet<String>();
		propsWithInstances = new HashSet<String>();
		classesNOInstances = new HashSet<String>();
		propsNOInstances = new HashSet<String>();
	}

	public Set<String> getClassesWithInstances() {
		return classesWithInstances;
	}

	public Set<String> getPropsWithInstances() {
		return propsWithInstances;
	}

	public Set<String> getClassesNOInstances() {
		return classesNOInstances;
	}

	public Set<String> getPropsNOInstances() {
		return propsNOInstances;
	}


	/**
	 * In this function we check that each conjunct of a query can give results. If a query contains at least one conjunct
	 * that does not have any assertions related to it then the query is removed.
	 *
	 * @param rewriting
	 * @param dataOnto
	 * @return
	 */
	public ArrayList<Clause> getQueriesWithAnswers ( ArrayList<Clause> rewriting, OWLOntology dataOnto) {

		ArrayList<Clause> queriesWithAnswers = new ArrayList<Clause>();
		for ( Clause cl : rewriting )
			if ( cl.getHead().getName().toString().equals("Q"))
				queriesWithAnswers.add(cl);

//		queriesWithAnswers.addAll(rewriting);

		Set<Clause> queriesWithNOAnswers = new HashSet<Clause>();

		//get concepts and roles that have assertions
		for ( OWLAxiom axiom : dataOnto.getAxioms() ) {
			if ( axiom instanceof OWLClassAssertionAxiom ) {
				OWLClassAssertionAxiom clAsAx = (OWLClassAssertionAxiom) axiom;
				classesWithInstances.add( clAsAx.getClassesInSignature().iterator().next().getIRI().getFragment().toString() );
//				System.out.println(clAsAx.getClassesInSignature().iterator().next().getIRI().getFragment().toString());
			}
			else if ( axiom instanceof OWLAnnotationAssertionAxiom ) {
				propsWithInstances.add(((OWLAnnotationAssertionAxiom) axiom).getProperty().getIRI().getFragment());
//				System.out.println(((OWLAnnotationAssertionAxiom) axiom).getProperty().getIRI().getFragment());
			}
			else if ( axiom instanceof OWLObjectPropertyAssertionAxiom ) {
				propsWithInstances.add( ((OWLObjectPropertyAssertionAxiom) axiom).getObjectPropertiesInSignature().iterator().next().getIRI().getFragment() );
			}
			else if ( axiom instanceof OWLDataPropertyAssertionAxiom ) {
				propsWithInstances.add( ((OWLDataPropertyAssertionAxiom) axiom).getDataPropertiesInSignature().iterator().next().getIRI().getFragment());
			}
		}

		//identify clauses with predicates that not all body atoms have instances
		for ( Clause clause : rewriting ) {
//			System.out.println( "REW CLAUSE = " + clause );
			Term[] body = clause.getBody();
			for ( Term bodyAtom : body ) {
//				System.out.println( "Atom = " + bodyAtomPredicate );
				if ( bodyAtom.getArity() == 1 ) {
					if ( !classesWithInstances.contains(bodyAtom.getName()) )
					{
						queriesWithNOAnswers.add(clause);
						classesNOInstances.add(bodyAtom.getName());
					}
				}
				else {
					if ( !propsWithInstances.contains(bodyAtom.getName()) ) {
						queriesWithNOAnswers.add(clause);
						propsNOInstances.add(bodyAtom.getName());
					}
				}
			}

		}

		for ( Clause cl: queriesWithNOAnswers )
			queriesWithAnswers.remove(cl);

		return queriesWithAnswers;
	}


	/**
	 * In this function we check that each conjunct of a query can give results. If a query contains at least one conjunct
	 * that does not have any assertions related to it then the query is removed.
	 *
	 * @param rewriting
	 * @param dataOnto
	 * @return
	 * @throws OWLOntologyCreationException
	 */
	public ArrayList<Clause> getQueriesWithAnswersFromMultipleOntos( ArrayList<Clause> rewriting, String dataOntoPath) throws OWLOntologyCreationException {

		ArrayList<Clause> queriesWithAnswers = new ArrayList<Clause>();
		queriesWithAnswers.addAll(rewriting);

		for ( int i = 1 ; i <= 4 ; i++) {
			String ontologyPath = dataOntoPath+ "sample_10" + i + ".rdf";
			System.out.println(ontologyPath);
			IRI iri = IRI.create(ontologyPath);
			OWLOntology dataOnto = manager.loadOntology(iri);

			Set<Clause> queriesWithNOAnswers = new HashSet<Clause>();

			//get concepts and roles that have assertions
			for ( OWLAxiom axiom : dataOnto.getAxioms() ) {
				if ( axiom instanceof OWLClassAssertionAxiom ) {
					OWLClassAssertionAxiom clAsAx = (OWLClassAssertionAxiom) axiom;
					classesWithInstances.add( clAsAx.getClassesInSignature().iterator().next().getIRI().getFragment().toString() );
	//				System.out.println(clAsAx.getClassesInSignature().iterator().next().getIRI().getFragment().toString());
				}
				if ( axiom instanceof OWLAnnotationAssertionAxiom ) {
					propsWithInstances.add(((OWLAnnotationAssertionAxiom) axiom).getProperty().getIRI().getFragment());
	//				System.out.println(((OWLAnnotationAssertionAxiom) axiom).getProperty().getIRI().getFragment());
				}
			}

			//identify clauses with predicates that not all body atoms have instances
			for ( Clause clause : rewriting ) {
	//			System.out.println( "REW CLAUSE = " + clause );
				Term[] body = clause.getBody();
				for ( Term bodyAtom : body ) {
	//				System.out.println( "Atom = " + bodyAtomPredicate );
					if ( bodyAtom.getArity() == 1 ) {
						if ( !classesWithInstances.contains(bodyAtom.getName()) )
						{
							queriesWithNOAnswers.add(clause);
							classesNOInstances.add(bodyAtom.toString());
						}
					}
					else {
						if ( !propsWithInstances.contains(bodyAtom.getName()) ) {
							queriesWithNOAnswers.add(clause);
							propsNOInstances.add(bodyAtom.getName());
						}
					}
				}

			}

			for ( Clause cl: queriesWithNOAnswers )
				queriesWithAnswers.remove(cl);
		}

		return queriesWithAnswers;
	}

	/**
	 * Check that every atom of the query has answers
	 *
	 * @param rewritingOfQuery
	 * @return
	 * @throws SQLException 
	 */
	public ArrayList<Clause> getQueriesWithAnswersFromDBAtomCheck( ArrayList<Clause> rewritingOfQuery ) throws SQLException {

//		int maxSize = 0;
		ArrayList<Clause> rew = new ArrayList<Clause>();
		Evaluator ev = new Evaluator();

		for ( Clause clause :rewritingOfQuery ) {
			if ( clause.getHead().getName().equals("Q") ) {
				Term[] bodyAtoms = clause.getBody();
				boolean allAtomsHaveAnswers = true;
				for( Term bodyAtom: bodyAtoms ) {
					int numRows = 0;
					String sqlQuery = ev.getSQLFromAtom(bodyAtom);
					try {
						numRows = ev.evaluateSQLandReturnNumOfAnws(null,sqlQuery);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if ( numRows == 0 ) {
						allAtomsHaveAnswers = false;
						if (!atomsWithNOAnswers.contains(bodyAtom.getName()))
						{
							System.err.println("BodyAtom with no Answers = " + bodyAtom.getName());
							atomsWithNOAnswers.add(bodyAtom.getName().toString());
						}
						break;
					}
				}
				if ( allAtomsHaveAnswers ) {
					rew.add(clause);
//					if ( clause.getBody().size() > maxSize )
//						maxSize = clause.getBody().size();
				}
			}
		}

//		System.out.println("Maximum Query Size = " + maxSize);
		System.out.println("Queries where all atoms have answers = " + rew.size());

		return rew;
	}

	
	/**
	 * Evaluate clause and keep it if it has answers
	 * 
	 * @param rewritingOfQuery
	 * @return
	 * @throws SQLException 
	 */
	public ArrayList<Clause> getQueriesWithAnswersFromDB( ArrayList<Clause> rewritingOfQuery, String dbConn ) throws SQLException {

//		int maxSize = 0;
		ArrayList<Clause> rew = new ArrayList<Clause>();
		Evaluator ev = new Evaluator(dbConn);

		for ( Clause clause :rewritingOfQuery ) {
			if ( clause.getHead().getName().equals("Q") ) {
				String sqlQuery = ev.getSQLavenet(clause);
				int numRows = 0;
				try {
					numRows  = ev.evaluateSQLandReturnNumOfAnws(null,sqlQuery);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if ( numRows != 0 ) {
					rew.add(clause);
//					if ( clause.getBody().size() > maxSize )
//						maxSize = clause.getBody().size();
				}
			}
		}

//		System.out.println("Maximum Query Size = " + maxSize);
		System.out.println("Queries that have answers = " + rew.size());

		return rew;
	}

	/**
	 * This method will be used to reorder the query predicates so that the atom with the smallest size goes first and so on
	 * 
	 * @param initialRewriting
	 * @param dbConn
	 * @return
	 * @throws SQLException 
	 */
	public ArrayList<Clause> reorderQueriersInRewriting ( ArrayList<Clause> initialRewriting, String dbConn ) throws SQLException {
		ArrayList<Clause> result = new ArrayList<Clause>();

		Evaluator eval = new Evaluator(dbConn);

		for ( Clause cl : initialRewriting ) {
			for ( Term bodyTerm : cl.getBody() ) {
				//WRITE CODE
				String sqlQuery = eval.getSQLFromAtom(bodyTerm);
				int numRows = 0;
				try {
					numRows  = eval.evaluateSQLandReturnNumOfAnws(null,sqlQuery);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}

		return result;
	}
}
