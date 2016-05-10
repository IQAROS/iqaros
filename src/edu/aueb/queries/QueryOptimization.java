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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.oxford.comlab.perfectref.parser.DLliteParser;
import org.oxford.comlab.perfectref.rewriter.Clause;
import org.oxford.comlab.perfectref.rewriter.EntParInJoinsWNoAns;
import org.oxford.comlab.perfectref.rewriter.Substitution;
import org.oxford.comlab.perfectref.rewriter.Term;

public class QueryOptimization {

	private String JOIN_SEPERATOR = "-IQAROS-";
	
//	we will count only joins
//	private int atomsThatCauseClausesNotToHaveAnswers;
	private int joinsThatCauseClausesNotToHaveAnswers = 0;
	
	public void setJoinsThatCauseClausesNotToHaveAnswers (int n) {
		joinsThatCauseClausesNotToHaveAnswers = n;
	}

//	public int getAtomsThatCauseClausesNotToHaveAnswers() {
//		return atomsThatCauseClausesNotToHaveAnswers;
//	}

	public int getJoinsThatCauseClausesNotToHaveAnswers() {
		return joinsThatCauseClausesNotToHaveAnswers;
	}
	
//	Set<String> clausesCounted = new HashSet<String>();

	/**
	 * avenet - additions for optimizations regarding atoms with no answers in DB
	 */
	//set of terms that do not have any answers
	public Set<String> atomicEntitiesWithNoAnswers = new HashSet<String>();
	//set of terms that contains the body of the optimization queries (join queries that have no answer in the DB)
	public HashMap<String,Set<Term[]>> joinsWithNoAnswers = new HashMap<String,Set<Term[]>>();
	//list of all the entities (concepts) that appear first in the query of the optimization file
	public Set<String> joinEntityWithNoAnswersFirst = new HashSet<String>();
	//list of all the entities (concepts or roles) that appear second in the query of the optimization file
	public Set<String> joinEntityWithNoAnswersSecond = new HashSet<String>();
	//mapping of each term (body atom in the optimization file) to the queries (joins) that has no answer in the DB
	public HashMap<String,Set<Clause>> termsToClausesWithNoAnswers = new HashMap<String, Set<Clause>>();


	public QueryOptimization(String path) {
		//joins are stored as clauses
		
		System.out.println("Loading Emptiness Optimization STARTED");
		long start = System.currentTimeMillis();

		DLliteParser parser = new DLliteParser();
		ArrayList<Clause> clauses =  new ArrayList<Clause>();
		try {
			clauses = parser.getQueryArrayList(path);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Loaded clauses: " + clauses.size());
		for ( Clause cl : clauses )
		{
			Term[] body = cl.getBody();
			//body has length 1
			if (body.length == 1 ) {
				atomicEntitiesWithNoAnswers.add(body[0].getName());
			}
			else { //body has length 2
				//concept or role
				Term boyTerm0 = body[0];
				//concept or role
				Term bodyTerm1 = body[1];
				//create a unique identifier for each join that has no answers
				//16-10-2015
				//problem if case R(x,y)^S(x,y) and R(x,y)^S(y,x) both have no answers
				addTojoinsWithNoAnswers(boyTerm0.getName()+JOIN_SEPERATOR+bodyTerm1.getName(),body);
//				joinsWithNoAnswers.put(boyTerm0.getName()+JOIN_SEPERATOR+bodyTerm1.getName(), body);
				//add to a list that the specific body element participates in a join in clause cl
				addToTermsToClausesWithNoAnswers(body[0].getName(), cl);
				//it appears as the first element
				joinEntityWithNoAnswersFirst.add(body[0].getName());
				addToTermsToClausesWithNoAnswers(body[1].getName(), cl);
				joinEntityWithNoAnswersSecond.add(body[1].getName());
			}
		}
		//we will count only joins
//		atomsThatCauseClausesNotToHaveAnswers = 0;
		joinsThatCauseClausesNotToHaveAnswers = 0;
//		clausesCounted = new HashSet<String>();
		System.out.println("Loading Emptiness Optimization ENDED in " + (System.currentTimeMillis() - start) + "ms");
	} 


	private void addTojoinsWithNoAnswers(String string, Term[] body) {
		Set<Term[]> joinTerm = joinsWithNoAnswers.get(string);
		if (joinTerm == null) {
			joinTerm = new HashSet<Term[]>();
		}
		joinTerm.add(body);
		joinsWithNoAnswers.put(string, joinTerm);
		
	}


	private void addToTermsToClausesWithNoAnswers(String term, Clause cl) {
		Set<Clause> setOfClauses = termsToClausesWithNoAnswers.get(term);
		if (setOfClauses == null)
			setOfClauses = new HashSet<Clause>();
		setOfClauses.add(cl);
		termsToClausesWithNoAnswers.put(term, setOfClauses);
	}

	/**
	 * Store information about the optimization queries
	 * 1. In case a query with a single atom in the body (meaning atom that does not have instances in the DB) then this query
	 * should not be evaluated (or the queries produced after it -- why is that last part???)
	 * 2. In case the query contains an atom that participates in a join with no answers then we store the following info
	 * 		i. which atom of the join is present
	 * 		ii. what is the index of this atom in the join (first atom -meaning concept- or second -meaning concept or role)
	 */
	public void markNewlyCreatedClause( Clause clause, Term extraAtom  ) {
		//use the information of the optimization about concepts and roles with empty answers
		if ( atomicEntitiesWithNoAnswers.contains(extraAtom.getName())) {
			clause.setToBeEvaluated(false);
//			if ( !clausesCounted.contains(clause.m_canonicalRepresentation) ) {
//				atomsThatCauseClausesNotToHaveAnswers++;
//				clausesCounted.add(clause.m_canonicalRepresentation);
//			}
			return;
		}
		else if ( joinEntityWithNoAnswersFirst.contains(extraAtom.getName()) ) {
			EntParInJoinsWNoAns e = new EntParInJoinsWNoAns();
			e.setEntityParticipatingInJoinWithNoAnswers(extraAtom);
			e.setEntParInJoinWNoAnswsIndInOptQuery(0);
			clause.addEntitiesParticipatingInJoinsWithNoAnswers(e);
		}
		else if ( joinEntityWithNoAnswersSecond.contains(extraAtom.getName()) ) {
			EntParInJoinsWNoAns e = new EntParInJoinsWNoAns();
			e.setEntityParticipatingInJoinWithNoAnswers(extraAtom);
			e.setEntParInJoinWNoAnswsIndInOptQuery(1);
			clause.addEntitiesParticipatingInJoinsWithNoAnswers(e);
		}
	}

	public void markClauseWRTAnswers(Clause clauseFromH1, Clause clauseFromH2, Clause clause) {

		/**
		 * If one of the two participating clauses does not have any answer then no
		 * answer will exist also for the created clause
		 */
		if ( !clauseFromH1.getToBeEvaluated() || !clauseFromH2.getToBeEvaluated() ) {
			clause.setToBeEvaluated(false);
			joinsThatCauseClausesNotToHaveAnswers++;

			return;
		}

		/**
		 * Otherwise we have to check wrt the entities participating in joins and
		 * 1. find that a clause has no answer, or
		 * 2. mark the created clause
		 */
		//each set contains all the entities of the specific clause that participate in a join with no answers
		Set<EntParInJoinsWNoAns> clause1EntitiesPar = clauseFromH1.getEntitiesParticipatingInJoinsWithNoAnswers();
		Set<EntParInJoinsWNoAns> clause2EntitiesPar = clauseFromH2.getEntitiesParticipatingInJoinsWithNoAnswers();

		//no mark for the newly created clause
		if ( clause1EntitiesPar.isEmpty() && clause2EntitiesPar.isEmpty() )
		{
			return;
		}
		//clause has an entity that participates in a join with no answers
		else if ( clause1EntitiesPar.isEmpty() && !clause2EntitiesPar.isEmpty() )
		{
			clause.setEntitiesParticipatingInJoinsWithNoAnswers(clause2EntitiesPar);
		}
		//clause has an entity that participates in a join with no answers
		else if ( !clause1EntitiesPar.isEmpty() && clause2EntitiesPar.isEmpty() )
		{
			clause.setEntitiesParticipatingInJoinsWithNoAnswers(clause1EntitiesPar);
		}
		//we have to check if the entities from clause1 participate with any of the entities of clause2 in a join with no answers
		else if ( !clause1EntitiesPar.isEmpty() && !clause2EntitiesPar.isEmpty() )
		{
			for (EntParInJoinsWNoAns clause2Entity: clause2EntitiesPar)
			{
				//clause2Term is a term of clauseFromH2
				Term clause2Term = clause2Entity.getEntityParticipatingInJoinWithNoAnswers();
				for ( EntParInJoinsWNoAns clause1Entity: clause1EntitiesPar )
				{
					boolean clauseFoundNotToHaveAns = false;
					//clause1Term is a term of clauseFromH1
					Term clause1Term = clause1Entity.getEntityParticipatingInJoinWithNoAnswers();

					//joinTerms is the set of clauses that contain the terms that do not have answers (read from Opt file)
					Set<Term[]> joinTerms = joinsWithNoAnswers.get(clause1Term.getName()+JOIN_SEPERATOR+clause2Term.getName());
					if ( joinTerms == null )
						joinTerms = joinsWithNoAnswers.get(clause2Term.getName()+JOIN_SEPERATOR+clause1Term.getName());
					if (joinTerms == null ) {
						continue;
					}
					
//					Term[] joinTerm = joinsWithNoAnswers.get(clause1Term.getName()+JOIN_SEPERATOR+clause2Term.getName());
//					if ( joinTerm == null )
//						joinTerm = joinsWithNoAnswers.get(clause2Term.getName()+JOIN_SEPERATOR+clause1Term.getName());
//					if (joinTerm == null ) {
//						continue;
//					}
					
					/**
					 * Could also be performed with subsumption checks
					 */
					for ( Term[] joinTerm: joinTerms )
					{	
						
						Term joinTerm0, joinTerm1;
						
						//now we have to find if these terms are the same (up to variable renaming) in the body of newClause
						if ( ( joinTerm[0].getName().equals(clause1Term.getName()) ) && ( joinTerm[1].getName().equals(clause2Term.getName() ) ) ) 
						{
							joinTerm0 = joinTerm[0];
							joinTerm1 = joinTerm[1];
						}
						else if ( ( joinTerm[1].getName().equals(clause1Term.getName()) ) && ( joinTerm[0].getName().equals(clause2Term.getName() ) ) )
						{
							joinTerm0 = joinTerm[1];
							joinTerm1 = joinTerm[0];
						}
						else {
							clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause1Entity);
							clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause2Entity);
							continue;
						}
						//since we have mapped the names of the terms now we have to find if there exist proper mappings of their variables
						{
							//A(x)
							if ( joinTerm0.getArity() == 1 )
							{
								//x
								Term joinTerm0VarorConst= joinTerm0.getArguments()[0];
								//B(x)
								if (joinTerm1.getArity() == 1)
								{
									if (clause1Term.getArguments()[0].equals(clause2Term.getArguments()[0]))
									{
										clause.setToBeEvaluated(false);
										clauseFoundNotToHaveAns = true;
										joinsThatCauseClausesNotToHaveAnswers++;
//										System.out.println("0: CL with no answers = " + clause);
									}
								}
								//R(x,_) or R(_,x)
								else
								{
									//R(x,_)
									if ( joinTerm0VarorConst.equals(joinTerm1.getArgument(0) ) ) 
									{
										if ( clause1Term.getArguments()[0].equals(clause2Term.getArguments()[0]) ) 
										{
											clause.setToBeEvaluated(false);
											clauseFoundNotToHaveAns = true;
											joinsThatCauseClausesNotToHaveAnswers++;
//											System.out.println("1: CL with no answers = " + clause);
										}
									}
									//R(_,x)
									else if ( joinTerm0VarorConst.equals(joinTerm1.getArgument(1)) ) 
										if ( clause1Term.getArguments()[0].equals(clause2Term.getArguments()[1]) ) 
										{
											clause.setToBeEvaluated(false);
											clauseFoundNotToHaveAns = true;
											joinsThatCauseClausesNotToHaveAnswers++;
//											System.out.println("2: CL with no answers = " + clause);
										}
								}
							}
							//R(x,y)
							else if (joinTerm0.getArity() == 2 )
							{
								//x
								Term joinTerm0VarorConst0 = joinTerm0.getArguments()[0];
								//y
								Term joinTerm0VarorConst1 = joinTerm0.getArguments()[1];
	//							A(x) or A(y)
								if (joinTerm1.getArity() == 1)
								{
									if ( joinTerm0VarorConst0.equals(joinTerm1.getArgument(0) ) &&
											clause1Term.getArgument(0).equals(clause2Term.getArgument(0) ) )
									{
										clause.setToBeEvaluated(false);
										clauseFoundNotToHaveAns = true;
										joinsThatCauseClausesNotToHaveAnswers++;
//										System.out.println("3: CL with no answers = " + clause);
									}
									else if ( joinTerm0VarorConst1.equals( joinTerm1.getArgument(0) ) && 
											clause1Term.getArgument(1).equals( clause2Term.getArgument(0) ) )
									{
										clause.setToBeEvaluated(false);
										clauseFoundNotToHaveAns = true;
										joinsThatCauseClausesNotToHaveAnswers++;
//										System.out.println("4: CL with no answers = " + clause);
									}
								}
								//S(x,y), S(y,x), S(x,_), S(_,y)
								else
								{
									//S(x,y) or S(x,_)
									if ( joinTerm0VarorConst0.equals( joinTerm1.getArgument(0) ) ) // && joinTerm0VarorConst1.equals(joinTerm1.getArgument(1) ) )
									{
										//S(x,y)
										if ( joinTerm0VarorConst1.equals(joinTerm1.getArgument(1) ) )
										{
											if ( clause1Term.getArgument(0).equals(clause2Term.getArgument(0) ) && clause1Term.getArgument(1).equals(clause2Term.getArgument(1) ) ) 
											{
												clause.setToBeEvaluated(false);
												clauseFoundNotToHaveAns = true;
												joinsThatCauseClausesNotToHaveAnswers++;
//												System.out.println("5: CL with no answers = " + clause);
											}
										}
	//									S(x,_)
										else {
											//avenet 14/4/2016: after seeing the uniprot results the algorithm does not identify R(x,y)S(x,constant)
											if ( clause1Term.getArgument(0).equals(clause2Term.getArgument(0) ) || clause2Term.getArgument(0).isConstant() ) 
//											if ( clause1Term.getArgument(0).equals(clause2Term.getArgument(0) ) ) 
											{
												clause.setToBeEvaluated(false);
												clauseFoundNotToHaveAns = true;
												joinsThatCauseClausesNotToHaveAnswers++;
//												System.out.println("6: CL with no answers = " + clause);
											}
										}
									}
									//S(y,x) or S(y,_)
									else if ( joinTerm0VarorConst0.equals( joinTerm1.getArgument(1) ) ) //&& joinTerm0VarorConst1.equals(joinTerm1.getArgument(0) ) )
									{
										//S(y,x)
										if ( joinTerm0VarorConst1.equals(joinTerm1.getArgument(0) ) )
										{
											if ( clause1Term.getArgument(0).equals(clause2Term.getArgument(1) ) && clause1Term.getArgument(1).equals(clause2Term.getArgument(0) ) ) 
											{
												clause.setToBeEvaluated(false);
												clauseFoundNotToHaveAns = true;
												joinsThatCauseClausesNotToHaveAnswers++;
//												System.out.println("7: CL with no answers = " + clause);
											}
										}
										//S(y,_)
										else 
										{
											//avenet 14/4/2016: after seeing the uniprot results the algorithm does not identify R(x,y)S(x,constant)
											if ( clause1Term.getArgument(0).equals(clause2Term.getArgument(1) ) || clause2Term.getArgument(1).isConstant() )
//											if ( clause1Term.getArgument(0).equals(clause2Term.getArgument(1) ) ) 
											{
												clause.setToBeEvaluated(false);
												clauseFoundNotToHaveAns = true;
												joinsThatCauseClausesNotToHaveAnswers++;
//												System.out.println("8: CL with no answers = " + clause);
											}
										}
									}
								}
							}
						}
						if (!clauseFoundNotToHaveAns)
						{
							clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause1Entity);
							clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause2Entity);
						}
					}
				}
			}
		}
	}



			//for every entity of Cl2 that participates in a join with no answers
//			for ( EntParInJoinsWNoAns clause2Entity : clause2EntitiesPar )
//			{
//				//term
//				Term clause2Term = clause2Entity.getEntityParticipatingInJoinWithNoAnswers();
//				//position in join (1: concept, 2: concept or role)
//				int clause2TermIndex = clause2Entity.getEntParInJoinWNoAnswsIndInOptQuery();
//				//joins (stored as clauses) with no answers that this term participates
//				Set<Clause> possibleCl2Participants = termsToClausesWithNoAnswers.get(clause2Term.getFunctionalPrefix());
//				for ( Clause possClause: possibleCl2Participants ) {
//					Term bodyTermToBeFound = null;
//					//ean einai to 2o atomo sto query tote psaxnoume to 1o sto clause1
//					if ( clause2TermIndex == 1 )
//						bodyTermToBeFound = possClause.getBody()[0];
//					//ean einai to 1o atomo sto query tote psaxnoume to 2o sto clause1
//					else if ( clause2TermIndex == 0 )
//						bodyTermToBeFound = possClause.getBody()[1];
//					if (bodyTermToBeFound == null)
//						System.err.println("bodyTermToBeFound is null");
//
//					//is the term to be found a concept or role
//					int bodyTermToBeFoundLength = bodyTermToBeFound.getArguments().length;
//
//					for ( EntParInJoinsWNoAns clause1Entity : clause1EntitiesPar )
//					{
//						//
//						Term clause1Term = clause2Entity.getEntityParticipatingInJoinWithNoAnswers();
//						int clause1TermIndex = clause2Entity.getEntParInJoinWNoAnswsIndInOptQuery();
//
//						//term indexes must be different. 1 for one and 2 for the other and names must be the same
//						if ( clause1TermIndex != clause2TermIndex && clause1Term.getFunctionalPrefix().equals(bodyTermToBeFound.getFunctionalPrefix())) {
//							//all we have to do now is find if there is a correct variable mapping
//							//It is a concept - Hence both are concepts so the clause has no answers
//							/**
//							 * Here we might have to check vars
//							 */
//							if ( bodyTermToBeFoundLength == 1 ) {//??do we need this extra? ??  && ( bodyTermToBeFound.getArguments().equals( clause2Term.getArguments() ) ) ) {
//								clause.setToBeEvaluated(false);
//							}
//							//it is a role and we have to see how the variables mix and match
//							else if (bodyTermToBeFoundLength == 2 ) {
////								clause1Term - bodyTermToBeFound - possClause
//								//concept + role in the newly created clause
//								if ( clause1TermIndex == 0 && clause2TermIndex == 1 || clause1TermIndex == 0 && clause2TermIndex == 1 ) {
//									Term[] possClauseBody = possClause.getBody();
//									//concept
//									Term conceptInPossCl = possClauseBody[0];
//									Term term0VarconceptInPossCl = conceptInPossCl.getVariableOrConstant();
//									//termToBeFound =  role
//									Term possClauseBodyTerm1 = possClauseBody[1];
//									Term term1Var0 = possClauseBodyTerm1.getArgument(0);
//									Term term1Var1 = possClauseBodyTerm1.getArgument(1);
//									//possible clause contains A(x), R(x,_)
//									if ( term0VarconceptInPossCl.toString().equals(term1Var0.toString() ) ) {
//										//we have to check that body of clause has the same pattern
//										Term t1 = null, t2 = null;
//										//find the concept of the opt clause in the query
//										for ( Term t: clause.getBody() )
//											if ( t.getFunctionalPrefix().equals(conceptInPossCl.getFunctionalPrefix()) ) {
//												t1 = t;
//												break;
//											}
//										if ( t1 != null) {
//											//find the role of the opt clause in the query
//											for ( Term t: clause.getBody() )
//												if ( t.getFunctionalPrefix().equals(possClauseBodyTerm1) ) {
//													t2 = t;
//													break;
//												}
//										}
//										if ( t1!=null && t2!=null && t1.getArgument(0).equals(t2.getArgument(0) ) )
//										{
//											clause.setToBeEvaluated(false);
//										}
//										else {
//											clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause1Entity);
//											clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause2Entity);
//										}
//									}
//									//possible clause contains A(x),R(_,x)
//									else if ( term0VarconceptInPossCl.toString().equals( term1Var1.toString() ) ) {
//										//we have to check that body of clause has the same pattern
//										Term t1 = null, t2 = null;
//										//find the concept of the opt clause in the query
//										for ( Term t: clause.getBody() )
//											if ( t.getFunctionalPrefix().equals(conceptInPossCl.getFunctionalPrefix()) ) {
//												t1 = t;
//												break;
//											}
//										if ( t1 != null) {
//											//find the role of the opt clause in the query
//											for ( Term t: clause.getBody() )
//												if ( t.getFunctionalPrefix().equals(possClauseBodyTerm1) ) {
//													t2 = t;
//													break;
//												}
//										}
//										if ( t1 != null && t2 != null && t1.getArgument(0).equals(t2.getArgument(1) ) )
//										{
//											clause.setToBeEvaluated(false);
//										}
//										else {
//											clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause1Entity);
//											clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause2Entity);
//										}
//									}
//								}
//							}
//						}
//						else {
//							clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause1Entity);
//							clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause2Entity);
//						}
//					}
//				}
//			}
//		}
//	}

	/**
	 * Used in extension of the last atom where no info has been given during the creation of the (original) final atom rewriting
	 * @param currentClause
	 * @param newClause
	 * @param extraAtom 
	 */
	public void markNewlyCreatedClauseByExtension(Clause initialClauseFromTree, Term extraAtom, Clause newClause) {

		//if the newAtom that has been added does not have any answers then the new clause cannot be evaluated
		if ( atomicEntitiesWithNoAnswers.contains(extraAtom.getName())) {
			newClause.setToBeEvaluated(false);
//			atomsThatCauseClausesNotToHaveAnswers++;
			joinsThatCauseClausesNotToHaveAnswers++;
			return;
		}
		//set of EntParInJoinsWNoAns that are present in the clause and participate in joins with no answers
		Set<EntParInJoinsWNoAns> entsParInInClause = initialClauseFromTree.getEntitiesParticipatingInJoinsWithNoAnswers();
		/*
		 * if the initialClause does not contain any atom of the join
		 * that has zero answers then the created query should be evaluated
		 */
		if ( entsParInInClause.isEmpty() )
			return;
		/*
		 * if the initialClause has atoms that participate in joins with no answers
		 * we have to check if extraAtom belongs to these joins
		 */
		else
		{
			//we first have to see if extraAtom participates in any of the joins
			if ( joinEntityWithNoAnswersFirst.contains( extraAtom.getName() ) || joinEntityWithNoAnswersSecond.contains( extraAtom.getName() ) ) {
				//we now have to check every EntParInJoinsWNoAns of the initialClause to find the terms that are the joins with no answers
				for ( EntParInJoinsWNoAns e: entsParInInClause ) {
					//term that participates in join with no answers -- the other term is the extraAtom
					Term clauseTerm = e.getEntityParticipatingInJoinWithNoAnswers();
//					Term[] joinTerm = joinsWithNoAnswers.get(clauseTerm.getName()+JOIN_SEPERATOR+extraAtom.getName());
//					if ( joinTerm == null )
//						joinTerm = joinsWithNoAnswers.get(extraAtom.getName()+JOIN_SEPERATOR+clauseTerm.getName());
//					if (joinTerm == null ) {
//						continue;
//					}
					//search if clauseTerm+extraAtom exists in joinsWithNoAnswers
					Set<Term[]> joinTerms = joinsWithNoAnswers.get(clauseTerm.getName()+JOIN_SEPERATOR+extraAtom.getName());
					if ( joinTerms == null )
						joinTerms = joinsWithNoAnswers.get(extraAtom.getName()+JOIN_SEPERATOR+clauseTerm.getName());
					if (joinTerms == null ) {
						continue;
					}
					else {
					
						/**
						 * Could also be performed with subsumption checks
						 */
						for ( Term[] joinTerm: joinTerms )
						{	
							
							Term joinTerm0, joinTerm1;
							
							//now we have to find if these terms are the same (up to variable renaming) in the body of newClause
							if ( ( joinTerm[0].getName().equals(clauseTerm.getName()) ) && ( joinTerm[1].getName().equals(extraAtom.getName() ) ) ) 
							{
								joinTerm0 = joinTerm[0];
								joinTerm1 = joinTerm[1];
							}
							else if ( ( joinTerm[1].getName().equals(clauseTerm.getName()) ) && ( joinTerm[0].getName().equals(extraAtom.getName() ) ) )
							{
								joinTerm0 = joinTerm[1];
								joinTerm1 = joinTerm[0];
							}
							else {
//								clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause1Entity);
//								clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause2Entity);
								continue;
							}
							//since we have mapped the names of the terms now we have to find if there exist proper mappings of their variables
							{
								//A(x)
								if ( joinTerm0.getArity() == 1 )
								{
									//x
									Term joinTerm0VarorConst= joinTerm0.getArguments()[0];
									//B(x)
									if (joinTerm1.getArity() == 1)
									{
										if (clauseTerm.getArguments()[0].equals(extraAtom.getArguments()[0]))
										{
											newClause.setToBeEvaluated(false);
//											clauseFoundNotToHaveAns = true;
											joinsThatCauseClausesNotToHaveAnswers++;
//											System.out.println("0: CL with no answers = " + newClause);
										}
									}
									//R(x,_) or R(_,x)
									else
									{
										//R(x,_)
										if ( joinTerm0VarorConst.equals(joinTerm1.getArgument(0) ) ) 
										{
											if ( clauseTerm.getArguments()[0].equals(extraAtom.getArguments()[0]) ) 
											{
												newClause.setToBeEvaluated(false);
//												clauseFoundNotToHaveAns = true;
												joinsThatCauseClausesNotToHaveAnswers++;
//												System.out.println("1: CL with no answers = " + newClause);
											}
										}
										//R(_,x)
										else if ( joinTerm0VarorConst.equals(joinTerm1.getArgument(1)) ) 
											if ( clauseTerm.getArguments()[0].equals(extraAtom.getArguments()[1]) ) 
											{
												newClause.setToBeEvaluated(false);
//												clauseFoundNotToHaveAns = true;
												joinsThatCauseClausesNotToHaveAnswers++;
//												System.out.println("2: CL with no answers = " + newClause);
											}
									}
								}
								//R(x,y)
								else if (joinTerm0.getArity() == 2 )
								{
									//x
									Term joinTerm0VarorConst0 = joinTerm0.getArguments()[0];
									//y
									Term joinTerm0VarorConst1 = joinTerm0.getArguments()[1];
		//							A(x) or A(y)
									if (joinTerm1.getArity() == 1)
									{
										if ( joinTerm0VarorConst0.equals(joinTerm1.getArgument(0) ) &&
												clauseTerm.getArgument(0).equals(extraAtom.getArgument(0) ) )
										{
											newClause.setToBeEvaluated(false);
//											clauseFoundNotToHaveAns = true;
											joinsThatCauseClausesNotToHaveAnswers++;
//											System.out.println("3: CL with no answers = " + newClause);
										}
										else if ( joinTerm0VarorConst1.equals( joinTerm1.getArgument(0) ) && 
												clauseTerm.getArgument(1).equals( extraAtom.getArgument(0) ) )
										{
											newClause.setToBeEvaluated(false);
//											clauseFoundNotToHaveAns = true;
											joinsThatCauseClausesNotToHaveAnswers++;
//											System.out.println("4: CL with no answers = " + newClause);
										}
									}
									//S(x,y), S(y,x), S(x,_), S(_,y)
									else
									{
										//S(x,y) or S(x,_)
										if ( joinTerm0VarorConst0.equals( joinTerm1.getArgument(0) ) ) // && joinTerm0VarorConst1.equals(joinTerm1.getArgument(1) ) )
										{
											//S(x,y)
											if ( joinTerm0VarorConst1.equals(joinTerm1.getArgument(1) ) )
											{
												if ( clauseTerm.getArgument(0).equals(extraAtom.getArgument(0) ) && clauseTerm.getArgument(1).equals(extraAtom.getArgument(1) ) ) 
												{
													newClause.setToBeEvaluated(false);
//													clauseFoundNotToHaveAns = true;
													joinsThatCauseClausesNotToHaveAnswers++;
//													System.out.println("5: CL with no answers = " + newClause);
												}
											}
		//									S(x,_)
											else {
												if ( clauseTerm.getArgument(0).equals(extraAtom.getArgument(0) ) ) 
												{
													newClause.setToBeEvaluated(false);
//													clauseFoundNotToHaveAns = true;
													joinsThatCauseClausesNotToHaveAnswers++;
//													System.out.println("6: CL with no answers = " + newClause);
												}
											}
										}
										//S(y,x) or S(y,_)
										else if ( joinTerm0VarorConst0.equals( joinTerm1.getArgument(1) ) ) //&& joinTerm0VarorConst1.equals(joinTerm1.getArgument(0) ) )
										{
											//S(y,x)
											if ( joinTerm0VarorConst1.equals(joinTerm1.getArgument(0) ) )
											{
												if ( clauseTerm.getArgument(0).equals(extraAtom.getArgument(1) ) && clauseTerm.getArgument(1).equals(extraAtom.getArgument(0) ) ) 
												{
													newClause.setToBeEvaluated(false);
//													clauseFoundNotToHaveAns = true;
													joinsThatCauseClausesNotToHaveAnswers++;
//													System.out.println("7: CL with no answers = " + newClause);
												}
											}
											//S(y,_)
											else 
											{
												if ( clauseTerm.getArgument(0).equals(extraAtom.getArgument(1) ) )
												{
													newClause.setToBeEvaluated(false);
//													clauseFoundNotToHaveAns = true;
													joinsThatCauseClausesNotToHaveAnswers++;
//													System.out.println("8: CL with no answers = " + newClause);
												}
											}
										}
									}
								}
							}
//							if (!clauseFoundNotToHaveAns)
//							{
//								clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause1Entity);
//								clause.addEntitiesParticipatingInJoinsWithNoAnswers(clause2Entity);
//							}
						}
					}
				}
//					/**
//					 * Could also be performed with subsumption checks
//					 */
//						for ( Term[] joinTerm: joinTerms )
//						{	//now we have to find if these terms are the same (up to variable renaming) in the body of newClause
//							//concept
//							Term joinTerm0 = joinTerm[0];
//							//concept or role
//							Term joinTerm1 = joinTerm[1];
//							/**
//							 * Shouldnt I have to check if the variable of joinTerm1 is the same as the joinTerm0??
//							 */
//							if ( joinTerm1.getArity() == 1 && clauseTerm.getVariableOrConstant().equals( extraAtom.getVariableOrConstant() ) ) {
//								newClause.setToBeEvaluated(false);
//								joinsThatCauseClausesNotToHaveAnswers++;
//								System.out.println("10: CL with no answers = " + newClause);
//								return;
//							}
//							else //if (joinTerm1.getArity() ==2 )
//							{
//								//A(x),R(x,y) pattern in the join
//								if ( joinTerm0.getArgument(0).equals( joinTerm1.getArgument(0) ) ) {
//									//set to comment because there will be no problem with Argument 0
//	//								if ( clauseTerm.getArity() == 1 )
//									{
//										if ( clauseTerm.getArgument(0).equals( extraAtom.getArgument(0) ) )
//										{
//											newClause.setToBeEvaluated(false);
//											joinsThatCauseClausesNotToHaveAnswers++;
//											System.out.println("11: CL with no answers = " + newClause + "\t\t" + joinTerm[0] + "," + joinTerm[1]);
//											return;
//										}
//									}
//								}
//								//A(x),R(y,x) pattern in join
//								else {
//									if ( clauseTerm.getArity() == 1 && clauseTerm.getArgument(0).equals( extraAtom.getArgument(1) ) ) {
//										newClause.setToBeEvaluated(false);
//										joinsThatCauseClausesNotToHaveAnswers++;
//										System.out.println("12: CL with no answers = " + newClause);
//										return;
//									}
//									else if (clauseTerm.getArity() == 2 && clauseTerm.getArgument(1).equals( extraAtom.getArgument(0) ) ) {
//										newClause.setToBeEvaluated(false);
//										joinsThatCauseClausesNotToHaveAnswers++;
//										System.out.println("13: CL with no answers = " + newClause);
//										return;
//									}
//								}
//							}
//						}
//					}
//				}
			}
		}
//		System.out.println("JJOINS WITH NO ANSWERS = " + joinsThatCauseClausesNotToHaveAnswers);
	}
}
