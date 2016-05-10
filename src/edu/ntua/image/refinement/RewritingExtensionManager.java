/*
 * #%L
 * IQAROS
 *
 * $Id$
 * $HeadURL$
 * %%
 * Copyright (C) 2011 by the Image, Video and Multimedia Laboratory, NTUA
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

package edu.ntua.image.refinement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.oxford.comlab.perfectref.rewriter.Clause;
import org.oxford.comlab.perfectref.rewriter.PI;
import org.oxford.comlab.perfectref.rewriter.Substitution;
import org.oxford.comlab.perfectref.rewriter.Term;
import org.oxford.comlab.perfectref.rewriter.TermFactory;
import org.oxford.comlab.perfectref.rewriter.Variable;

import edu.aueb.queries.QueryOptimization;
import edu.ntua.image.datastructures.Tree;
import edu.ntua.image.datastructures.TreeNode;
import edu.ntua.image.incremental.Incremental;
import edu.ntua.image.redundancy.RedundancyElimination;

public class RewritingExtensionManager {

	private static final TermFactory m_termFactory = new TermFactory();
	private boolean bothVarsInCQ = false;
	private Set<Variable> joinVars;
	private Set<TreeNode<Clause>> processedNodes;
	private RedundancyElimination redundancyEliminator;
	private boolean useRDFoxOptimization = false;

	private HashMap<String, Substitution> substitutionsMap = new HashMap<String, Substitution>();

	public HashMap<String, Substitution> getsubstitutionsMap() {
		return substitutionsMap;
	}


	private boolean renameExtraAtom = false;
	private Term equalRole = null;

	public RewritingExtensionManager(RedundancyElimination redundancyManager, boolean useRDFoxOpt) {
		redundancyEliminator=redundancyManager;
		useRDFoxOptimization = useRDFoxOpt;
	}

//	public Set<Clause> extendRewriting( Tree<Clause> tree, Term extraAtom, ArrayList<PI> pis, Set<Term> mainQueryVars, HashMap<String, Substitution> substitMap ) {
//		return extendRewriting(tree, extraAtom, pis, mainQueryVars, substitMap, null);
//	}

	public Set<Clause> extendRewriting( Tree<Clause> tree, Term extraAtom, Set<Term> rewOfNewAtom, Set<Term> mainQueryVars,
			HashMap<String, Substitution> substitMap, QueryOptimization qOpt ) {
		this.substitutionsMap = substitMap;

		System.out.println("IN FINAL ATOM PROCESSING");
		
//		for (Term t: rewOfNewAtom)
//			System.out.println("\t" + t);

		int extraAtomArity = extraAtom.getArity();
		Term[] extraAtomArguments = extraAtom.getArguments();
		joinVars = new HashSet<Variable>();
		processedNodes = new HashSet<TreeNode<Clause>>();

		if (extraAtomArity == 1 && mainQueryVars.contains(extraAtomArguments[0]))
			joinVars.add((Variable)extraAtomArguments[0]);
		else if (extraAtomArity == 2) {
			//avenet - constants
			if ( extraAtomArguments[0].isVariable() )
				joinVars.add((Variable)extraAtomArguments[0]);
			if ( extraAtomArguments[1].isVariable() )
				joinVars.add((Variable)extraAtomArguments[1]);
			if (!mainQueryVars.contains( extraAtomArguments[0]))
				joinVars.remove( extraAtomArguments[0] );
			else if (!mainQueryVars.contains( extraAtomArguments[1]))
				joinVars.remove( extraAtomArguments[1] );
		}
		if (joinVars.isEmpty()) {
			System.err.println( "\ncomputeRewritingOfAtomAs Tree ERROR" );
			System.exit(0);
		}

		Term clauseHead = tree.getRootElement().getNodeLabel().getHead();
		Set<Clause> result = new HashSet<Clause>();

//		System.out.print("Computing rew of final atom " + extraAtom );
		long startFinalAtomRew = System.currentTimeMillis();
//		Set<Term> rewOfNewAtom = computeRewritingOfAtom( extraAtom, pis, mainQueryVars, qOpt);
//		Set<Term> rewOfNewAtom = computeRewritingOfAtom( extraAtom, pis, mainQueryVars);
//		System.out.println(" with size " + rewOfNewAtom.size() + " in " + (System.currentTimeMillis() - startFinalAtomRew) + "ms");

		redundancyEliminator.initialise(this, tree.getRootElement().getSubTreeAsSet(), rewOfNewAtom);

		Queue<TreeNode<Clause>> queue = new LinkedList<TreeNode<Clause>>();
		queue.add( tree.getRootElement() );
		while (!queue.isEmpty()) {
			TreeNode<Clause> currentNode = queue.poll();
			Clause currentClause = currentNode.getNodeLabel();

			ArrayList<Variable> currentClauseVars = currentClause.getVariables();
			Term[] currentClauseBody = currentClause.getBody();

			if (processedNodes.contains(currentNode))
				continue;
			processedNodes.add(currentNode);
			boolean subTreeHasBeenCopied = false;
			Set<Clause> tempResult = new HashSet<Clause>();
			if (!isEligibleForProcessing( currentClause ) )
				continue;
			/*
			 * Clause or extension of the clause will be redundant hence no need to extend
			 */
			boolean clauseIsRedundant = redundancyEliminator.isClauseRedundant(currentClause,joinVars);
			if ( /*!currentClause.getToBeEvaluated() ||*/ clauseIsRedundant) {
				queue.addAll(currentNode.getChildren());
//				System.out.println("\t\t2: " + currentClause + " " + currentClause.getSubsumer() + "\t\t" + currentClause.getToBeEvaluated());
				continue;
			}
			/*
			 * In this variable I have also added the restriction that a clause that has no answers
			 * should not be extended. It should only be checked for merging
			 */
			boolean isEligibleForExtension = isEligibleForExtension(currentClause);

			for (Term subsTerm : rewOfNewAtom) {
				Term[] subsTermArgs = subsTerm.getArguments();
				int subsTermArity = subsTerm.getArity();

				if (subsTermArity == 1 ) {//&& currentClauseVars.contains( subsTermArgs[0] )) {
					if ( conceptAtomAppearsInQueryBody( subsTerm , currentClauseBody ) ) {
						//4-Aug-2015
						//added check for toBeEvaluated
						result.addAll( getNonRedundantChildrenOf( currentNode ) );
//						for ( Clause ccc : getNonRedundantChildrenOf(currentNode))
//							System.out.println("7: " + ccc+ "\t\t" + ccc.getToBeEvaluated());
						subTreeHasBeenCopied = true;
						break;
					}
					else
						if (isEligibleForExtension && !subsTerm.getName().contains("AUX$") && !isInteger(subsTerm.getName() ) ) {
							Clause newClause;
							if (renameExtraAtom) {
								Substitution subst = substitutionsMap.get(currentClause.m_canonicalRepresentation);
								if ( subst == null )
									subst = new Substitution();
								newClause = createNewQueriesWithExtraAtom( subsTerm.apply(subst, m_termFactory), currentClauseBody, clauseHead);
//								newClause = createNewQueriesWithExtraAtom( subsTerm.apply(substitutionsMap.get(currentClause.m_canonicalRepresentation), m_termFactory), currentClauseBody, clauseHead);
								if (newClause == null)
									System.out.println("Created null clause");
								renameExtraAtom = false;
							}
							else {
								newClause = createNewQueriesWithExtraAtom(subsTerm, currentClauseBody, clauseHead);
							}

							/*
							 * RDFox Opt
							 */
							if (useRDFoxOptimization) {
								newClause.setToBeSentToRDFox( currentClause.getToBeSentToRDFox() && subsTerm.getToBeSentToRDFox() );
							}

							/*
							 * DB opt
							 */
							if (qOpt!=null) {
								qOpt.markNewlyCreatedClauseByExtension(currentClause, subsTerm, newClause);
								//avenet 13/3/2016 - test Fly Q5
								if (newClause.getToBeEvaluated())
								{
//									System.out.println("1: " + newClause + "\t" + newClause.getToBeEvaluated() );
									tempResult.add( newClause );
									redundancyEliminator.checkAndMarkClauseAsPossiblyNonRedundant(currentClause, subsTerm, newClause);
								}
							}
							else {
//								System.out.println("2: " + newClause + "\t" + newClause.getToBeEvaluated() );
								tempResult.add( newClause );
								redundancyEliminator.checkAndMarkClauseAsPossiblyNonRedundant(currentClause, subsTerm, newClause);
							}
						}
				}
				else if (subsTermArity == 2 ) {// && (currentClauseVars.contains(subsTermArgs[0]) || currentClauseVars.contains(subsTermArgs[1]))) {
					Substitution sub = roleAtomAppearsInQueryBody(subsTerm, currentClause);
					if ( sub != null ) {
						if (sub.isEmpty()) {
							//4-Aug-2015
							result.addAll( getNonRedundantChildrenOf( currentNode ) );
//							for ( Clause ccc : getNonRedundantChildrenOf(currentNode))
//								System.out.println("6: " + ccc+ "\t\t" + ccc.getToBeEvaluated());
							subTreeHasBeenCopied = true;
							break;
						}
						else {
							substitutionsMap.put(currentClause.m_canonicalRepresentation, sub);
							Variable mappedVariable = sub.keySet().iterator().next();
							//currentNode contains the mappedVariable
							if ( currentClause.getVariables().contains( mappedVariable ) ) {
								for (TreeNode<Clause> currentChildNode : currentNode.getChildren() ) {
									Clause currentChildClause = currentChildNode.getNodeLabel();

									//currentChild does not contain equalRole and the term that replaces it is a concept
									if ( !currentChildClause.m_canonicalRepresentation.contains( equalRole.toString() ) &&
										findTermThatReplacesEqualRole( currentClause , currentChildClause ).getArity() == 1 ) {
										TreeNode<Clause> newCurrentNode = renameGraph( currentNode , sub );
										//4-Aug-2015
//										result.addAll( newCurrentNode.getSubTreeAsSet());
										//Add to result only those to be evaluated
										//Maybe here we should also run getNonRedundantChildrenOf()
										for ( Clause c: newCurrentNode.getSubTreeAsSet())
											//avenet 13/3/2016 - test Fly Q5
											if (c.getToBeEvaluated())
											{
//												System.out.println("5: " + c + "\t" + c.getToBeEvaluated() );
												result.add(c);
											}
										processedNodes.add(currentChildNode);
									} else
										;
								}
							}
							else {
								//4-Aug-2015
								result.addAll( getNonRedundantChildrenOf( currentNode ) );
//								System.out.println(currentNode.getNodeLabel() + "\t" + subsTerm);
//								for ( Clause ccc : getNonRedundantChildrenOf(currentNode))
//									System.out.println("3: " + ccc+ "\t\t" + ccc.getToBeEvaluated());
								subTreeHasBeenCopied = true;
								break;
							}
						}
					}
					else
						if (isEligibleForExtension && !subsTerm.getName().contains("AUX$") && !isInteger(subsTerm.getName() ) ) {
							Clause newClause;
							if (renameExtraAtom) {
								Substitution subst = substitutionsMap.get(currentClause.m_canonicalRepresentation);
								if ( subst == null )
									subst = new Substitution();
								newClause = createNewQueriesWithExtraAtom( subsTerm.apply(subst, m_termFactory), currentClauseBody, clauseHead);
//								newClause = createNewQueriesWithExtraAtom( subsTerm.apply(substitutionsMap.get(currentClause.m_canonicalRepresentation), m_termFactory), currentClauseBody, clauseHead);
								renameExtraAtom = false;
							}
							else
								newClause = createNewQueriesWithExtraAtom( subsTerm, currentClauseBody, clauseHead);
//							new Incremental().markClauseWRTAnswers(currentClause, clauseFromH2, newClause);
							/*
							 * RDFox Opt
							 */
							if (useRDFoxOptimization) {
								newClause.setToBeSentToRDFox( currentClause.getToBeSentToRDFox() && subsTerm.getToBeSentToRDFox() );
							}

							/*
							 * DB optimization
							 */
							if (qOpt!=null) {
//								System.out.println( "cl1 = " + extraAtom );
								qOpt.markNewlyCreatedClauseByExtension(currentClause, subsTerm, newClause);
//								System.out.println(newClause.getToBeEvaluated());

								//avenet 13/3/2016 - test Fly Q5
								if ( newClause.getToBeEvaluated() )
								{
//									System.out.println("4: " + newClause + "\t" + newClause.getToBeEvaluated() );
									tempResult.add( newClause );
									redundancyEliminator.checkAndMarkClauseAsPossiblyNonRedundant(currentClause, subsTerm, newClause);
								}
							}
							else {
//								System.out.println("2: " + newClause + "\t" + newClause.getToBeEvaluated() );
								tempResult.add( newClause );
								redundancyEliminator.checkAndMarkClauseAsPossiblyNonRedundant(currentClause, subsTerm, newClause);
							}
						}
				}
				if (subTreeHasBeenCopied)
					break;
			}
			if (!subTreeHasBeenCopied) {
				for (TreeNode<Clause> child : currentNode.getChildren())
					if (!processedNodes.contains(child))
						queue.add(child);
				result.addAll( tempResult );
				redundancyEliminator.flush(currentClause);
			}
		}
		return result;
	}

	public boolean isInteger( String input ) {
	   try {
	      Integer.parseInt( input );
	      return true;
	   } catch(NumberFormatException e){} {
	      return false;
	   }
	}

//	public boolean isEligibleForProcessing(Clause clause) {
//		return clause.getVariables().containsAll(joinVars);
//	}

	private Term findTermThatReplacesEqualRole(Clause originalClause, Clause newClause) {

		Term[] newBody = newClause.getBody();

		for ( int i = 0 ; i < newBody.length ; i++ )
			if ( !originalClause.m_canonicalRepresentation.contains( newBody[i].toString() ) )
				return newBody[i];

		return null;
	}

	public boolean isEligibleForProcessing(Clause clause) {

		ArrayList<Variable> clauseVariables = clause.getVariables();

		if (clauseVariables.containsAll(joinVars) )
			return true;
		else {
			Substitution sub = substitutionsMap.get(clause.m_canonicalRepresentation);
			if ( joinVars.size() == 1 && findMappedVariableInClause( clause, sub , (Variable)joinVars.toArray()[0] ) ) {
				renameExtraAtom = true;
				return true;
			} else if ( findMappedVariableInClause( clause , sub , (Variable)joinVars.toArray()[0] ) &&
			findMappedVariableInClause( clause , sub , (Variable)joinVars.toArray()[1] ) ) {
				renameExtraAtom = true;
				return true;
			}
		}
		return false;
	}

	private boolean findMappedVariableInClause( Clause c, Substitution s, Term var ) {

		Term currentVar = var;
		Set<Term> checkedVars = new HashSet<Term>();
		ArrayList<Variable> clauseVariables = c.getVariables();

		if ( s == null )
			return false;

		while ( !checkedVars.contains( currentVar ) && s.get( currentVar ) != null) {
			Term tempTerm = s.get( currentVar );
			if ( tempTerm != null )
				if ( clauseVariables.contains( tempTerm ) )
					return true;
				else {
					checkedVars.add( currentVar );
					currentVar = tempTerm;
				}
		}
		return false;
	}

	/* 14/3/2016
	 * This function checks if the current clause contains AUX or if it has no answers
	 *
	 * In both cases the clause need not be extended because the result will either be
	 * rejected or will have no answer
	 */
	private boolean isEligibleForExtension(Clause currentClause) {
		if (containsAUX(currentClause) || !currentClause.getToBeEvaluated())
			return false;
		return true;
	}

	private Clause createNewQueriesWithExtraAtom( Term extraAtom , Term[] body, Term head) {

		Term[] newBody = new Term[body.length + 1];
		for (int i=0 ; i<body.length ; i++) {
			newBody[i] = body[i];
//			System.out.println(body[i]);
		}
		newBody[body.length] = extraAtom;

//		System.out.println(new Clause( newBody , head ));
//		System.out.println();
		return new Clause( newBody , head );
	}

	private Set<Clause> getNonRedundantChildrenOf( TreeNode<Clause> node ) {
		Set<Clause> result = new HashSet<Clause>();
		Set<TreeNode<Clause>> visited = new HashSet<TreeNode<Clause>>();
		Stack<TreeNode<Clause>> stack = new Stack<TreeNode<Clause>>();
		stack.push(node);
		while (!stack.isEmpty()) {
			TreeNode<Clause> currentNode = stack.pop();
			Clause clause = currentNode.getNodeLabel();
			if (!visited.contains(currentNode)){
				//avenet 4-Aug-2015
				if (!containsAUX(clause) && redundancyEliminator.isClauseNonRedundant(clause,joinVars) && clause.getToBeEvaluated() ) {
					redundancyEliminator.checkAndMarkClauseAsNonRedundant(clause);
					result.add(clause);
					redundancyEliminator.addActiveSubsumer(clause);
				}
				processedNodes.add(currentNode);
				visited.add(currentNode);
				stack.addAll(currentNode.getChildren());
			}
		}
		return result;
	}

	public boolean containsAUX( Clause clause ) {
		Term[] m_body = clause.getBody();
		for(int i=0; i < clause.getBody().length; i++){
			//Auxiliary predicates introduced in the clausification
			if (m_body[i].getName().contains("AUX$"))
				return true;
			try{
				//Auxiliary symbols introduced in the normalization (HermiT)
				Integer.parseInt(m_body[i].getName());
				return true;
			}
			catch(NumberFormatException e){}
		}
		return false;
	}

	//Since the check contains the whole string, this takes into account the variables also
	public boolean conceptAtomAppearsInQueryBody(Term conceptAtom, Term[] queryBodyAtoms) {
		for (Term bodyAtom : queryBodyAtoms)
			if (bodyAtom.toString().equals(conceptAtom.toString()))
				return true;
		return false;
	}

	public Substitution roleAtomAppearsInQueryBody(Term roleAtom, Clause currentQuery ) {
		equalRole = null;
		for (Term bodyAtom : currentQuery.getBody())
			if (bodyAtom.getArity() == 2 ) {
				Substitution s = isEqualRoleTerm(currentQuery, roleAtom, bodyAtom );
				if ( s!= null )
					return s;
			}
		return null;
	}

	public Substitution isEqualRoleTerm( Clause currentQuery,  Term roleAtom, Term bodyAtom ) {
		if ( bodyAtom.getName().equals( roleAtom.getName() ) ) {
			Term[] roleAtomVariables = roleAtom.getArguments();
			Term[] bodyAtomVariables = bodyAtom.getArguments();
			ArrayList<Variable> currentClauseVars = currentQuery.getVariables();
			if (bodyAtomVariables[1].equals( roleAtomVariables[1] ) && bodyAtomVariables[0].equals( roleAtomVariables[0] ))
				return new Substitution();

//			if ( bothVarsInCQ ) {
//				if ( bodyAtomVariables[0].toString().equals( roleAtomVariables[0].toString() ) && ( !currentQuery.isBound( ( Variable )bodyAtomVariables[1] ) ) )
//					return true;
//				else if ( bodyAtomVariables[1].toString().equals( roleAtomVariables[1].toString() ) && ( !currentQuery.isBound( ( Variable )bodyAtomVariables[0] ) ) )
//					return true;
//			}
			if (!bothVarsInCQ) {
				Substitution s = new Substitution();
				if ( bodyAtomVariables[0].toString().equals( roleAtomVariables[0].toString() ) ) {
					equalRole = roleAtom;
					s.put( (Variable) roleAtomVariables[1], bodyAtomVariables[1]);
					return s;
				}
				else if ( bodyAtomVariables[1].toString().equals( roleAtomVariables[1].toString() ) ) {
					equalRole = roleAtom;
					s.put( (Variable) roleAtomVariables[0], bodyAtomVariables[0]);
					return s;
				}
			}
		}
		return null;
	}

	/**
	 * In this new optimized version I also take into account the emptiness optimization to compute
	 * the rew of the final atom. Since this rew is just a set I do not need to keep queries that have no answers
	 * 
	 * However it is not correct to through away all the atoms that have no answers because they might produce merges
	 * @param extraAtom
	 * @param pis
	 * @param mainQueryVars
	 * @return
	 */
//	public Set<Term> computeRewritingOfAtom(Term extraAtom, ArrayList<PI> pis, Set<Term> mainQueryVars, QueryOptimization qOpt) {
//
//		Set<Term> result = new HashSet<Term>();
//		int extraAtomArity = extraAtom.getArity();
//		Term[] extraAtomArguments = extraAtom.getArguments();
//		Term head = null;
//
//		if ( extraAtomArity == 1 && mainQueryVars.contains( extraAtomArguments[0] ) )
//			head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[0] );
//		else if ( extraAtomArity == 2 )
//			if ( mainQueryVars.contains( extraAtomArguments[0] ) && mainQueryVars.contains( extraAtomArguments[1] ) ) {
//				head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[0], extraAtomArguments[1]);
//				bothVarsInCQ = true;
//			}
//			else if ( mainQueryVars.contains( extraAtomArguments[0] ) )
//				head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[0] );
//			else if ( mainQueryVars.contains( extraAtomArguments[1] ) )
//				head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[1] );
//		TreeNode<Term> rootElement = new TreeNode<Term>( extraAtom );
//		if ( head != null ) {
//			Stack<TreeNode<Term>> toVisit = new Stack<TreeNode<Term>>();
//			HashMap<String,TreeNode<Term>> m_canonicalsToRewOfExtraAtom = new HashMap<String,TreeNode<Term>>();
//			toVisit.push(rootElement);
//			Set<TreeNode<Term>> visited = new HashSet<>();
//			m_canonicalsToRewOfExtraAtom.put(extraAtom.toString(),rootElement);
//			while(!toVisit.isEmpty()) {
//				TreeNode<Term> currentTermNode = toVisit.pop();
//				visited.add(currentTermNode);
//				Term[] body =  new Term[1];
//				body[0] = currentTermNode.getNodeLabel();
//				Clause clause = new Clause( body , head );
//				for ( PI pi : pis ) {
//					Clause cl = Incremental.applyPositiveInclusion(pi, 0, clause);
//					if ( cl != null && !qOpt.atomicEntitiesWithNoAnswers.contains(cl.getBody()[0].getName())) {
//						if ( !( cl.getBody()[0].getArity() == 1 && Integer.parseInt( cl.getBody()[0].getArgument( 0 ).toString().substring(1, cl.getBody()[0].getArgument( 0 ).toString().length())) > 500 ) ) {
//							Term newAtom = cl.getBody()[0];
//							/*
//							 * RDFox optimization
//							 */
//							if ( useRDFoxOptimization && ( pi.m_type == 2 || pi.m_type == 3 || pi.m_type == 5 || pi.m_type == 6 || pi.m_type == 8 ||
//									pi.m_type == 9 || pi.m_type == 10 ) )
//									newAtom.setToBeSentToRDFox(false);
//
//							/**
//							 * 19/4/2016
//							 * Here we need the toVisit to add the new term so that we can produce a complete rew
//							 */
//							TreeNode<Term> newTermNode = null;
//							TreeNode<Term> nodeInRew = m_canonicalsToRewOfExtraAtom.get(newAtom.toString());
//							if (nodeInRew == null) {
//								newTermNode = new TreeNode<Term>( newAtom );
//								m_canonicalsToRewOfExtraAtom.put(newAtom.toString(), newTermNode );
//								if (!visited.contains(newTermNode) )
//									toVisit.push( newTermNode );
//							}
////							else
////								newTermNode = m_canonicalsToRewOfExtraAtom.get( newAtom.toString() );
//
//							result.add(newAtom);
////							currentTermNode.addChild( newTermNode );
//						}
//					}
//				}
//			}
//		}
//		return result;
//	}

	/**
	 * In this method I do not take into account the fact that the rew of the final atom is computed as a set
	 * hence I do not need to keep queries that I know have no answers. However I need to know if 
	 * an atom will participate in a merge... If it wont I have no problem of throughing it out.
	 *
	 * This is the original method
	 *
	 * @param currentT1
	 * @param currentMapping
	 * @return
	 */
	public Set<Term> computeRewritingOfAtom(Term extraAtom, ArrayList<PI> pis, Set<Term> mainQueryVars) {

		Set<Term> result = new HashSet<Term>();
		int extraAtomArity = extraAtom.getArity();
		Term[] extraAtomArguments = extraAtom.getArguments();
		Term head = null;

		if ( extraAtomArity == 1 && mainQueryVars.contains( extraAtomArguments[0] ) )
			head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[0] );
		else if ( extraAtomArity == 2 )
			if ( mainQueryVars.contains( extraAtomArguments[0] ) && mainQueryVars.contains( extraAtomArguments[1] ) ) {
				head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[0], extraAtomArguments[1]);
				bothVarsInCQ = true;
			}
			else if ( mainQueryVars.contains( extraAtomArguments[0] ) )
				head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[0] );
			else if ( mainQueryVars.contains( extraAtomArguments[1] ) )
				head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[1] );
		TreeNode<Term> rootElement = new TreeNode<Term>( extraAtom );
		if ( head != null ) {
			Stack<TreeNode<Term>> toVisit = new Stack<TreeNode<Term>>();
			HashMap<String,TreeNode<Term>> m_canonicalsToRewOfExtraAtom = new HashMap<String,TreeNode<Term>>();
			toVisit.push(rootElement);
			m_canonicalsToRewOfExtraAtom.put(extraAtom.toString(),rootElement);
			while(!toVisit.isEmpty()) {
				TreeNode<Term> currentTermNode = toVisit.pop();
				Term[] body =  new Term[1];
				body[0] = currentTermNode.getNodeLabel();
				Clause clause = new Clause( body , head );
				for ( PI pi : pis ) {
					Clause cl = Incremental.applyPositiveInclusion(pi, 0, clause);
					if ( cl != null ) {
						if ( !( cl.getBody()[0].getArity() == 1 && Integer.parseInt( cl.getBody()[0].getArgument( 0 ).toString().substring(1, cl.getBody()[0].getArgument( 0 ).toString().length())) > 500 ) ) {
							Term newAtom = cl.getBody()[0];
							/*
							 * RDFox optimization
							 */
							if ( useRDFoxOptimization && ( pi.m_type == 1 || pi.m_type == 4 || 
									pi.m_type == 7 || pi.m_type == 10 || pi.m_type == 11 ) )
									newAtom.setToBeSentToRDFox(false);
//							if ( useRDFoxOptimization && ( pi.m_type == 2 || pi.m_type == 3 || pi.m_type == 5 || pi.m_type == 6 || pi.m_type == 8 ||
//									pi.m_type == 9 || pi.m_type == 10 ) )
//									newAtom.setToBeSentToRDFox(false);

							TreeNode<Term> newTermNode = null;
							TreeNode<Term> nodeInRew = m_canonicalsToRewOfExtraAtom.get(newAtom.toString());
							if (nodeInRew == null) {
								newTermNode = new TreeNode<Term>( newAtom );
								m_canonicalsToRewOfExtraAtom.put(newAtom.toString(), newTermNode );
								toVisit.push( newTermNode );
							}
							//Commented on 19/4/2016 - no need to compute a tree
//							else
//								newTermNode = m_canonicalsToRewOfExtraAtom.get( newAtom.toString() );

//							currentTermNode.addChild( newTermNode );
							result.add(newAtom);
						}
					}
				}
			}
		}
//		return rootElement.getSubTreeAsSet();
		return result;
	}
	
	
	public ArrayList<Clause> computeRewritingOfAtomAsSetOfClauses(Clause originalClause, ArrayList<PI> pis, Set<Term> mainQueryVars, QueryOptimization qOpt) {

		ArrayList<Clause> result = new ArrayList<Clause>();
		Term extraAtom = originalClause.getBody()[0];
		int extraAtomArity = extraAtom.getArity();
		Term[] extraAtomArguments = extraAtom.getArguments();
		Term head = originalClause.getHead();

//		if ( extraAtomArity == 1 && mainQueryVars.contains( extraAtomArguments[0] ) )
//			head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[0] );
//		else if ( extraAtomArity == 2 )
//			if ( mainQueryVars.contains( extraAtomArguments[0] ) && mainQueryVars.contains( extraAtomArguments[1] ) ) {
//				head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[0], extraAtomArguments[1]);
//				bothVarsInCQ = true;
//			}
//			else if ( mainQueryVars.contains( extraAtomArguments[0] ) )
//				head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[0] );
//			else if ( mainQueryVars.contains( extraAtomArguments[1] ) )
//				head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[1] );
		TreeNode<Term> rootElement = new TreeNode<Term>( extraAtom );
		if ( head != null ) {
			Stack<TreeNode<Term>> toVisit = new Stack<TreeNode<Term>>();
			HashMap<String,TreeNode<Term>> m_canonicalsToRewOfExtraAtom = new HashMap<String,TreeNode<Term>>();
			toVisit.push(rootElement);
			m_canonicalsToRewOfExtraAtom.put(extraAtom.toString(),rootElement);
			while(!toVisit.isEmpty()) {
				TreeNode<Term> currentTermNode = toVisit.pop();
				Term[] body =  new Term[1];
				body[0] = currentTermNode.getNodeLabel();
				Clause clause = new Clause( body , head );
				for ( PI pi : pis ) {
					Clause cl = Incremental.applyPositiveInclusion(pi, 0, clause);
					if ( cl != null )
						if ( !( cl.getBody()[0].getArity() == 1 && Integer.parseInt( cl.getBody()[0].getArgument( 0 ).toString().substring(1, cl.getBody()[0].getArgument( 0 ).toString().length())) > 500 ) ) {
							Term newAtom = cl.getBody()[0];
							/*
							 * RDFox optimization
							 */
							if ( useRDFoxOptimization && ( pi.m_type == 2 || pi.m_type == 3 || pi.m_type == 5 || pi.m_type == 6 || pi.m_type == 8 ||
									pi.m_type == 9 || pi.m_type == 10 ) )
									cl.setToBeSentToRDFox(false);

							TreeNode<Term> newTermNode = null;
							TreeNode<Term> nodeInRew = m_canonicalsToRewOfExtraAtom.get(newAtom.toString());
							if (nodeInRew == null) {
								newTermNode = new TreeNode<Term>( newAtom );
								m_canonicalsToRewOfExtraAtom.put(newAtom.toString(), newTermNode );
								toVisit.push( newTermNode );
							}
							//Commented on 19/4/2016 - no need to compute a tree
//							else
//								newTermNode = m_canonicalsToRewOfExtraAtom.get( newAtom.toString() );

//							currentTermNode.addChild( newTermNode );
							if ( !qOpt.atomicEntitiesWithNoAnswers.contains(cl.getBody()[0].getName()))
								result.add(cl);
						}
				}
			}
		}
//		return rootElement.getSubTreeAsSet();
		return result;
	}


	private TreeNode<Clause> renameGraph(TreeNode<Clause> currentT1, Substitution currentMapping) {

		HashMap<TreeNode<Clause>,TreeNode<Clause>> oldNode2NewNode = new HashMap<TreeNode<Clause>, TreeNode<Clause>>();

		Stack<TreeNode<Clause>> stack = new Stack<TreeNode<Clause>>();
		stack.push(currentT1);

		Set<TreeNode<Clause>> visitedNodes = new HashSet<TreeNode<Clause>>();

		TreeNode<Clause> newCurrentT1 = new TreeNode<Clause>( getNewQuery(  currentMapping, currentT1.getNodeLabel() ) );
		oldNode2NewNode.put(currentT1, newCurrentT1);
		substitutionsMap.put(newCurrentT1.getNodeLabel().m_canonicalRepresentation, substitutionsMap.get(currentT1.getNodeLabel().m_canonicalRepresentation ) );

		while ( !stack.isEmpty() ) {
			TreeNode<Clause> currentNode = stack.pop();
			visitedNodes.add(currentNode);
//			Substitution currentMapping = substitutionsMap.get( currentNode.getNodeLabel().m_canonicalRepresentation );

			TreeNode<Clause> currentNewNode = oldNode2NewNode.get( currentNode );

			for ( TreeNode<Clause> child : currentNode.getChildren() ) {
				if (visitedNodes.contains(child))
					continue;
				stack.push(child);
				TreeNode<Clause> newChildNode = new TreeNode( getNewQuery( currentMapping, child.getNodeLabel() ) );
				currentNewNode.addChild( newChildNode );
				oldNode2NewNode.put(child, newChildNode);
				substitutionsMap.put(newChildNode.getNodeLabel().m_canonicalRepresentation, currentMapping);
			}
		}
		return newCurrentT1;
	}


	/**
	 * Creates a new query by applying the mgu of two atoms to a given query
	 * @param mgu
	 * @param clause
	 * @return
	 */
	private Clause getNewQuery(Substitution mgu, Clause clause){

		if ( mgu == null )
			return clause;

		Set<Term> newBody = new LinkedHashSet<Term>();
        //Copy the atoms from the main premise
        for (int index=0; index < clause.getBody().length; index++)
			newBody.add(clause.getBody()[index].apply(mgu, m_termFactory));

        //New body and head
        Term[] body = new Term[newBody.size()];
        newBody.toArray(body);
        Term head = clause.getHead().apply(mgu, m_termFactory);

        Clause newQuery = new Clause(body, head);

		return newQuery;
	}
}