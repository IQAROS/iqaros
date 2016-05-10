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

package edu.ntua.image.incremental;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.oxford.comlab.perfectref.parser.DLliteParser;
import org.oxford.comlab.perfectref.rewriter.Clause;
import org.oxford.comlab.perfectref.rewriter.Constant;
import org.oxford.comlab.perfectref.rewriter.EntParInJoinsWNoAns;
import org.oxford.comlab.perfectref.rewriter.PI;
import org.oxford.comlab.perfectref.rewriter.Substitution;
import org.oxford.comlab.perfectref.rewriter.Term;
import org.oxford.comlab.perfectref.rewriter.TermFactory;
import org.oxford.comlab.perfectref.rewriter.Variable;
import org.semanticweb.owl.model.OWLEntity;

import edu.aueb.queries.QueryOptimization;
import edu.ntua.image.Configuration;
import edu.ntua.image.Configuration.RedundancyEliminationStrategy;
import edu.ntua.image.datastructures.LabeledGraph;
import edu.ntua.image.datastructures.Tree;
import edu.ntua.image.datastructures.TreeNode;
import edu.ntua.image.redundancy.NonRedundantClausesTracker;
import edu.ntua.image.redundancy.RedundancyElimination;
import edu.ntua.image.redundancy.SimpleRedundancyEliminator;
import edu.ntua.image.refinement.RewritingExtensionManager;

public class Incremental {

	/**
	 * Return information to be stored
	 */
	public int rewSizeOriginal;
	public int rewSize;
	public long rewTime;
	public long subTime;
	public long totalTime;

	public int getRewSizeOriginal() {
		return rewSizeOriginal;
	}

	public int getRewSize() {
		return rewSize;
	}

	public long getRewTime() {
		return rewTime;
	}

	public long getSubTime() {
		return subTime;
	}

	public long getTotalTime() {
		return totalTime;
	}

	public int getJoinsThatCauseClausesNotToHaveAnswers() {
		if (qOpt==null)
			return 0;
		return qOpt.getJoinsThatCauseClausesNotToHaveAnswers();
	}

	/**
	 * END Return information to be stored
	 */

	protected static Logger	logger = Logger.getLogger( Incremental.class );

	private static TermFactory m_termFactory;
	private static int localSkolemIndex;

	private Map<String,TreeNode<Clause>> m_canonicalsToDAGNodes;
	private RewritingExtensionManager m_refine;
	private RedundancyElimination redundancyEliminator;
	private Configuration config;
	private QueryOptimization qOpt = null;
	private boolean useRDFoxOptimization = false;

	private HashMap<String, Substitution> substitutionsMap = new HashMap<String, Substitution>();


	public HashMap<String, Substitution> getsubstitutionsMap() {
		HashMap<String, Substitution> refineSubs = m_refine.getsubstitutionsMap();
		if ( refineSubs != null && refineSubs.size() > substitutionsMap.size() )
			return refineSubs;
		return substitutionsMap;
	}

	private Term equalRole = null;
	private boolean renameExtraAtom = false;

	public Incremental() {
		this(new Configuration());
	}

	public Incremental(Configuration c) {
		this(c, null);
	}

	public Incremental(QueryOptimization qOpt) {
		this(new Configuration(), qOpt);
	}

	/*
	 * Use RDFox Optimization
	 *
	 * This means compute a rewriting that does not produces clauses that are not needed by RDFox to produce answers
	 */
	public Incremental(QueryOptimization qOpt, boolean useRDFoxOpt) {
		this(new Configuration(), qOpt, useRDFoxOpt);
	}

	public Incremental(Configuration c, QueryOptimization qOpt) {
		this(c, qOpt, false);
	}

	public Incremental(Configuration c, QueryOptimization qOpt, boolean useRDFoxOpt) {
		if (c != null)
			this.config = c;
		else
			config = new Configuration();
		m_termFactory = new TermFactory();
		m_canonicalsToDAGNodes = new HashMap<String,TreeNode<Clause>>();
		if (config.redundancyElimination == RedundancyEliminationStrategy.Full_Subsumption)
			redundancyEliminator = new SimpleRedundancyEliminator();
		else if (config.redundancyElimination == RedundancyEliminationStrategy.Restricted_Subsumption)
			redundancyEliminator = new NonRedundantClausesTracker();
		m_refine = new RewritingExtensionManager(redundancyEliminator, useRDFoxOpt);
		localSkolemIndex=1;

		/*
		 * RDFox optimization
		 */
		if (useRDFoxOpt) {
			useRDFoxOptimization = useRDFoxOpt;
		}

		/*
		 * Emptiness optimization
		 */
//		if (optimizationPath != null && optimizationPath != "") {
//			long start = System.currentTimeMillis();
//			qOpt = new QueryOptimization(optimizationPath);
//            System.out.println("Optimization Took " + (System.currentTimeMillis() - start) + "ms");
//		}
		this.qOpt = qOpt;
		if (qOpt != null)
			qOpt.setJoinsThatCauseClausesNotToHaveAnswers(0);

		PropertyConfigurator.configure("./logger.properties");
	}


	public ArrayList<Clause> computeUCQRewriting(ArrayList<PI> tBoxAxioms, Clause originalQuery) {
		return computeUCQRewriting(tBoxAxioms, originalQuery, true);
	}

	public ArrayList<Clause> computeUCQRewriting(ArrayList<PI> tBoxAxioms, Clause originalQuery, boolean print) {
		long t1 = System.currentTimeMillis();
		logger.info( "Rewriting query: " + originalQuery + "\n");
		Term[] queryBody = new Term[originalQuery.getBody().length];

		//experiments with Fly q3 have shown that the method used at the end runs better
//		//If there was just a single term then the result is in incrementalTree
//		if( queryBody.length == 1 ) {
//			ArrayList<Clause> ucqRewriting = m_refine.computeRewritingOfAtomAsSetOfClauses(originalQuery, tBoxAxioms, new HashSet<Term>( originalQuery.getDistinguishedVariables()), qOpt);
//			//run refine mode
//			ucqRewriting = redundancyEliminator.pruneAUX( ucqRewriting, true );
//			logger.info("Finalising in " + (System.currentTimeMillis() - t1) + "msec with size: " + ucqRewriting.size() );
//			return ucqRewriting;
//		}

		//find right order of query so that each processed atom has common variables with previously processed atoms
		queryBody = findRightOrderOfQuery(originalQuery);

		HashMap<Term,Tree<Clause>> termToTreeUCQRews = new HashMap<Term,Tree<Clause>>();
		Set<Term> activeVariablesSoFar = new HashSet<Term>( originalQuery.getDistinguishedVariables() );
		logger.info("Starting with term: " + queryBody[0] );
		Tree<Clause> incrementalTree = computeRewritingOfAtomAsTree(queryBody[0], tBoxAxioms, activeVariablesSoFar, true);
		logger.info("term had UCQ size: " + incrementalTree.getRootElement().getSubTreeAsSet().size() + " in " + (System.currentTimeMillis() - t1) + " ms" );
		//19/4/2016
		if (queryBody.length != 1 )
		{
			long startPruning = System.currentTimeMillis();
//			pruneSubTreeWithNoAnswers(incrementalTree);
			logger.info("New prunned tree computed with size: " + incrementalTree.getRootElement().getSubTreeAsSet().size() + " in " + (System.currentTimeMillis() - startPruning) + " ms" );
		}
		addNewActiveVariables(activeVariablesSoFar,queryBody[0].getArguments());
		int maxUCQOfConceptTerm=0;
		Term conceptTermWithMaxUCQRew=null;
		ArrayList<Clause> ucqOfConceptTermWithMaxUCQRew=null;
		Tree<Clause> treeOfConceptTermWithMaxUCQRew = null;
		/**
		 * Compute the rewriting tree for the concepts of the query.
		 * We do not compute it for roles because (probably) we do not know if its vars are bound etc.
		 *
		 *
		 * 19/4/2016
		 * DO WE STILL NEED THIS?
		 * We have used an optimization that in the final atom rew keeps only the atoms that have answers.
		 * So not sure if we need this. Do we spend time to compute the rew of some atoms more than once???
		 */
		for (int i=1 ; i < queryBody.length ; i++) {
			if (queryBody[i].getArguments().length == 1) {
//				logger.info( "\nPREPROCESSING: computing UCQ for concept term: " + queryBody[i] );
				//compute the tree for each of the body concepts
				Tree<Clause> treeUCQRew = computeRewritingOfAtomAsTree(queryBody[i], tBoxAxioms, activeVariablesSoFar, false);

				//store it wrt the body atom
				termToTreeUCQRews.put(queryBody[i], treeUCQRew);
				//compute the arraylist of each tree
				ucqOfConceptTermWithMaxUCQRew = new ArrayList<Clause> (treeUCQRew.getRootElement().getSubTreeAsSet());
				int currentTermUCQSize=ucqOfConceptTermWithMaxUCQRew.size();
				//identify the tree with the largest size and store relevant info (size, body atom, tree)
				if (currentTermUCQSize > maxUCQOfConceptTerm) {
					maxUCQOfConceptTerm=currentTermUCQSize;
					conceptTermWithMaxUCQRew=queryBody[i];
					treeOfConceptTermWithMaxUCQRew = treeUCQRew;
				}
			}
			addNewActiveVariables(activeVariablesSoFar,queryBody[i].getArguments());
		}
		//END of preprocessing

		activeVariablesSoFar = new HashSet<Term>( originalQuery.getDistinguishedVariables() );
		addNewActiveVariables(activeVariablesSoFar,queryBody[0].getArguments());
		ArrayList<Clause> ucqRewriting = new ArrayList<Clause>();
		for (int i = 1 ; i < queryBody.length ; i++) {
			long st=System.currentTimeMillis();
			Tree<Clause> bodyRew = termToTreeUCQRews.get(queryBody[i]);
			//If it is null then it is a role
			if( bodyRew == null ) {
				bodyRew = computeRewritingOfAtomAsTree(queryBody[i], tBoxAxioms, activeVariablesSoFar, false);
			}
			//otherwise it is a concept and we skip it if it is the one with the largest UCQ. We will connect it last.
			else if (queryBody[i]==conceptTermWithMaxUCQRew)
				continue;
			boolean checkOfconceptTermWithMaxUCQRewAndQueryBodyLength = conceptTermWithMaxUCQRew == null && i == queryBody.length-1;
			if (checkOfconceptTermWithMaxUCQRewAndQueryBodyLength) {
				logger.info("Before finalisation UCQ size: " + incrementalTree.getRootElement().getSubTreeAsSet().size() + " in "+ (System.currentTimeMillis() -t1) + "ms");
				long t = System.currentTimeMillis();
				//Run only join mode
//				incrementalTree = joinTreeWithExtraAtomTree(incrementalTree, bodyRew);
//				ucqRewriting = new ArrayList<Clause>( incrementalTree.getRootElement().getSubTreeAsSet() );
//				printTree(incrementalTree);
//				
//				printTree(computeRewritingOfAtomAsTree(queryBody[i], tBoxAxioms, activeVariablesSoFar, false));
				
				// run refinement mode - 4-Aug-2015
				ucqRewriting = new ArrayList<Clause>( m_refine.extendRewriting(incrementalTree, queryBody[i], getSubTreeAsTerms(bodyRew.getRootElement()), activeVariablesSoFar, substitutionsMap, qOpt) );
				logger.info("Finalising with term: " + queryBody[i] + " with UCQ size: " + bodyRew.getRootElement().getSubTreeAsSet().size() + " in " + (System.currentTimeMillis() - t) + "msec" );
			}
			else {
				logger.info("conjoining term: " + queryBody[i] + " with rew size " + bodyRew.getRootElement().getSubTreeAsSet().size() );
				incrementalTree = joinTreeWithExtraAtomTree( incrementalTree , bodyRew );
//				logger.info("term had UCQ size: " + bodyRew.getRootElement().getSubTreeAsSet().size());
				logger.info("start pruning tree with size " + incrementalTree.getRootElement().getSubTreeAsSet().size());
				long startPrunning = System.currentTimeMillis();
//				pruneSubTreeWithNoAnswers(incrementalTree);
				logger.info("Pruning Completed in " + (System.currentTimeMillis() - startPrunning));
			}
			if ( !checkOfconceptTermWithMaxUCQRewAndQueryBodyLength ) {
				logger.info("current size is: " + incrementalTree.getRootElement().getSubTreeAsSet().size() + " joinned in = " + (System.currentTimeMillis()-st) + " ms");
//				for ( Clause c : incrementalTree.getRootElement().getSubTreeAsSet() )
//					System.out.println( c + "\t\t" + substitutionsMap.get(c.m_canonicalRepresentation) );
				logger.debug( "and tree size is: " + incrementalTree.getRootElement().getSubTreeAsSet().size() );
			}
			addNewActiveVariables(activeVariablesSoFar,queryBody[i].getArguments());
		}
		if( conceptTermWithMaxUCQRew != null ) {
			logger.info("Before finalisation UCQ size: " + incrementalTree.getRootElement().getSubTreeAsSet().size() + " in "+ (System.currentTimeMillis() -t1) + "ms");
//			logger.debug("Finalising with term: " + conceptTermWithMaxUCQRew + " with UCQ size: " + maxUCQOfConceptTerm);
			long t = System.currentTimeMillis();
			//run join mode
//			incrementalTree = joinTreeWithExtraAtomTree(incrementalTree, treeOfConceptTermWithMaxUCQRew );
//			ucqRewriting = new ArrayList<Clause>( incrementalTree.getRootElement().getSubTreeAsSet() );
			//run refinement mode - 4-Aug-2015
			ucqRewriting = new ArrayList<Clause>( m_refine.extendRewriting(incrementalTree, conceptTermWithMaxUCQRew, getSubTreeAsTerms(termToTreeUCQRews.get(conceptTermWithMaxUCQRew).getRootElement()), activeVariablesSoFar, substitutionsMap, qOpt) );
			logger.info("Finalising with term: " + conceptTermWithMaxUCQRew + " with UCQ size: " + maxUCQOfConceptTerm + " in " + (System.currentTimeMillis() - t) + "msec" );
		}

		//If there was just a single term then the result is in incrementalTree
		if( queryBody.length == 1 ) {
			ucqRewriting = new ArrayList<Clause>( incrementalTree.getRootElement().getSubTreeAsSet() );
			//run refine mode
			ucqRewriting = redundancyEliminator.pruneAUX( ucqRewriting, true );
			logger.info("Finalising in " + (System.currentTimeMillis() - t1) + "msec with size: " + ucqRewriting.size() );
			rewSizeOriginal = ucqRewriting.size();
			rewSize = rewSizeOriginal;
			rewTime = (System.currentTimeMillis() - t1);
			totalTime = (System.currentTimeMillis() - t1);
			return ucqRewriting;
		}

		long auxT = System.currentTimeMillis();
		//run join mode
//		ucqRewriting = redundancyEliminator.pruneAUX( ucqRewriting );
//		logger.info("prunning AUX in " + (System.currentTimeMillis() - auxT) + "ms" );

		rewSizeOriginal = ucqRewriting.size();
		rewTime = (System.currentTimeMillis() - t1);
		logger.info( "Size of UCQ before subsumption = " + rewSizeOriginal + " computed in " + rewTime + "ms" );

		long st=System.currentTimeMillis();
//		System.out.println("Clauses before subsumption");
//		for ( Clause cla: ucqRewriting )
//			System.out.println(cla);
		ucqRewriting = redundancyEliminator.removeRedundantClauses(ucqRewriting);
		rewSize = ucqRewriting.size();
		subTime = (System.currentTimeMillis()-st);
		logger.info("Subsumption produced UCQ size = " + rewSize + " and required: " + subTime + "ms");

		totalTime = (System.currentTimeMillis() - t1);
		logger.info( "\nFINAL non-redundant UCQ = " + rewSize + " in TOTAL " + totalTime +"ms\n");

//		printTree(incrementalTree);
//		System.out.println("ACTIVE VARS = " + activeVariablesSoFar);
//		System.out.println(qOpt);
		if (qOpt!=null)
			System.out.println( "Joins with no answers = " + qOpt.getJoinsThatCauseClausesNotToHaveAnswers() );
		return ucqRewriting;
	}

	private Set<Term> getSubTreeAsTerms(TreeNode<Clause> rootElement) {
		Set<Term> list = new HashSet<Term>();
		Stack<TreeNode<Clause>> stack = new Stack<TreeNode<Clause>>();
		Set<TreeNode<Clause>> alreadyAdded = new HashSet<TreeNode<Clause>>();
//		alreadyAdded.add( this );
		stack.add(rootElement);
		while (!stack.isEmpty()) {
			TreeNode<Clause> current = stack.pop();
			list.add(current.getNodeLabel().getBody()[0]);
			alreadyAdded.add( current );
			for ( TreeNode<Clause> n : current.getChildren() )
				if ( !alreadyAdded.contains( n ) )
					stack.add( n );
		}
		return list;
	}

	private void addNewActiveVariables(Set<Term> queryActiveVars, Term[] arguments) {
		queryActiveVars.add( arguments[0] );
		if (arguments.length==2)
			queryActiveVars.add( arguments[1] );
	}

	public Tree<Clause> computeRewritingOfAtomAsTree(Term extraAtom, ArrayList<PI> pis , Set<Term> queryVarsSoFar, boolean firstTree) {
		int extraAtomArity = extraAtom.getArity();
		Term[] extraAtomArguments = extraAtom.getArguments();
		Term head = null;

		if (firstTree)
			head = m_termFactory.getFunctionalTerm( "Q", queryVarsSoFar.toArray(new Term[queryVarsSoFar.size()]) );
		else if ( extraAtomArity == 1 && queryVarsSoFar.contains( extraAtomArguments[0] ) )
			head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[0] );
		else if ( extraAtomArity == 2 )
			if ( queryVarsSoFar.contains( extraAtomArguments[0] ) && queryVarsSoFar.contains( extraAtomArguments[1] ) )
				head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[0], extraAtomArguments[1]);
			else if ( queryVarsSoFar.contains( extraAtomArguments[0] ) )
				head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[0] );
			else if ( queryVarsSoFar.contains( extraAtomArguments[1] ) )
				head = m_termFactory.getFunctionalTerm( "Q", extraAtomArguments[1] );
		if ( head == null ) {
			System.err.println( "\ncomputeRewritingOfAtomAs Tree ERROR: " + extraAtom + " " + queryVarsSoFar + " " + firstTree);
			System.exit(0);
		}
		Term[] ebody = new Term[1];
		ebody[0] = extraAtom;
		Clause rootElementClause = new Clause(ebody , head);

		/*
		 * RDFox opt
		 * Since the clause is created with new Clause() constructor the value should be true for toBeSentToRDFox
		 */
		/*
		 * DB optimization
		 */
		if ( qOpt!= null ) // no chance of the extraAtom to be being AUX&& !extraAtom.getFunctionalPrefix().contains("AUX$") )
			//rootElementClause has the sole body atom extraAtom
			qOpt.markNewlyCreatedClause(rootElementClause, extraAtom);

		TreeNode<Clause> rootElement = new TreeNode<Clause>( rootElementClause );
		Tree<Clause> rewritingDAG = new Tree<Clause>(rootElement);

		HashMap<String, TreeNode<Clause>> m_canonicalsToRewOfExtraAtom = new HashMap<String, TreeNode<Clause>>();
		m_canonicalsToRewOfExtraAtom.put(rootElement.getNodeLabel().m_canonicalRepresentation, rootElement );
		Stack<TreeNode<Clause>> toVisit = new Stack<TreeNode<Clause>>();
		toVisit.push(rootElement);
		while(!toVisit.isEmpty()) {
			TreeNode<Clause> currentNode = toVisit.pop();
			Clause currentNodeClause = currentNode.getNodeLabel();
			for (PI pi : pis) {
				Clause cl = applyPositiveInclusion(pi, 0, currentNodeClause);
				if (cl != null) {
					logger.debug( cl + " was produced by " + currentNodeClause + "\t" + pi.m_left + " " + pi.m_type + " " + pi.m_right);
					Term bodyAtom = cl.getBody()[0];

					/*
					 * Optimization about tagging the produced clause wrt being run on RDFox
					 */
					if ( useRDFoxOptimization &&
						//no need to check if it needs to be evaluated because the default value is true
						( pi.m_type == 1 || pi.m_type == 4 ||
						pi.m_type == 7 ||
						pi.m_type == 11 ||
						pi.m_type == 10 ) )
//							cl.setToBeSentToRDFox(true);
//						else
//							( pi.m_type == 2 || pi.m_type == 3 || pi.m_type == 5 || pi.m_type == 6
//								|| pi.m_type == 8 || pi.m_type == 9 ) )
							cl.setToBeSentToRDFox(false);
					logger.debug(cl.getToBeSentToRDFox() + "\n");
					/*
					 * DB optimization
					 *
					 * Optimization about tagging the produced clause wrt if it has answers in the DB
					 */
					if ( qOpt!=null ) {
						if ( !bodyAtom.getFunctionalPrefix().contains("AUX$"))
							qOpt.markNewlyCreatedClause(cl, bodyAtom);
						else
							cl.setToBeEvaluated(false);
					}

					if (!( bodyAtom.getArity() == 1 && Integer.parseInt( bodyAtom.getArgument( 0 ).toString().substring(1, bodyAtom.getArgument( 0 ).toString().length())) > 500 )) {
						TreeNode<Clause> newClauseNode = null;
						TreeNode<Clause> nodeInRew = m_canonicalsToRewOfExtraAtom.get(cl.m_canonicalRepresentation);
						if (nodeInRew == null) {
							newClauseNode = new TreeNode<Clause>( cl );
							m_canonicalsToRewOfExtraAtom.put(cl.m_canonicalRepresentation, newClauseNode );
							toVisit.push( newClauseNode );
						}
						else
							newClauseNode = nodeInRew;

						currentNode.addChild( newClauseNode );
					}
				}
			}
		}
		localSkolemIndex++;
		return rewritingDAG;
	}

	private Tree<Clause> joinTreeWithExtraAtomTree( Tree<Clause> t1 , Tree<Clause> t2 ) {

		Tree<Clause> finalTree = new Tree<Clause>();
		ClauseExtensionCache clauseExtensionCache = new ClauseExtensionCache();
		//Do we need to keep the nodes generated in previous runs??
		m_canonicalsToDAGNodes = new HashMap<String,TreeNode<Clause>>();

		TreeNode<Clause> t1Root = t1.getRootElement();
		TreeNode<Clause> t2Root = t2.getRootElement();

		Clause t1RootClause = t1Root.getNodeLabel();
		Clause t2RootClause = t2Root.getNodeLabel();

		Set<TreeNode<Clause>> alreadyUsedT1 = new HashSet<TreeNode<Clause>>();

		if (isEligibleForProcessing( t1RootClause, t2RootClause , substitutionsMap.get( t1RootClause.m_canonicalRepresentation ) ) ) {
			Clause rootClause = clauseExtensionCache.extendClauseWithTerm( t1RootClause, t2RootClause );
			//If t1RootClause OR t2RootClause are not elidgible for answering OR t1RootClause contains a

			TreeNode<Clause> rootNode = new TreeNode<Clause>( rootClause );

			finalTree.setRootElement( rootNode );

			Substitution sub = clauseExtensionCache.isContainedIn(t1RootClause, t2RootClause);

//			if (subs != null)
//				System.out.println( sub + "\t\t" + t1RootClause + "\t\t" + t2RootClause );
//			for ( Substitution sub : subs )
			/**
			 * 3-Aug-2015
			 * ERROR????
			 */
			if ( sub != null ) {
				//brikame to idio atomo akrivws
				if ( sub.isEmpty() ) {
					//commented 3-Aug-2015
//					if ( t2RootClause.getBody()[0].getArity() == 1 || sub.isEmpty() )
						return t1;
					//role
					//commented 3-Aug-2015
//					else if ( t2RootClause.getBody()[0].getArity() == 2 ) {
//						rootNode.addChild( t1Root );
//						return finalTree;
//					}
				}
				else {
					Variable mappedVariable = sub.keySet().iterator().next();
					//mappedVariable exists in t1.rootNode
					if ( t1RootClause.getVariables().contains( mappedVariable )  ) {
						//We do not create t1+t2 because it has already been created (finalTree.setRootElement( rootNode ))
						for ( TreeNode<Clause> t1Child : t1Root.getChildren() ) {
							Clause t1ChildClause = t1Child.getNodeLabel();

							//t1ChildClause does not contain equalRole which has been replaced by a concept
							if ( !t1ChildClause.m_canonicalRepresentation.contains( equalRole.toString() ) ) {
								Term termThatReplaces = findTermThatReplacesEqualRole( t1RootClause , t1ChildClause );
								if ( termThatReplaces!= null && termThatReplaces.getArity() == 1) {

									TreeNode<Clause> t1NewChild = renameGraph(t1Child,sub);
									rootNode.addChild( t1NewChild );

									addSubstitutionToSubTree(sub, t1NewChild );
									alreadyUsedT1.add( t1Child );
								}
							} //else
								//;
						}
					}
					else if (t2.getRootElement().getNodeLabel().getBody()[0].getArity() == 1 ) {
						addSubstitutionToSubTree( sub, t1.getRootElement() );
//						return t1;
					}
					else if ( t2.getRootElement().getNodeLabel().getBody()[0].getArity() == 2 ) {
						addSubstitutionToSubTree( sub, t1.getRootElement() );
						rootNode.addChild( t1.getRootElement() );
//						return finalTree;
					}
				}
			}
			m_canonicalsToDAGNodes.put(rootClause.m_canonicalRepresentation, rootNode);
		}
		else
			return t1;

//		printTree(finalTree);

		Queue<TreeNode<Clause>> queueT1 = new LinkedList<TreeNode<Clause>>(Collections.singleton(t1.getRootElement()));
		while (!queueT1.isEmpty()) {
			TreeNode<Clause> currentT1 = queueT1.poll();

			if (alreadyUsedT1.contains(currentT1))
				continue;
			alreadyUsedT1.add(currentT1);

			Clause currentT1Clause = currentT1.getNodeLabel();
			if ( isEligibleForProcessing( currentT1Clause, t2.getRootElement().getNodeLabel(), substitutionsMap.get(currentT1Clause.m_canonicalRepresentation) ) ) {
				Queue<TreeNode<Clause>> queueT2 = new LinkedList<TreeNode<Clause>>(Collections.singleton(t2.getRootElement()));

				Set<TreeNode<Clause>> alreadyUsedT2 = new LinkedHashSet<TreeNode<Clause>>();
				while (!queueT2.isEmpty()) {
					TreeNode<Clause> currentT2 = queueT2.poll();

					if (alreadyUsedT2.contains( currentT2 ))
						continue;
					alreadyUsedT2.add(currentT2);

//					System.out.println( "ADDING " + currentT1Clause + "\tTO\t" + currentT2.getNodeLabel() + " \tWITH\t" + renameExtraAtom + "\t\t" + substitutionsMap.get(currentT1Clause.m_canonicalRepresentation) );

					Clause currentClauseExtended;
					if ( renameExtraAtom ) {
						currentClauseExtended = addTermToBodyOfClauseNoEqCheck(currentT1Clause, getNewQuery( substitutionsMap.get(currentT1Clause.m_canonicalRepresentation), currentT2.getNodeLabel() ) );
						renameExtraAtom = false;
					}
					else
						currentClauseExtended = addTermToBodyOfClauseNoEqCheck(currentT1Clause, currentT2.getNodeLabel());

					TreeNode<Clause> currentNodeInFinalTree = getEquivalentNodeAlreadyInDAG( currentClauseExtended );
					if (currentNodeInFinalTree == null)
						currentNodeInFinalTree = new TreeNode<Clause>( currentClauseExtended );

//					System.out.println( "CURRENT NODE = " + currentNodeInFinalTree.getNodeLabel() );

//					boolean subTreeHasBeenCopied=false;
					//bazoume sto currentT1+currentT2 san paidia tou ton currentT1+paidiaTouCurrentT2
					for (TreeNode<Clause> childOfCurrentT2 : currentT2.getChildren()) {
//						Set<Substitution> subs = clauseExtensionCache.isContainedInSet( currentT1Clause, childOfCurrentT2.getNodeLabel() );
						Substitution sub = clauseExtensionCache.isContainedIn( currentT1Clause, childOfCurrentT2.getNodeLabel() );
//						if (subs != null )
//						for ( Substitution sub :subs )
						if ( sub != null ) {
							//identical substitution
							if ( sub.isEmpty() ) {
								currentNodeInFinalTree.addChild( currentT1 );
								alreadyUsedT1.addAll(currentT1.getChildren());
//								subTreeHasBeenCopied=true;
//								break;
							}
							//exists substitution
							else {
								Variable mappedVariable = sub.entrySet().iterator().next().getKey();

								Clause newClause = clauseExtensionCache.extendClauseWithTerm( currentT1Clause, childOfCurrentT2.getNodeLabel() );
								TreeNode<Clause> newNode = getEquivalentNodeAlreadyInDAG(newClause);
								if ( newNode == null )
									newNode = new TreeNode<Clause> ( newClause );

								currentNodeInFinalTree.addChild( newNode );
								m_canonicalsToDAGNodes.put(newClause.m_canonicalRepresentation, newNode);
//								substitutionsMap.put(newClause.m_canonicalRepresentation, addSubstitution( substitutionsMap.get( newClause.m_canonicalRepresentation ) , substitutionsMap.get(currentT1Clause.m_canonicalRepresentation) ) );

								//mappedVariable exists in currentT1Clause
								if ( currentT1Clause.getVariables().contains(mappedVariable) ) {

									boolean childOfCurrentT2InQ = false;
									//must see if equalRole is contained in its children (or replaced by a role) or if it has been replaced by a concept
									for ( TreeNode<Clause> childOfCurrentT1Node : currentT1.getChildren() ) {
										Clause childOfCurrentT1Clause = childOfCurrentT1Node.getNodeLabel();
										//not contained in children and instead exists a concept
//										//equalRole can be null only when possitives are used in isContained
//										if ( equalRole == null )
//											equalRole = findTermThatReplacesEqualRole(currentT1Clause, childOfCurrentT2.getNodeLabel());
										if ( !childOfCurrentT1Clause.m_canonicalRepresentation.contains( equalRole.toString() ) &&
												findTermThatReplacesEqualRole( currentT1Clause , childOfCurrentT1Clause ).getArity() == 1 ) {

											TreeNode<Clause> newChildOfCurrentT1 = renameGraph( childOfCurrentT1Node, sub );
											newNode.addChild(newChildOfCurrentT1);
											addSubstitutionToSubTree(sub, newChildOfCurrentT1);
											alreadyUsedT1.add( childOfCurrentT1Node );
										} else
											childOfCurrentT2InQ = true;
//											if ( !queueT2.contains(childOfCurrentT2) )
//												queueT2.add(childOfCurrentT2);
									}
									if ( childOfCurrentT2InQ )
										queueT2.add(childOfCurrentT2);
								}
								//mappedVariable does not exist in currentT1Clause
								else {
									newNode.addChild(currentT1);
//									avenet 25/06/2012
//									currentNodeInFinalTree.addChild( currentT1 );
									addSubstitutionToSubTree( sub, currentT1 );
//									alreadyUsedT1.addAll(currentT1.getChildren());
//									subTreeHasBeenCopied=true;
//									break;
								}
							}
						}
						else {
							Clause newClause = clauseExtensionCache.extendClauseWithTerm( currentT1Clause, childOfCurrentT2.getNodeLabel() );
							TreeNode<Clause> newNode = getEquivalentNodeAlreadyInDAG(newClause);
							if ( newNode == null )
								newNode = new TreeNode<Clause> ( newClause );
							queueT2.add(childOfCurrentT2);
							currentNodeInFinalTree.addChild( newNode );
							m_canonicalsToDAGNodes.put(newClause.m_canonicalRepresentation, newNode);
						}
					}
//					if( subTreeHasBeenCopied )
//						break;
					//bazoume ston currentT1+currentT2 san paidia tou ta paidiaToucurrentT1+currentT2
					Set<TreeNode<Clause>> currentT1Children = currentT1.getChildren();
					for (TreeNode<Clause> childOfCurrentT1 : currentT1Children) {
						if (isEligibleForProcessing(childOfCurrentT1.getNodeLabel() , t2.getRootElement().getNodeLabel(), substitutionsMap.get(childOfCurrentT1.getNodeLabel().m_canonicalRepresentation) )) {
							//term appears in the body of clause to be joined
//							Set<Substitution> subs = clauseExtensionCache.isContainedInSet(childOfCurrentT1.getNodeLabel(), currentT2.getNodeLabel());
							Substitution sub = clauseExtensionCache.isContainedIn(childOfCurrentT1.getNodeLabel(), currentT2.getNodeLabel());

//							if (subs!=null)
//							for ( Substitution sub :subs )
							if ( sub != null ) {
								//identical substitution (role or concept)
								if ( sub.isEmpty() )
									currentNodeInFinalTree.addChild( childOfCurrentT1 );
								//not identical substitution (role)
								else {
									Variable mappedVariable = sub.entrySet().iterator().next().getKey();

									Clause newClause = clauseExtensionCache.extendClauseWithTerm( childOfCurrentT1.getNodeLabel() , currentT2.getNodeLabel() );
									TreeNode<Clause> newNode = getEquivalentNodeAlreadyInDAG(newClause);
									if ( newNode == null )
										newNode = new TreeNode<Clause> ( newClause );

									currentNodeInFinalTree.addChild( newNode );
									m_canonicalsToDAGNodes.put(newClause.m_canonicalRepresentation, newNode);
//									substitutionsMap.put(newClause.m_canonicalRepresentation, addSubstitution( substitutionsMap.get( newClause.m_canonicalRepresentation ) , substitutionsMap.get(childOfCurrentT1.getNodeLabel().m_canonicalRepresentation) ) );

									//mappedVariable exists in currentT1Clause0
									if ( currentT1Clause.getVariables().contains(mappedVariable) ) {
										boolean childOfCurrentT1InQ = false;
										//must see if equalRole is contained in its children (or replaced by a role) or if it has been replaced by a concept
										for ( TreeNode<Clause> childOfChildOfCurrentT1 : childOfCurrentT1.getChildren() ) {
											Clause childOfChildOfCurrentT1Clause = childOfChildOfCurrentT1.getNodeLabel();
											//replaced by a concept
											if ( !childOfChildOfCurrentT1Clause.m_canonicalRepresentation.contains( equalRole.toString() ) &&
													findTermThatReplacesEqualRole( childOfCurrentT1.getNodeLabel(), childOfChildOfCurrentT1Clause ).getArity() == 1 ) {
												TreeNode<Clause> newChildOfCurrentT1 = renameGraph(childOfCurrentT1 , sub);
												newNode.addChild(newChildOfCurrentT1);
												addSubstitutionToSubTree(sub, newChildOfCurrentT1);
												alreadyUsedT1.add( childOfCurrentT1 );
											} else
												childOfCurrentT1InQ = true;
//												if ( !queueT1.contains(childOfCurrentT1) )
//													queueT1.add(childOfCurrentT1);
										}
										if ( childOfCurrentT1InQ )
											queueT1.add(childOfCurrentT1);
									} else {
										newNode.addChild(childOfCurrentT1);
//										avenet 25/06/2012 swap comments
//										currentNodeInFinalTree.addChild(childOfCurrentT1);
//										addSubstitutionToSubTree(sub, childOfCurrentT1);
									}
								}
							}
							else {
								Clause newClause;
								if (renameExtraAtom) {
									newClause= clauseExtensionCache.extendClauseWithTerm( childOfCurrentT1.getNodeLabel(), getNewQuery(substitutionsMap.get( childOfCurrentT1.getNodeLabel().m_canonicalRepresentation ), currentT2.getNodeLabel() ) );
									renameExtraAtom = false;
								}
								else
									newClause= clauseExtensionCache.extendClauseWithTerm( childOfCurrentT1.getNodeLabel(), currentT2.getNodeLabel() );
								TreeNode<Clause> newNode = getEquivalentNodeAlreadyInDAG(newClause);
								if ( newNode == null )
									newNode = new TreeNode<Clause>(newClause);

								currentNodeInFinalTree.addChild( newNode );

								queueT1.add(childOfCurrentT1);
								m_canonicalsToDAGNodes.put(newClause.m_canonicalRepresentation, newNode);
							}
						}
					}
				}
			}
		}

//		if (print) {
//			System.out.println("\n\n");
//			for ( Entry<String, TreeNode<Clause>> entry : m_canonicalsToDAGNodes.entrySet())
//				System.out.println( entry.getKey() );
//			System.out.println("\n\n");
//			printTree(finalTree);
//			System.out.println("\n\n");
//
//		}

//		System.out.println( "\n\nSubstitutions: ");
//		for ( String en : substitutionsMap.keySet() ) {
//			System.out.println( en+ "\t\t" + substitutionsMap.get(en) );
//		}
//		printTree(finalTree);
		return finalTree;
	}


	private Term findTermThatReplacesEqualRole(Clause originalClause, Clause newClause) {

		Term[] newBody = newClause.getBody();
		for ( int i = 0 ; i < newBody.length ; i++ )
			if ( !originalClause.m_canonicalRepresentation.contains( newBody[i].toString() ) )
				return newBody[i];
		return null;
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
	public Clause getNewQuery(Substitution mgu, Clause clause){

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

        return new Clause(body, head);
	}



	private void addSubstitutionToSubTree(Substitution sub,	TreeNode<Clause> node) {

		Set<Clause> subTreeAsSet = node.getSubTreeAsSet();
		subTreeAsSet.add(node.getNodeLabel());

//		System.out.println( "Add Sub to subtree" + sub + "\t\t\t" + subTreeAsSet);

		for ( Clause clause : subTreeAsSet ) {
			//replace the substitution of clause with one that also contains sub
//			System.out.println( "\nAdding Substitution to " + clause + "\t\t" + substitutionsMap.get(clause.m_canonicalRepresentation) + "\t\t" + sub + "\n");
			Substitution newSub = null;
			if (substitutionsMap.get(clause.m_canonicalRepresentation)!=null) {
				newSub = addSubstitution(substitutionsMap.get(clause.m_canonicalRepresentation), sub);
//				System.out.println ( "\t\t\t\t\t" + sub + "+" + substitutionsMap.get(clause.m_canonicalRepresentation) + " = " + newSub);
				if ( !substitutionsMap.get(clause.m_canonicalRepresentation).isEmpty() )
					substitutionsMap.remove(clause.m_canonicalRepresentation);
			}
			else
				newSub = sub;

			substitutionsMap.put(clause.m_canonicalRepresentation, newSub);
//			System.out.println(clause + "\t\t" + substitutionsMap.get(clause.m_canonicalRepresentation) + "\n");
		}

	}

	private TreeNode<Clause> getEquivalentNodeAlreadyInDAG( Clause newClause ){
		return m_canonicalsToDAGNodes.get(newClause.m_canonicalRepresentation);
	}

	private Clause addTermToBodyOfClauseNoEqCheck(Clause clauseFromH1, Clause clauseFromH2) {

		Term[] body = clauseFromH1.getBody();
		Term[] newBody = new Term[body.length + 1];

		for (int i = 0 ; i < body.length ; i++)
			newBody[i] = body[i];
		newBody[body.length] = clauseFromH2.getBody()[0];
		return new Clause( newBody, clauseFromH1.getHead() );
	}

	private Substitution isEqualRoleTerm( Clause currentQuery, Term extraRoleAtom, Term bodyAtom ) {

		Substitution s = new Substitution();

		if (bodyAtom.getName().equals(extraRoleAtom.getName())) {
			Term[] extraRoleAtomVariables = extraRoleAtom.getArguments();
			Term[] bodyAtomVariables = bodyAtom.getArguments();

			if (bodyAtomVariables[1].equals( extraRoleAtomVariables[1] ) && bodyAtomVariables[0].equals( extraRoleAtomVariables[0]))
				return s;
			else if ( bodyAtomVariables[0].toString().equals( extraRoleAtomVariables[0].toString() ) ) {
				if ( extraRoleAtomVariables[1].isVariable() )
					s.put( (Variable) extraRoleAtomVariables[1], bodyAtomVariables[1]);
				else
				{
					if ( bodyAtomVariables[1].isVariable() )
						s.put((Variable) bodyAtomVariables[1], extraRoleAtomVariables[1]);
					else
						return null;
				}
				return s;
			}
			else if ( bodyAtomVariables[1].toString().equals( extraRoleAtomVariables[1].toString() ) ) {
				if (extraRoleAtomVariables[0].isVariable() )
					s.put( (Variable) extraRoleAtomVariables[0], bodyAtomVariables[0]);
				else
				{
					if ( bodyAtomVariables[0].isVariable() )
						s.put((Variable) bodyAtomVariables[0], extraRoleAtomVariables[0]);
					else
						return null;
				}
				return s;
			}
		}
		return null;
	}

	//Returns true if avar(extra) is a subset of var(clause)
	public boolean isEligibleForProcessing(Clause clause, Clause atomClause , Substitution sub) {

		ArrayList<Variable> atomClauseDistinguishedVars = atomClause.getDistinguishedVariables();
		ArrayList<Variable> clauseVariables = clause.getVariables();

		if ( clauseVariables.containsAll( atomClauseDistinguishedVars ) )
			return true;
		else //one distinguished variable
			if ( atomClauseDistinguishedVars.size() == 1 ) {
				if ( findMappedVariableInClause( clause , sub , atomClauseDistinguishedVars.get( 0 ) ) ) {
					renameExtraAtom = true;
					return true;
				}
				else if ( findMappedVariableInClause( clause , sub , atomClauseDistinguishedVars.get( 0 ) ) &&
					findMappedVariableInClause( clause , sub , atomClauseDistinguishedVars.get( 1 ) ) ) {
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

		if ( clauseVariables.contains( currentVar ) )
			return true;

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

	public Term[] findRightOrderOfQuery(Clause query) {
		Set<Term> queryBodyTerms = new HashSet<Term>();
		Set<Term> queryDistVars = new HashSet<Term>(query.getDistinguishedVariables());

		for (Term origQueryTerm : query.getBody())
			queryBodyTerms.add( origQueryTerm );

		LabeledGraph<Term, Term, Term> queryGraph = new LabeledGraph<Term,Term,Term>();
		for (Term origQueryTerm : query.getBody()) {

			Term[] variablesOfTerm = origQueryTerm.getArguments();
			if (variablesOfTerm.length == 2)
				queryGraph.addEdge(variablesOfTerm[0], variablesOfTerm[1], origQueryTerm);
			else
				queryGraph.addLabel(variablesOfTerm[0], origQueryTerm);
		}
		Term[] body = new Term[queryBodyTerms.size()];
		Set<Term> rootVarsThatAreDistinct = new HashSet<Term>();
		Set<Term> rootVarsThatAreNotDistinct = new HashSet<Term>();
		Stack<Term> toVisit = new Stack<Term>();
//		for (Term vertice : queryGraph.getElements())
////			if (queryGraph.getPredecessors(vertice).isEmpty() )
//				// These lines are for starting from the rootDistinguished variables and not from any root variable
//				if (queryDistVars.contains(vertice))
//					rootVarsThatAreDistinct.add(vertice);
//				else
//					rootVarsThatAreNotDistinct.add(vertice);
////				toVisit.add(vertice);	//This is for starting from any rootVariable
//		// These lines are for starting from the rootDistinguished variables and not from any root variable
//		toVisit.addAll(rootVarsThatAreNotDistinct);
//		toVisit.addAll(rootVarsThatAreDistinct);
//
//		if (toVisit.isEmpty())
			toVisit.addAll(queryDistVars);
		Set<Term> visitedNodes = new HashSet<Term>();
		int orderedQueryIndex=0;
		Set<Term> coneptAtoms = new HashSet<Term>();
		while (!toVisit.isEmpty()) {
			Term currentVar = toVisit.pop();
			if (visitedNodes.contains(currentVar))
				continue;
			for (LabeledGraph<Term,Term,Term>.Edge edge : queryGraph.getSuccessors(currentVar))
				if (!visitedNodes.contains(edge.getToElement())) {
					body[orderedQueryIndex++] = edge.getEdgeLabel();
					toVisit.add(edge.getToElement());
				}
			for (LabeledGraph<Term,Term,Term>.Edge edge : queryGraph.getPredecessors(currentVar))
				if (!edge.getToElement().equals(currentVar) && !visitedNodes.contains(edge.getToElement())) {
					body[orderedQueryIndex++] = edge.getEdgeLabel();
					toVisit.add(edge.getToElement());
				}
			visitedNodes.add( currentVar );
			coneptAtoms.addAll(queryGraph.getLabelsOfNode(currentVar));
		}
		for (Term conceptAtom : coneptAtoms)
			body[orderedQueryIndex++] = conceptAtom;
		orderedQueryIndex=0;
		return body;
	}

	public static Clause applyPositiveInclusion(PI pi, int atomIndex, Clause clause){

		Term newAtom = null;
		Term g = clause.getBody()[atomIndex];

		if (pi.m_right.equals(g.getName())) {
			/**
			 * unary atom
			 *
			 * We consider that we are not going to have queries of the form q(x)<-R(x,"a"),C("a")
			 *
			 * Hence, when a query with a constant in a concept is issued then we do not apply any PI to that concept
			 */
			if (g.getArity() == 1) {
				Term arg0 = g.getArguments()[0];
				if ( arg0.isConstant() )
					return null;
				Variable var1 = (Variable)arg0;
				switch (pi.m_type) {
				case 1:
					newAtom = m_termFactory.getFunctionalTerm(pi.m_left, m_termFactory.getVariable(var1.m_index));
					break;
				case 4:
						newAtom = m_termFactory.getFunctionalTerm(pi.m_left, new Term[]{m_termFactory.getVariable(var1.m_index), m_termFactory.getVariable(500+(localSkolemIndex))});
					break;
				case 7:
						newAtom = m_termFactory.getFunctionalTerm(pi.m_left, new Term[]{m_termFactory.getVariable(500+(localSkolemIndex)), m_termFactory.getVariable(var1.m_index)});
					break;
				default:
					return null;
				}
			}
			// Binary atom
			else {

				Term arg1 = g.getArguments()[0];
				Term arg2 = g.getArguments()[1];
				Variable var1 = null;
				Variable var2 = null;

				if ( arg1.isConstant() ) {
					if (arg2.isVariable() ) {
						var2 = (Variable)arg2;
					}
					else
						return null;
				}
				else {
					var1 = (Variable)arg1;
					if (arg2.isVariable() ) {
						var2 = (Variable)arg2;
					}
				}

				if(arg2.isVariable() && !clause.isBound((Variable)arg2))
					switch(pi.m_type){
					case 2:
						newAtom = m_termFactory.getFunctionalTerm(pi.m_left, arg1);
						break;
					case 5:
							newAtom = m_termFactory.getFunctionalTerm(pi.m_left, new Term[]{arg1, m_termFactory.getVariable(500+(localSkolemIndex))});
						break;
					case 8:
							newAtom = m_termFactory.getFunctionalTerm(pi.m_left, new Term[]{m_termFactory.getVariable(500+(localSkolemIndex)), arg1});
						break;
					case 10:
						newAtom = m_termFactory.getFunctionalTerm(pi.m_left, new Term[]{arg1, arg2});
						break;
					case 11:
						newAtom = m_termFactory.getFunctionalTerm(pi.m_left, new Term[]{arg2, arg1});
						break;
					default:
						return null;
					}
				else if( arg1.isVariable() && !clause.isBound((Variable)arg1) )
					switch(pi.m_type){
					case 3:
						newAtom = m_termFactory.getFunctionalTerm(pi.m_left, arg2);
						break;
					case 6:
							newAtom = m_termFactory.getFunctionalTerm(pi.m_left, new Term[]{arg2, m_termFactory.getVariable(500+(localSkolemIndex))});
						break;
					case 9:
							newAtom = m_termFactory.getFunctionalTerm(pi.m_left, new Term[]{m_termFactory.getVariable(500+(localSkolemIndex)), arg2});
						break;
					case 10:
						newAtom = m_termFactory.getFunctionalTerm(pi.m_left, new Term[]{arg1, arg2});
						break;
					case 11:
						newAtom = m_termFactory.getFunctionalTerm(pi.m_left, new Term[]{arg2, arg1});
						break;
					default:
						return null;
					}
				else
					switch (pi.m_type) {
					case 10:
						newAtom = m_termFactory.getFunctionalTerm(pi.m_left, new Term[]{arg1, arg2});
						break;
					case 11:
						newAtom = m_termFactory.getFunctionalTerm(pi.m_left, new Term[]{arg2, arg1});
						break;
					default:
						return null;
					}
			}
		}
		else
			return null;

		Set<Term> newBody = new LinkedHashSet<Term>();
		newBody.add(newAtom);
		// Copy the other atoms from the clause
	    for (int index=0; index < clause.getBody().length; index++)
	    	 if (index != atomIndex)
		    	 newBody.add(clause.getBody()[index].apply(Substitution.IDENTITY, m_termFactory));

	    // New body and head
	    Term[] body = new Term[newBody.size()];
	    newBody.toArray(body);
	    Term head = clause.getHead().apply(Substitution.IDENTITY, m_termFactory);

	    return new Clause(body, head);
	}

	private Substitution addSubstitution(Substitution s, Substitution sub) {

		if ( s == null )
			return sub;

		if ( sub == null )
			return null;

		Substitution subst = new Substitution();
		for ( Entry<Variable, Term> e : s.entrySet() )
			subst.put(e.getKey(), e.getValue());
		subst.putAll(sub);
		return subst;
	}

	/**
	 * 19/4/2016
	 * @param originalTree
	 */
	private void pruneSubTreeWithNoAnswers(Tree<Clause> originalTree) {
		TreeNode<Clause> root = originalTree.getRootElement();

		Stack<TreeNode<Clause>> toVisit = new Stack<TreeNode<Clause>>();
		Set<String> visited = new HashSet<String>();
		toVisit.push(root);
		int i = 0;
		while ( !toVisit.isEmpty() ) {
//			System.out.println(i);
//			i++;
			TreeNode<Clause> parentNode = toVisit.pop();
			if (visited.contains(parentNode.getNodeLabel().m_canonicalRepresentation))
				continue;
			visited.add(parentNode.getNodeLabel().m_canonicalRepresentation);
			Set<TreeNode<Clause>> parentNodeChildren = parentNode.getChildren();

//			System.out.println(parentNode.getNodeLabel() + " has #of direct children = " + parentNodeChildren.size() +
//					" and #of children " + parentNode.getSubTreeAsSet().size());
			Set<TreeNode<Clause>> childrenToBeRemoved = new HashSet<>();
			int counter = 0, numOfChildrenRemoved = 0;
			for ( TreeNode<Clause> child: parentNodeChildren ) {
				Set<Clause> childs = child.getSubTreeAsSet();
				if ( checkThatSubTreeDoesNotHaveAnswers(childs) ) {
					counter++;
					numOfChildrenRemoved+=childs.size();
					childrenToBeRemoved.add(child);
				}
				else {
					Set<TreeNode<Clause>> childChildren = child.getChildren();
					for ( TreeNode<Clause> childChild: childChildren) {
						if (!visited.contains(childChild.getNodeLabel().m_canonicalRepresentation)) {
							toVisit.push(childChild);
						}
//						else
//							System.out.println("KYKLOS");
					}
				}
			}
			for (TreeNode<Clause> childToBeRemoved: childrenToBeRemoved)
				parentNode.removeChild(childToBeRemoved);
//			System.out.println(parentNode.getNodeLabel() + " has removed " + counter + "\t\t" + numOfChildrenRemoved +
//					" and has #of total children " + parentNode.getSubTreeAsSet().size() );
//
//			System.out.println("Original Tree has size " + originalTree.getRootElement().getSubTreeAsSet().size());
		}
	}

	private boolean checkThatSubTreeDoesNotHaveAnswers(Set<Clause> clauses) {

		for (Clause clause:clauses)
			if (clause.getToBeEvaluated()==true && !clause.m_canonicalRepresentation.contains("AUX"))
				return false;
		return true;
	}


	private class ClauseExtensionCache {
		private Map<Clause,Set<Clause>> nodesToPossitive = new HashMap<Clause,Set<Clause>> ();
		private Map<Clause,Set<Clause>> nodesToNegative = new HashMap<Clause,Set<Clause>> ();
		private Map<String,Clause> clausesToTheirExtension = new HashMap<String,Clause> ();

		private Set<Substitution> isContainedInSet(Clause clauseFromH1, Clause clauseFromH2) {
			Set<Substitution> result = new HashSet<Substitution>();
			Term[] body = clauseFromH1.getBody();
			Term termFromH2 = clauseFromH2.getBody()[0];
			String termString = termFromH2.toString();
			if (termFromH2.getArity() == 1) {
				for (int i = 0 ; i < body.length ; i++)
					if ( termString.equals( body[i].toString() ) )
						result.add(new Substitution());
//						return new Substitution();
			}
			else if (termFromH2.getArity() == 2) {
				equalRole = null;
				for (int i = 0 ; i < body.length ; i++) {
					Substitution sub = isEqualRoleTerm(clauseFromH1, termFromH2, body[i]);
					if ( sub != null ) {
						substitutionsMap.put(clauseFromH1.m_canonicalRepresentation, addSubstitution( substitutionsMap.get(clauseFromH1.m_canonicalRepresentation ), sub ) );
						equalRole = body[i];
						result.add(sub);
//						return sub;
					}
				}
			}
			if ( result.isEmpty() )
				return null;
			else return result;
		}

		private Substitution isContainedIn(Clause clauseFromH1, Clause clauseFromH2) {
			Term[] body = clauseFromH1.getBody();
			Term termFromH2 = clauseFromH2.getBody()[0];
			String termString = termFromH2.toString();
			if (termFromH2.getArity() == 1) {
				for (int i = 0 ; i < body.length ; i++)
					if ( termString.equals( body[i].toString() ) )
						return new Substitution();
			}
			else if (termFromH2.getArity() == 2) {
				equalRole = null;
				for (int i = 0 ; i < body.length ; i++) {
					Substitution sub = isEqualRoleTerm(clauseFromH1, termFromH2, body[i]);
					if ( sub != null ) {
						substitutionsMap.put(clauseFromH1.m_canonicalRepresentation, addSubstitution( substitutionsMap.get(clauseFromH1.m_canonicalRepresentation ), sub ) );
						equalRole = body[i];
						return sub;
					}
				}
			}
			return null;
		}

		// Adds new term to the body of clause and creates a new clause. We also keep track of the terms of the body of the clause that are equal to term
		private Clause extendClauseWithTerm(Clause clauseFromH1, Clause clauseFromH2) {
			Clause cl = clausesToTheirExtension.get(clauseFromH1.toString()+clauseFromH2.toString());
			if (cl!=null)
				return cl;
			Term[] body = clauseFromH1.getBody();
			Term[] newBody = new Term[body.length + 1];
			Term termFromH2 = clauseFromH2.getBody()[0];

//			if (termFromH2.getArity() == 1)
				for (int i = 0 ; i < body.length ; i++) {
					newBody[i] = body[i];
//					System.out.println(body[i]);
				}
//				System.out.println();
//			else if (termFromH2.getArity() == 2)
//				for (int i = 0 ; i < body.length ; i++)
//					newBody[i] = body[i];
			newBody[body.length] = termFromH2;
			cl = new Clause(newBody, clauseFromH1.getHead());
			/*
			 * RDFox Opt
			 */
			if (useRDFoxOptimization )
				cl.setToBeSentToRDFox( clauseFromH1.getToBeSentToRDFox() && clauseFromH2.getToBeSentToRDFox() );

			//avenet 3-Aug-2015
			/*
			 * DB opt
			 *
			 * Mark computed clause wrt to possible answers
			 */
			if (qOpt!=null)
				qOpt.markClauseWRTAnswers(clauseFromH1, clauseFromH2, cl);

			clausesToTheirExtension.put(clauseFromH1.toString()+clauseFromH2.toString(), cl);

			return cl;
		}
	}


	public void printTree( Tree<Clause> tree) {

		System.out.println("START");
//		Queue<Node<T>> queue = new LinkedList<Node<T>>();
		Set<TreeNode<Clause>> queue = new HashSet<TreeNode<Clause>>();
		queue.add( tree.getRootElement() );
		Set<TreeNode<Clause>> alreadyAddedToQueue = new HashSet<TreeNode<Clause>>();
		Set<TreeNode<Clause>> alreadyPrinted = new HashSet<TreeNode<Clause>>();
		alreadyAddedToQueue.add( tree.getRootElement() );

		while ( !queue.isEmpty() ) {
//			Node<T> cn = queue.poll();
			TreeNode<Clause> cn = queue.iterator().next();
			queue.remove(cn);
			if ( alreadyPrinted.contains(cn) )
				continue;
//			if ( !cn.getNodeLabel().containsAUXPredicates() )
			System.out.println( cn.getNodeLabel() + "\t\t\t" + cn.getNodeLabel().getToBeEvaluated());
//			System.out.println( cn.getNodeLabel() + "\t\t\t" + substitutionsMap.get(cn.getNodeLabel().m_canonicalRepresentation));
			alreadyPrinted.add( cn );
			Set<TreeNode<Clause>> cnChl = cn.getChildren();
			for ( TreeNode<Clause> nc : cnChl )
			{
//				if ( !nc.getNodeLabel().containsAUXPredicates() )
				System.out.println( "\t\t\t\t" + nc.getNodeLabel() + "\t\t" + nc.getNodeLabel().getToBeEvaluated());
//				System.out.println( "\t\t\t\t" + nc.getNodeLabel() + "\t\t" + substitutionsMap.get(nc.getNodeLabel().m_canonicalRepresentation));
				if ( !alreadyAddedToQueue.contains( nc ))
				{
					queue.add( nc );
					alreadyAddedToQueue.add( nc );
				}
			}
		}
		System.out.println("END Printing tree \n");
	}
}