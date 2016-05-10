package edu.aueb.queries;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.oxford.comlab.perfectref.rewriter.Clause;
import org.oxford.comlab.perfectref.rewriter.Constant;
import org.oxford.comlab.perfectref.rewriter.FunctionalTerm;
import org.oxford.comlab.perfectref.rewriter.Term;
import org.oxford.comlab.perfectref.rewriter.TermFactory;
import org.oxford.comlab.perfectref.rewriter.Variable;


public class ClauseParser {
	
	private static TermFactory m_termFactory;
	
	public static void main(String args[]) throws FileNotFoundException, IOException {
//		String query = "/Users/avenet/query.txt";
//		Clause c = new ClauseParser().parseClause((new BufferedReader(new FileReader(query))).readLine());
//		System.out.println(c.m_canonicalRepresentation);
//		Term[] body = c.getBody();
//		for (Term b: body)
//			System.out.println(b + "\t\t" +b.get.getClass());
//		m_termFactory = new TermFactory();
//		Constant con = m_termFactory.getConstant("Tassos");
//		System.out.println(con.isConstant());
	}

	private String IMPLIES_SYMBOL = "<-";
	private String VARIABLE_SYMBOL = Variable.VARIABLE_PREFIX;
	
	public ClauseParser() {}
	
	public Clause parseClause(String s) {
		
		m_termFactory = new TermFactory();
		
		int pos = s.indexOf(IMPLIES_SYMBOL);
		
		String headString = s.substring(0,pos).trim();
		String bodyString = s.substring(pos + 2).trim();
		
		Term head = parseAtom(headString);
		
		ArrayList<String> v = split(bodyString);

		Term body[] = new Term[v.size()];
		ArrayList<Term> bodyTerms = new ArrayList<Term>();
		
		for (int i = 0; i < v.size(); i++) {
			Term newAtom = parseAtom(v.get(i));
			if (!bodyTerms.contains(newAtom)) {
				bodyTerms.add(newAtom);
				body[i] = newAtom;
			}
		}

		return new Clause(body, head);
	}
	
	public Term parseAtom(String s) {

		int pos1 = s.indexOf("(");
		int pos2 = s.lastIndexOf(")");

		if (pos1 < 0) {
			return m_termFactory.getFunctionalTerm(s, null);
		} else {
			
			String pre = s.substring(0, pos1).trim();
			String arg = s.substring(pos1 + 1, pos2).trim();
						
			ArrayList<String> v = split(arg);
						
			ArrayList<Term> terms = new ArrayList<Term>();
			for (String c : v) {
				terms.add(parseTermString(c));
			}
			
			return m_termFactory.getFunctionalTerm(pre, (Term[])terms.toArray(new Term[] {}));
		}
	}	
	
	private Term parseTermString(String s) {
		
		if (s.startsWith(VARIABLE_SYMBOL)) {
			return new Variable(Integer.parseInt(s.substring(1)));
		} else {
			return new Constant(s);
		}
	}
	
	
	private ArrayList<String> split(String s) {
		ArrayList<String> res = new ArrayList<String>();
		
		char[] arr = s.toCharArray();
	
		int c = 0;
		int start = 0;
		
		for (int i = 0; i < arr.length; i++) {
			boolean close = false;
	
			if (arr[i] == '(') {
				c++;
			} else if (arr[i] == ')') {
				c--;
				close = true;
			}
		
			if (c == 0 && arr[i] == ',') {
				res.add(s.substring(start, i).trim());
				i = i + 1;
				
				while (i < arr.length && (arr[i] == ' ')) {
					i++;
				}
				start = i;
				
			}
			if (c == 0 && close) {
				res.add(s.substring(start, i + 1).trim());
				i = i + 1;
				
				while (i < arr.length && (arr[i] == ' ' || arr[i] == ',')) {
					i++;
				}
				start = i;
			}
				
		}
		
		if (start <= arr.length - 1) {
			res.add(s.substring(start));
		}
		
		return res;
		
	}
	
}
