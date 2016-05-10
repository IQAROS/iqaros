package org.oxford.comlab.perfectref.rewriter;

import java.util.HashMap;

public class Constant extends Term {
    public String name;


//    changed from protected
//    27/6/2011

    public Constant(String index) {
        name=index;
    }

    @Override
	public String getName() {
        return name;
    }

    @Override
	public int getArity() {
        return 0;
    }

    @Override
	public Term getArgument(int argumentIndex) {
        throw new IndexOutOfBoundsException();
    }

    @Override
	public Term[] getArguments() {
        throw new IndexOutOfBoundsException();
    }

    @Override
	public boolean contains(Term term) {
        return equals(term);
    }

    @Override
	public Term apply(Substitution substitution,TermFactory termFactory) {
        return this;
    }

    @Override
	public Term offsetVariables(TermFactory termFactory,int offset) {
        return this;
    }

    @Override
    public Term renameVariables(TermFactory termFactory, HashMap<Variable, Integer> mapping) {
        return this;
    }

    @Override
	public int getMinVariableIndex() {
        return -1;
    }

    @Override
	public void toString(StringBuffer buffer) {
        buffer.append(getName());
    }

    @Override
	public int getDepth()
    {
    	return 0;
    }

    @Override
	public Term getVariableOrConstant()
    {
    	return this;
    }

    @Override
	public String getFunctionalPrefix()
    {
    	return "";
    }

    public void setName(String name)
    {
    	this.name = name;
    }

    @Override
	public boolean equals(Object that){
    	if(that instanceof Constant)
			return this.getName().equals(((Constant)that).getName());
    	return false;
    }

    @Override
	public int hashCode(){
    	return this.name.hashCode();
    }

	@Override
	public boolean isConstant() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean isVariable() {
		// TODO Auto-generated method stub
		return false;
	}

}
