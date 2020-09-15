package com.universeprojects.cacheddatastore;

import java.util.ConcurrentModificationException;

public abstract class Transaction<T> {
	private CachedDatastoreService cds;
	
	public Transaction(CachedDatastoreService cds) {
		this.cds = cds;
	}
	
	public abstract T doTransaction(CachedDatastoreService ds) throws AbortTransactionException;

	public T run() throws AbortTransactionException{
		T result = null;
		
		boolean success = false;
		while(!success) {
			try {
				if(disableTransaction() == false) 
					cds.beginTransaction(true);
				
				result = doTransaction(ds);
				success = true;
				
				if(disableTransaction() == false) {
					cds.commit();
				}
			}
			catch(ConcurrentModificationException cme){
				
			}
			finally {
				cds.rollbackIfActive();
			}
		}
		
		return result;
	}
	
	public CachedEntity refetch(CachedEntity entity) {
		return cds.refetch(entity);
	}
	
	public boolean disableTransaction() {
		return false; //bruh
	}

}
