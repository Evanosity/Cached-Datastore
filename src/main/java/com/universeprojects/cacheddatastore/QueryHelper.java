package com.universeprojects.cacheddatastore;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery.Builder;
import com.universeprojects.cacheddatastore.CachedDatastoreService.QueryOperator;
import com.google.cloud.datastore.Value;

public class QueryHelper {
	
	final private CachedDatastoreService cds;
	
	public QueryHelper(CachedDatastoreService cds) {
		this.cds = cds;
	}
	
	/**
	 * 
	 * @param kind
	 * @param fieldName
	 * @param operator
	 * @param equalToValue
	 * @return
	 */
	public Long getFilteredList_Count(String kind, String fieldName, QueryOperator operator, Object equalToValue) {
		return getFilteredList_Count(kind, 5000, fieldName, operator, equalToValue);
	}
	public Long getFilteredList_Count(String kind, String fieldName, QueryOperator operator, Object equalToValue, String fieldName2, QueryOperator operator2, Object equalToValue2) {
		return getFilteredList_Count(kind, 5000, fieldName, operator, equalToValue, fieldName2, operator2, equalToValue2);
	}
	public Long getFilteredList_Count(String kind, String fieldName, QueryOperator operator, Object equalToValue, String fieldName2, QueryOperator operator2, Object equalToValue2, String fieldName3, QueryOperator operator3, Object equalToValue3) {
		return getFilteredList_Count(kind, 5000, fieldName, operator, equalToValue, fieldName2, operator2, equalToValue2, fieldName3, operator3, equalToValue3);
	}
	
	//If we do specify a limit; should we cap this?
	public Long getFilteredList_Count(String kind, Integer limit, String fieldName, QueryOperator operator, Object equalToValue) {
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add(fieldName);
		
		List<QueryOperator> operators = new ArrayList<QueryOperator>();
		operators.add(operator);
		
		List<Value<?>> equalToValues = new ArrayList<Value<?>>();
		equalToValues.add((Value<?>) equalToValue);
		
		Builder builder = cds.resolveKeyQueryBuilder(fieldNames, operators, equalToValues);
		
		return cds.getCountForQuery(cds.resolveKeyQuery(kind, builder, limit));
	}
	public Long getFilteredList_Count(String kind, Integer limit, String fieldName, QueryOperator operator, Object equalToValue, String fieldName2, QueryOperator operator2, Object equalToValue2) {
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add(fieldName);
		fieldNames.add(fieldName2);
		
		List<QueryOperator> operators = new ArrayList<QueryOperator>();
		operators.add(operator);
		operators.add(operator2);
		
		List<Value<?>> equalToValues = new ArrayList<Value<?>>();
		equalToValues.add((Value<?>) equalToValue);
		equalToValues.add((Value<?>) equalToValue2);
		
		Builder builder = cds.resolveKeyQueryBuilder(fieldNames, operators, equalToValues);
		
		return cds.getCountForQuery(cds.resolveKeyQuery(kind, builder, limit));
	}
	public Long getFilteredList_Count(String kind, Integer limit, String fieldName, QueryOperator operator, Object equalToValue, String fieldName2, QueryOperator operator2, Object equalToValue2, String fieldName3, QueryOperator operator3, Object equalToValue3) {
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add(fieldName);
		fieldNames.add(fieldName2);
		fieldNames.add(fieldName3);

		List<QueryOperator> operators = new ArrayList<QueryOperator>();
		operators.add(operator);
		operators.add(operator2);
		operators.add(operator3);
		
		List<Value<?>> equalToValues = new ArrayList<Value<?>>();
		equalToValues.add((Value<?>) equalToValue);
		equalToValues.add((Value<?>) equalToValue2);
		equalToValues.add((Value<?>) equalToValue3);
	
		Builder builder = cds.resolveKeyQueryBuilder(fieldNames, operators, equalToValues);
		
		return cds.getCountForQuery(cds.resolveKeyQuery(kind, builder, limit));
	}

	//Ok, so let's think about all the methods we need.
	//getFilteredList - up to 3 operators.
	
	/**
	 * Priority level for the sort will be implied by what number we're at.
	 * @param kind
	 * @param limit
	 * @param fieldName
	 * @param operator
	 * @param equalToValue
	 * @return
	 */
	public List<CachedEntity> getFilteredList_Sorted(String kind, Integer limit, String fieldName, QueryOperator operator, Object equalToValue, boolean sortDirection){
		Map<String, Boolean> sortDirections = new HashMap<String, Boolean>();
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add(fieldName);
		
		sortDirections.put(fieldName, sortDirection);
		
		List<QueryOperator> operators = new ArrayList<QueryOperator>();
		operators.add(operator);
		
		List<Value<?>> equalToValues = new ArrayList<Value<?>>();
		equalToValues.add((Value<?>) equalToValue);
		
		//First, we generate a new builder with the given property filters.
		Builder builder = cds.resolveKeyQueryBuilder(fieldNames, operators, equalToValues);
		//then, we apply the sort orders to our builder.
		builder = cds.resolveSortedKeyQueryBuilder(builder, sortDirections);
		
		//then, we resolve the query and fetch the entities.
		return cds.fetchAsList(cds.resolveKeyQuery(kind, builder, limit));
		
	}
	public List<CachedEntity> getFilteredList_Sorted(String kind, Integer limit, String fieldName, QueryOperator operator, Object equalToValue, boolean sortDirection, String fieldName2, QueryOperator operator2, Object equalToValue2, boolean sortDirection2){
		Map<String, Boolean> sortDirections = new HashMap<String, Boolean>();
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add(fieldName);
		fieldNames.add(fieldName2);
		
		sortDirections.put(fieldName, sortDirection);
		sortDirections.put(fieldName2, sortDirection2);
		
		List<QueryOperator> operators = new ArrayList<QueryOperator>();
		operators.add(operator);
		operators.add(operator2);
		
		List<Value<?>> equalToValues = new ArrayList<Value<?>>();
		equalToValues.add((Value<?>) equalToValue);
		equalToValues.add((Value<?>) equalToValue2);
		
		//First, we generate a new builder with the given property filters.
		Builder builder = cds.resolveKeyQueryBuilder(fieldNames, operators, equalToValues);
		//then, we apply the sort orders to our builder.
		builder = cds.resolveSortedKeyQueryBuilder(builder, sortDirections);
		
		//then, we resolve the query and fetch the entities.
		return cds.fetchAsList(cds.resolveKeyQuery(kind, builder, limit));
		
	}
	public List<CachedEntity> getFilteredList_Sorted(String kind, Integer limit, String fieldName, QueryOperator operator, Object equalToValue, boolean sortDirection, String fieldName2, QueryOperator operator2, Object equalToValue2, boolean sortDirection2, String fieldName3, QueryOperator operator3, Object equalToValue3, boolean sortDirection3){
		Map<String, Boolean> sortDirections = new HashMap<String, Boolean>();
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add(fieldName);
		fieldNames.add(fieldName2);
		fieldNames.add(fieldName3);
		
		sortDirections.put(fieldName, sortDirection);
		sortDirections.put(fieldName2, sortDirection2);
		sortDirections.put(fieldName3, sortDirection3);
		
		List<QueryOperator> operators = new ArrayList<QueryOperator>();
		operators.add(operator);
		operators.add(operator2);
		
		List<Value<?>> equalToValues = new ArrayList<Value<?>>();
		equalToValues.add((Value<?>) equalToValue);
		equalToValues.add((Value<?>) equalToValue2);
		equalToValues.add((Value<?>) equalToValue3);
		
		
		//First, we generate a new builder with the given property filters.
		Builder builder = cds.resolveKeyQueryBuilder(fieldNames, operators, equalToValues);
		//then, we apply the sort orders to our builder.
		builder = cds.resolveSortedKeyQueryBuilder(builder, sortDirections);
		
		//then, we resolve the query and fetch the entities.
		return cds.fetchAsList(cds.resolveKeyQuery(kind, builder, limit));
		
	}

	public List<Key> getFilteredList_Keys(String kind, String fieldName, QueryOperator operator, Object equalToValue) {
		return getFilteredList_Keys(kind, null, fieldName, operator, equalToValue);
	}
	public List<Key> getFilteredList_Keys(String kind, String fieldName, QueryOperator operator, Object equalToValue, String fieldName2, QueryOperator operator2, Object equalToValue2) {
		return getFilteredList_Keys(kind, null, fieldName, operator, equalToValue, fieldName2, operator2, equalToValue2);
	}
	public List<Key> getFilteredList_Keys(String kind, String fieldName, QueryOperator operator, Object equalToValue, String fieldName2, QueryOperator operator2, Object equalToValue2, String fieldName3, QueryOperator operator3, Object equalToValue3) {
		return getFilteredList_Keys(kind, null, fieldName, operator, equalToValue, fieldName2, operator2, equalToValue2, fieldName3, operator3, equalToValue3);
	}
	
	public List<Key>getFilteredList_Keys(String kind, Integer limit, String fieldName, QueryOperator operator, Object equalToValue){
		
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add(fieldName);
		
		List<QueryOperator> operators = new ArrayList<QueryOperator>();
		operators.add(operator);
		
		List<Value<?>> equalToValues = new ArrayList<Value<?>>();
		equalToValues.add((Value<?>) equalToValue);
		
		Builder builder = cds.resolveKeyQueryBuilder(fieldNames, operators, equalToValues);
		
		return cds.fetchKeys(cds.resolveKeyQuery(kind, builder, limit));
	}
	public List<Key>getFilteredList_Keys(String kind, Integer limit, String fieldName, QueryOperator operator, Object equalToValue, String fieldName2, QueryOperator operator2, Object equalToValue2){
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add(fieldName);
		fieldNames.add(fieldName2);

		List<QueryOperator> operators = new ArrayList<QueryOperator>();
		operators.add(operator);
		operators.add(operator2);
		
		List<Value<?>> equalToValues = new ArrayList<Value<?>>();
		equalToValues.add((Value<?>) equalToValue);
		equalToValues.add((Value<?>) equalToValue2);
	
		Builder builder = cds.resolveKeyQueryBuilder(fieldNames, operators, equalToValues);
		
		return cds.fetchKeys(cds.resolveKeyQuery(kind, builder, limit));	
		
	}
	public List<Key>getFilteredList_Keys(String kind, Integer limit, String fieldName, QueryOperator operator, Object equalToValue, String fieldName2, QueryOperator operator2, Object equalToValue2, String fieldName3, QueryOperator operator3, Object equalToValue3){
		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add(fieldName);
		fieldNames.add(fieldName2);
		fieldNames.add(fieldName3);

		List<QueryOperator> operators = new ArrayList<QueryOperator>();
		operators.add(operator);
		operators.add(operator2);
		operators.add(operator3);
		
		List<Value<?>> equalToValues = new ArrayList<Value<?>>();
		equalToValues.add((Value<?>) equalToValue);
		equalToValues.add((Value<?>) equalToValue2);
		equalToValues.add((Value<?>) equalToValue3);
		
		Builder builder = cds.resolveKeyQueryBuilder(fieldNames, operators, equalToValues);
	
		return cds.fetchKeys(cds.resolveKeyQuery(kind, builder, limit));
	}
	
	public List<CachedEntity> fetchChildren(Key parent, int limit){
		
		
		return null;
	}
}