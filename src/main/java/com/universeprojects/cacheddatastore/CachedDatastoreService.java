package com.universeprojects.cacheddatastore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.google.cloud.datastore.Cursor;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.KeyQuery.Builder;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.Filter;
import com.google.cloud.datastore.StructuredQuery.OrderBy;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.cloud.datastore.Value;

public class CachedDatastoreService {
	
	private Logger log = Logger.getLogger(this.getClass().toString());
	private static ConcurrentHashMap<String, InstanceCacheWrapper> instanceCache = new ConcurrentHashMap<String, InstanceCacheWrapper>();
	
	public static boolean singleEntityMode = false;
	public static boolean singlePutMode = false;
	final public static boolean statsTracking = false;
	final public static String MC_GETS = "Stats_MC_GETS";
	final public static String DS_GETS = "Stats_DS_GETS";
	final public static String QUERIES = "Stats_QUERIES";
	final public static String QUERY_ENTITIES = "Stats_QUERY_ENTITIES";
	final public static String MC_QUERY_ENTITIES = "Stats_MC_QUERY_ENTITIES";
	final public static String MC_QUERIES = "Stats_MC_QUERIES";

	final public static String QUERYKEYCACHE_QUERIES = "Stats_MC_QUERYKEYCACHE_QUERIES";
	final public static String QUERYKEYCACHE_MC_ENTITIES = "Stats_MC_QUERYKEYCACHE_MC_ENTITIES";
	final public static String QUERYKEYCACHE_DB_ENTITIES = "Stats_MC_QUERYKEYCACHE_DB_ENTITIES";
	
	final public String mcPrefix = "MCENTITY";
	boolean cacheEnabled = true;
	boolean queryModelCacheEnabled = false;
	
	Set<CachedEntity> entitiesToBulkPut = new HashSet<CachedEntity>();
	Set<Key> entitiesToBulkDelete = new HashSet<Key>();
	boolean bulkPutMode = false;
	
	Datastore ds = null;
	//MemoryStoreService mss = null;
	
	boolean enforceEntityFetchWithinTransaction = false;
	Set<Key> transactionallyFetchedEntities = null;
	Set<Key> transactionalltyDeletedEntities = null;
	Map<Key, Entity> transactionallyChargedEntities = null;
	
	//PreparedQuery pq = null;
	Cursor lastQuery_endCursor = null;
	
	private static RemoteApiOptions options = null;
	private static boolean disableRemoteAPI = false;
	
	Map<String,List<Long>>preallocatedIds = new HashMap<String, List<Long>>();
	private Map<String, Integer> autoPreallocationCount = new HashMap<String, Integer>();
	
	private Map<String, CachedEntity> entitiesFetchedThisRequest;
	private Set<String> entitiesPutThisRequest;
	private Set<String> entitiesDeletedThisRequest;
	
	private Transaction currentTransaction = null;
	
	public class EntityNotFetchedWithinTransactionException extends RuntimeException{
		
		private static final long serialVersionUID = 9206555409260795859L;
		private Entity entity;
		public EntityNotFetchedWithinTransactionException(Entity entity) {
			super("The entity"+entity.getKey()+" was not fetched within the active transaction.");
			this.entity = entity;
		}
		
		public Entity getEntity() { return entity; }
	}
	
	public enum QueryOperator{
		EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL
	}
	
	
	private boolean isEntityFetchedThisRequest(Key entityKey) {
		if(entitiesFetchedThisRequest == null) return false;
		
		return entitiesFetchedThisRequest.containsKey(entityKey.toString());
	}
	
	{
		if (entitiesPutThisRequest==null) return false;
		
		return entitiesPutThisRequest.contains(entityKey.toString());
	}
	
	private void trackFetchedEntityThisRequest(CachedEntity entity)
	{
		if (entitiesFetchedThisRequest==null) entitiesFetchedThisRequest = new HashMap<>();
		
		entitiesFetchedThisRequest.put(entity.getKey().toString(), entity);
	}
	
	private void trackFetchedEntityThisRequest(Collection<CachedEntity> entities)
	{
		if (entitiesFetchedThisRequest==null) entitiesFetchedThisRequest = new HashMap<>();
		
		for(CachedEntity entity:entities)
			trackFetchedEntityThisRequest(entity);
	}
	
	private CachedEntity getTrackedFetchedEntityThisRequest(Key entityKey)
	{
		if (entitiesFetchedThisRequest==null) return null;
		
		return entitiesFetchedThisRequest.get(entityKey.toString());
	}
	
	private void trackPutEntityThisRequest(CachedEntity entity)
	{
		if (entitiesPutThisRequest==null) entitiesPutThisRequest = new HashSet<>();
		
		entitiesPutThisRequest.add(entity.getKey().toString());
		entity.setAttribute("onePutStacktrace", new RuntimeException("This is the stacktrace for the first place this entity was put."));
	}
	
	private void trackPutEntityThisRequest(Collection<CachedEntity> entities)
	{
		if (entitiesPutThisRequest==null) entitiesPutThisRequest = new HashSet<>();
		
		for(CachedEntity entity:entities)
			trackPutEntityThisRequest(entity);
	}
	
	/**
	 * Here, we "get" a cachedentity. first, we check MemoryStore. If it's not there, check the DB.
	 * @param entityKey
	 * @return
	 * @throws EntityNotFoundException
	 */
	public CachedEntity get(Key entityKey) throws DatastoreException{
		CachedEntity result;
		if(entityKey == null) return null;
		
		if(singleEntityMode) {
			result = getTrackedFetchedEntityThisRequest(entityKey);
			if(result!=null) return result;
		}
		
		//LOGIC; add transactionactive here later.
		if(cacheEnabled) {
			//result = CachedEntity.wrap((Entity)mc.get(mcPrefix+entityKey.toString()));
		}
	}
	
	public Integer countEntities(KeyQuery q) {
		return fetchKeys(q).size();
	}
	
	/**
	 * Returns a list of entities based on a query.
	 * @param kq
	 * @return
	 */
	public List<CachedEntity> fetchAsList(KeyQuery kq){
		
		List<Key> keys = fetchKeys(kq);
				
		return fetchEntitiesFromKeys(keys);
	}
	

	
	/**
	 * Given a list of keys, fetch all of the entities from either memorystore or the DB.
	 * The order of this list actually matters, as this method will be used for sorted queries.
	 * @param keys - the iterable of the 
	 * @return - the fetched entities, sorted.
	 */
	public List<CachedEntity> fetchEntitiesFromKeys(Iterable<Key> keys){
		if(keys==null) return null;
		
		//First, we grab entities from memorystore. IDK how to do this lol.
		List<String> entityKeyStrings = new ArrayList<>();
		Map<String, Object> entitiesFromMC = null;
		
		//Then, we check to see what entities we actually got from memorystore.
		List<Key> unfetchedKeys = new ArrayList<>();
		
		//then, we fetch the missing entities from the DB.
		Map<Key, Entity> entitiesFromDB = null;
		if(unfetchedKeys.isEmpty()==false) {
			entitiesFromDB = (Map<Key, Entity>) ds.get(unfetchedKeys);
		}
		
		// Then we combine both lists into a single result. This keeps the sort order of the keys.
		
		List<CachedEntity> result = new ArrayList<>();
		for(Key key:keys) {
			if(key == null) {
				result.add(null);
				continue;
			}
			CachedEntity mcEntity = null;
			CachedEntity dbEntity = null;
			
			if(entitiesFromMC != null && entitiesFromMC.isEmpty()==false) {
				String keyString = null;
				//mcEntity = CachedEntity.wrap(obj);
			}
			if(entitiesFromDB != null && entitiesFromDB.isEmpty()==false) {
				//dbEntity = CachedEntity.wrap(entit)
			}
			
			if(mcEntity!=null && dbEntity!=null) {
				throw new IllegalStateException("Both the memcache and the datastore entities were fetched. This shouldn't ever happen.");
			}
			
			if(mcEntity != null) {
				result.add(mcEntity);
			}
			else if(dbEntity != null) {
				result.add(dbEntity);
			}
		}
		
		return result;
	}
	
	/**
	 * Returns a list of keys from the DB based on a query.
	 * This is an EXTREMELY efficient method to call because it only counts as one read.
	 * @param query - the query we're running.
	 * @return
	 */
	public List<Key> fetchKeys(KeyQuery query){
		QueryResults<Key> results = ds.run(query);
		List<Key> toReturn;
		
		while(results.hasNext()) {
			toReturn.add(results.next());
		}
		
		if(statsTracking);
			//incrementStat(QUERYKEYCACHE_QUERIES);
		
		return toReturn;
	}
	
	/**
	 * Returns a count of entities given a keyquery. This runs a single search against the DB, as we can't query the cache.
	 * @param q - the keyquery we're returning the count for.
	 * @return the number of entities that were discovered by our query.
	 */
	public Long getCountForQuery(KeyQuery query) {
		return (long) fetchKeys(query).size();	
		
	}
	
	/*
	 * Below this is methods written by evan for efficient and easy queries
	 */
	

	
	/**
	 * Given a builder, a kind and a limit, generate our query.
	 * @param kind - the kind of entity we're searching for.
	 * @param builder - the unfinished keyquery.builder
	 * @param limit - the max number of entities this query can fetch.
	 * @return
	 */
	public KeyQuery resolveKeyQuery(String kind, Builder builder, Integer limit) {
		return builder.setKind(kind).setLimit(limit).build();
	}
	
	/**
	 * Returns a key-only query with the specified filters. Doesn't execute the query; handle that upstream.
	 * @param kind - the kind of entity we're searching for.
	 * @param rawQuery - a string-entry<value<?>-operator> map of the query. String is property, value is the value we're
	 * comparing operator to, and operator is the what we're comparing value to.
	 * @param limit - the max number of results this query will return.
	 * @param keyOnly - a boolean, are we grabbing full entities or just keys?
	 * @return the KeyQuery.Builder - This is so we can apply more things to the query before we build it.
	 */
	public Builder resolveKeyQueryBuilder(List<String> fieldNames, List<QueryOperator> operators, List<Value<?>> equalToValues) {
		
		List<PropertyFilter> filters = resolvePropertyFiltersFromLists(fieldNames, operators, equalToValues);
		
		//This is UNHOLY but it should work....
		//We grab the last element, and then the entire list except for the last element.
		return Query.newKeyQueryBuilder().setFilter(CompositeFilter.and(filters.get(filters.size()), filters.toArray(new PropertyFilter[filters.size()-2])));
	}
	
	/**
	 * Given a keyBuilder and a string-boolean map of propertyName-ascending, apply sorted modifiers.
	 * @param builder
	 * @param directions
	 * @return
	 */
	public Builder resolveSortedKeyQueryBuilder(Builder builder, Map<String, Boolean> directions) {
		
		for(Map.Entry<String, Boolean>entry:directions.entrySet()) {
			
			//ascending
			if(entry.getValue()) 
				builder.addOrderBy(OrderBy.asc(entry.getKey()));

			//not ascending
			else 
				builder.addOrderBy(OrderBy.desc(entry.getKey()));
			
		}
		
		return builder;
	}
	
	/**
	 * Given a list of fieldnames, a list of operators and a list of values, generate a list of property filters.
	 * Throws IllegalArgumentException if all the lists aren't the same size.
	 * @param rawQuery - the map that we're pulling from
	 * @return a list of the given property filters.
	 */
	public List<PropertyFilter> resolvePropertyFiltersFromLists(List<String> fieldNames, List<QueryOperator> operators, List<Value<?>> equalToValues){
		
		if(fieldNames.size() != operators.size() || fieldNames.size() != equalToValues.size()) throw new IllegalStateException("Failed to resolve query.");

		List<PropertyFilter> filters = new ArrayList<>();
		
		for(int i = 0; i < fieldNames.size(); i++) {
			
			filters.add(resolvePropertyFilter(fieldNames.get(i), operators.get(i), equalToValues.get(i)));
				
		}
		return filters;
	}
	
	/**
	 * Turns a property, a value and an operator into a propertyfilter.
	 * @param propertyName - the name of the property we're gonna be filtering by.
	 * @param value - the value we're comparing the property to.
	 * @param operator - how we're comparing any given property to the value.
	 * @return
	 */
	public PropertyFilter resolvePropertyFilter(String propertyName, QueryOperator operator, Value<?> value) {
		switch(operator) {
		case EQUAL:
			return PropertyFilter.eq(propertyName, value);
		case LESS_THAN:
			return PropertyFilter.lt(propertyName, value);
		case LESS_THAN_OR_EQUAL:
			return PropertyFilter.le(propertyName, value);
		case GREATER_THAN:
			return PropertyFilter.gt(propertyName, value);
		case GREATER_THAN_OR_EQUAL:
			return PropertyFilter.ge(propertyName, value);
		default:
			throw new RuntimeException("Unhandled Operator type.");
		}
	}
}