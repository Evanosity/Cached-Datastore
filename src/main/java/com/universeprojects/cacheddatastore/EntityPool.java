package com.universeprojects.cacheddatastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Value;

public class EntityPool {
	final private CachedDatastoreService ds;
	Map<Object, CachedEntity> pool = new HashMap<>();
	
	Set<Key> queue = null;
	
	public EntityPool(CachedDatastoreService ds) {
		this.ds = ds;
	}
	
	public void addToQueue(Object...keyList) {
		if(keyList == null) return;
		
		List<Key> keysToLoad = new ArrayList<Key>();
		
		for(Object o:keyList) {
			if(o == null) continue;
			else if(o instanceof CachedEntity) addEntityDirectly((CachedEntity)o);
			
		}
	}
	
	public Map<Key, CachedEntity> loadEntities(Object...keyList){
		List<Key> keysToLoad = new ArrayList<Key>();
		
		if(keyList != null) {
			addToQueue(keyList);
		}
		
		if(queue!=null) {
			keysToLoad.addAll(queue);
			queue.clear();
		}
		
		if(keysToLoad.isEmpty()) return new HashMap<>();
		
		Map<Key, CachedEntity> entities = ds.getAsMap(keysToLoad);
		
		pool.putAll(entities);
		
		return entities;
	}
	
	public CachedEntity get(Object entityKey) {
		if(entityKey == null) return null;
		if(pool.containsKey(entityKey) == false) 
			throw new IllegalArgumentException("The entityKey '"+entityKey+"' was not preloaded into the EntityPool. All entities should be bulk loaded into a pool before they can be accessed.");
		return pool.get(entityKey);
	}
	
	public List<CachedEntity> get(List<Key> entityKeys){
		if(entityKeys == null) return null;
		
		List<CachedEntity> result = new ArrayList<CachedEntity>();
		for(Key key:entityKeys) {
			result.add(get(key));
		}
		
		return result;
	}
	
	public int getFailedFetchCount() {
		int count = 0;
		for(CachedEntity e:pool.values())
			if(e == null)
				count++;
		
		return count;
	}
	
	public List<Object> getFailedFetchKeys(){
		List<Object> result = new ArrayList<>();
		for(Object key:pool.keySet())
			if(pool.get(key)==null)
				result.add(key);
		
		return result;
			
	}
	
	//Figure out how to handle embeddedentities.
	public void addEmbeddedEntityDirectly(Value<?>...entity) {
		
	}
	
	public void addEntityDirectly(CachedEntity...entity) {
		for(CachedEntity e:entity) {
			Key key = e.getKey();
			if(pool.containsKey(key)==false) pool.put(key, e);
		}
	}
	
	public void addNullEntityDirectly(Key...keys) {
		for(Key key:keys) {
			pool.put(key, null);
		}
	}
	
	public boolean contains(Key key) {
		return pool.containsKey(key);
	}
}
