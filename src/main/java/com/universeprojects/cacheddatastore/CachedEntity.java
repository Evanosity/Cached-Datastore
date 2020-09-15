package com.universeprojects.cacheddatastore;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Value;

/**
 * This object wraps an entity from google cloud datastore. Allows for passing of generic objects into
 * any property; however, if the cast to a value fails, the transaction fails.
 * @author Evan
 *
 */
public class CachedEntity implements Cloneable,Serializable{
	
	private transient Map<String,Object> attributes;
	
	private static final long serialVersionUID = 2120780631668954935L;
	protected Entity entity;
	boolean unsavedChanges = false;
	boolean deleted = false;
	boolean newEntity = false;
	public static String projectID = "";
	
	public CachedEntity(Key key) {
		this(Entity.newBuilder(key).build());
		newEntity = true;
	}
	
	//generates a new entity of the specified kind.
	public CachedEntity(String kind, CachedDatastoreService cds) {
		//this(Entity.newBuilder(Key.newBuilder(projectID, kind, cds.getPreallocatedIdFor(kind)).build()).build());		
		newEntity = true;
		unsavedChanges = true;
	}
	
	@Deprecated
	public CachedEntity(String kind) {
	}
	
	public CachedEntity(String kind, Key parent, CachedDatastoreService cds) {
		//this(Entity.newBuilder(cds.ds.newKeyFactory().newKey(cds.getPreallocatedIdFor(kind))).build());
	}
	/**
	 * Build HELLA constructors. Kind+ID, Key, etc.
	 * @param entity
	 */
	private CachedEntity(Entity entity) {
		this.entity = entity;
	}
	
	public boolean isDeleted(){
		return deleted;
	}
	
	public Entity getEntity() {
		return entity;
	}
	
	public void refetch(CachedDatastoreService ds) {
		try {
			entity = ds.ds.get(getKey());
			
			//if(ds.cacheEnabled && ds.isTransactionActive()) {
			//	ds.addEntityToTransaction(getKey());
			//}
		}
		catch(DatastoreException e){
			throw new IllegalStateException("Unable to refetch. Entity "+getKey()+" was deleted.");
		}
	}
	
	public CachedEntity clone(CachedDatastoreService cds) {
		//CachedEntity newEntity = new CachedEntity(getKind(), (CachedDatastoreService) getParent());
		//CachedDatastoreService.copyFieldValues(this, newEntity);
		//return newEntity;
		return null;
	}
	
	protected void setId(long id) {
		//LOGIC; make sure this works.
		if(entity.getKey() instanceof Key)
			throw new IllegalStateException("You cannot set an ID for an entity that already has a complete key.");
		
	}
	
	protected void updateStoredKey(Key originalKey, Key newKey) {
		//LOGIC: come back to this.
		//for(String field:this.getProperties().keySet());
	}
	
	public boolean equals(Object object) {
		if(object == null) return false;
		if(object == this) return true;
		
		//this syntax is unholy
		if(object instanceof CachedEntity) return this.entity.equals(((CachedEntity)object).entity);
		else if(object instanceof Entity) return this.entity.equals(object);
		else return false;
	}
	
	//LOGIC; what the fuck is this?
	public String getAppId() {
		return getKey().getProjectId();
	}
	
	public Key getKey() {
		return entity.getKey();
	}
	
	public String getUrlSafeKey() {
		return getKey().toUrlSafe();
	}
	
	public String getKind() {
		return getKey().getKind();
	}
	
	public Long getId() {
		return getKey().getId();
	}
	
	public String getNamespace() {
		return getKey().getNamespace();
	}
	
	public Key getParent() {
		return getKey().getParent();
	}
	
	public int hashCode() {
		return entity.hashCode();
	}
	
	public String toString() {
		return entity.toString();
	}
	
	public Map<String, Value<?>> getProperties(){
		return entity.getProperties();
	}
	
	public Value<?> getProperty(String propertyName){
		return entity.getValue(propertyName);
	}

	public boolean hasProperty(String propertyName) {
		return entity.contains(propertyName);
	}
	
	public void removeProperty(String propertyName) {
		//entity
	}
	
	public boolean setProperty(String propertyName, Object value) {
		try {
			entity = Entity.newBuilder(entity.getKey()).set(propertyName, (Value<?>) value).build();
			unsavedChanges = true;
			
			return true;
		}
		catch(DatastoreException de){
			return false;
		}
	}
	
	public static CachedEntity wrap(Entity obj) {
		if(obj == null) return null;
		return new CachedEntity(obj);
	}
	
	public boolean isUnsaved() {
		return unsavedChanges;
	}
	
	public void setAttribute(String attributeKey, Object value) {
		if(attributes == null) attributes = new HashMap<>();
		attributes.put(attributeKey, value);
	}
	
	public Object getAttribute(String attributeKey) {
		if(attributes == null) return null;
		return attributes.get(attributeKey);
	}
	
	public Map<String,Object> getAttributes(){
		return attributes;
	}

}