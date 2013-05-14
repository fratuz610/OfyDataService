/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.holiday69.dataservice.impl;

import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.datastore.ReadPolicy.Consistency;
import com.googlecode.objectify.*;
import it.holiday69.dataservice.DataService;
import it.holiday69.dataservice.query.FieldFilter;
import it.holiday69.dataservice.query.OrderFilter;
import it.holiday69.dataservice.query.Query;
import java.util.*;

public class OfyDataService extends DataService {

	private final static int CHUNK_SIZE = 500;
	
	private Objectify _ofy;
	
	public OfyDataService() {
		this(false);
	}
	
	public OfyDataService(boolean eventualConsistency) {
		
		if(eventualConsistency)
			_ofy = ObjectifyService.begin(new ObjectifyOpts().setConsistency(Consistency.EVENTUAL));
		else
			_ofy = ObjectifyService.begin();
	}
	
	// GENERIC METHODS
	public <T> void put(T object) {
		_ofy.put(object);
	}
	
	@Override
	public <T> void putAll(Iterable<T> keysOrEntities) {
		_ofy.put(keysOrEntities);
	}
	
	@Override
	public <T> T get(Class<T> classOfT) {
		try {
			return _ofy.query(classOfT).get();
		} catch (NotFoundException e) {
			return null;
		}	
	}
	
	public <T> T get(Key<T> key) {
		try {
			return _ofy.get(key);
		} catch (NotFoundException e) {
			return null;
		}
	}
	
	@Override
	public <T, V> T get(V key, Class<T> classOfT) {
		try {
			if(key instanceof Long)
				return _ofy.get(classOfT, (Long)key);
			else if(key instanceof String)
				return _ofy.get(classOfT, (String)key);
			else
				throw new RuntimeException("Only String or Long keys are supported by the backing Datastore");
		} catch (NotFoundException e) {
			return null;
		}
	}
	
	@Override
	public <T> T get(String fieldName, Object fieldValue, Class<T> classOfT) {
		
		try {
			return _ofy.query(classOfT).filter(fieldName, fieldValue).get();
		} catch (NotFoundException e) {
			return null;
		}
	}
	
	@Override
	public <T> T get(Query query, Class<T> classOfT) {
		
		try {
			return getOfyQuery(query, classOfT).get();
		} catch(NotFoundException e) {
			return null;
		}
	}
		
	@Override
	public <T> List<T> getList(Query query, Class<T> classOfT) {
		
		try {
			return getMany(query, classOfT);
		} catch(NotFoundException e) {
			return new LinkedList<T>();
		}
	}
	
	@Override
	public <T> List<T> getList(String fieldName, Object fieldValue, Class<T> classOfT) {
		
		try {
			return getMany(new Query().filter(fieldName, fieldValue), classOfT);
		} catch (NotFoundException e) {
			return new LinkedList<T>();
		}
	}
	
	@Override
	public <T> List<T> getList(Class<T> classOfT) {
		
		try {
			return getMany(new Query(), classOfT);
		} catch (NotFoundException e) {
			return new LinkedList<T>();
		}
	}
	
	public <T> Map<Key<T>, T> getListFromKeys(List<Key<T>> keyList) {
		
		try {
			return _ofy.get(keyList);
		} catch (NotFoundException e) {
			return new HashMap<Key<T>, T>();
		}
	}
	
	/**
	 * Returns result sets populated with multiple queries when a potentially high number of objects could be returned.
	 * This is faster than running one unique query with a high "limit" value
	 * @param srcQuery The query to execute
	 * @param classOfT The class to return objects for
	 * @return A list of objects of class classOfT matching the specified query
	 */
	public <T> List<T> getMany(Query srcQuery, Class<T> classOfT) {
    return getOfyQuery(srcQuery, classOfT).prefetchSize(CHUNK_SIZE).chunkSize(CHUNK_SIZE).list();
	}
	
	@Override
	public <T> void delete(T object) {
		_ofy.delete(object);
	}
	
	@Override
	public <T> void deleteAll(Iterable<T> keysOrEntities) {
		_ofy.delete(keysOrEntities);
	}
	
	@Override
	public <T> void deleteAll(Class<T> classOfT) {
		deleteAll(new Query(), classOfT);
	}
	
	@Override
	public <T> void deleteAll(Query query, Class<T> classOfT) {
    _ofy.delete(getOfyQuery(query, classOfT).listKeys());
	}
		
	public <T> long getMaxID(Class<T> classOfT) {
		
		QueryResultIterator<Key<T>> keyList = _ofy.query(classOfT).fetchKeys().iterator();
		
		long maxID = 0;
		
		while(keyList.hasNext()) {
			
			Key<T> entityKey = keyList.next();
			
			long currentID = entityKey.getId();
			
			if(maxID < currentID)
				maxID = currentID;
			
		}
		
		return maxID;
	}
	
	@Override
	public <T> long getResultSetSize(Class<T> classOfT) {
		return _ofy.query(classOfT).count();
	}

	@Override
	public <T> long getResultSetSize(Query query, Class<T> classOfT) {
		return getOfyQuery(query, classOfT).count();
	}

	@Override
	public <T> long getResultSetSize(String fieldName, Object fieldValue, Class<T> classOfT) {
		return _ofy.query(classOfT).filter(fieldName, fieldValue).count();
	}
	
	public <T> List<Key<T>> getKeyList(Query query, Class<T> classOfT) {
    return getOfyQuery(query, classOfT).prefetchSize(CHUNK_SIZE).chunkSize(CHUNK_SIZE).listKeys();
	}
	
	private <T> com.googlecode.objectify.Query<T> getOfyQuery(Query srcQuery, Class<T> classOfT) {
		
		com.googlecode.objectify.Query<T> ofyQuery = _ofy.query(classOfT);
		
		for(FieldFilter fieldFilter : srcQuery.getFieldFilterList()) {
			
			switch(fieldFilter.getFieldFilterType()) {
				case EQUAL: ofyQuery = ofyQuery.filter(fieldFilter.getFieldName(), fieldFilter.getFieldValue()); break;
				case GREATER_THAN: ofyQuery = ofyQuery.filter(fieldFilter.getFieldName() + " >", fieldFilter.getFieldValue()); break;
				case GREATER_THAN_INC: ofyQuery = ofyQuery.filter(fieldFilter.getFieldName() + " >=", fieldFilter.getFieldValue()); break;
				case LOWER_THAN: ofyQuery = ofyQuery.filter(fieldFilter.getFieldName() + " <", fieldFilter.getFieldValue()); break;
				case LOWER_THAN_INC: ofyQuery = ofyQuery.filter(fieldFilter.getFieldName() + " <=", fieldFilter.getFieldValue()); break;
			}
			
		}
		
		for(OrderFilter orderFilter : srcQuery.getOrderFilterList()) {
			
			switch(orderFilter.getOrderType()) {
				case ASCENDING: ofyQuery = ofyQuery.order(orderFilter.getFieldName()); break;
				case DESCENDING: ofyQuery = ofyQuery.order("-" + orderFilter.getFieldName()); break;
			}
		}
		
		return ofyQuery.limit(srcQuery.getLimit()).offset(srcQuery.getOffset());
	}

	
	
		
}

