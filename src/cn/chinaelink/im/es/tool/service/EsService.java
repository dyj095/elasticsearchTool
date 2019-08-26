/**
 * 
 */
package cn.chinaelink.im.es.tool.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * ElasticSearch抽象服务类
 * 
 * @author duyanjun
 * @since 2017/08/29 杜燕军 新建
 */
public class EsService {
	
	// UTF-8编码字符集
	protected static final String CHATSET_UTF8 = "UTF-8";
	
	// HTTP头信息_Content-Type
	protected static final String HTTP_HEADER_CONTENT_TYPE = "Content-Type";
	
	// application/json的HTTP头信息
	protected static final String HTTP_HEADER_APPLICATION_JSON = "application/json";
	
	protected String url;
	
	private Logger log = Logger.getLogger(EsService.class.getName());
	/**
	 * 构造方法
	 */
	public EsService(String url){
		this.url = url;
	}
	
	/**
	 * 查询索引是否存在
	 * 
	 * @param indexName
	 *            索引名
	 */
	public boolean isExitIndex(String indexName) {
		boolean isExit = false;
		HttpClient httpClient = new DefaultHttpClient();
		HttpGet requests = new HttpGet(url);
		try {
			// 执行Post执行
			HttpResponse response = httpClient.execute(requests);
			HttpEntity entity = response.getEntity();
			String res = convertStreamToString(entity.getContent());
			if(log.isLoggable(Level.INFO)){
				log.info("res:" + res);
			}
			if (res != null && !"".equals(res)) {
				JSONObject resJson = JSONObject.fromObject(res);
				if (resJson.containsKey("error")) {
					String state = resJson.getString("status");
					String reson = resJson.getJSONObject("error").getString("reason");
					if(log.isLoggable(Level.INFO)){
						log.info("state:" + state + "    reson:" + reson);
					}
				} else {
					isExit = true;
				}
			}
		} catch (UnsupportedEncodingException e) {
        	if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (ClientProtocolException e) {
        	if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (IOException e) {
        	if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return isExit;
	}
	
	/**
	 * 创建索引
	 * 
	 * @param indexName
	 *            索引名
	 */
	public boolean createIndex(String indexName) {
		boolean isSuccess = false;
		HttpClient httpClient = new DefaultHttpClient();
		HttpPut request = new HttpPut(url);
		try {
			JSONObject setting = new JSONObject();

			JSONObject indexObj = new JSONObject();
			JSONObject indexJson = new JSONObject();
			indexJson.put("number_of_shards", 3);
			indexJson.put("number_of_replicas", 2);
			indexObj.put("index", indexJson);
			setting.put("settings", indexObj);
			/*JSONObject setting = new JSONObject();
			setting.put("acknowledged", true);*/
			// 封装post信息对象
			StringEntity reqEntity = new StringEntity(setting.toString(),CHATSET_UTF8);
			// 设置Content-Type
			request.addHeader(HTTP_HEADER_CONTENT_TYPE,HTTP_HEADER_APPLICATION_JSON);
			// 设置post信息
			request.setEntity(reqEntity);
			// 执行Post执行
			HttpResponse response = httpClient.execute(request);
			HttpEntity entity = response.getEntity();
			String res = convertStreamToString(entity.getContent());
			if(log.isLoggable(Level.INFO)){
				log.info("res:" + res);
			}
			if (res != null && !"".equals(res)) {
				JSONObject resJson = JSONObject.fromObject(res);
				if (resJson.containsKey("error")) {
					String state = resJson.getString("status");
					String reson = resJson.getJSONObject("error").getString("reason");
					if(log.isLoggable(Level.INFO)){
						log.info("state:" + state + "    reson:" + reson);
					}
				} else {
					isSuccess = true;
				}
			}
		} catch (UnsupportedEncodingException e) {
        	if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (ClientProtocolException e) {
        	if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (IOException e) {
        	if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return isSuccess;
	}
	
	/**
	 * 查询索引是否存在
	 * 
	 * @param indexName
	 *            索引名
	 */
	public boolean isExitMapping(String index, String type) {
		boolean isExit = false;
		HttpClient httpClient = new DefaultHttpClient();
		String url = this.url + "/_mapping";
		HttpGet requests = new HttpGet(url);
		try {
			// 执行Post执行
			HttpResponse response = httpClient.execute(requests);
			HttpEntity entity = response.getEntity();
			String res = convertStreamToString(entity.getContent());
			if(log.isLoggable(Level.INFO)){
				log.info("res:" + res);
			}
			if (res != null && !"".equals(res)) {
				JSONObject resJson = JSONObject.fromObject(res);
				if (resJson.containsKey("error")) {
					String state = resJson.getString("status");
					String reson = resJson.getJSONObject("error").getString("reason");
					if(log.isLoggable(Level.INFO)){
						log.info("state:" + state + "    reson:" + reson);
					}
				} else {
					isExit = true;
				}
			}
		} catch (UnsupportedEncodingException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (ClientProtocolException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (IOException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return isExit;
	}
	
	
	/**
	 * 创建Mapping
	 * 
	 * @param index
	 * @param type
	 */
	public boolean createMapping(String mappingStr) {	
		boolean isSuccess = false;
		HttpClient httpClient = new DefaultHttpClient();
		String _url = this.url + "/_mapping?update_all_types";
		HttpPut request = new HttpPut(_url);
		try {
			// 封装post信息对象
			StringEntity reqEntity = new StringEntity(mappingStr,CHATSET_UTF8);
			// 设置Content-Type
			request.addHeader(HTTP_HEADER_CONTENT_TYPE,HTTP_HEADER_APPLICATION_JSON);
			// 设置post信息
			request.setEntity(reqEntity);
			// 执行Post执行
			HttpResponse response = httpClient.execute(request);
			HttpEntity entity = response.getEntity();
			String res = convertStreamToString(entity.getContent());
			if(log.isLoggable(Level.INFO)){
				log.info("createMapping.res:" + res);
			}
			if (res != null && !"".equals(res)) {
				JSONObject resJson = JSONObject.fromObject(res);
				if (resJson.containsKey("error")) {
					String state = resJson.getString("status");
					String reson = resJson.getJSONObject("error").getString("reason");
					log.warning("url: " + _url + "    state:" + state + "    reson:" + reson);
				} else {
					isSuccess = true;
				}
			}
		} catch (UnsupportedEncodingException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (ClientProtocolException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (IOException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		}  catch (Exception e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		}finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return isSuccess;
	}
	
	/**
	 * 批量插入记录
	 * 
	 * @param jsonFilePath
	 *            json文件路径
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 */
	public boolean bulkInsertDocs(JSONArray docs, String idKeyName) {
		boolean isSuccess = false;
		HttpClient httpClient = new DefaultHttpClient();
		String type = "";
		String index = "";
		int ind = this.url.lastIndexOf("/");
		if (ind > -1) {
			String _url = this.url;
			type = _url.substring(ind + 1);
			
			_url = _url.substring(0, ind);
			ind = _url.lastIndexOf("/");
			if (ind > -1) {
				index = _url.substring(ind + 1);
			}
		}
		try {
			StringBuffer stringBuffer = new StringBuffer();
			int i = 1;
			int length = docs.size();
			for (Object object : docs) {
				JSONObject item = (JSONObject) object;
				if (item != null && item.containsKey(idKeyName)) {
					JSONObject action_and_meta_data = new JSONObject();
					action_and_meta_data.put("_index", index);
					action_and_meta_data.put("_type", type);
					action_and_meta_data.put("_id", item.get(idKeyName));
					JSONObject indexJson = new JSONObject();
					indexJson.put("index", action_and_meta_data);
					stringBuffer.append(indexJson.toString());
					stringBuffer.append("\r\n");
					stringBuffer.append(item.toString());
					if (i >= length){
						// 最后一行必须以"\n"结尾
						stringBuffer.append("\n");
					} else {
						// 每行必须以"\r\n"结尾
						stringBuffer.append("\r\n");
					}
					i++;
				}
				
			}
			String url = this.url + "/_bulk";
			HttpPost request = new HttpPost(url);

			// 封装post信息对象
			StringEntity reqEntity = new StringEntity(stringBuffer.toString(),CHATSET_UTF8);
			// 设置Content-Type
			request.addHeader(HTTP_HEADER_CONTENT_TYPE,"application/x-ndjson");
			// 设置post信息
			request.setEntity(reqEntity);
			// 执行Post执行
			HttpResponse response = httpClient.execute(request);
			HttpEntity entity = response.getEntity();
			String res = convertStreamToString(entity.getContent());
			
			if (res != null && !"".equals(res)) {
				JSONObject resJson = JSONObject.fromObject(res);
				if (resJson.containsKey("error")) {
					String state = resJson.getString("status");
					String reson = resJson.getJSONObject("error").getString("reason");
					if(log.isLoggable(Level.INFO)){
						log.info("state:" + state + "    reson:" + reson);
					}
				} else {
					isSuccess = true;
				}
			}
		} catch (IOException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return isSuccess;
	}
	
	/**
	 * 批量更新记录
	 * 
	 * @param jsonFilePath
	 *            json文件路径
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 */
	public boolean bulkUpdateDocs(JSONArray docs, String idKeyName, List<String> updateFilters) {
		boolean isSuccess = false;
		HttpClient httpClient = new DefaultHttpClient();
		String type = "";
		String index = "";
		int ind = this.url.lastIndexOf("/");
		if (ind > -1) {
			String _url = this.url;
			type = _url.substring(ind + 1);
			
			_url = _url.substring(0, ind);
			ind = _url.lastIndexOf("/");
			if (ind > -1) {
				index = _url.substring(ind + 1);
			}
		}
		try {
			StringBuffer stringBuffer = new StringBuffer();
			int i = 1;
			int length = docs.size();
			for (Object object : docs) {
				JSONObject item = (JSONObject) object;
				if (item != null) {
					JSONObject action_and_meta_data = new JSONObject();
					action_and_meta_data.put("_index", index);
					action_and_meta_data.put("_type", type);
					action_and_meta_data.put("_id", item.get(idKeyName));
					JSONObject indexJson = new JSONObject();
					indexJson.put("update", action_and_meta_data);
					stringBuffer.append(indexJson.toString());
					stringBuffer.append("\r\n");

					JSONObject updateDoc = new JSONObject();
					if (updateFilters == null || updateFilters.isEmpty()) {
						updateDoc.put("doc", item.toString());
					} else {
						JSONObject doc = new JSONObject();
						doc.put(idKeyName, item.get(idKeyName));
						for (String key : updateFilters) {
							if (item.containsKey(key)) {
								doc.put(key, item.get(key));
							}
						}
						updateDoc.put("doc", doc.toString());
					}
					stringBuffer.append(updateDoc.toString());
					if (i >= length){
						// 最后一行必须以"\n"结尾
						stringBuffer.append("\n");
					} else {
						// 每行必须以"\r\n"结尾
						stringBuffer.append("\r\n");
					}
					i++;
				}
				
			}
			String _url = this.url + "/_bulk";
			HttpPost request = new HttpPost(_url);

			// 封装post信息对象
			StringEntity reqEntity = new StringEntity(stringBuffer.toString(),CHATSET_UTF8);
			// 设置Content-Type
			request.addHeader(HTTP_HEADER_CONTENT_TYPE,"application/x-ndjson");
			// 设置post信息
			request.setEntity(reqEntity);
			// 执行Post执行
			HttpResponse response = httpClient.execute(request);
			HttpEntity entity = response.getEntity();
			String res = convertStreamToString(entity.getContent());
			
			if (res != null && !"".equals(res)) {
				JSONObject resJson = JSONObject.fromObject(res);
				if (resJson.containsKey("error")) {
					String state = resJson.getString("status");
					String reson = resJson.getJSONObject("error").getString("reason");
					if(log.isLoggable(Level.INFO)){
						log.info("state:" + state + "    reson:" + reson);
					}
				} else {
					isSuccess = true;
				}
			}
		} catch (IOException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return isSuccess;
	}
	
	/**
	 * 更新记录
	 * 
	 * @param index
	 * @param type
	 * @param messageId
	 * @return
	 */
	public boolean updateDocument(String _id, String updateJson){
		boolean isSuccess = false;
		HttpClient httpClient = new DefaultHttpClient();
		String url = this.url + "/" + _id + "/_update";
		HttpPost request = new HttpPost(url);
		try {
			// 封装post信息对象
			StringEntity reqEntity = new StringEntity(updateJson,CHATSET_UTF8);
			// 设置Content-Type
			request.addHeader(HTTP_HEADER_CONTENT_TYPE,HTTP_HEADER_APPLICATION_JSON);
			// 设置post信息
			request.setEntity(reqEntity);
			// 执行Post执行
			HttpResponse response = httpClient.execute(request);
			HttpEntity entity = response.getEntity();
			String res = convertStreamToString(entity.getContent());
			if(log.isLoggable(Level.INFO)){
				log.info("res:" + res);
			}
			if (res != null && !"".equals(res)) {
				JSONObject resJson = JSONObject.fromObject(res);
				if (resJson.containsKey("error")) {
					String state = resJson.getString("status");
					String reson = resJson.getJSONObject("error").getString("reason");
					if(log.isLoggable(Level.INFO)){
						log.info("state:" + state + "    reson:" + reson);
					}
				} else {
					isSuccess = true;
				}
			}
		} catch (UnsupportedEncodingException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (ClientProtocolException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (IOException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (Exception e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return isSuccess;
	}
	
	/**
	 * 更新记录
	 * 
	 * @param index
	 * @param type
	 * @param messageId
	 * @return
	 */
	public boolean deleteDocument(String index, String type, String _id){
		boolean isSuccess = false;
		HttpClient httpClient = new DefaultHttpClient();
		String _url = this.url + "/" + _id;
		HttpDelete request = new HttpDelete(_url);
		try {
			// 执行Post执行
			HttpResponse response = httpClient.execute(request);
			HttpEntity entity = response.getEntity();
			String res = convertStreamToString(entity.getContent());
			if(log.isLoggable(Level.INFO)){
				log.info("res:" + res);
			}
			if (res != null && !"".equals(res)) {
				JSONObject resJson = JSONObject.fromObject(res);
				if (resJson.containsKey("error")) {
					String state = resJson.getString("status");
					String reson = resJson.getJSONObject("error").getString("reason");
					if(log.isLoggable(Level.INFO)){
						log.info("state:" + state + "    reson:" + reson);
					}
				} else {
					isSuccess = true;
				}
			}
		} catch (UnsupportedEncodingException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (ClientProtocolException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (IOException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return isSuccess;
	}
	
	/**
	 * 根据查询条件删除记录
	 * 
	 * @param index
	 * @param type
	 * @param messageId
	 * @return
	 */
	public boolean deleteDocumentsByQuery(String query){
		boolean isSuccess = false;
		HttpClient httpClient = new DefaultHttpClient();
		
		String _url = this.url + "/_delete_by_query";
		HttpPost request = new HttpPost(_url);
		try {
			/*POST twitter/_delete_by_query
			{
			  "query": { 
			    "match": {
			      "message": "some message"
			    }
			  }
			}*/
			// 封装post信息对象
			StringEntity reqEntity = new StringEntity(query,CHATSET_UTF8);
			// 设置Content-Type
			request.addHeader(HTTP_HEADER_CONTENT_TYPE,HTTP_HEADER_APPLICATION_JSON);
			// 设置post信息
			request.setEntity(reqEntity);
			// 执行Post执行
			HttpResponse response = httpClient.execute(request);
			HttpEntity entity = response.getEntity();
			String res = convertStreamToString(entity.getContent());
			if (res != null && !"".equals(res)) {
				JSONObject resJson = JSONObject.fromObject(res);
				if (resJson.containsKey("error")) {
					String state = resJson.getString("status");
					String reson = resJson.getJSONObject("error").getString("reason");
					log.warning("state:" + state + "    reson:" + reson);
				} else {
					isSuccess = true;
				}
			}
		} catch (UnsupportedEncodingException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (ClientProtocolException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (IOException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return isSuccess;
	}
	
	/**
	 * 根据ID获取记录
	 * 
	 * @param index
	 * @param type
	 * @param id
	 */
	public JSONObject getDocumentById(String id){
		JSONObject source = null;
		HttpClient httpClient = new DefaultHttpClient();
		String _url = this.url + "/_search?q=_id:" + id;
		HttpGet httpGep = new HttpGet(_url);
		String res = "";
		try {
			// 执行Post执行
			HttpResponse response = httpClient.execute(httpGep);
			HttpEntity entity = response.getEntity();
			res = convertStreamToString(entity.getContent());
			if(log.isLoggable(Level.INFO)){
				log.info("res:" + res);
			}
			if (res != null && !"".equals(res)) {
				JSONObject resJson = JSONObject.fromObject(res);
				if (resJson.containsKey("error")) {
					String state = resJson.getString("status");
					String reson = resJson.getJSONObject("error").getString("reason");
					if(log.isLoggable(Level.INFO)){
						log.info("state:" + state + "    reson:" + reson);
					}
				} else {
					JSONObject hits = resJson.getJSONObject("hits");
					if (hits != null) {
						JSONArray hitsArray = hits.getJSONArray("hits");
						for (Object object : hitsArray) {
							JSONObject item = (JSONObject)object;
							source = item.getJSONObject("_source");
						}
					}
				}
			}
		} catch (UnsupportedEncodingException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (ClientProtocolException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (IOException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (Exception e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "结果：" + res, e);
        	}
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return source;
	}
	
	/**
	 * 根据ID获取记录
	 * 
	 * @param index
	 * @param type
	 * @param id
	 */
	public JSONObject getUpdateDocumentById(String id){
		JSONObject source = null;
		HttpClient httpClient = new DefaultHttpClient();
		String url = this.url + "/_search?q=_id:" + id;
		HttpGet httpGep = new HttpGet(url);
		String res = "";
		try {
			// 执行Post执行
			HttpResponse response = httpClient.execute(httpGep);
			HttpEntity entity = response.getEntity();
			res = convertStreamToString(entity.getContent());
			if(log.isLoggable(Level.INFO)){
				log.info("res:" + res);
			}
			if (res != null && !"".equals(res)) {
				JSONObject resJson = JSONObject.fromObject(res);
				if (resJson.containsKey("error")) {
					String state = resJson.getString("status");
					String reson = resJson.getJSONObject("error").getString("reason");
					if(log.isLoggable(Level.INFO)){
						log.info("state:" + state + "    reson:" + reson);
					}
				} else {
					JSONObject hits = resJson.getJSONObject("hits");
					if (hits != null) {
						JSONArray hitsArray = hits.getJSONArray("hits");
						for (Object object : hitsArray) {
							JSONObject item = (JSONObject)object;
							source = item.getJSONObject("_source");
						}
					}
				}
			}
		} catch (UnsupportedEncodingException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (ClientProtocolException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (IOException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (Exception e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "结果：" + res, e);
        	}
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return source;
	}
	
	/**
	 * 根据query查询条件获取记录数
	 * 
	 * @param index
	 * @param type
	 * @param query
	 */
	public int getDocumentsCount(String query){
		int count = 0;
		HttpClient httpClient = new DefaultHttpClient();
		String _url = this.url + "/_count";
		HttpPost request = new HttpPost(_url);
		try {
			/*{
			    "query": {
			        "boolean": {
			            "must": [
			                {"match": {"senderUserName": "wuxiaoying"}},
			                {"match": {"isRead": "1"}}
			            ]
			        }
			    }
			}*/
			// 封装post信息对象
			StringEntity reqEntity = new StringEntity(query.toString(),CHATSET_UTF8);
			// 设置Content-Type
			request.addHeader(HTTP_HEADER_CONTENT_TYPE,HTTP_HEADER_APPLICATION_JSON);
			// 设置post信息
			request.setEntity(reqEntity);
			// 执行Post执行
			HttpResponse response = httpClient.execute(request);
			HttpEntity entity = response.getEntity();
			String res = convertStreamToString(entity.getContent());
			if(log.isLoggable(Level.INFO)){
				log.info("res:" + res);
			}
			if (res != null && !"".equals(res)) {
				JSONObject resJson = JSONObject.fromObject(res);
				if (resJson.containsKey("error")) {
					String state = resJson.getString("status");
					String reson = resJson.getJSONObject("error").getString("reason");
					if(log.isLoggable(Level.INFO)){
						log.info("state:" + state + "    reson:" + reson);
					}
				} else {
					count = resJson.getInt("count");
				}
			}
		} catch (UnsupportedEncodingException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (ClientProtocolException e) {
			if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (IOException e) {
        	if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return count;
	}
		
	/**
	 * 根据ID获取记录
	 * 
	 * @param index
	 * @param type
	 * @param id
	 */
	public List<JSONObject> getDocuments(String query){
		List<JSONObject> result = new ArrayList<JSONObject>();
		String _url = this.url + "/_search";
		String res = postHttpUrl(_url, query);
		if (res != null && !"".equals(res)) {
			JSONObject resJson = JSONObject.fromObject(res);
			if (resJson.containsKey("error")) {
				String state = resJson.getString("status");
				String reson = resJson.getJSONObject("error").getString("reason");
				if(log.isLoggable(Level.INFO)){
					log.info("state:" + state + "    reson:" + reson);
				}
				log.warning("incur error when query, result: state:" + state + "    reson:" + reson);
				log.warning("url:" + _url + ".query=" + query);
			} else {
				JSONObject hits = resJson.getJSONObject("hits");
				if (hits != null) {
					JSONArray hitsArray = hits.getJSONArray("hits");
					for (Object object : hitsArray) {
						JSONObject item = (JSONObject)object;
						JSONObject source = item.getJSONObject("_source");
						result.add(source);
					}
				}
			}
		}
		return result;
	}
	

	
	/**
	 * 根据ID获取记录
	 * 
	 * @param index
	 * @param type
	 * @param id
	 */
	public List<JSONObject> getDocuments(String query,String sources){
		List<JSONObject> result = new ArrayList<JSONObject>();
		String _url = this.url + "/_search";
		if (sources != null && !"".equals(sources)) {
			_url = _url + "?_source=" + sources;
		}
		String res = postHttpUrl(_url, query);
		if (res != null && !"".equals(res)) {
			JSONObject resJson = JSONObject.fromObject(res);
			if (resJson.containsKey("error")) {
				String state = resJson.getString("status");
				String reson = resJson.getJSONObject("error").getString("reason");
				if(log.isLoggable(Level.INFO)){
					log.info("state:" + state + "    reson:" + reson);
				}
				log.warning("incur error when query, result: state:" + state + "    reson:" + reson);
				log.warning("url:" + _url + ".query=" + query);
			} else {
				JSONObject hits = resJson.getJSONObject("hits");
				if (hits != null && hits.size() > 0) {
					JSONArray hitsArray = hits.getJSONArray("hits");
					for (Object object : hitsArray) {
						JSONObject item = (JSONObject)object;
						JSONObject source = item.getJSONObject("_source");
						result.add(source);
					}
				}
			}
		}
		return result;
	}
	
	/**
	 * 根据ID获取记录
	 * 
	 * @param index
	 * @param type
	 * @param id
	 */
	public List<JSONObject> getUpdateDocuments(String query, String sources){
		List<JSONObject> result = new ArrayList<JSONObject>();
		String _url = this.url + "/_search";
		if (sources != null && !"".equals(sources)) {
			_url = _url + "?_source=" + sources;
		}
		String res = postHttpUrl(_url, query);
		if (res != null && !"".equals(res)) {
			JSONObject resJson = JSONObject.fromObject(res);
			if (resJson.containsKey("error")) {
				String state = resJson.getString("status");
				String reson = resJson.getJSONObject("error").getString("reason");
				if(log.isLoggable(Level.INFO)){
					log.info("state:" + state + "    reson:" + reson);
				}
				log.warning("incur error when query, result: state:" + state + "    reson:" + reson);
				log.warning("url:" + _url + ".query=" + query);
			} else {
				JSONObject hits = resJson.getJSONObject("hits");
				if (hits != null) {
					JSONArray hitsArray = hits.getJSONArray("hits");
					for (Object object : hitsArray) {
						JSONObject item = (JSONObject)object;
						JSONObject source = item.getJSONObject("_source");
						result.add(source);
					}
				}
			}
		}
		return result;
	}
	
	/**
	 * 执行查询
	 * 
	 * @param index
	 * @param type
	 * @param id
	 */
	public String getSearchResult(String query){
		String _url = this.url + "/_search";
		String res = postHttpUrl(_url, query);
		return res;
	}
	
	/**
	 * 提交HTTP请求
	 * 
	 * @param url
	 * @param paramJson
	 * @return
	 */
	protected String postHttpUrl(String url, String paramJson){
		HttpClient httpClient = new DefaultHttpClient();
		HttpPost request = new HttpPost(url);
		String res = null;
		try {
			// 封装post信息对象
			StringEntity reqEntity = new StringEntity(paramJson,CHATSET_UTF8);
			// 设置Content-Type
			request.addHeader(HTTP_HEADER_CONTENT_TYPE,HTTP_HEADER_APPLICATION_JSON);
			// 设置post信息
			request.setEntity(reqEntity);
			// 执行Post执行
			HttpResponse response = httpClient.execute(request);
			HttpEntity entity = response.getEntity();
			if(null != entity){
				res = convertStreamToString(entity.getContent());
			}
		} catch (UnsupportedEncodingException e) {
        	if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (ClientProtocolException e) {
        	if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} catch (IOException e) {
        	if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		}  catch (Exception e) {
        	if(log.isLoggable(Level.WARNING)){
        		log.log(Level.WARNING, "", e);
        	}
		} finally {
			if (httpClient != null) {
				httpClient.getConnectionManager().shutdown();
			}
		}
		return res;
	}
	
	public String convertStreamToString(InputStream is) {      
		StringBuffer buffer = new StringBuffer();
		// 读取原始json文件
        BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(is, "UTF-8")); 
			String s = null;
			while ((s = reader.readLine()) != null) {
				// 创建一个包含原始json串的json对象
				buffer.append(s);
			}
		} catch (UnsupportedEncodingException e) {
		} catch (IOException e) {
		}     
        return buffer.toString();
     }
}
