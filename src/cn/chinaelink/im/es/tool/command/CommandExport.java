/**
 * 
 */
package cn.chinaelink.im.es.tool.command;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import cn.chinaelink.im.es.tool.service.EsService;

/**
 * 导出指定ES的数据文件格式为json
 * 命令格式：export [http://ip:port/index_name/type_name] [output path] [type: 0: 单文件	1:output path目录下第1000条一个文件]
 * 
 * @author duyanjun
 * @since 2019/08/26 杜燕军 新建
 */
public class CommandExport implements Command {
	
	// 源数据ES服务器的Url
	private String url;
	// 导出数据文件的保存路径
	private String outPath;
	
	private int type;
	
	/**
	 * 构造方法
	 * 
	 * @param args
	 *            命令参数
	 */
	public CommandExport(String[] args){
		if (args.length > 0) {
			this.url = args[0];
			if (args.length > 1) {
				this.outPath = args[1];
				if (args.length > 2) {
					this.type = Integer.parseInt(args[2]);
				}
			} else {
				String fileName = "data.json";
				int index = this.url.lastIndexOf("/");
				if (index > -1) {
					fileName = this.url.substring(index +1) + ".json";
				}
				this.outPath = new File("").getAbsolutePath() + File.separatorChar + fileName;
			}
		}
	}

	@Override
	public void execute() {
		/*PUT /elink/_settings
		{ 
		  "max_result_window" : 900000000
		} 
		*/
		if (this.url == null || "".equals(this.url)) {
			System.out.println("命令格式不正确，请输入url参数");
		} else {
			System.out.println("开始导出数据............");
			System.out.println("URL:" + this.url);
			System.out.println("outPath:" + this.outPath);
			EsService service = new EsService(this.url);
			
			JSONObject query = new JSONObject();
			JSONObject matchAll = new JSONObject();
			matchAll.put("match_all", new JSONObject());
			query.put("query", matchAll);
			
			int count = service.getDocumentsCount(query.toString());
			int start = 0;
			int limit = 1000;
			int totalCount = 0;

			JSONObject resObj = new JSONObject();
			JSONArray resArray = new JSONArray();
			int fileIndex = 1;
			while (start < count) {
				JSONObject queryJson = new JSONObject();
				JSONObject matchAllJson = new JSONObject();
				matchAllJson.put("match_all", new JSONObject());
				queryJson.put("query", matchAll);
				queryJson.put("from", start);
				queryJson.put("size", limit);				
				List<JSONObject> res = service.getDocuments(queryJson.toString());
				start = start + limit;
				if (res != null && !res.isEmpty()) {
					if (this.type == 1) {
						JSONObject dataObj = new JSONObject();
						dataObj.put("data", res);
						writeDataToFile(dataObj, fileIndex);
						fileIndex ++;
					} else {
						resArray.addAll(res);
					}
					totalCount += res.size();
					res.clear();
				}
				System.out.println("数据已导出：" + totalCount);
			}
			if (this.type == 0) {
				resObj.put("data", resArray);
				writeDataToFile(resObj);
			}
			System.out.println("数据导出完成，总数：" + totalCount + "，保存路径:" + this.outPath);
		}

	}
	
	private void writeDataToFile(JSONObject json){
		File exportFile = new File(this.outPath);
		try {
			if (!exportFile.exists()) {
				exportFile.createNewFile();
			}
			FileWriter fw = new FileWriter(exportFile);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(json.toString());
			bw.flush();
			bw.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void writeDataToFile(JSONObject json, int index){
		int ind = this.outPath.lastIndexOf("/");
		String fileName = "";
		if (ind > -1) {
			fileName = this.outPath.substring(ind + 1);
		}
		File outputFilePath = new File(this.outPath);
		if (!outputFilePath.exists()) {
			outputFilePath.mkdirs();
		}
		File exportFile = new File(this.outPath + File.separatorChar + fileName + "_" + index + ".json");
		try {
			if (!exportFile.exists()) {
				exportFile.createNewFile();
			}
			FileWriter fw = new FileWriter(exportFile);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(json.toString());
			bw.flush();
			bw.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
