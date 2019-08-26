/**
 * 
 */
package cn.chinaelink.im.es.tool.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import cn.chinaelink.im.es.tool.service.EsService;

/**
 * 导入指定ES的数据文件格式为json
 * 命令格式：import [http://ip:9200/index_name/type_name] [input path] [idKeyName] [type 0:文件 1:目录下的所有文件]
 * 
 * @author duyanjun
 * @since 2019/08/26 杜燕军 新建
 */
public class CommandImport implements Command {
	
	// 源数据ES服务器的Url
	private String url;
	// 导出数据文件的保存路径
	private String inputPath;
	
	private String idKeyName;
	
	private int type;

	/**
	 * 构造方法
	 * 
	 * @param args
	 * 		命令参数
	 */
	public CommandImport(String[] args){
		if (args.length > 0) {
			this.url = args[0];
			if (args.length > 1) {
				this.inputPath = args[1];
				if (args.length > 2) {
					this.idKeyName = args[2];
					if (args.length > 3) {
						this.type = Integer.parseInt(args[3]);
					}
				} else {
					this.idKeyName = "_id";
				}
			} else {
				String fileName = "data.json";
				int index = this.url.lastIndexOf("/");
				if (index > -1) {
					fileName = this.url.substring(index +1) + ".json";
				}
				this.inputPath = new File("").getAbsolutePath() + File.separatorChar + fileName;
			}
		}
	}
	
	@Override
	public void execute() {
		JSONArray dataArray = null;
		if (this.type == 0) {
			dataArray = readData();
		} else {
			dataArray = readMultiFileData();
		}
		System.out.println("数据解析完成，总数：" + dataArray.size() + ",开始执行数据导入.....");
		System.out.println("URL:" + this.url);
		System.out.println("outPath:" + this.inputPath);

		EsService service = new EsService(this.url);
		int total = dataArray.size();
		int start = 0;
		int limit = 1000;
    	if (!dataArray.isEmpty() && limit > 0) {
    		int end = start + limit;
    		if (end >= total) {
    			end = total;
    		}
        	List<?> resList = null;
    		while (start < end) {
        		resList = dataArray.subList(start, end);
        		JSONArray submitArray = new JSONArray();
        		submitArray.addAll(resList);
        		service.bulkInsertDocs(submitArray, idKeyName);
        		System.out.println("数据已导入了：" + end);
        		start = end;
        		end = start + limit;
        		if (end >= total) {
        			end = total;
        		}
    		} 
    	} 
		System.out.println("数据导入完成............");

	}
	
	private JSONArray readData(){
		JSONArray dataArray = new JSONArray();

		File inputFile = new File(this.inputPath);
		if (inputFile.exists()) {
			try {
				FileReader fr = new FileReader(inputFile);
				BufferedReader br = new BufferedReader(fr);
				String dataJson = br.readLine();
				JSONObject dataObj = JSONObject.fromObject(dataJson);
				dataArray.addAll(dataObj.getJSONArray("data"));
			} catch (IOException e) {
				// TODO: handle exception
			}
		} else {
			System.out.println("inputPath文件不存在，请确认输入路径是否正确");
		}
		return dataArray;
	}
	
	private JSONArray readMultiFileData(){
		JSONArray dataArray = new JSONArray();
		File inputFile = new File(this.inputPath);
		if (inputFile.exists()) {
			File[] dataFile = inputFile.listFiles();
			if (dataFile != null && dataFile.length > 0) {
				for (File file : dataFile) {
					try {
						FileReader fr = new FileReader(file);
						BufferedReader br = new BufferedReader(fr);
						String dataJson = br.readLine();
						JSONObject dataObj = JSONObject.fromObject(dataJson);
						dataArray.addAll(dataObj.getJSONArray("data"));
					} catch (IOException e) {
						// TODO: handle exception
					}
				}
			}
		} else {
			System.out.println("inputPath文件不存在，请确认输入路径是否正确");
		}
		return dataArray;
	}

}
