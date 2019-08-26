/**
 * 
 */
package cn.chinaelink.im.es.tool.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;

import cn.chinaelink.im.es.tool.service.EsService;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * 创建mapping
 * 命令格式：createMapping [http://ip:9200/index_name/type_name] [input path]
 * 
 * @author duyanjun
 * @since 2019/08/26 杜燕军 新建
 */
public class CommandCreateMapping implements Command {
	
	// 源数据ES服务器的Url
	private String url;
	// 导出数据文件的保存路径
	private String inputPath;

	/**
	 * 构造方法
	 * 
	 * @param args
	 */
	public CommandCreateMapping(String[] args){
		if (args.length > 0) {
			this.url = args[0];
			if (args.length > 1) {
				this.inputPath = args[1];
			}
		}
	}
	
	@Override
	public void execute() {
		String mapping = getMappings();
		System.out.println("mapping数据加载完成" + ",开始执行创建mapping.....");
		System.out.println("URL:" + this.url);
		System.out.println("inputPath:" + this.inputPath);
		EsService service = new EsService(this.url);
		service.createMapping(mapping);
		System.out.println("mapping创建完成");
	}
	
	public String getMappings() {
		StringBuffer buffer = new StringBuffer();
		BufferedReader br = null;
		// 读取原始json文件并进行操作和输出
		try {
			// 读取原始json文件
			br = new BufferedReader(new FileReader(this.inputPath));
			String s = null;
			while ((s = br.readLine()) != null) {
				// 创建一个包含原始json串的json对象
				buffer.append(s);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return buffer.toString();
	}

}
