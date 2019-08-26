/**
 * 
 */
package cn.chinaelink.im.es.tool.command;

import cn.chinaelink.im.es.tool.service.EsService;

/**
 * 创建索引
 * 命令格式：createIndex [http://ip:9200/index_name]
 * 
 * @author duyanjun
 * @since 2019/08/26 杜燕军 新建
 */
public class CommandCreateIndex implements Command {
	
	// 源数据ES服务器的Url
	private String url;

	/**
	 * 构造方法
	 * 
	 * @param args
	 */
	public CommandCreateIndex(String[] args){
		if (args.length > 0) {
			this.url = args[0];
		}
	}
	
	@Override
	public void execute() {
		EsService service = new EsService(this.url);
		String indexName = "elink";
		int index = this.url.lastIndexOf("/");
		if (index > -1) {
			indexName = this.url.substring(index + 1);
		}
		boolean res = service.createIndex(indexName);
		if (res) {
			System.out.println("索引" + indexName + "创建成功");
		} else {
			System.out.println("索引" + indexName + "创建失败");
		}

	}

}
