/**
 * 
 */
package cn.chinaelink.im.es.tool;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Scanner;

import cn.chinaelink.im.es.tool.command.Command;

/**
 * 应用程序入口
 * @author duyanjun
 *
 */
public class Application {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
while (true) {
			
			System.out.println("--------------------------ES工具程序命令说明---------------------------");
			System.out.println("export: 导出指定es的数据，文件格式为json;命令格式：export [http://ip:port/index_name/type_name] [output path] [type: 0: 单文件	1:output path目录下第1000条一个文件]");
			System.out.println("import: 将json数据文件导入到指定的es中;  命令格式：import [http://ip:9200/index_name/type_name] [input path] [idKeyName] [type 0:文件 1:目录下的所有文件]");
			System.out.println("createIndex:创建索引;命令格式：createIndex [http://ip:9200/index_name]");
			System.out.println("createMapping:创建索引;命令格式：createMapping [http://ip:9200/index_name/type_name] [input path]");
			System.out.println("quit: 退出");
			System.out.println("---------------------------------------------------------------------");
			Scanner stdin = new Scanner(System.in);
			String commandLine = stdin.nextLine();
			String[] commands = commandLine.split(" ");
			String command = "";
			if (commands != null && commands.length > 0) {
				command = commands[0];
				String[] commandArgs = new String[commands.length - 1];
				if (commands.length > 1) {
					for (int i = 1; i <= commands.length -1; i++) {
						commandArgs[i-1] = commands[i];
					}
				}
				if (command != null && command.length() > 0) {
					String preStr = command.substring(0, 1).toUpperCase();
					String end = command.substring(1);
					String comm = preStr + end;
					String className = "cn.chinaelink.im.es.tool.command.Command"
							+ comm;
					Class<?> claz;
					try {
						claz = Class.forName(className);
						Constructor<?> constructor = claz
								.getConstructor(String[].class);
						if (constructor != null) {
							Command comd = (Command) constructor.newInstance(new Object[]{commandArgs});
							comd.execute();
						}
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					} catch (SecurityException e) {
						e.printStackTrace();
					} catch (NoSuchMethodException e) {
						e.printStackTrace();
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
					} catch (InstantiationException e) {
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						e.printStackTrace();
					} catch (InvocationTargetException e) {
						e.printStackTrace();
					}
				}
			}
		}

	}

}
