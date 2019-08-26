/**
 * (C) Copyright 2009, 2011 E-Link Science Technology Co.,Ltd.
 */
package cn.chinaelink.im.es.tool.command;

/**
 * 退出应用程序
 * 
 * @author duyanjun
 * @since 2019/08/26 杜燕军 新建
 */
public class CommandQuit implements Command {

	public CommandQuit(String[] args) {
	}

	@Override
	public void execute() {
		System.out.println("程序正在停止...");
		System.exit(1);
	}

}
