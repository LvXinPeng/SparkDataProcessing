package com.utils;

/**
 * 
 * <p>ClassName: DataFrame</p>
 * <p>Description: 数据帧</p>
 * <p>Author: lIUYL</p>
 * <p>Date: 2017年11月22日</p>
 */
public class DataFrame {
	

	//数据帧头
	private DataFrameHeader header;
	
	//消息体，JSON格式
	private String body;
	
	

	public DataFrameHeader getHeader() {
		return header;
	}

	public void setHeader(DataFrameHeader header) {
		this.header = header;
	}


	public String getBody() {
		return body;
	}


	public void setBody(String body) {
		this.body = body;
	}
	

	public DataFrame(DataFrameHeader header, String body) {
		super();
		this.header = header;
		this.body = body;
	}
	

	public DataFrame() {
	}

}
