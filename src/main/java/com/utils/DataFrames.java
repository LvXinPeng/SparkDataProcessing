package com.utils;

import com.svw.mos.mcloud.commons.exception.AppException;
import com.svw.mos.mcloud.commons.util.CompressionUtil;
import com.svw.mos.mcloud.commons.util.HexConverUtil;
import com.svw.mos.mcloud.commons.util.security.AESUtil;
import com.svw.mos.mcloud.commons.util.security.SHAUtil;
import org.apache.commons.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
//import com.xinpeng.DataFrameHeader;

/**
 * 
 * <p>ClassName: DataFrame</p>
 * <p>Description: 数据帧编码解码解密类</p>
 * <p>Author: lIUYL</p>
 * <p>Date: 2017年11月22日</p>
 */
public class DataFrames {
	
	
	private static final Logger LOGGER= LoggerFactory.getLogger(DataFrames.class);
	
	
	/**
	 * 解码 将消息字节解码成数据帧对象
	 * @param bytes 消息字节
	 * @param sKey 解密秘钥
	 * @return 消息帧对象
	 */
	public static DataFrame fromBytes(byte[] bytes,String sKey) {

		DataFrame dataFrame = new DataFrame();
		ByteBuffer byteBuf = ByteBuffer.wrap(bytes);
		DataFrameHeader header = new DataFrameHeader();
		header.fromBytes(byteBuf);
		dataFrame.setHeader(header);

		int bodyLen = header.getContentLength();
		
		//没有消息体
		if(bodyLen==0) {
			return dataFrame;
		}
		
		byte[] bodyBytes = new byte[bodyLen]; // 获取消息体的字节
		byteBuf.get(bodyBytes);		
		
		
		//1、解压
		if(header.getIsCompressed()) { //已压缩，需要解压
			try {
				bodyBytes=CompressionUtil.decompress(bodyBytes);
			} catch (IOException e) {
				throw new AppException("解压出现异常");
			}
		}
		
	
		String body=null;
		
		//2、解密
		if(header.getNeedBodyEncrypt() && 1==header.getBodyEncryAlg()) { //已经加密，需要解密
			bodyBytes=AESUtil.decrypt(bodyBytes, sKey,sKey);
			LOGGER.debug("已经对消息体加密，用skey={},进行解密完成",sKey);
			
		}
		
		
		body= StringUtils.newStringUtf8(bodyBytes);
		
		//3、验签
		if(header.getNeedBodySign()) {
			
			StringBuilder sb=new StringBuilder(sKey);
			sb.append(header.getTimeStamp().getTime());
			sb.append(header.getClientId());
			sb.append(header.getServiceId());
			sb.append(body);
			
			//根据消息内容生成签名字节
	 		byte [] signBytes=SHAUtil.encodeSHA256ToBytes(sb.toString());
	 		//将生成的字节转成16进制字符串
	 		String bodySign=HexConverUtil.conver16HexStr(signBytes);
	 		//将原消息中的签名字节转成16进制字符串
	 		String sourceBodySign=HexConverUtil.conver16HexStr(header.getBodySign());
	 		if(!bodySign.equals(sourceBodySign)) { //签名验证失败
	 			throw new AppException("签名不正确");
	 		}
		}
		
		
		dataFrame.setBody(body);
	
		return dataFrame;

	} 
	
	
	 /**
	  * 编码 将数据帧对象转成消息字节
	  * @param dataFrame 数据帧对象
	  * @param sKey 加密秘钥
	  * @return 消息字节
	  */
	 public  static byte[] toBytes(DataFrame dataFrame,String sKey)  {
		    
		 	DataFrameHeader header=dataFrame.getHeader();
		 	
		 	String body=dataFrame.getBody();
		 	
		 	if(body ==null || body.trim().equals("")) {
		 		header.setContentLength(0);
		 		ByteBuffer byteBuf = ByteBuffer.allocate(header.getHeaderSize());
				byteBuf.put(header.toBytes());
			
				return byteBuf.array();
		 	}
		 	
		   //1、是否需要签名
		   if(header.getNeedBodySign()) {
			   
		 		//构建签名内容： skey+Timestamp+Client ID+serviceid+body
				StringBuilder sb=new StringBuilder(sKey);
				sb.append(header.getTimeStamp().getTime());
				sb.append(header.getClientId());
				sb.append(header.getServiceId());
				sb.append(dataFrame.getBody());
				
		 		byte [] signBytes=SHAUtil.encodeSHA256ToBytes(sb.toString());
		 		header.setBodySign(signBytes);
		 	}
		   
			byte [] bodyBytes=StringUtils.getBytesUtf8(dataFrame.getBody());
			
		   //2、是否需要加密
			if(header.getNeedBodyEncrypt() && 1==header.getBodyEncryAlg()){
				bodyBytes=AESUtil.encrypt(bodyBytes, sKey,sKey);
				
			}
			
			//3、是否需要压缩
			if(header.getIsCompressed()) { //已压缩
		 		try {
					bodyBytes=CompressionUtil.compress(bodyBytes);
				} catch (IOException e) {
					header.setIsCompressed(false);
				}
		 		
		 	}
		 	
			//设置消息体内容长度
		 	header.setContentLength(bodyBytes.length);
			ByteBuffer byteBuf = ByteBuffer.allocate(header.getHeaderSize() + bodyBytes.length);
			byteBuf.put(header.toBytes());
			byteBuf.put(bodyBytes);
			
			return byteBuf.array();
			
		 
	 }
	
	
	
	
	private DataFrames() {}
}
