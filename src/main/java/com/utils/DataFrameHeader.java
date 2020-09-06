package com.utils;


import com.svw.mos.mcloud.commons.exception.AppException;
import org.apache.commons.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;

//import org.apache.tomcat.util.codec.binary.StringUtils;

/**
 *
 * <p>ClassName: DataFrameHeaderV1</p>
 * <p>Description: 消息头的第1个版本</p>
 * <p>Author: LIUYL</p>
 * <p>Date: 2018年5月28日</p>
 */
public class DataFrameHeader {

    protected static final String CHARSET_UTF8="utf-8";


    //合法数据帧标识，固定为0xAA，如果不是直接丢弃,占1个字节
    private int magicNo=0xAA;

    //协议版本号，无符号字符0-255，根据项目情况预先规划，占1个字节
    private int protocolVersion;


    //当前帧的body部门的长度，无符号整型，占2个字节
    private int contentLength;


    //32个字节, md5(vin)
    private String clientId;



    //当前时间戳，参考时间定义后端需要验证指令的时效性，指令时效性60秒，需要同步终端时间和云端时间，占长度7个字节，
    private Date timeStamp;

    // 服务类型，无符号性整数，含义取决于业务上对服务的分类，占1个字节，
    private int serviceType;


    //服务ID,全局唯一，无符号性整数，最多支持65000个服务 ,占2个字节
    private int serviceId;



    //客户端在同步响应时，需要将消息写入该队列中
    //客户端向后台发消息时无需设置该字段的值
    //客户端在处理一个来自后台的同步请求，并需要响应时，需要将响应的消息写入该队列，参考队列命名规则
    //占2个字节
    private int responseTopicId;


    //一次请求的事件ID,对同一台设备ID必须唯一
    //对某个请求进行响应时，eventId设置为请求消息中的eventId
    //占 2个字节
    private int eventId;


    //isSyn  7
    //needResponse 6
    //isResponse 5
    //isCompressed 4
    //needBodySign 3
    //needBodyEncrypt 2
    //bodyEncrypeAlg  0-1


    private boolean isSyn;

    //是否需要响应 是否需要返回(1:true,0:false 如果为1则发送处理结果)，占下标的第6位
    private boolean needResponse;

    //表示当前消息是否是一个对请求的响应（1:true,0:false）,占下标的的第5位
    private boolean isResponse;

    //是否压缩，压缩算法固定为gzip(1:true,0:false) ,占下标的第3位
    private boolean isCompressed;

    //保留位，占下标的第2位
    private boolean needBodySign;

    //是否需要加密
    private boolean needBodyEncrypt;

    //0:不加密
    //1: AES CBC 128,16字节IV
    //2:保留
    private int bodyEncryAlg;


    //32
    private byte[] bodySign;

    private static final Logger LOGGER = LoggerFactory.getLogger(DataFrameHeader.class);



    public int getHeaderSize() {
        if(this.getNeedBodySign()) {
            return 83;
        }
        return 51 ;
    }


    /**
     * 从ByteBuffer提取数据解析到当前对象数据域中
     *
     * @param byteBuf
     */

    public void fromBytes(ByteBuffer byteBuf) {

        //规范码
        int magicNoVal=Byte.toUnsignedInt(byteBuf.get());

        if(magicNoVal !=0xAA) {
            throw  new IllegalArgumentException("magicNo not eq 0xAA");
        }
        this.setMagicNo(magicNo);

        //协议版本
        this.setProtocolVersion(Byte.toUnsignedInt(byteBuf.get()));

        //body的字节长度
        int bodyLen=Short.toUnsignedInt(byteBuf.getShort());
        this.setContentLength(bodyLen);

        //clientId
        byte [] bytes=new byte[32];
        byteBuf.get(bytes);
        this.setClientId(StringUtils.newStringUtf8(bytes));
        // 7 bytes [毫秒2byte][分钟1byte][小时1byte][日1byte][月1byte][年1byte 实际年份-2000] 低位有效
        Calendar calendar = Calendar.getInstance();
        // 毫秒2byte,short, 包含秒和毫秒,第5,6字节

        int sms = Short.toUnsignedInt(byteBuf.getShort());
        int seconds = sms / 1000;
        int ms = sms - seconds * 1000;
        calendar.set(Calendar.SECOND, seconds);
        calendar.set(Calendar.MILLISECOND, ms);
        // 分钟1byte，第7字节
        calendar.set(Calendar.MINUTE, Byte.toUnsignedInt(byteBuf.get()));
        // 小时1byte，24小时制，第8字节
        calendar.set(Calendar.HOUR_OF_DAY, Byte.toUnsignedInt(byteBuf.get()));
        // 日1byte,1开始，第9字节
        calendar.set(Calendar.DAY_OF_MONTH, Byte.toUnsignedInt(byteBuf.get()));
        // 月1byte，第10字节
        calendar.set(Calendar.MONTH, Byte.toUnsignedInt(byteBuf.get()) - 1);
        // 年1byte 实际年份-2000，第11字节
        calendar.set(Calendar.YEAR, Byte.toUnsignedInt(byteBuf.get()) + 2000);
        this.setTimeStamp(calendar.getTime());

        //服务类型
        this.setServiceType(Byte.toUnsignedInt(byteBuf.get()));
        //服务ID
        this.setServiceId(Short.toUnsignedInt(byteBuf.getShort()));

        //响应消息的topicId,主要用于后台发送同步消息是，需要终端向指定的主题中发送结果消息，以便后台进程读取
        this.setResponseTopicId(Short.toUnsignedInt(byteBuf.getShort()));

        //事件id,对同一台设备的eventId必须是唯一的
        this.setEventId(Short.toUnsignedInt(byteBuf.getShort()));



        //isSyn  7
        //needResponse 6
        //isResponse 5
        //isCompressed 4
        //needBodySign 3
        //needBodyEncrypt 2
        //bodyEncrypeAlg  0-1

        int temp=byteBuf.get();

        int isSync=temp>>7 & 0x01;
        this.setIsSyn(isSync==1);

        int needReponse=temp>>6 & 0x01;
        this.setNeedResponse(needReponse==1);

        int isResponseVal=	temp>>5 & 0x01;
        this.setIsResponse(isResponseVal==1);

        int isCompressedVal=temp>>4 & 0x01;
        this.setIsCompressed(isCompressedVal==1);

        int needBodySignVal=temp>>3 & 0x01;
        this.setNeedBodySign(needBodySignVal==1);

        int needBodyEncryptVal=temp>>2 & 0x01;
        this.setNeedBodyEncrypt(needBodyEncryptVal==1);

        int bodyEncrypeAlg=temp & 0x03;
        this.setBodyEncryAlg(bodyEncrypeAlg);

        if(this.needBodySign) {
            byte [] dst=new byte[32];
            byteBuf.get(dst);
            this.setBodySign(dst);
        }


    }

    /**
     * 将当前对象转化为ByteBuffer 对象
     * @return
     */
    public byte[] toBytes() {

        // 先定义字节缓存空间长度，采用allocateDirect 启用非copy模式
        ByteBuffer byteBuf = ByteBuffer.allocate(this.getHeaderSize());

        // 1 byte
        int protocolVersionVal = this.getProtocolVersion();

        // 1、放入非法验证表示字段0xAA
        int magicNoVal = 0xaa;

        byteBuf.put((byte) magicNoVal);

        //2、协议版本 一个字节
        byteBuf.put((byte) protocolVersionVal);

        // contentLenth
        byteBuf.putShort((short) this.getContentLength());

        //clientid
        byteBuf.put(StringUtils.getBytesUtf8(this.getClientId()));


        // 7 bytes [毫秒2byte][分钟1byte][小时1byte][日1byte][月1byte][年1byte 实际年份-2000] 低位有效
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(this.getTimeStamp());

        // 毫秒2byte
        byteBuf.putShort((short) (calendar.get(Calendar.SECOND) * 1000 + calendar.get(Calendar.MILLISECOND)));
        // 分钟1byte
        byteBuf.put((byte) calendar.get(Calendar.MINUTE));
        // 小时1byte 24小时制
        byteBuf.put((byte) calendar.get(Calendar.HOUR_OF_DAY));
        // 日1byte 1开始
        byteBuf.put((byte) calendar.get(Calendar.DAY_OF_MONTH));
        // 月1byte 1-12月
        byteBuf.put((byte) (calendar.get(Calendar.MONTH) + 1));
        // 年1byte 实际年份-2000
        byteBuf.put((byte) (calendar.get(Calendar.YEAR) - 2000));

        // 1 byte serviceType ，第12字节
        byteBuf.put((byte) getServiceType());

        // 2 bytes Service ID ,第13,14，无符号性，以int 代表
        byteBuf.putShort((short) getServiceId());

        // 2 bytes Response Service ID ,第15,16，无符号性，以int 代表
        byteBuf.putShort((short) getResponseTopicId());

        // 2 bytes Event ID 第17,18，无符号性，以int 代表
        byteBuf.putShort((short) this.getEventId());

        // 1 byte 标志, 第19,无符号型
        /**
         * isSyn 7
         * isNeedResponse 6
         * isResponse 5
         * isCompressed 4
         * needBodySign 3
         * needBodyEncrypt  2
         * bodyEncryptAlg 1-0
         *
         * 前面是高位序，后面是低位序
         */
        int temp = (this.getIsSyn() ? 1 : 0) << 7 | (this.getNeedResponse() ? 1 : 0) << 6
                | (this.getIsResponse() ? 1 : 0) << 5 | (this.getIsCompressed() ? 1 : 0) << 4 | (this.getNeedBodySign() ? 1 : 0) << 3
                | (this.getNeedBodyEncrypt()?1:0)<<2 |this.getBodyEncryAlg();

        byteBuf.put((byte) temp);


        //是否需要签名
        if (this.getNeedBodySign()) {
            if(this.getBodySign()==null) {
                throw new AppException(" bodySign can’t be null");
            }
            if(this.bodySign.length !=32) {
                throw new AppException(" bodySign’s bytes lenth should be 32 ");
            }
            byteBuf.put(bodySign);

        }

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("End of header encode!");

        byteBuf.flip();

        return byteBuf.array();
    }

    public int getMagicNo() {
        return magicNo;
    }

    public void setMagicNo(int magicNo) {
        this.magicNo = magicNo;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public int getContentLength() {
        return contentLength;
    }

    public void setContentLength(int contentLength) {
        this.contentLength = contentLength;
    }


    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getServiceType() {
        return serviceType;
    }

    public void setServiceType(int serviceType) {
        this.serviceType = serviceType;
    }

    public int getServiceId() {
        return serviceId;
    }

    public void setServiceId(int serviceId) {
        this.serviceId = serviceId;
    }

    public int getResponseTopicId() {
        return responseTopicId;
    }

    public void setResponseTopicId(int responseTopicId) {
        this.responseTopicId = responseTopicId;
    }

    public int getEventId() {
        return eventId;
    }

    public void setEventId(int eventId) {
        this.eventId = eventId;
    }

    public boolean getIsSyn() {
        return isSyn;
    }

    public void setIsSyn(boolean isSyn) {
        this.isSyn = isSyn;
    }

    public boolean getNeedResponse() {
        return needResponse;
    }

    public void setNeedResponse(boolean needResponse) {
        this.needResponse = needResponse;
    }

    public boolean getIsResponse() {
        return isResponse;
    }

    public void setIsResponse(boolean isResponse) {
        this.isResponse = isResponse;
    }

    public boolean getIsCompressed() {
        return isCompressed;
    }

    public void setIsCompressed(boolean isCompressed) {
        this.isCompressed = isCompressed;
    }

    public boolean getNeedBodySign() {
        return needBodySign;
    }

    public void setNeedBodySign(boolean needBodySign) {
        this.needBodySign = needBodySign;
    }

    public boolean getNeedBodyEncrypt() {
        return needBodyEncrypt;
    }

    public void setNeedBodyEncrypt(boolean needBodyEncrypt) {
        this.needBodyEncrypt = needBodyEncrypt;
    }

    public int getBodyEncryAlg() {
        return bodyEncryAlg;
    }

    public void setBodyEncryAlg(int bodyEncryAlg) {
        this.bodyEncryAlg = bodyEncryAlg;
    }

    public byte[] getBodySign() {
        return bodySign;
    }

    public void setBodySign(byte[] bodySign) {
        this.bodySign = bodySign;
    }


}
