package com.mr.utils;

import com.google.protobuf.Message;

/**
 * pb格式与string之间进行转换的接口
 * 
 * @author cangyingzhijia
 *
 */
public interface ProtobufMessageCodec<T extends Message> {
	/**
	 * 将message序列化为string
	 * @param message 要序列化的message对象
	 * @return 序列化得到的string结果
	 */
	String serializeToString(Message message) throws Exception;

	/**
	 * 从input构造Message对象
	 * @param input 序列化的对象窜
	 * @param builder 要构造的message builder对象
	 * @return
	 */
	T parseFromString(final String input, final Message.Builder builder);

}
