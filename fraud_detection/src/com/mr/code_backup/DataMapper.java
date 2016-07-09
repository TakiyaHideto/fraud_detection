package com.mr.code_backup;

import java.io.IOException;
import java.lang.ArrayIndexOutOfBoundsException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import com.mr.config.Properties;
import com.mr.protobuffer.OriginalBidLog;
import com.mr.protobuffer.OriginalClickLog;
import com.mr.protobuffer.OriginalShowLog;
import com.mr.protobuffer.OriginalWinLog;
import com.mr.utils.CommUtil;
import com.mr.utils.AuxiliaryFunction;
import com.mr.utils.TextMessageCodec;

public class DataMapper{
	public static class OriginalBidLogMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text();
        private FileSystem fs = null;
        private Set<String> orderSet = new HashSet<String>();
        private Map<String,String> campaignTable = new HashMap<String,String>();
        private Map<String,String> orderTable = new HashMap<String,String>();

        protected void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            Counter setupTestCt = context.getCounter("OriginalBidLogMapper", "setupTestCt");
            Counter tanxConvOrder = context.getCounter("OriginalBidLogMapper", "tanxConvOrder");
            fs = FileSystem.get(conf);
            String importPath = (String) context.getConfiguration().get("importPath");
            String orderTanxPath = (String) context.getConfiguration().get("orderTanxPath");
            // CommUtil.fetchDsp3NormalTableInfo(fs, importPath + Properties.Mapper.MP_ORDER_PATH_NEW, Properties.Mapper.ORDER_TABLE_LEN_NEW, this.orderTable);
            // CommUtil.fetchDsp3NormalTableInfo(fs, importPath + Properties.Mapper.MP_CAMPAIGN_PATH_NEW, Properties.Mapper.CAMPAIGN_TABLE_LEN_NEW, this.campaignTable);
            CommUtil.readDataFromOrderTanx(fs, orderTanxPath, this.orderSet, tanxConvOrder);
        } 

		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			// create counters
            Counter validInputCt = context.getCounter("OriginalBidLogMapper", "validInputCt");
            Counter validOutputCt = context.getCounter("OriginalBidLogMapper", "validOutputCt");
            Counter otherExceptionCt = context.getCounter("OriginalBidLogMapper", "otherExceptionCt");
            Counter protoBufferExceptCt = context.getCounter("OriginalBidLogMapper", "protoBufferExceptCt");
            Counter dataHandlerExceptCt = context.getCounter("OriginalBidLogMapper", "dataHandlerExceptCt");
            Counter arrayIndexOutOfBoundsExceptionCt = context.getCounter("OriginalBidLogMapper", "arrayIndexOutOfBoundsExceptionCt");

            // initialization
            LinkedList<String> featureValueList = new LinkedList<String>();
            TextMessageCodec TMC = new TextMessageCodec();
            OriginalBidLog.OriginalBid bidLog;
            try{
                bidLog = (OriginalBidLog.OriginalBid) TMC.parseFromString(value.toString(), OriginalBidLog.OriginalBid.newBuilder());  
                validInputCt.increment(1);
            } catch (RuntimeException e){
                protoBufferExceptCt.increment(1);
                return;
            } catch (Exception e){
                otherExceptionCt.increment(1);
                return;
            }

            // key info	
            String sessionId = bidLog.getSessionId();

            // value info
            try{
                DataHandler dataHandler = new DataHandler(this.campaignTable,
                                                this.orderTable,  
                                                this.orderSet);
                featureValueList = dataHandler.processBidLog(bidLog, context);
            } catch (ArrayIndexOutOfBoundsException e){
                arrayIndexOutOfBoundsExceptionCt.increment(1);
                return;
            } catch (ParseException e){
                dataHandlerExceptCt.increment(1);
                return;
            }

            // output key-value
            for (String fv : featureValueList){
                if (!fv.equals("")){
                    String[] infoElements = fv.split(Properties.Base.BS_SEPARATOR_TAB);
                    key_result.set(sessionId + Properties.Base.BS_SEPARATOR_UNDERLINE + infoElements[1]);
                    value_result.set(fv);
                    context.write(key_result, value_result);
                    validOutputCt.increment(1);
                }
            }
		}
	}

	public static class OriginalWinLogMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text();

		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			// create counters
            Counter validInputCt = context.getCounter("OriginalWinLogMapper", "validInputCt");
            Counter invalidInputCt = context.getCounter("OriginalWinLogMapper", "invalidInputCt");
            Counter protoBufferExceptCt = context.getCounter("OriginalWinLogMapper", "protoBufferExceptCt");
            Counter dataHandlerExceptCt = context.getCounter("OriginalWinLogMapper", "dataHandlerExceptCt");

            // initialization
            LinkedList<String> featureValueList = new LinkedList<String>();
			TextMessageCodec TMC = new TextMessageCodec();
            OriginalWinLog.WinNoticeLogMessage winLog;
            try{
                winLog = (OriginalWinLog.WinNoticeLogMessage) TMC.parseFromString(value.toString().trim(), OriginalWinLog.WinNoticeLogMessage.newBuilder());   
            } catch (Exception e){
                protoBufferExceptCt.increment(1);
                return;
            }

           	// key info - value info
            String idGroup = winLog.getData();
            String sessionId = AuxiliaryFunction.extractSessionId(idGroup);
            String orderId = AuxiliaryFunction.extractOrderId(idGroup);
            try{
                DataHandler dataHandler = new DataHandler();
                featureValueList = dataHandler.processWinLog(winLog, context);
            } catch (Exception e){
                dataHandlerExceptCt.increment(1);
                return;
            }
          
            // output key-value
            try {
                for (String fv : featureValueList)
                    if (!fv.equals("")){
                        key_result.set(sessionId + Properties.Base.BS_SEPARATOR_UNDERLINE + orderId);
                        value_result.set(fv);
                        context.write(key_result, value_result);
                    }
            } catch (Exception e){
                invalidInputCt.increment(1);
            }
		}
	}

	public static class OriginalShowLogMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text();

		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			// create counters
            Counter validInputCt = context.getCounter("OriginalShowLogMapper", "validInputCt");
            Counter invalidInputCt = context.getCounter("OriginalShowLogMapper", "invalidInputCt");
            Counter protoBufferExceptCt = context.getCounter("OriginalShowLogMapper", "protoBufferExceptCt");
            Counter dataHandlerExceptCt = context.getCounter("OriginalShowLogMapper", "dataHandlerExceptCt");
            
            // initialization
            LinkedList<String> featureValueList = new LinkedList<String>();
			TextMessageCodec TMC = new TextMessageCodec();
			OriginalShowLog.ImpLogMessage showLog;
			try{
				showLog = (OriginalShowLog.ImpLogMessage) TMC.parseFromString(value.toString().trim(), OriginalShowLog.ImpLogMessage.newBuilder());	
			} catch (Exception e){
                protoBufferExceptCt.increment(1);
                return;
            }

           	// key info - value info
            String idGroup = showLog.getData();
            String sessionId = AuxiliaryFunction.extractSessionId(idGroup);
            String orderId = AuxiliaryFunction.extractOrderId(idGroup);
            try{
                DataHandler dataHandler = new DataHandler();
                featureValueList = dataHandler.processShowLog(showLog, context);
            } catch (Exception e){
                dataHandlerExceptCt.increment(1);
                return;
            }

            // output key-value
            try {
                // if (AuxiliaryFunction.adxTaobaoSession(sessionId) || 
                //     AuxiliaryFunction.adxBaiduSession(sessionId)){
                    for (String fv : featureValueList)
                        if (!fv.equals("")){
                            key_result.set(sessionId + Properties.Base.BS_SEPARATOR_UNDERLINE + orderId);
                            value_result.set(fv);
                            context.write(key_result, value_result);
                        }
                // }
            } catch (Exception e){
                invalidInputCt.increment(1);
            }
		}
	}

	public static class OriginalClickLogMapper extends Mapper<Object, Text, Text, Text>{
		private Text key_result = new Text();
        private Text value_result = new Text();

		@Override
		protected void map(Object key, Text value, Context context)	throws IOException, InterruptedException{
			// create counters
            Counter validInputCt = context.getCounter("OriginalClickLogMapper", "validInputCt");
            Counter invalidInputCt = context.getCounter("OriginalClickLogMapper", "invalidInputCt");
            Counter protoBufferExceptCt = context.getCounter("OriginalClickLogMapper", "protoBufferExceptCt");
            Counter dataHandlerExceptCt = context.getCounter("OriginalClickLogMapper", "dataHandlerExceptCt");
            
            // initialization
            LinkedList<String> featureValueList = new LinkedList<String>();
			TextMessageCodec TMC = new TextMessageCodec();
			OriginalClickLog.ClickLogMessage clickLog;
			try{
				clickLog = (OriginalClickLog.ClickLogMessage) TMC.parseFromString(value.toString().trim(), OriginalClickLog.ClickLogMessage.newBuilder());	
			} catch (Exception e){
                protoBufferExceptCt.increment(1);
                return;
            }

            // key info - value info
            String idGroup = clickLog.getData();
            String sessionId = AuxiliaryFunction.extractSessionId(idGroup);
            String orderId = AuxiliaryFunction.extractOrderId(idGroup);
            try{
                DataHandler dataHandler = new DataHandler();
                featureValueList = dataHandler.processClickLog(clickLog, context);
            } catch (Exception e){
                dataHandlerExceptCt.increment(1);
                return;
            }

            // output key-value
            try {
                if (AuxiliaryFunction.adxTaobaoSession(sessionId) || 
                    AuxiliaryFunction.adxBaiduSession(sessionId)){
                    for (String fv : featureValueList)
                        if (!fv.equals("")){
                            key_result.set(sessionId + Properties.Base.BS_SEPARATOR_UNDERLINE + orderId);
                            value_result.set(fv);
                            context.write(key_result, value_result);
                        }
                }
            } catch (Exception e){
                invalidInputCt.increment(1);
            }
		}
	}
}
















