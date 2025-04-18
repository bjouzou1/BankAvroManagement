package bank.avro.serializers.in.priv.bytes.deserializer;

// -----( IS Java Code Template v1.2

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
// --- <<IS-START-IMPORTS>> ---
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import com.stellantis.bsq.adapter.kafka.avro.notify.QuoteEventNotification;
import com.stellantis.som.adapter.kafka.avro.serializers.AvroDeserializer;
// --- <<IS-END-IMPORTS>> ---

public final class eventNotification

{
	// ---( internal utility methods )---

	final static eventNotification _instance = new eventNotification();

	static eventNotification _newInstance() { return new eventNotification(); }

	static eventNotification _cast(Object o) { return (eventNotification)o; }

	// ---( server methods )---




	public static final void execute (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(execute)>> ---
		// @sigtype java 3.5
		// [i] object:0:required bytes
		// [i] field:0:required topic_name
		// [o] field:0:required payload
		// [o] record:0:required status
		// [o] - field:0:required code
		// [o] - field:0:required message
		// pipeline
		IDataCursor inputPipelineCursor = pipeline.getCursor();
		byte[]  bytes = (byte[]) IDataUtil.get( inputPipelineCursor, "bytes");
		String topic_name = IDataUtil.getString(inputPipelineCursor, "topic_name");  
		String payload = null;
		String code = "OK"; 
		String message = "Success";    
		     
		//		if (bytesObject != null) { 
		//			//bytes = inputString.getBytes(StandardCharsets.UTF_8);
		//			try {
		//				bytes = getByteArrays(bytesObject);
		//			} catch (IOException e) {
		//				// TODO Auto-generated catch block
		//				e.printStackTrace();
		//			}
		//			} else { 
		//			throw new ServiceException("Input parameter \'bytes\' was not found."); 
		//			}
		 
		IDataCursor outputPipelineCursor = pipeline.getCursor();
		 
		   try {
		
				AvroDeserializer<QuoteEventNotification> avroQuoteEventNotificationDeserializer = new AvroDeserializer<QuoteEventNotification>();
				QuoteEventNotification quoteEventNotification =  avroQuoteEventNotificationDeserializer.deserialize(topic_name, bytes);		
				avroQuoteEventNotificationDeserializer.close();
			    payload = quoteEventNotification.toString();  
				// pipeline
				IDataUtil.put(outputPipelineCursor, "payload", payload);  
			   
		    } catch (Exception e) { 
		    	code= "KO" ; 
		    	message = " exception:  Message Error  " + e.getMessage() + " Localised Message Error : " + e.getLocalizedMessage() ; 
		    }
		
		
		 
		inputPipelineCursor.destroy();
		
		
		
		// status
		IData status = IDataFactory.create();
		IDataCursor statusCursor = status.getCursor();
		IDataUtil.put(statusCursor, "code", code);
		IDataUtil.put(statusCursor, "message", message);
		statusCursor.destroy();
		IDataUtil.put(outputPipelineCursor, "status", status); 
		outputPipelineCursor.destroy();
		// --- <<IS-END>> ---

                
	}



	public static final void run (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(run)>> ---
		// @sigtype java 3.5
		// [i] object:0:required bytes
		// [i] field:0:required topic_name
		// [o] field:0:required payload
		// [o] record:0:required status
		// [o] - field:0:required code
		// [o] - field:0:required message
		String code = "OK";
		String message = "Success";
		IDataCursor outputPipelineCursor = pipeline.getCursor();
		
		try {  
		// pipeline  
		IDataCursor inputPipelineCursor = pipeline.getCursor();
		Object byteArrays =  IDataUtil.get( inputPipelineCursor, "bytes" );
		
		String topic_name = IDataUtil.getString(inputPipelineCursor, "topic_name");   
		byte[] bytes = null;
		String payload = null; 
		
		
												if (byteArrays != null) { 
												try {
													bytes = getByteArrays(byteArrays);
												} catch (IOException e) {
													// TODO Auto-generated catch block
													e.printStackTrace();
												}
															   
												} else { 
													code= "KO" ; 
													message = "Input parameter bytes\' was not found.";
													throw new ServiceException("Input parameter \'bytes\' was not found."); 
												}
			
		
		
		
		 
		
		
				AvroDeserializer<QuoteEventNotification> avroQuoteEventNotificationDeserializer = new AvroDeserializer<QuoteEventNotification>();
				System.out.print(" before AvroDeserialise");
				QuoteEventNotification quoteEventNotification =  avroQuoteEventNotificationDeserializer.deserialize(topic_name, bytes);		
				System.out.print(" After AvroDeserialise");
				avroQuoteEventNotificationDeserializer.close();
			    payload = quoteEventNotification.toString();  
				// pipelin
				IDataUtil.put(outputPipelineCursor, "payload", payload);
				inputPipelineCursor.destroy();
				
				
				
				// status
				IData status = IDataFactory.create();
				IDataCursor statusCursor = status.getCursor();
				IDataUtil.put(statusCursor, "code", code);
				IDataUtil.put(statusCursor, "message", message);
				statusCursor.destroy();
				IDataUtil.put(outputPipelineCursor, "status", status); 
				outputPipelineCursor.destroy();
			   
		    } catch (Exception e) { 
		    	code= "KO" ; 
		    	message = " exception:   " + e.getMessage() + "  "   + e.getStackTrace().toString(); 
				// status
				IData status = IDataFactory.create();
				IDataCursor statusCursor = status.getCursor();
				IDataUtil.put(statusCursor, "code", code);
				IDataUtil.put(statusCursor, "message", message);
				statusCursor.destroy();
				IDataUtil.put(outputPipelineCursor, "status", status); 
				outputPipelineCursor.destroy();
		    }
		// --- <<IS-END>> ---

                
	}



	public static final void service (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(service)>> ---
		// @sigtype java 3.5
		// [i] object:0:required bytes
		// [i] field:0:required topic_name
		// [o] field:0:required payload
		// [o] record:0:required status
		// [o] - field:0:required code
		// [o] - field:0:required message
		// pipeline
		IDataCursor inputPipelineCursor = pipeline.getCursor();
		byte[]  bytes = (byte[]) IDataUtil.get( inputPipelineCursor, "bytes");
		String topic_name = IDataUtil.getString(inputPipelineCursor, "topic_name");  
		String payload = null;
		String code = "OK"; 
		String message = "Success";
		 
										 
		
		IDataCursor outputPipelineCursor = pipeline.getCursor();
		 
		   try {
		
				AvroDeserializer<QuoteEventNotification> avroQuoteEventNotificationDeserializer = new AvroDeserializer<QuoteEventNotification>();
				QuoteEventNotification quoteEventNotification =  avroQuoteEventNotificationDeserializer.deserialize(topic_name, bytes);		
				avroQuoteEventNotificationDeserializer.close();
			    payload = quoteEventNotification.toString();  
				// pipeline
				IDataUtil.put(outputPipelineCursor, "payload", payload);
			   
		    } catch (Exception e) { 
		    	code= "KO" ; 
		    	message = " exception:  Message Error  " + e.getMessage() + " Localised Message Error : " + e.getLocalizedMessage() ; 
		    }
		
		
		 
		inputPipelineCursor.destroy();
		
		
		
		// status
		IData status = IDataFactory.create();
		IDataCursor statusCursor = status.getCursor();
		IDataUtil.put(statusCursor, "code", code);
		IDataUtil.put(statusCursor, "message", message);
		statusCursor.destroy();
		IDataUtil.put(outputPipelineCursor, "status", status); 
		outputPipelineCursor.destroy();
		// --- <<IS-END>> ---

                
	}

	// --- <<IS-START-SHARED>> ---
	public static byte[] getByteArrays (Object obj) throws IOException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    ObjectOutputStream os = new ObjectOutputStream(out); 
	    os.writeObject(obj);
	    return out.toByteArray();
	
	}
	
	public static final InputStream objectToInputStream(Object theObject) {
	    try {
	        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
	        ObjectOutputStream objectOutputStream = new ObjectOutputStream(
	                byteArrayOutputStream);
	
	        objectOutputStream.writeObject(theObject);
	
	        objectOutputStream.flush();
	        objectOutputStream.close();
	
	        return new ByteArrayInputStream(
	                byteArrayOutputStream.toByteArray());
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	
	    return null;
	}
	// --- <<IS-END-SHARED>> ---
}

