package bank.avro.serializers.in.priv.stream.deserializer;

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




	public static final void run (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(run)>> ---
		// @sigtype java 3.5
		// [i] object:0:required stream
		// [i] field:0:required topic_name
		// [o] field:0:required payload
		// [o] record:0:required status
		// [o] - field:0:required code
		// [o] - field:0:required message
		// pipeline
		IDataCursor inputPipelineCursor = pipeline.getCursor();
			String	topic_name = IDataUtil.getString( inputPipelineCursor, "topic_name" );
			Object	inputStream =  IDataUtil.get( inputPipelineCursor, "stream" );
			byte[] bytes = null;
		
		String payload = null;
		String code = "OK"; 
		String message = "Success";
		IDataCursor outputPipelineCursor = pipeline.getCursor();
		
		try {
			
			bytes= readFully(objectToInputStream(inputStream));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		 
		   try {
		
				AvroDeserializer<QuoteEventNotification> avroQuoteEventNotificationDeserializer = new AvroDeserializer<QuoteEventNotification>();
				QuoteEventNotification quoteEventNotification =  avroQuoteEventNotificationDeserializer.deserialize(topic_name, bytes);		
				avroQuoteEventNotificationDeserializer.close();
			    payload = quoteEventNotification.toString();  
				 
			    
			    // pipeline
				IDataUtil.put(outputPipelineCursor, "payload", payload);
			   
		    } catch (Exception e) { 
		    	code= "KO" ; 
		    	message = e.getMessage();  
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



	public static final void service (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(service)>> ---
		// @sigtype java 3.5
		// [i] object:0:required stream
		// [i] field:0:required topic_name
		// [o] field:0:required payload
		// [o] record:0:required status
		// [o] - field:0:required code
		// [o] - field:0:required message
		// pipeline
		IDataCursor inputPipelineCursor = pipeline.getCursor();
			String	topic_name = IDataUtil.getString( inputPipelineCursor, "topic_name" );
			InputStream	inputStream = (InputStream) IDataUtil.get( inputPipelineCursor, "stream" );
			byte[] bytes = null;
		
		String payload = null;
		String code = "OK"; 
		String message = "Success";
		IDataCursor outputPipelineCursor = pipeline.getCursor();
		
		try {
			bytes= readFully(inputStream);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		 
		   try {
		
				AvroDeserializer<QuoteEventNotification> avroQuoteEventNotificationDeserializer = new AvroDeserializer<QuoteEventNotification>();
				QuoteEventNotification quoteEventNotification =  avroQuoteEventNotificationDeserializer.deserialize(topic_name, bytes);		
				avroQuoteEventNotificationDeserializer.close();
			    payload = quoteEventNotification.toString();  
				 
			    
			    // pipeline
				IDataUtil.put(outputPipelineCursor, "payload", payload);
			   
		    } catch (Exception e) { 
		    	code= "KO" ; 
		    	message = e.getMessage();  
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
	public static byte[] readFully(InputStream input) throws IOException
	{
	    byte[] buffer = new byte[8192];
	    int bytesRead;
	    ByteArrayOutputStream output = new ByteArrayOutputStream();
	    while ((bytesRead = input.read(buffer)) != -1)
	    {
	        output.write(buffer, 0, bytesRead);
	    }
	    return output.toByteArray();
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

