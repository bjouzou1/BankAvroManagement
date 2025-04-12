package bank.avro.serializers.in.priv.string.deserializer;

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import com.stellantis.som.adapter.kafka.avro.notify.QuoteEventNotification;
import com.stellantis.som.adapter.kafka.avro.serializers.AvroDeserializer;

public final class eventNotification

{
	// ---( internal utility methods )---

	final static eventNotification _instance = new eventNotification();

	static eventNotification _newInstance() { return new eventNotification(); }

	static eventNotification _cast(Object o) { return (eventNotification)o; }

	// ---( server methods )---




	public static final void service (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(service)>> ---
		// @sigtype java 3.5
		// [i] field:0:required bytes
		// [i] field:0:required topic_name
		// [o] field:0:required payload
		// [o] record:0:required status
		// [o] - field:0:required code
		// [o] - field:0:required message
		// pipeline
		IDataCursor inputPipelineCursor = pipeline.getCursor();
		String	inputString = IDataUtil.getString( inputPipelineCursor, "bytes" );
		String topic_name = IDataUtil.getString(inputPipelineCursor, "topic_name");
		byte[] bytes = null;
		
		if (inputString != null) { 
			bytes = inputString.getBytes(StandardCharsets.UTF_8);
			} else { 
			throw new ServiceException("Input parameter \'bytes\' was not found."); 
			}
		
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
}

