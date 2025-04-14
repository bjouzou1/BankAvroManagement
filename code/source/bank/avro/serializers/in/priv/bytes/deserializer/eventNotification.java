package bank.avro.serializers.in.priv.bytes.deserializer;

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.lang3.SerializationUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
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
		// [i] object:0:required byte
		// [i] field:0:required topic_name
		// [o] field:0:required payload
		// [o] record:0:required status
		// [o] - field:0:required code
		// [o] - field:0:required message
		// pipeline
		IDataCursor inputPipelineCursor = pipeline.getCursor();
		byte byteArrays =  (byte) IDataUtil.get( inputPipelineCursor, "byte" );
		String topic_name = IDataUtil.getString(inputPipelineCursor, "topic_name");  
		byte[] bytes = null;
		String payload = null;
		String code = "OK";
		String message = "Success";
		//				if (byteArrays != null) { 
		//				try {
		//					bytes = getByteArrays(byteArrays);
		//				} catch (IOException e) {
		//					// TODO Auto-generated catch block
		//					e.printStackTrace();
		//				}
		//							   
		//				} else { 
		//					code= "KO" ; 
		//					message = "Input parameter bytes\' was not found.";
		//					throw new ServiceException("Input parameter \'bytes\' was not found."); 
		//				}
		//		
		try {
			bytes = getByteArrays(byteArrays);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		IDataCursor outputPipelineCursor = pipeline.getCursor();
		 
		   try {
		
				AvroDeserializer<QuoteEventNotification> avroQuoteEventNotificationDeserializer = new AvroDeserializer<QuoteEventNotification>();
				QuoteEventNotification quoteEventNotification =  avroQuoteEventNotificationDeserializer.deserialize(topic_name, bytes);		
				avroQuoteEventNotificationDeserializer.close();
			    payload = quoteEventNotification.toString();  
				// pipelin
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
	public static byte[] getByteArrays (Object obj) throws IOException {
		 
		    ByteArrayOutputStream out = new ByteArrayOutputStream();
		    ObjectOutputStream os = new ObjectOutputStream(out);
		    os.writeObject(obj);
		    return out.toByteArray();
	}
	// --- <<IS-END-SHARED>> ---
}

