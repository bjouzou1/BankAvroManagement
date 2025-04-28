package bank.avro.serializers.in.priv.string.deserializer;

// -----( IS Java Code Template v1.2

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
// --- <<IS-START-IMPORTS>> ---
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import com.stellantis.som.adapter.kafka.avro.interfaces.Serialization;
// --- <<IS-END-IMPORTS>> ---

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
		
		
		String payload = null;
		String code = "OK";
		String message = "Success";
		IDataCursor outputPipelineCursor = pipeline.getCursor();
		 
		try {
			if (inputString != null) {
				bytes = getByteArrays(inputString);
			} else {
				code= "KO" ; 
				message = " Exception:  Message Error  " + "Input parameter \'bytes\' was not found." ;
				throw new ServiceException("Input parameter \'bytes\' was not found.");
			}
			bytes = getByteArrays(inputString);
			payload = Serialization.getValueAsString(topic_name, bytes);		
		
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
	// --- <<IS-END-SHARED>> ---
}

