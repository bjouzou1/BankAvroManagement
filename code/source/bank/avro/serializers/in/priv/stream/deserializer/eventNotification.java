package bank.avro.serializers.in.priv.stream.deserializer;

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import com.stellantis.som.adapter.kafka.avro.interfaces.Serialization;

public final class eventNotification

{
	// ---( internal utility methods )---

	final static eventNotification _instance = new eventNotification();

	static eventNotification _newInstance() { return new eventNotification(); }

	static eventNotification _cast(Object o) { return (eventNotification)o; }

	// ---( server methods )---




	public static final void service (IData pipeline)  throws ServiceException
	{
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



                
	}
	
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
}

