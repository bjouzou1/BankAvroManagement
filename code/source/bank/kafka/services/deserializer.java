package bank.kafka.services;

// -----( IS Java Code Template v1.2

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
// --- <<IS-START-IMPORTS>> ---
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.stellantis.som.adapter.kafka.avro.notify.QuoteEventNotification;
import com.stellantis.som.adapter.kafka.avro.serializers.AvroDeserializer;
import com.stellantis.som.adapter.kafka.avro.serializers.AvroTypeConfig;
// --- <<IS-END-IMPORTS>> ---

public final class deserializer

{
	// ---( internal utility methods )---

	final static deserializer _instance = new deserializer();

	static deserializer _newInstance() { return new deserializer(); }

	static deserializer _cast(Object o) { return (deserializer)o; }

	// ---( server methods )---




	public static final void AvroDeserializer (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(AvroDeserializer)>> ---
		// @sigtype java 3.5
		IDataCursor inputPipelineCursor = pipeline.getCursor();
		byte[]  bytes =  (byte[]) IDataUtil.get( inputPipelineCursor, "bytes" );
		String topic_name = IDataUtil.getString(inputPipelineCursor, "topic_name");  
		String payload = null;
		String code = "OK";
		String message = "Success";
		
										
		
		IDataCursor outputPipelineCursor = pipeline.getCursor();
		   try {
			   
		//		AvroDeserializer<QuoteEventNotification> avroDeserializer = new AvroDeserializer<QuoteEventNotification>();
		//		QuoteEventNotification quoteEventNotification  =  avroDeserializer.deserialize(topic_name, bytes);
		//		avroDeserializer.close();
		//	    payload = quoteEventNotification.toString(); 
		// pipeline
		IDataUtil.put(outputPipelineCursor, "payload", payload);
		   
		} catch (Exception e) { 
		code= "KO" ; 
		message = " exception:   " + e.getMessage() + "  "   + e.getStackTrace(); 
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
	public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
	    private Class<T> targetType;
	    private  final Logger LOGGER = LoggerFactory.getLogger(AvroDeserializer.class);
	
	    @SuppressWarnings("unchecked")
		public AvroDeserializer() {
	        // Not possible with spring cloud to use a parameterized constructor
	        // So we need to use a default constructor and set the targetType manually
	        AvroTypeConfig.registerLogicalTypes();
	        this.targetType = (Class<T>) QuoteEventNotification.class;
	    }
	
	    
	
	    
	    @Override
	    public T deserialize(String topic, byte[] data) {
	        try {
	
	            if (data == null) {
	            	LOGGER.debug("Null data received for topic '{}'", topic);
	                return null;
	            }
	
	            LOGGER.debug("Deserializing data for topic '{}'", topic);
	
	            T instance = targetType.getDeclaredConstructor().newInstance();
	            Schema schema = instance.getSchema();
	
	            SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(schema);
	
	            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
	            T result = datumReader.read(null, decoder);
	
	            LOGGER.debug("Deserialized data: {}", result);
	            return result;
	
	        } catch (Exception e) {
	            throw new SerializationException("Error deserializing Avro message for topic :  " + topic + "   Cause:   " + e.getCause() ,e);
	        }
	    }
	}
	
		    
	// --- <<IS-END-SHARED>> ---
}

