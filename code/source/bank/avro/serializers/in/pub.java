package bank.avro.serializers.in;

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;

public final class pub

{
	// ---( internal utility methods )---

	final static pub _instance = new pub();

	static pub _newInstance() { return new pub(); }

	static pub _cast(Object o) { return (pub)o; }

	// ---( server methods )---




	public static final void test (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(test)>> ---
		// @sigtype java 3.5
		// [i] object:0:required bytes
		// [o] field:0:required payload
		
		// pipeline
		IDataCursor pipelineCursor = pipeline.getCursor();
			Object	bytes = IDataUtil.get( pipelineCursor, "bytes" );
		pipelineCursor.destroy();
		
		// pipeline
		IDataCursor pipelineCursor_1 = pipeline.getCursor();
		IDataUtil.put( pipelineCursor_1, "payload", "payload" );
		pipelineCursor_1.destroy();
		// --- <<IS-END>> ---

                
	}
}

