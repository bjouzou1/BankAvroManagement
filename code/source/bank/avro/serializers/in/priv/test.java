package bank.avro.serializers.in.priv;

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;

public final class test

{
	// ---( internal utility methods )---

	final static test _instance = new test();

	static test _newInstance() { return new test(); }

	static test _cast(Object o) { return (test)o; }

	// ---( server methods )---




	public static final void testService (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(testService)>> ---
		// @sigtype java 3.5
		// [i] object:0:required bytes
		// [i] field:0:required topic_name
		
		// pipeline
		IDataCursor pipelineCursor = pipeline.getCursor();
			Object	bytes = IDataUtil.get( pipelineCursor, "bytes" );
			String	topic_name = IDataUtil.getString( pipelineCursor, "topic_name" );
		pipelineCursor.destroy();
		
		// pipeline
		// --- <<IS-END>> ---

                
	}
}

