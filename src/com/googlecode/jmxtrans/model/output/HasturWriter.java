package com.googlecode.jmxtrans.model.output;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.jmxtrans.jmx.ManagedGenericKeyedObjectPool;
import com.googlecode.jmxtrans.jmx.ManagedObject;
import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.model.Server;
import com.googlecode.jmxtrans.util.BaseOutputWriter;
import com.googlecode.jmxtrans.util.JmxUtils;
import com.googlecode.jmxtrans.util.LifecycleException;
import com.googlecode.jmxtrans.util.SocketFactory;
import com.googlecode.jmxtrans.util.ValidationException;

/**
 * This output writer sends data to a host/port combination in the Hastur
 * format.
 *
 * @author tobert
 */
public class HasturWriter extends BaseOutputWriter {
	private static final Logger log = LoggerFactory.getLogger(HasturWriter.class);
	public static final String ROOT_PREFIX = "rootPrefix";

  // optional
	private Integer port;
	private String rootPrefix = "servers";

	/** */
	public void validateSetup(Query query) throws ValidationException {
		Object portObj = this.getSettings().get(PORT);
		if (portObj instanceof String) {
			port = Integer.parseInt((String) portObj);
		} else if (portObj instanceof Integer) {
			port = (Integer) portObj;
		}

		if (port == null) {
      port = 8125;
		}

		String rootPrefixTmp = (String) this.getSettings().get(ROOT_PREFIX);
		if (rootPrefixTmp != null) {
			rootPrefix = rootPrefixTmp;
		}
	}

	/** */
	public void doWrite(Query query) throws Exception {
    DatagramSocket socket = null;
		try {
			List<String> typeNames = this.getTypeNames();

			for (Result result : query.getResults()) {
				if (isDebugEnabled()) {
					log.debug(result.toString());
				}

				Map<String, Object> resultValues = result.getValues();

				if (resultValues != null) {
					for (Entry<String, Object> values : resultValues.entrySet()) {
						if (JmxUtils.isNumeric(values.getValue())) {
							StringBuilder sb = new StringBuilder();

              sb.append("{\"type\":\"gauge\",name\":\"");
							sb.append(JmxUtils.getKeyString(query, result, values, typeNames, rootPrefix));
              sb.append("\",\"value\":");
							sb.append(values.getValue().toString());
              sb.append(",\"timestamp\":");
							sb.append(result.getEpoch() * 1000);
              sb.append(",\"labels\":{\"via\":\"jmxtrans\"}}\n");

							String line = sb.toString();
							if (isDebugEnabled()) {
								log.debug("Hastur Message: " + line.trim());
							}

              socket = new DatagramSocket();
              DatagramPacket msg = new DatagramPacket(line.getBytes(),
                                                      line.length(),
                                                      InetAddress.getLocalHost(),
                                                      8125);
              socket.send(msg);
						}
					}
				}
			}
		} finally {
      if(socket != null) {
        socket.close();
      }
		}
	}
}
