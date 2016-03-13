/**
 * 
 */
package CS555.PACKAGES;

import java.io.Serializable;
import java.util.Map;

/**
 * @author amchakra
 *
 */
public class DataNodeBroadCastPacket implements Serializable{

	private String DataNodeIp;
	private int DataNodePort;
	private String DataNodeHexID;
	private String DataNodeHostName;
	private Map<String, String> DatNodeFileInfoMap;
	
	public DataNodeBroadCastPacket (String DataNodeIp, int DataNodePort, String DataNodeHexID, String DataNodeHostName, Map<String, String> DatNodeFileInfoMap) {
		this.DataNodeIp = DataNodeIp;
		this.DataNodePort = DataNodePort;
		this.DataNodeHexID = DataNodeHexID;
		this.DataNodeHostName = DataNodeHostName;
		this.DatNodeFileInfoMap = DatNodeFileInfoMap;
	}

	public String getBroadCastNodesHexID() {
		return this.DataNodeHexID;
	}
	
	public int getBoroadCastNodePort() {
		return this.DataNodePort;
	}
	
	public String getBroadCastNodeIP() {
		return this.DataNodeIp;
	}
	
	public String getBroadCastNodeHostName() {
		return this.DataNodeHostName;
	}
	
	public Map<String, String> getBroadCastNodeFileMap() {
		return this.DatNodeFileInfoMap;
	}
}
