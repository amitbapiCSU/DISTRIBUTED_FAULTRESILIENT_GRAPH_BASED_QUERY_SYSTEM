/**
 * 
 */
package CS555.PACKAGES;

import java.io.Serializable;

/**
 * @author amchakra
 *
 */
public class DataNodeInfoPacket implements Serializable{

	private String HexID;
	private String NodeAddr;
	private String NodeHostName;
	private int NodePort;
	
	public DataNodeInfoPacket(String HexID, String NodeAddr, String NodeHostName, int NodePort) {
		this.HexID = HexID;
		this.NodeAddr = NodeAddr;
		this.NodeHostName = NodeHostName;
		this.NodePort = NodePort;
	}
	
	public String getHexID() {
		return this.HexID;
	}
	
	public String getDataNodeHostName() {
		return this.NodeHostName;
	}
	
	public String getDataNodeAddress(){
		return this.NodeAddr;
	}
	
	public int getNodePort () {
		return this.NodePort;
	}
}
