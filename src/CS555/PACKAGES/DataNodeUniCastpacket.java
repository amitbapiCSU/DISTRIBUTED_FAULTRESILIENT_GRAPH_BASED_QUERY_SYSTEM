/**
 * 
 */
package CS555.PACKAGES;

import java.io.Serializable;
import java.util.Set;

/**
 * @author amchakra
 *
 */
public class DataNodeUniCastpacket implements Serializable{

	private DataNodeInfoPacket datanodeinfo;
	private Set<String> listofCommonfiles;
	
	public DataNodeUniCastpacket (DataNodeInfoPacket datanodeinfo, Set<String> listofCommonfiles) {
		this.datanodeinfo = datanodeinfo;
		this.listofCommonfiles = listofCommonfiles; 
	}
	
	public DataNodeInfoPacket getUnicastNodeInfo() {
		return this.datanodeinfo;
	}
	
	public Set<String> getListofCommonFileInfo() {
		return this.listofCommonfiles;
	}
	
}
