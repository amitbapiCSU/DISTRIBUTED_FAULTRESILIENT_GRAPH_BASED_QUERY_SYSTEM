/**
 * 
 */
package CS555.PACKAGES;

import java.io.Serializable;
import java.util.List;

/**
 * @author amchakra
 *
 */
public class ForwardDataNodeInfoPacket implements Serializable{

	private List<DataNodeInfoPacket> dataNodeInfos = null;
	
	public ForwardDataNodeInfoPacket(List<DataNodeInfoPacket> dataNodeInfos) {
		this.dataNodeInfos = dataNodeInfos;
	}
	
	public List<DataNodeInfoPacket> getDataNodesInfo() {
		return this.dataNodeInfos;
	}
	
}
