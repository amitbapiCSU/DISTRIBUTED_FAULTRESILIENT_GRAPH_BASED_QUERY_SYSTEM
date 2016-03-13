/**
 * 
 */
package CS555.PACKAGES;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author amchakra
 *
 */
public class QueryNodeQueryPacket implements Serializable{

	Set<String> AvailableNetworkFileList;
	Map<String,CopyOnWriteArrayList<String>> QueryFunctionVariableMap;
//	CopyOnWriteArrayList<DataNodeInfoPacket> ToContactDatanodeList;
	
	public QueryNodeQueryPacket(Set<String> AvailableNetworkFileList, Map<String,CopyOnWriteArrayList<String>> QueryFunctionVariableMap) { // , CopyOnWriteArrayList<DataNodeInfoPacket> ToContactDatanodeList
		this.AvailableNetworkFileList = AvailableNetworkFileList;
		this.QueryFunctionVariableMap = QueryFunctionVariableMap;
//		this.ToContactDatanodeList = ToContactDatanodeList;
	}
	
	public Set<String> getAvailableNetworkQueryFileList(){
		return this.AvailableNetworkFileList;
	}
	
	public Map<String,CopyOnWriteArrayList<String>> getQueryFuctionVariableMap() {
		return this.QueryFunctionVariableMap;
	}
	
//	public CopyOnWriteArrayList<DataNodeInfoPacket> gettoContactDataNodeList() {
//		return this.ToContactDatanodeList;
//	}
	
}
