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
public class MasterNodeQueryNodeFileNodeInfoPacket implements Serializable{

	Map<String,Set<String>> YearWiseAvailableFileList;
	Map<String,DataNodeInfoPacket> YearWiseDataNodeInfoList;
	
	public MasterNodeQueryNodeFileNodeInfoPacket(Map<String,Set<String>> YearWiseAvailableFileList, Map<String,DataNodeInfoPacket> YearWiseDataNodeInfoList) {
		this.YearWiseAvailableFileList = YearWiseAvailableFileList;
		this.YearWiseDataNodeInfoList = YearWiseDataNodeInfoList;
	}
	
	public Map<String,Set<String>> getYearWiseAvailableFileList() {
		return this.YearWiseAvailableFileList;
	}
	
	public Map<String,DataNodeInfoPacket> getYearWiseAvailableDataNodeList() {
		return this.YearWiseDataNodeInfoList;
	}
	
//	Set<String> AvailableFileList;
//	CopyOnWriteArrayList<DataNodeInfoPacket> DatanodeList;
	
//	public MasterNodeQueryNodeFileNodeInfoPacket(Set<String> AvailableFileList, CopyOnWriteArrayList<DataNodeInfoPacket> DatanodeList) {
//		this.AvailableFileList = AvailableFileList;
//		this.DatanodeList = DatanodeList;
//	}
//	
//	public Set<String> getAvailableFileList() {
//		return this.AvailableFileList;
//	}
//	
//	public CopyOnWriteArrayList<DataNodeInfoPacket> getDataNodeList() {
//		return this.DatanodeList;
//	}
}
