/**
 * 
 */
package CS555.MASTERNODE_INFO;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import CS555.PACKAGES.DataNodeInfoPacket;
import CS555.PACKAGE_FORMATS.PackageFormats;

/**
 * @author amchakra
 *
 */
public class MasterNodeHeartBeatCommunication implements Runnable{
	
	private Map<String, Long> FileNameStoretimeMap;
	private Map<String, DataNodeInfoPacket> DataNodeHexIDInfoMap;
	private CopyOnWriteArrayList<DataNodeInfoPacket> LivedatanodeInfo;
	private Map<String, CopyOnWriteArrayList<DataNodeInfoPacket>> FileDataNodeMap;
	private Map<String, CopyOnWriteArrayList<String>> DataNodeFileInfoMap;
	
	
	MasterNodeHeartBeatCommunication(Map<String, DataNodeInfoPacket> DataNodeHexIDInfoMap, CopyOnWriteArrayList<DataNodeInfoPacket> LivedatanodeInfo, Map<String, CopyOnWriteArrayList<DataNodeInfoPacket>> FileDataNodeMap, Map<String, CopyOnWriteArrayList<String>> DataNodeFileInfoMap, Map<String, Long> FileNameSizeMap) {
		this.DataNodeHexIDInfoMap = DataNodeHexIDInfoMap;
		this.LivedatanodeInfo = LivedatanodeInfo;
		this.FileDataNodeMap = FileDataNodeMap;
		this.DataNodeFileInfoMap = DataNodeFileInfoMap;
		this.FileNameStoretimeMap = FileNameSizeMap;
	}
	
	private boolean isContains(DataNodeInfoPacket dtndpckt) {
		
		boolean isContain = false;
		
		for (DataNodeInfoPacket pckt : LivedatanodeInfo) {
			if((pckt.getHexID()).equals(dtndpckt.getHexID())) {
				isContain = true;
				break;
			}
		}
		
		return isContain;
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			while(true) {
				if(DataNodeHexIDInfoMap.size() > 0) {
					for (String hxID : DataNodeHexIDInfoMap.keySet()) {
						DataNodeInfoPacket dtndInfo = DataNodeHexIDInfoMap.get(hxID);
						// TRY TO CONNECT WITH THE DATA-NODE
						try {
							
							Socket dtndsckt = new Socket(dtndInfo.getDataNodeAddress(), dtndInfo.getNodePort(), null, 0);
							
							OutputStream dtndout = dtndsckt.getOutputStream();
			    			DataOutputStream dtnddout = new DataOutputStream(dtndout);
			    			
			    			dtnddout.writeInt(PackageFormats.MASTER_NODE_DATA_NODE_HEARTBEAT);
			    			dtnddout.writeUTF("YOU ALIVE ?");
			    			
			    			dtndsckt.close();
			    			
			    			if(!isContains(dtndInfo)) {
			    				LivedatanodeInfo.add(dtndInfo);
			    			}
							
						} catch (Exception e) {
							System.out.println("UNABLE TO REACH THE DATA NODE :: "+dtndInfo.getDataNodeHostName());
							// NEED To DO FILE MIGRATIONS TO MAKE REPLICATION LEVEL CONSISTENT
							
							if(DataNodeHexIDInfoMap.size() > 0) {
								
								// DELETE THE DEAD NODE FROM LIVE NODE's MAP
								DataNodeHexIDInfoMap.remove(dtndInfo.getHexID());
								
								// DELETE THE NODE's FILE-LIST from DATA-NODE -- FILE-LIST MAP after Fetching all TO REPLICATON file-items
								CopyOnWriteArrayList<String> toForwardFileList = DataNodeFileInfoMap.get(hxID);
								DataNodeFileInfoMap.remove(dtndInfo.getHexID());
								
								// DELETE THE DEAD NODE's INFO FROM THE FILE-TIME-INFO -- DATA-NODE MAP
								for (String fileTimeInfo : FileDataNodeMap.keySet()) {
									CopyOnWriteArrayList<DataNodeInfoPacket> nodelist = FileDataNodeMap.get(fileTimeInfo);
									int indx = 0;
									for (DataNodeInfoPacket node : nodelist) {
										if ((node.getHexID()).equals(dtndInfo.getHexID())) {
											nodelist.remove(indx);
											break;
										}
										indx++;
									}
								}
								
								// CALL REPLICATION FAILURE THREAD 
								new Thread(new MasterNodeFailureCoping(dtndInfo.getHexID(), toForwardFileList, DataNodeHexIDInfoMap, FileDataNodeMap, DataNodeFileInfoMap, FileNameStoretimeMap)).start();
							}
						}
					}
					Thread.sleep(10000);
				}
			}
		} catch (Exception e) {
			System.out.println("ERROR IN HEARTBEAT COMMUNICATION");
			e.printStackTrace();
		}
		
	}

}
