/**
 * 
 */
package CS555.MASTERNODE_INFO;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import CS555.PACKAGES.QueryNodeInfoPacket;
import CS555.PACKAGES.UploaderDataNodeFileInfo;
import CS555.PACKAGE_FORMATS.PackageFormats;

/**
 * @author amchakra
 *
 */
public class MasterQueryNodeNotificationThread implements Runnable{
	
	private Map<String, CopyOnWriteArrayList<QueryNodeInfoPacket>> QueryNodeTimeStampQueryMap;
	private Map<String, Long> FileNameStoreTimeStampMap;
	private long time = new Date().getTime()/1000;
	
	public MasterQueryNodeNotificationThread(Map<String, CopyOnWriteArrayList<QueryNodeInfoPacket>> QueryNodeTimeStampQueryMap, Map<String, Long> FileNameStoreTimeStampMap) {
		this.QueryNodeTimeStampQueryMap = QueryNodeTimeStampQueryMap;
		this.FileNameStoreTimeStampMap = FileNameStoreTimeStampMap;
	}
	
	private boolean isQueryEffectedByLaterStagedFile(QueryNodeInfoPacket qryinfo, String StoredfileName) {
		
		String QueryNodesQueryTimeInfo = qryinfo.getQueryNodeQueryTimeInfo();
		String LaterStagedFilesTimeInfo = (StoredfileName.split("_"))[1];
		boolean isQueryEffected = false;
		
		boolean yearPrsent = qryinfo.isYearPresentinQuery();
		boolean monthPresent = qryinfo.isMonthPresentinQuery();
		boolean datePresent = qryinfo.isDatePresentinQuery();
		
		if(yearPrsent) { // YEAR PRESENT
			if (monthPresent && datePresent) { // YEAR,MONTH and DATE PRESENT
				if(LaterStagedFilesTimeInfo.equals(QueryNodesQueryTimeInfo)) {
					isQueryEffected = true;
				}
			} else if (monthPresent) { // YEAR,MONTH PRESENT
				if(LaterStagedFilesTimeInfo.startsWith(QueryNodesQueryTimeInfo)) {
					isQueryEffected = true;
				}
			} else if (datePresent) { // YEAR,DATE PRESENT
				if(LaterStagedFilesTimeInfo.startsWith(QueryNodesQueryTimeInfo.substring(0,4)) && LaterStagedFilesTimeInfo.endsWith(QueryNodesQueryTimeInfo.substring(4))) {
					isQueryEffected = true;
				}
			} else { // YEAR PRESENT ONLY
				if(LaterStagedFilesTimeInfo.startsWith(QueryNodesQueryTimeInfo)) {
					isQueryEffected = true;
				}
			}
		} else { // MONTH, DATE or EITHER ONE OF THEM 
			
			String flmmddtmInfo = LaterStagedFilesTimeInfo.substring(4);
			if (monthPresent && datePresent) { // MONTH and DATE PRESENT
				if(flmmddtmInfo.equals(QueryNodesQueryTimeInfo)) {
					isQueryEffected = true;
				}
			} else if (monthPresent) { // MONTH PRESENT
				if(flmmddtmInfo.startsWith(QueryNodesQueryTimeInfo)) {
					isQueryEffected = true;
				}
			} else if (datePresent) { // DATE PRESENT
				if(flmmddtmInfo.endsWith(QueryNodesQueryTimeInfo)) {
					isQueryEffected = true;
				}
			}
			
		}
		
		return  isQueryEffected;
	}
	
	private void contactQueryNodetoSendNotification(QueryNodeInfoPacket qrypckt, String queryNodehexID) {
		
		try {
			
			Socket toConnectQueryNode = new Socket(qrypckt.getQueryNodeIP(),qrypckt.getQueryNodePort(),null,0);
			
			OutputStream qrydout = toConnectQueryNode.getOutputStream();
			InputStream qryndin = toConnectQueryNode.getInputStream();
			
			DataInputStream qryccin = new DataInputStream(qryndin);
			DataOutputStream qryccout = new DataOutputStream(qrydout);
			
			qryccout.writeInt(PackageFormats.MASTER_NODE_QUERY_NODE_QUERY_NOTIFICATION);
			qryccout.writeUTF(qrypckt.getQueryNodesQuery());
			
			toConnectQueryNode.close();
			
		} catch (Exception e) {
			System.out.println("UNABLE TO CONTACT QUERY NODE To SEND NOTIFICATIONS :: PROBABLY DOWN DELETE HIS INFO FROM MAP");
			QueryNodeTimeStampQueryMap.remove(queryNodehexID);
			e.printStackTrace();
		}
	}
	
	private int getQueryInformationIndexinQueryNodeList(QueryNodeInfoPacket qryinfo, CopyOnWriteArrayList<QueryNodeInfoPacket> queryNodeoinfoList) {
		int effectedQuerysIndex = -1;
		
		for(int i = 0; i < queryNodeoinfoList.size(); i++) {
			QueryNodeInfoPacket crntQueryPckt = queryNodeoinfoList.get(i);
			if((crntQueryPckt.getQueryNodesQuery()).equals(qryinfo.getQueryNodesQuery())) {
				effectedQuerysIndex = i;
				break;
			}
			
		}
		return effectedQuerysIndex;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			while(true) {
				for(Entry<String, Long> storedFileInfo : FileNameStoreTimeStampMap.entrySet()) {
					String StoredFileName = storedFileInfo.getKey();
					Long StoredFileTimeStamp = storedFileInfo.getValue();
					
					for(String queryNodehexID : QueryNodeTimeStampQueryMap.keySet()) {
						CopyOnWriteArrayList<QueryNodeInfoPacket> queryNodeInfoList = QueryNodeTimeStampQueryMap.get(queryNodehexID);
//						int index = 0;
						for(QueryNodeInfoPacket qryInfo : queryNodeInfoList) {
							
							if(StoredFileTimeStamp > qryInfo.getQueryNodesQueryTimeStamp()) {
								
								// THAT IS FILE STORED AFTER THE QUERY NODE QUERIED :
								
								System.out.println(StoredFileName+" FILE STAGED AFTER THE QUERY NODE's QUERY :: "+queryNodehexID);
								
								// NOW CHECK WHETHER THE LATER STAGED FILE WOULD EFFECT THE QUERY NODE OR NOT
								
								if(isQueryEffectedByLaterStagedFile(qryInfo,StoredFileName)) {
									
									queryNodeInfoList.remove(getQueryInformationIndexinQueryNodeList(qryInfo,queryNodeInfoList));
									contactQueryNodetoSendNotification(qryInfo, queryNodehexID);
									
									System.out.println("EFFECTED QUERY :: "+qryInfo.getQueryNodesQuery());
									System.out.println("QUERY NODE ID :: "+qryInfo.getQueryNodeHexID());
									System.out.println("QUERY INF0 LIST SIZE :: "+queryNodeInfoList.size());
									
									if(queryNodeInfoList.size() == 0) {
										QueryNodeTimeStampQueryMap.remove(queryNodehexID);
									}
									
								}
								
							}
//							index++;
						}
					}
				}
				Thread.sleep(20000);
			} 
		}catch (Exception e) {
			System.out.println("PROBLEM IN MASTER NODE SEND NOTIFICATION");
			e.printStackTrace();
		}
	}

	
}
