/**
 * 
 */
package CS555.MASTERNODE_INFO;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import CS555.PACKAGES.DataNodeInfoPacket;
import CS555.PACKAGES.ForwardDataNodeInfoPacket;
import CS555.PACKAGES.MasterNodeQueryNodeFileNodeInfoPacket;
import CS555.PACKAGES.QueryNodeInfoPacket;
import CS555.PACKAGE_FORMATS.PackageFormats;


/**
 * @author amchakra
 *
 */
public class MasterInfoNodeThreadedConnection implements Runnable{
	
	
	private Socket masterchannel;
	private Map<String, Long> fileNameStoreTimeMap;
	private Map<String,DataNodeInfoPacket> NodeInfoMap; 
	private Map<String, CopyOnWriteArrayList<DataNodeInfoPacket>> fileDataNodeMap;
	private Map<String, CopyOnWriteArrayList<String>> datanodefileInfoMap;
	private Map<String, CopyOnWriteArrayList<QueryNodeInfoPacket>> queryNodeTimeQueryMap;
	private CopyOnWriteArrayList<String> QuerynodeQueryInfo;
	private long masterNodeStartTime;
	
	private OutputStream mstrout = null;
	private InputStream mstrin = null;
	
	private DataInputStream mstrccin = null;
	private DataOutputStream mstrcout = null;
	// private ObjectOutputStream oos = null;
	private Object rcvdmessage = null;
	
	MasterInfoNodeThreadedConnection(Socket masterchannel, Map<String,DataNodeInfoPacket> NodeInfoMap, Map<String, CopyOnWriteArrayList<DataNodeInfoPacket>> fileDataNodeMap, Map<String, CopyOnWriteArrayList<String>> datanodefileInfoMap, Map<String, Long> FileNameSizeMap, Map<String, CopyOnWriteArrayList<QueryNodeInfoPacket>> queryNodeTimeQueryMap, CopyOnWriteArrayList<String> QuerynodeQueryInfo, long masterNodeStartTime) {
		this.masterchannel = masterchannel;
		this.NodeInfoMap = NodeInfoMap;
		this.fileDataNodeMap = fileDataNodeMap;
		this.datanodefileInfoMap = datanodefileInfoMap;
		this.fileNameStoreTimeMap = FileNameSizeMap;
		this.queryNodeTimeQueryMap = queryNodeTimeQueryMap;
		this.QuerynodeQueryInfo = QuerynodeQueryInfo;
		this.masterNodeStartTime = masterNodeStartTime;
	}
	
	private boolean isExistsFile(String flnm) {
		boolean isExists = false;
		
		System.out.println("IS EXIST :: FILEMAP SIZE :: "+fileNameStoreTimeMap.size());
		
		if(fileNameStoreTimeMap.size() > 0) {
			List<String> stagedFileNames = new ArrayList<String>(fileNameStoreTimeMap.keySet());
//			if(stagedFileNames.contains(flnm)) {
//				isExists = true;
//			}
			for(String stgdflnm : stagedFileNames) {
				
				System.out.println("STAGED FILE NAME :: "+stgdflnm+" ::  TO SEARCh FILE NAME :: "+flnm);
				
				if(stgdflnm.equals(flnm)) {
					System.out.println("FILE EXISTS IN THE NETWORK :: NEED TO DO UPDATE ");
					isExists = true;
					break;
				}
			}
			stagedFileNames.clear();
		}
		return isExists;
	}
	
	private ForwardDataNodeInfoPacket getForwardTwoDataNodesInfo2Uploader(String filenm) {
		
		try {
			if(isExistsFile(filenm)) {
				// FILE EXISTS :: SO RETURN THE 3 NODE's INFORMATION
				
				String yrmnthdtTimeInfo = (filenm.split("_"))[1];
				List<DataNodeInfoPacket> stagedFileinDataNodes = fileDataNodeMap.get(yrmnthdtTimeInfo);
				
				ForwardDataNodeInfoPacket fwpckt = new ForwardDataNodeInfoPacket(stagedFileinDataNodes);
				System.out.println("FILE EXISTS :: LIST SIZE :: "+stagedFileinDataNodes.size());
				return fwpckt;
				
			} else {
				// FILE DOESN'T EXISTS IN THE OVERLAY :: SO RETURN RANDOM 3 DATA NODE's INFO
				
				List<String> HexIds = new ArrayList<String>(NodeInfoMap.keySet());
				Collections.shuffle(HexIds);
				
				if(HexIds.size() >= 3) {
					List<String> tmpList = HexIds.subList(0,3);
					List<DataNodeInfoPacket> DataNodeInfoList = new ArrayList<DataNodeInfoPacket>();
					
					for(String hxid : tmpList) {
						DataNodeInfoList.add(NodeInfoMap.get(hxid));
						
					}
					tmpList.clear();
					HexIds.clear();
					ForwardDataNodeInfoPacket fwpckt = new ForwardDataNodeInfoPacket(DataNodeInfoList);
					System.out.println("LIST SIZE :: "+DataNodeInfoList.size());
		//			DataNodeInfoList.clear();
		//			for(DataNodeInfoPacket dtndpckt :  fwpckt.getDataNodesInfo()) {
		//				System.out.println("*****************************************************************************************************");
		//				System.out.println("DATA NODE NAME : "+dtndpckt.getDataNodeHostName());
		//				System.out.println("DATA NODE IP : "+dtndpckt.getDataNodeAddress());
		//				System.out.println("DATA NODE PORT : "+dtndpckt.getNodePort());
		//				System.out.println("DATA NODE HEX-ID : "+dtndpckt.getHexID());
		//				System.out.println("*****************************************************************************************************");
		//
		//			}
					return fwpckt;
				} else {
					System.out.println("NOT POSSIBLE TO GET REPLICATION LEVEL 3");
					return null;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	private void PopulateOwnNodeInfoList() {
		
		DataNodeInfoPacket pckt = (DataNodeInfoPacket)rcvdmessage;
		
		String rcvdhxID = pckt.getHexID();
		if(NodeInfoMap.containsKey(rcvdhxID)) {
			System.out.println("ALREADY HEXID EXISTS IN THE MAP");	
		}
		NodeInfoMap.put(rcvdhxID, pckt);
		
	}
	
	private ForwardDataNodeInfoPacket ForwardLiveDataNodesInfo(String rqstdNodeHexID) {
		List<DataNodeInfoPacket> fwddataNodesList = new ArrayList<DataNodeInfoPacket>();
		
		for(String hxID : NodeInfoMap.keySet()) {
			if(!hxID.equals(rqstdNodeHexID)) {
				fwddataNodesList.add(NodeInfoMap.get(hxID));
			}
		}
		
		ForwardDataNodeInfoPacket fwdpckt = new ForwardDataNodeInfoPacket(fwddataNodesList);
		return fwdpckt;
	}
	
	private boolean isContains(CopyOnWriteArrayList<DataNodeInfoPacket> DataNodeList, DataNodeInfoPacket dtndpckt) {
		
		synchronized (DataNodeList) {
			boolean isContain = false;
			
			for (DataNodeInfoPacket pckt : DataNodeList) {
				if((pckt.getHexID()).equals(dtndpckt.getHexID())) {
					isContain = true;
					break;
				}
			}
			
			return isContain;
		}
	}
	
	private boolean isFileExistsInDATANODE(CopyOnWriteArrayList<String> FileList, String FileName) {
		
		synchronized (FileList) {
			boolean isExist = false;
			
			for(String flnm : FileList) {
				if(flnm.equals(FileName)) {
					isExist = true;
					break;
				}
			}
			return isExist;
		}
		
	}
	
	private void PopulateFileDataNodeInfoMap(String fileName, String timeinfooffilename, int flSize, DataNodeInfoPacket rcvddtndpckt) {
		
		if(fileDataNodeMap.containsKey(timeinfooffilename)) {
			
			CopyOnWriteArrayList<DataNodeInfoPacket> dtndlst = fileDataNodeMap.get(timeinfooffilename);
			if(!isContains(dtndlst,rcvddtndpckt)) {
				dtndlst.add(rcvddtndpckt);
				fileDataNodeMap.put(timeinfooffilename, dtndlst);
			}
			
		} else {
			
			CopyOnWriteArrayList<DataNodeInfoPacket> dtndlst = new CopyOnWriteArrayList<DataNodeInfoPacket>();
			dtndlst.add(rcvddtndpckt);
			fileDataNodeMap.put(timeinfooffilename, dtndlst);
			
		}
		
		String dataNodeHexID = rcvddtndpckt.getHexID();
		if(datanodefileInfoMap.containsKey(dataNodeHexID)) {
			
			CopyOnWriteArrayList<String> fileList = datanodefileInfoMap.get(dataNodeHexID);
			if(!isFileExistsInDATANODE(fileList, fileName)) {
				fileList.add(fileName);
				datanodefileInfoMap.put(dataNodeHexID,fileList);
			}
			
		} else {
			CopyOnWriteArrayList<String> fileList = new CopyOnWriteArrayList<String>();
			fileList.add(fileName);
			datanodefileInfoMap.put(dataNodeHexID,fileList);
		}
		
		long currentTimeinMasterNode = new Date().getTime()/1000;
		
		if(!fileNameStoreTimeMap.containsKey(fileName)) {
			fileNameStoreTimeMap.put(fileName, currentTimeinMasterNode); // flSize NOT USED NOW
		}
		
	}
	
	private void ShowLiveNodeInfo() {
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$ LIVE NODEs IN THE NETWROK $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		for (String hxID : NodeInfoMap.keySet()) {
			DataNodeInfoPacket dtnd = NodeInfoMap.get(hxID);
			System.out.println("LIVE NODE :: "+dtnd.getDataNodeHostName()+" :: HEX ID :: "+dtnd.getHexID());
		}
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
	}
	
	private void ShowDataNodeFileMapInfo() {
		
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$ FILE's TIME INFORMATION -- DATA-NODE MAP $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		
		if (fileDataNodeMap.size() > 0) {
			for(String fltimeInfo : fileDataNodeMap.keySet()) {
				System.out.println("@@@@@@ FILE TIME INFO :: "+fltimeInfo+" STORED AT :: @@@@@@");
				CopyOnWriteArrayList<DataNodeInfoPacket> filedataNodeList = fileDataNodeMap.get(fltimeInfo);
				for (DataNodeInfoPacket pckt : filedataNodeList) {
					System.out.println("       "+pckt.getDataNodeHostName()+" :: "+pckt.getHexID());
				}
				System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
			}
		} else {
			System.out.println("                   NO INFORMATION AVAILABLE                    ");
		}
		
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% DATA-NODE -- FILE INFORMATION MAP %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		
		if(datanodefileInfoMap.size() > 0) {
			for(String hxId : datanodefileInfoMap.keySet()) {
				DataNodeInfoPacket dtnd = NodeInfoMap.get(hxId);
				System.out.println("@@@@@@@@@@@@ FILEs STORED AT :: "+dtnd.getDataNodeHostName()+" :: "+hxId+" @@@@@@@@@@@@@@@@@@@");
				CopyOnWriteArrayList<String> fllst = datanodefileInfoMap.get(hxId);
				for (String fl : fllst) {
					System.out.println("           "+fl);
				}
				System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
			}
		} else {
			System.out.println("                   NO INFORMATION AVAILABLE                    ");
		}
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% ALL STAGED FILE INFORMATION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		if(fileNameStoreTimeMap.size() > 0) {
			for (String fileName : fileNameStoreTimeMap.keySet()) {
				System.out.println("FILE NAME :: "+fileName + " :: TIME (in millis) :: "+fileNameStoreTimeMap.get(fileName));
			}
		} else {
			System.out.println("                   NO INFORMATION AVAILABLE                    ");
		}
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
	}
	
	private boolean isQueryExist(CopyOnWriteArrayList<QueryNodeInfoPacket> QueryInfoList,String queryNodeQuery) {
		
		boolean isExist = false;
		for(QueryNodeInfoPacket qurypckt : QueryInfoList) {
			if((qurypckt.getQueryNodesQuery()).equals(queryNodeQuery)) {
				isExist = true;
				break;
			}
		}
		
		return isExist;
	}
	
	public void SendQueryNodeFileandNodeList(String queryTimeInfo, boolean yearPrsent, boolean monthPresent, boolean datePresent, String QueryNodeQuery, String QueryNodeHexID, String QueryNodeIP, int QueryNodePort) {
		
		try {
			
			Map<String,Set<String>> YearWiseavailableFileList = new ConcurrentHashMap<String, Set<String>>();  //new HashSet<String>();
			Map<String,DataNodeInfoPacket> YearWiseDataNodeInfo = new ConcurrentHashMap<String, DataNodeInfoPacket>();
			
			// To SEND NOTIFICATION :: POPULATE QUERY NODE's HEXID and QUERY INFORMATION MAP 
			long currentTimeStamp = new Date().getTime()/1000;
			QueryNodeInfoPacket quryPCkt = new QueryNodeInfoPacket(QueryNodeHexID, currentTimeStamp, queryTimeInfo, QueryNodeIP, QueryNodePort, QueryNodeQuery, yearPrsent, monthPresent, datePresent);
			
			if(queryNodeTimeQueryMap.containsKey(QueryNodeHexID)) {
				// ADD INFORMATION TO THE LIST IF THE QUERY FOR THAT QUERY NODE DOESN't EXIST
				CopyOnWriteArrayList<QueryNodeInfoPacket> queryInfoList = queryNodeTimeQueryMap.get(QueryNodeHexID);
				if(!isQueryExist(queryInfoList,QueryNodeQuery)) {
					queryInfoList.add(quryPCkt);
					queryNodeTimeQueryMap.put(QueryNodeHexID, queryInfoList);
				}
				
			} else {
				CopyOnWriteArrayList<QueryNodeInfoPacket> queryInfoList = new CopyOnWriteArrayList<QueryNodeInfoPacket>();
				queryInfoList.add(quryPCkt);
				queryNodeTimeQueryMap.put(QueryNodeHexID, queryInfoList);
			}
			
			if(!QuerynodeQueryInfo.contains(QueryNodeQuery)) {
				QuerynodeQueryInfo.add(QueryNodeQuery);
			}
			
//			Set<String> correspondingNodeHexIDList = new HashSet<String>();
//			CopyOnWriteArrayList<DataNodeInfoPacket> datanodeList = new CopyOnWriteArrayList<DataNodeInfoPacket>();
			
			
			if(yearPrsent) { // YEAR PRESENT
			
				if (monthPresent && datePresent) { // YEAR, MONTH and DATE PRESENT
					
					// POPULATE LIST of NODEs
					for(String filetimeInfo : fileDataNodeMap.keySet()) {
						if(filetimeInfo.equals(queryTimeInfo)) {
							String yrInfo = filetimeInfo.substring(0,4);
							CopyOnWriteArrayList<DataNodeInfoPacket> nodeList = fileDataNodeMap.get(filetimeInfo);
							YearWiseDataNodeInfo.put(yrInfo, (nodeList.get(0)));
							
//							correspondingNodeHexIDList.add((nodeList.get(0)).getHexID());
							break;
//							for (DataNodeInfoPacket node : nodeList) {
//								correspondingNodeHexIDList.add(node.getHexID());
//							}
						}
					}
//					for (String hxID : correspondingNodeHexIDList) {
//						datanodeList.add(NodeInfoMap.get(hxID));
//					}
					
					// POPULATE LIST of AVAILABLE FILEs
					for (String fileName : fileNameStoreTimeMap.keySet()) {
						String fileTimeInfo = (fileName.split("_"))[1];
						String yrInfo = fileTimeInfo.substring(0,4);
						if(fileTimeInfo.equals(queryTimeInfo)) {
							if(YearWiseavailableFileList.containsKey(yrInfo)) {
								Set<String> availableFileList = YearWiseavailableFileList.get(yrInfo);
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							} else {
								Set<String> availableFileList = new HashSet<String>();
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							}
						}
					}
					
					
				} else if (monthPresent) { // YEAR and MONTH PRESENT
					
					// POPULATE LIST of NODEs
					for(String filetimeInfo : fileDataNodeMap.keySet()) {
						if(filetimeInfo.startsWith(queryTimeInfo)) {
							String yrInfo = filetimeInfo.substring(0,4);
							CopyOnWriteArrayList<DataNodeInfoPacket> nodeList = fileDataNodeMap.get(filetimeInfo);
							YearWiseDataNodeInfo.put(yrInfo, (nodeList.get(0)));
							
//							CopyOnWriteArrayList<DataNodeInfoPacket> nodeList = fileDataNodeMap.get(filetimeInfo);
//							correspondingNodeHexIDList.add((nodeList.get(0)).getHexID());
							break;
//							for (DataNodeInfoPacket node : nodeList) {
//								correspondingNodeHexIDList.add(node.getHexID());
//							}
						}
					}
					
//					for (String hxID : correspondingNodeHexIDList) {
//						datanodeList.add(NodeInfoMap.get(hxID));
//					}
					
					// POPULATE LIST of AVAILABLE FILEs
					for (String fileName : fileNameStoreTimeMap.keySet()) {
						String fileTimeInfo = (fileName.split("_"))[1];
						String yrInfo = fileTimeInfo.substring(0,4);
						if(fileTimeInfo.startsWith(queryTimeInfo)) {
//							availableFileList.add(fileName);
							if(YearWiseavailableFileList.containsKey(yrInfo)) {
								Set<String> availableFileList = YearWiseavailableFileList.get(yrInfo);
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							} else {
								Set<String> availableFileList = new HashSet<String>();
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							}
						}
					}
					
				} else if (datePresent) { // YEAR and DATE PRESENT
					
					// POPULATE LIST of NODEs
					for(String filetimeInfo : fileDataNodeMap.keySet()) {
						if(filetimeInfo.startsWith(queryTimeInfo.substring(0,4)) && filetimeInfo.endsWith(queryTimeInfo.substring(4))) {
							String yrInfo = filetimeInfo.substring(0,4);
							CopyOnWriteArrayList<DataNodeInfoPacket> nodeList = fileDataNodeMap.get(filetimeInfo);
							YearWiseDataNodeInfo.put(yrInfo, (nodeList.get(0)));
//							correspondingNodeHexIDList.add((nodeList.get(0)).getHexID());
							break;
//							for (DataNodeInfoPacket node : nodeList) {
//								correspondingNodeHexIDList.add(node.getHexID());
//							}
						}
					}
//					for (String hxID : correspondingNodeHexIDList) {
//						datanodeList.add(NodeInfoMap.get(hxID));
//					}
					
					// POPULATE LIST of AVAILABLE FILEs
					for (String fileName : fileNameStoreTimeMap.keySet()) {
						String fileTimeInfo = (fileName.split("_"))[1];
						String yrInfo = fileTimeInfo.substring(0,4);
						if(fileTimeInfo.startsWith(queryTimeInfo.substring(0,4)) && fileTimeInfo.endsWith(queryTimeInfo.substring(4))) {
//							availableFileList.add(fileName);
							if(YearWiseavailableFileList.containsKey(yrInfo)) {
								Set<String> availableFileList = YearWiseavailableFileList.get(yrInfo);
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							} else {
								Set<String> availableFileList = new HashSet<String>();
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							}
						}
					}
				} else { // YEAR PRESENT ONLY
					
					// POPULATE LIST of NODEs
					for(String filetimeInfo : fileDataNodeMap.keySet()) {
						if(filetimeInfo.startsWith(queryTimeInfo)) {
							String yrInfo = filetimeInfo.substring(0,4);
							CopyOnWriteArrayList<DataNodeInfoPacket> nodeList = fileDataNodeMap.get(filetimeInfo);
							YearWiseDataNodeInfo.put(yrInfo, (nodeList.get(0)));
							
//							CopyOnWriteArrayList<DataNodeInfoPacket> nodeList = fileDataNodeMap.get(filetimeInfo);
//							correspondingNodeHexIDList.add((nodeList.get(0)).getHexID());
							break;
//							for (DataNodeInfoPacket node : nodeList) {
//								correspondingNodeHexIDList.add(node.getHexID());
//							}
						}
					}
//					for (String hxID : correspondingNodeHexIDList) {
//						datanodeList.add(NodeInfoMap.get(hxID));
//					}
					
					// POPULATE LIST of AVAILABLE FILEs
					for (String fileName : fileNameStoreTimeMap.keySet()) {
						String fileTimeInfo = (fileName.split("_"))[1];
						String yrInfo = fileTimeInfo.substring(0,4);
						if(fileTimeInfo.startsWith(queryTimeInfo)) {
//							availableFileList.add(fileName);
							if(YearWiseavailableFileList.containsKey(yrInfo)) {
								Set<String> availableFileList = YearWiseavailableFileList.get(yrInfo);
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							} else {
								Set<String> availableFileList = new HashSet<String>();
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							}
						}
					}
					
				}
			
			} else { // YEAR NOT PRESENT
				
				if (monthPresent && datePresent) { // MONTH and DATE PRESENT
					
					// POPULATE LIST of NODEs
//					String prevflyrInfo = "";
					CopyOnWriteArrayList<String> processedYears = new CopyOnWriteArrayList<String>();
					for(String filetimeInfo : fileDataNodeMap.keySet()) {
						String flmmddtmInfo = filetimeInfo.substring(4);
						String crntflyrTimeInfo = filetimeInfo.substring(0,4);
//						if(!prevflyrInfo.equals(crntflyrTimeInfo)) {
//							prevflyrInfo = crntflyrTimeInfo;
						if(flmmddtmInfo.equals(queryTimeInfo)) {
							if(!processedYears.contains(crntflyrTimeInfo)) {
								CopyOnWriteArrayList<DataNodeInfoPacket> nodeList = fileDataNodeMap.get(filetimeInfo);
								YearWiseDataNodeInfo.put(crntflyrTimeInfo, (nodeList.get(0)));
//								correspondingNodeHexIDList.add((nodeList.get(0)).getHexID());
								processedYears.add(crntflyrTimeInfo);
							}
//								for (DataNodeInfoPacket node : nodeList) {
//									correspondingNodeHexIDList.add(node.getHexID());
//								}
						}
//						}
					}
//					for (String hxID : correspondingNodeHexIDList) {
//						datanodeList.add(NodeInfoMap.get(hxID));
//					}
					
					// POPULATE LIST of AVAILABLE FILEs
					for (String fileName : fileNameStoreTimeMap.keySet()) {
						String fileTimeInfo = (fileName.split("_"))[1];
						String flTmInfo = fileTimeInfo.substring(4);
						String yrInfo = fileTimeInfo.substring(0,4);
						if(flTmInfo.equals(queryTimeInfo)) {
//							availableFileList.add(fileName);
							if(YearWiseavailableFileList.containsKey(yrInfo)) {
								Set<String> availableFileList = YearWiseavailableFileList.get(yrInfo);
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							} else {
								Set<String> availableFileList = new HashSet<String>();
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							}
						}
					}
					
				} else if (monthPresent) { // MONTH PRESENT
					
					// POPULATE LIST of NODEs
//					String prevflyrInfo = "";
					CopyOnWriteArrayList<String> processedYears = new CopyOnWriteArrayList<String>();
					for(String filetimeInfo : fileDataNodeMap.keySet()) {
						String fltmInfo = filetimeInfo.substring(4);
						String crntflyrTimeInfo = filetimeInfo.substring(0,4);
//						if(!prevflyrInfo.equals(crntflyrTimeInfo)) {
//							System.out.println(prevflyrInfo+" ** "+crntflyrTimeInfo);
//							prevflyrInfo = crntflyrTimeInfo;
						if(fltmInfo.startsWith(queryTimeInfo)) {
							if(!processedYears.contains(crntflyrTimeInfo)) {
								CopyOnWriteArrayList<DataNodeInfoPacket> nodeList = fileDataNodeMap.get(filetimeInfo);
								System.out.println("To ADD NODE :: "+(nodeList.get(0)).getDataNodeHostName());
								YearWiseDataNodeInfo.put(crntflyrTimeInfo, (nodeList.get(0)));
//								correspondingNodeHexIDList.add((nodeList.get(0)).getHexID());
								processedYears.add(crntflyrTimeInfo);
							}
//								for (DataNodeInfoPacket node : nodeList) {
//									correspondingNodeHexIDList.add(node.getHexID());
//								}
						}
//						}
					}
//					for (String hxID : correspondingNodeHexIDList) {
//						datanodeList.add(NodeInfoMap.get(hxID));
//					}
					
					// POPULATE LIST of AVAILABLE FILEs
					for (String fileName : fileNameStoreTimeMap.keySet()) {
						String fileTimeInfo = (fileName.split("_"))[1];
						String fltmInfo = fileTimeInfo.substring(4);
						String yrInfo = fileTimeInfo.substring(0,4);
						if(fltmInfo.startsWith(queryTimeInfo)) {
//							availableFileList.add(fileName);
							if(YearWiseavailableFileList.containsKey(yrInfo)) {
								Set<String> availableFileList = YearWiseavailableFileList.get(yrInfo);
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							} else {
								Set<String> availableFileList = new HashSet<String>();
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							}
						}
					}
					
				} else if (datePresent) { // DATE PRESENT
					
					// POPULATE LIST of NODEs
//					String prevflyrInfo = "";
					CopyOnWriteArrayList<String> processedYears = new CopyOnWriteArrayList<String>();
					for(String filetimeInfo : fileDataNodeMap.keySet()) {
						String fltmInfo = filetimeInfo.substring(4);
						String crntflyrTimeInfo = filetimeInfo.substring(0,4);
//						if(!prevflyrInfo.equals(crntflyrTimeInfo)) {
//							prevflyrInfo = crntflyrTimeInfo;
						if(fltmInfo.endsWith(queryTimeInfo)) {
							if(!processedYears.contains(crntflyrTimeInfo)) {
								CopyOnWriteArrayList<DataNodeInfoPacket> nodeList = fileDataNodeMap.get(filetimeInfo);
								YearWiseDataNodeInfo.put(crntflyrTimeInfo, (nodeList.get(0)));
//								correspondingNodeHexIDList.add((nodeList.get(0)).getHexID());
								processedYears.add(crntflyrTimeInfo);
							}
//								for (DataNodeInfoPacket node : nodeList) {
//									correspondingNodeHexIDList.add(node.getHexID());
//								}
						}
//						}
					}
//					for (String hxID : correspondingNodeHexIDList) {
//						datanodeList.add(NodeInfoMap.get(hxID));
//					}
					
					// POPULATE LIST of AVAILABLE FILEs
					for (String fileName : fileNameStoreTimeMap.keySet()) {
						String fileTimeInfo = (fileName.split("_"))[1];
						String fltmInfo = fileTimeInfo.substring(4);
						String yrInfo = fileTimeInfo.substring(0,4);
						if(fltmInfo.endsWith(queryTimeInfo)) {
//							availableFileList.add(fileName);
							if(YearWiseavailableFileList.containsKey(yrInfo)) {
								Set<String> availableFileList = YearWiseavailableFileList.get(yrInfo);
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							} else {
								Set<String> availableFileList = new HashSet<String>();
								availableFileList.add(fileName);
								YearWiseavailableFileList.put(yrInfo, availableFileList);
							}
						}
					}
				}
				
			}
			
			MasterNodeQueryNodeFileNodeInfoPacket pckt = new MasterNodeQueryNodeFileNodeInfoPacket(YearWiseavailableFileList,YearWiseDataNodeInfo);
//			MasterNodeQueryNodeFileNodeInfoPacket pckt = new MasterNodeQueryNodeFileNodeInfoPacket(availableFileList,datanodeList);
			mstrcout.writeInt(PackageFormats.MASTER_NODE_QUERY_NODE_QUERY_RESPONSE);
			ObjectOutputStream oout = new ObjectOutputStream(mstrout);
			oout.writeObject(pckt);
			
		} catch (Exception e) {
			System.out.println("ERROR TO SEND QUERY NODE PROPER INFORMATION");
			e.printStackTrace();
		}
		
	}
	
	private void CalculateQueryThroughPut() {
		
		try {
			
			long numberofQueries = (long) QuerynodeQueryInfo.size();
			long numberofFiles = (long) fileNameStoreTimeMap.size();
			
			if((numberofQueries > 0) && (numberofFiles > 0)) {
				long ElapsedTime = ((new Date().getTime())/1000) - masterNodeStartTime;
				
				long QueryThroughPut = ((numberofQueries * 3600) * numberofFiles)/ElapsedTime;
				
				System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
				System.out.println("QUERY THROUGHPUT TILL NOW :: " + QueryThroughPut + " :: ELAPSED TIME :: " + ElapsedTime);
				System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
				
			} else {
				
				System.out.println("%%%%%%%%%%%%%%%%% NO QUERY THROUGHPUT TILL NOW %%%%%%%%%%%%%%%%%%%%%%%%");
				
			}
			
		} catch (Exception e) {
			System.out.println("ERROR IN CALCULATING QUERY THROUGHPUT");
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		try {
			
			mstrout = masterchannel.getOutputStream();
			mstrin = masterchannel.getInputStream();
			
			mstrccin = new DataInputStream(mstrin);
			mstrcout = new DataOutputStream(mstrout);
			
			while(true) {
				
				int type_of_pckt = mstrccin.readInt();
				
				System.out.println("TYPE OF FLAG :: MASTER : "+type_of_pckt);
				switch (type_of_pckt) {
					
					// DATA-NODE WANTS TO JOIN
					case PackageFormats.DATA_NODE_JOIN_REQUEST : 
					
						ObjectInputStream oin = new ObjectInputStream(mstrin);
						rcvdmessage = oin.readObject();
						
						if(rcvdmessage != null) {
							PopulateOwnNodeInfoList();
							mstrcout.writeInt(1);
							System.out.println("CURRENT NODE INFO SIZE :: "+NodeInfoMap.size());
						} else {
							System.out.println(" PACKAGE TYPE : "+PackageFormats.DATA_NODE_JOIN_REQUEST+"  : NULL OBJECT RECEIVED");
						}
						
					break;
					
					// FORWARD THREE RANDOM DATA-NODE's INFO or EXISTING 3 DATA-NODE's INFO
					case PackageFormats.UPLOADER_NODE_MASTER_NODE_FILE_STAGE_REQUEST :
						String fileName = mstrccin.readUTF();
						System.out.println("TO UPLOAD FILE NAME :: :: "+fileName);
						mstrcout.writeInt(PackageFormats.MASTER_NODE_UPLOADER_NODE_FILE_STAGE_RESPONSE);
						ObjectOutputStream mstrupoout = new ObjectOutputStream(mstrcout);
						mstrupoout.writeObject(getForwardTwoDataNodesInfo2Uploader(fileName));
						System.out.println("SENT IT :::: ");
							
					break;
					
					// DATA-NODE REQUEST LIVE NODE's INFO
					case PackageFormats.DATA_NODE_MASTER_NODE_LIVE_NODE_INFO_REQUEST :
						String contactedNodehexID = mstrccin.readUTF(); 
						
						mstrcout.writeInt(PackageFormats.MASTER_NODE_DATA_NODE_LIVE_NODE_INFO_RESPONSE);
						ObjectOutputStream mstrdtndoout = new ObjectOutputStream(mstrcout);
						mstrdtndoout.writeObject(ForwardLiveDataNodesInfo(contactedNodehexID));
						System.out.println("SENT IT LIVE NODE's INFO :::: ");
						
					break;
					
					case PackageFormats.DATA_NODE_MASTER_NODE_STAGED_FILE_INFO_ACKNOWLEDGEMENT:
						String FilenameTimeInfo = mstrccin.readUTF(); 
						String FileName = mstrccin.readUTF(); 
						int fileSize = mstrccin.readInt();
						ObjectInputStream ooin = new ObjectInputStream(mstrin);
						rcvdmessage = ooin.readObject();
						
						PopulateFileDataNodeInfoMap(FileName, FilenameTimeInfo, fileSize, (DataNodeInfoPacket)rcvdmessage);
						
					break;
						
					case PackageFormats.MASTER_NODE_LIVE_NODE_SHOW:
						ShowLiveNodeInfo();
					break;
					
					case PackageFormats.MASTER_NODE_FILE_INFO_SHOW:
						ShowDataNodeFileMapInfo();
					break;
					
					case PackageFormats.QUERY_NODE_MASTER_NODE_QUERY_REQUEST:
						String queryTimeInfo = mstrccin.readUTF();
						boolean yrPrsent = mstrccin.readBoolean();
						boolean mnthPresent = mstrccin.readBoolean();
						boolean dtPresent = mstrccin.readBoolean();
						String queryNodeQuery = mstrccin.readUTF();
						String queryNodeHexID = mstrccin.readUTF();
						String queryNodeIP = mstrccin.readUTF();
						int queryNodePort = mstrccin.readInt();
						SendQueryNodeFileandNodeList(queryTimeInfo,yrPrsent,mnthPresent,dtPresent,queryNodeQuery,queryNodeHexID,queryNodeIP,queryNodePort);
					break;
					
					case PackageFormats.SYSTEM_QUERY_THROUGHPUT_SHOW :
						CalculateQueryThroughPut();
					break;
				}
				
				
			}
			
			
			
		} catch (Exception e) {
//			System.out.println("ERROR IN CONNECTION OF MASTER-INFO NODE");
//			e.printStackTrace();
		}
	}

}
