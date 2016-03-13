/**
 * 
 */
package CS555.DATANODE;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import CS555.PACKAGES.DataNodeBroadCastPacket;
import CS555.PACKAGES.DataNodeInfoPacket;
import CS555.PACKAGES.DataNodeUniCastpacket;
import CS555.PACKAGES.ForwardDataNodeInfoPacket;
import CS555.PACKAGES.QueryNodeQueryPacket;
import CS555.PACKAGES.UploaderDataNodeFileInfo;
import CS555.PACKAGE_FORMATS.PackageFormats;

/**
 * @author amchakra
 *
 */
public class DataNodeThreadedConnection implements Runnable{
	
	private Socket dataNodecckt = null;
	private String datanodeHexID;
	private String dataNodeServerAddr;
	private String dataNodeHostName;
	private int dataNodePortNo;
	private Map<String, String> NodeFileListMap = null;
	Map<String,CopyOnWriteArrayList<DataNodeInfoPacket>> commonYearNodeInfo = null;
	Map<String,CopyOnWriteArrayList<DataNodeInfoPacket>> commonMonthNodeInfo = null;
	Map<String,CopyOnWriteArrayList<DataNodeInfoPacket>> commonDateNodeInfo = null;
	
	private OutputStream dtndout = null;
	private InputStream dtndin = null;
	
	private DataInputStream dtndcin = null;
	private DataOutputStream dtndcout = null;
	// private ObjectOutputStream oos = null;
	private Object rcvdmessage = null;
	
	public DataNodeThreadedConnection(String datanodeHexID, String dataNodeHostName, Socket dataNodecckt, String dataNodeServerAddr, int dataNodePortNo, Map<String, String> NodeFileListMap, Map<String,CopyOnWriteArrayList<DataNodeInfoPacket>> commonYearNodeInfo, Map<String,CopyOnWriteArrayList<DataNodeInfoPacket>> commonMonthNodeInfo, Map<String,CopyOnWriteArrayList<DataNodeInfoPacket>> commonDateNodeInfo) {
		this.datanodeHexID = datanodeHexID;
		this.dataNodeHostName = dataNodeHostName;
		this.dataNodecckt = dataNodecckt;
		this.dataNodeServerAddr = dataNodeServerAddr;
		this.dataNodePortNo = dataNodePortNo;
		this.NodeFileListMap = NodeFileListMap;
		this.commonYearNodeInfo = commonYearNodeInfo;
		this.commonMonthNodeInfo = commonMonthNodeInfo;
		this.commonDateNodeInfo = commonDateNodeInfo;
	}
	
	private void SendMasterNodeStagedFileInfo(String flname, String filenameTimeInfo, int FilebfrSize){
		try {
			
			Socket dataNodeMaterchannel = new Socket(PackageFormats.MASTER_NODE_IP_ADDRESS, PackageFormats.MASTER_NODE_IP_PORT, null, 0);
			
			OutputStream dtndmstrout = dataNodeMaterchannel.getOutputStream();
			InputStream dtndmstrin = dataNodeMaterchannel.getInputStream();
			
			DataInputStream dtndmstrccin = new DataInputStream(dtndmstrin);
			DataOutputStream dtndmstrccout = new DataOutputStream(dtndmstrout);
			
			DataNodeInfoPacket ownInfoPacket = new DataNodeInfoPacket(datanodeHexID, dataNodeServerAddr, dataNodeHostName, dataNodePortNo);
			
			dtndmstrccout.writeInt(PackageFormats.DATA_NODE_MASTER_NODE_STAGED_FILE_INFO_ACKNOWLEDGEMENT);
			dtndmstrccout.writeUTF(filenameTimeInfo);
			dtndmstrccout.writeUTF(flname);
			dtndmstrccout.writeInt(FilebfrSize);
			ObjectOutputStream dtndoout = new ObjectOutputStream(dtndmstrout);
			dtndoout.writeObject(ownInfoPacket);
			
			dataNodeMaterchannel.close();
			
		} catch (Exception e) {
		}
	}
	
	private void StartFileForwarding(String filename, String tocontactdataNode_IP, int tocontactdataNode_Port, String deadnodeHxID) {
		
		try {
			String filepath = "/tmp/amchakra_PROJ/";
			File inf = new File(filepath+filename);
			long fileSize_KB = inf.length();
			System.out.println("To FORWARD FILE SIZE : "+fileSize_KB+" FILE NAME : "+filename);
			byte[] flbytebfr = new byte[(int) fileSize_KB];
			FileInputStream fin = new FileInputStream(inf);
			BufferedInputStream bin = new BufferedInputStream(fin);
			bin.read(flbytebfr);
			fin.close();
			
			List<DataNodeInfoPacket> dataNodeInfos = new ArrayList<DataNodeInfoPacket>();
			
			Socket toConnectDataNode = new Socket(tocontactdataNode_IP,tocontactdataNode_Port,null,0);
			
			OutputStream upldrdtndout = toConnectDataNode.getOutputStream();
			InputStream upldrdtndin = toConnectDataNode.getInputStream();
			
			DataInputStream upldrdtndccin = new DataInputStream(upldrdtndin);
			DataOutputStream upldrdtndccout = new DataOutputStream(upldrdtndout);
			
			upldrdtndccout.writeInt(PackageFormats.DATA_NODE_DATA_NODE_FILE_FORWARD);
			upldrdtndccout.writeUTF(deadnodeHxID);
			upldrdtndccout.writeUTF(inf.getName());
			upldrdtndccout.writeInt(flbytebfr.length);
			upldrdtndccout.write(flbytebfr);
			ObjectOutputStream upldrdtndoout = new ObjectOutputStream(upldrdtndout);
			UploaderDataNodeFileInfo flupldpckt = new UploaderDataNodeFileInfo(inf.getName(),flbytebfr.length,dataNodeInfos);
			upldrdtndoout.writeObject(flupldpckt);
			
			toConnectDataNode.close();
			
			
		} catch (Exception e) {
			System.out.println("UNABLE TO REPLICATE FILE :: "+filename);
			e.printStackTrace();
		}
	}
	
	private void StartStoringandPropagatingFiles() {
		
		try {
			System.out.println("In STAGE and Propagate");
			// START RECEIVING FILE INFOs
			String FileName = dtndcin.readUTF();
			int fileBufferSize = dtndcin.readInt();
			byte[] filebuffweInfo = new byte[fileBufferSize];
			dtndcin.readFully(filebuffweInfo);
			
			ObjectInputStream dtndflooin = new ObjectInputStream(dtndin);
			UploaderDataNodeFileInfo flinfo = (UploaderDataNodeFileInfo)dtndflooin.readObject();
			
			// STORE THE FILE
			String filepath = "/tmp/amchakra_PROJ/";
			if (!Files.exists(Paths.get(filepath))) {
				Files.createDirectories(Paths.get(filepath));
			} else {
				System.out.println("DIRECTORY ALREADY EXISTS : ADDING FILES TO IT ... ");
			}
			
			File nwFile = new File(filepath+FileName);
			FileOutputStream fout = new FileOutputStream(nwFile); 
			DataOutputStream dfout = new DataOutputStream(fout);
			
			for(int bytecnt = 0; bytecnt < fileBufferSize; bytecnt++) {
				dfout.writeByte(filebuffweInfo[bytecnt]);
			}
			
			String[] tmparr = FileName.split("_");
			String yrmntdtInfo = tmparr[1];
			
			// STAGE FILE-LIST INFO YYYYMMDD -- FileName
			NodeFileListMap.put(yrmntdtInfo, FileName);
			// SEND STAGED FILE INFORMATION TO MASTER NODE
			SendMasterNodeStagedFileInfo(FileName, yrmntdtInfo, fileBufferSize);
			
			// START PROPAGATING FILES
			
			List<DataNodeInfoPacket> dataNodeInfo = flinfo.gettoUploadDataNodeInfo();
			
			if(dataNodeInfo.size() != 0) {
				
				String to_propagate_dataNode_IP = (dataNodeInfo.get(0)).getDataNodeAddress();
				int to_propagate_dataNode_Port = (dataNodeInfo.get(0)).getNodePort();
				dataNodeInfo.remove(0);
				System.out.println("TO PROPAGATE :: "+to_propagate_dataNode_IP+" :: "+to_propagate_dataNode_Port+" :: ");
				// START CONTACTING DATA-NODEs TO PROPAGATE FILES 
				try {
					
					Socket toConnectDataNode = new Socket(to_propagate_dataNode_IP,to_propagate_dataNode_Port,null,0);
					
					OutputStream upldrdtndout = toConnectDataNode.getOutputStream();
					InputStream upldrdtndin = toConnectDataNode.getInputStream();
					
					DataInputStream upldrdtndccin = new DataInputStream(upldrdtndin);
					DataOutputStream upldrdtndccout = new DataOutputStream(upldrdtndout);
					
					upldrdtndccout.writeInt(PackageFormats.DATA_NODE_DATA_NODE_FILE_PROPAGATE);
					upldrdtndccout.writeUTF(FileName);
					upldrdtndccout.writeInt(fileBufferSize);
					upldrdtndccout.write(filebuffweInfo);
					ObjectOutputStream upldrdtndoout = new ObjectOutputStream(upldrdtndout);
					UploaderDataNodeFileInfo flupldpckt = new UploaderDataNodeFileInfo(FileName,fileBufferSize,dataNodeInfo);
					upldrdtndoout.writeObject(flupldpckt);
					
					toConnectDataNode.close();
					
				} catch(Exception e) {
					System.out.println("UNABLE TO CONTACT DATA-NODE");
					e.printStackTrace();
				}
				
			} else {
				System.out.println("STOP PROPAGTING");
				
			}
			
			fout.close();
			
			
			
		} catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILES and PROPAGATING");
			e.printStackTrace();
		}
	}
	
	private void StartBroadCastingFileList(int flag, String deadNodeHexID) {
		// CONTACT MASTER-NODE to GET ALL LIVE DATA-NODE's INFo in the NETWORK
		try {
			
			Socket dataNodeMaterchannel = new Socket(PackageFormats.MASTER_NODE_IP_ADDRESS, PackageFormats.MASTER_NODE_IP_PORT, null, 0);
			
			OutputStream dtndmstrout = dataNodeMaterchannel.getOutputStream();
			InputStream dtndmstrin = dataNodeMaterchannel.getInputStream();
			
			DataInputStream dtndmstrccin = new DataInputStream(dtndmstrin);
			DataOutputStream dtndmstrccout = new DataOutputStream(dtndmstrout);
			
			
			dtndmstrccout.writeInt(PackageFormats.DATA_NODE_MASTER_NODE_LIVE_NODE_INFO_REQUEST);
			dtndmstrccout.writeUTF(datanodeHexID);
			
			int opertnFlag = dtndmstrccin.readInt();
			if(opertnFlag == PackageFormats.MASTER_NODE_DATA_NODE_LIVE_NODE_INFO_RESPONSE) {
				// GET LIVE NODE's INFO
				ObjectInputStream dtndooin = new ObjectInputStream(dtndmstrin);
				ForwardDataNodeInfoPacket rcvdfwrddpckt = (ForwardDataNodeInfoPacket)dtndooin.readObject();
				List<DataNodeInfoPacket> rcvdLiveDataNodesInfo = rcvdfwrddpckt.getDataNodesInfo();
				
				for(DataNodeInfoPacket dtndpckt : rcvdLiveDataNodesInfo) {
					
					System.out.println("*****************************************************************************************************");
					System.out.println("DATA NODE NAME : "+dtndpckt.getDataNodeHostName());
					System.out.println("DATA NODE IP : "+dtndpckt.getDataNodeAddress());
					System.out.println("DATA NODE PORT : "+dtndpckt.getNodePort());
					System.out.println("DATA NODE HEX-ID : "+dtndpckt.getHexID());
					System.out.println("*****************************************************************************************************");
					
					try {
						Socket brdcstNodeSocket = new Socket(dtndpckt.getDataNodeAddress(),dtndpckt.getNodePort(),null,0);
						
						OutputStream dtnddtndout = brdcstNodeSocket.getOutputStream();
						InputStream dtnddtndin = brdcstNodeSocket.getInputStream();
						
						DataInputStream dtnddtndccin = new DataInputStream(dtnddtndin);
						DataOutputStream dtnddtndccout = new DataOutputStream(dtnddtndout);
						
						DataNodeBroadCastPacket tobrdcstpckt = new DataNodeBroadCastPacket(dataNodeServerAddr, dataNodePortNo, datanodeHexID, dataNodeHostName, NodeFileListMap); 
						
						if(flag == 0) {
							dtnddtndccout.writeInt(PackageFormats.DATA_NODE_BROADCAST_INFO_REQUEST);
						} else {
							dtnddtndccout.writeInt(PackageFormats.DATA_NODE_AFTER_FAULT_TOLERATE_BROADCAST_INFO_REQUEST);
							dtnddtndccout.writeUTF(deadNodeHexID);
						}
						
						ObjectOutputStream dtndbrdcstoout = new ObjectOutputStream(dtnddtndout);
						dtndbrdcstoout.writeObject(tobrdcstpckt);
						
						brdcstNodeSocket.close();
						
					} catch (Exception e) {
						System.out.println("UNABLE TO BROADCASt :: NODE");
						e.printStackTrace();
					}
				}
			}
			dataNodeMaterchannel.close();
			
		} catch (Exception e) {
			System.out.println("Unable to Contact Master-Node");
			e.printStackTrace();
		}
	}
	
	private int getMaximumCommonSubStringLen(String BrdcstNodeFileTime, String UnicastNodeFileTimeInfo) {
		
		int str1len = BrdcstNodeFileTime.length();
		int str2len = UnicastNodeFileTimeInfo.length();
		int maxLen = 0;
		
		int[][] commonlenTable = new int[str1len+1][str2len+1];
		
		for(int i=0;i<=str1len;i++) {
			commonlenTable[0][i] = 0;
		}
		
		for(int j= 0; j<=str2len ; j++) {
			commonlenTable[j][0] = 0;
		}
		
		for(int str1i=1; str1i<=str1len;str1i++) {
			for(int str2i=1; str2i<=str2len; str2i++) {
				
				if(BrdcstNodeFileTime.charAt(str1i-1) == UnicastNodeFileTimeInfo.charAt(str2i-1)) {
					commonlenTable[str1i][str2i] = commonlenTable[(str1i-1)][(str2i-1)]+1;
					if(commonlenTable[str1i][str2i] >= maxLen) {
						maxLen = commonlenTable[str1i][str2i];
					}
				}
				
			}
		}
		
		System.out.println("MAX LEN of THE TWO FILENAMEs:: BRD :: "+BrdcstNodeFileTime+" :: UNI :: "+UnicastNodeFileTimeInfo+" :: "+maxLen);
		
		return maxLen;
		
	}
	
	private void StartUnicastingEdgeLables(DataNodeBroadCastPacket rcvdpckt) {
		
		String brdcstdataNodeIP = rcvdpckt.getBroadCastNodeIP();
		String brdcstNodeHexId = rcvdpckt.getBroadCastNodesHexID();
		int brdcstNodePort = rcvdpckt.getBoroadCastNodePort();
		String brdcastNodeHostName = rcvdpckt.getBroadCastNodeHostName();
		Map<String,String> brdcstNodeFileList = rcvdpckt.getBroadCastNodeFileMap();
		Set<String> listofCommonTimes = new HashSet<String>();
		
		DataNodeInfoPacket brdcstNodepckt = new DataNodeInfoPacket(brdcstNodeHexId,brdcstdataNodeIP,brdcastNodeHostName,brdcstNodePort);
		
		System.out.println("$$$$ BROADCAST NODE's :: NAME ::"+brdcastNodeHostName+" IP :: "+brdcstdataNodeIP+" :: PORT :: "+brdcstNodePort+" :: Hex ID :: "+brdcstNodeHexId+" $$$$");
		
		// For each of the BROADCASt NODE's File-List's File
		// Calculate Max-Common Length of the Received Node's
		// File-List
		
		for (String brdcstnodeFileTimeInfo : brdcstNodeFileList.keySet()) {
			for (String unicstnodeFileTimeInfo : NodeFileListMap.keySet()) {
				int commonlen = getMaximumCommonSubStringLen(brdcstnodeFileTimeInfo,unicstnodeFileTimeInfo);
				if(commonlen != 0) {
					if((commonlen == 4) || (commonlen == 5)) {
						//YEAR COMMON
						listofCommonTimes.add(unicstnodeFileTimeInfo.substring(0,4));
					} else if ((commonlen == 6) || (commonlen == 7)) {
						//YEAR-MONTH COMMON
						listofCommonTimes.add(unicstnodeFileTimeInfo.substring(0,6));
					} else if (commonlen == 8) {
						// YEAR-MONTH-DATE COMMON
						listofCommonTimes.add(unicstnodeFileTimeInfo);
					}
				}
			}
		}
		
		if(listofCommonTimes.size() > 0) {
			for (String edgelabel : listofCommonTimes) {
				System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				System.out.println("EDGE LABEL BETWEEN BRD :: "+brdcstNodeHexId+" ** "+brdcastNodeHostName+" :: UNI :: "+datanodeHexID+" ** "+dataNodeHostName+" :: "+edgelabel);
				System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				
				if(edgelabel.length() == 8) {
					
					// FILL UP NODE's TABLE WITH IT's REPLICATION NODE-INFO
					populateNodeTable(commonDateNodeInfo, edgelabel, brdcstNodepckt);
					
					/*if(commonDateNodeInfo.containsKey(edgelabel)) {
						
						List<DataNodeInfoPacket> tmppcktlist = commonDateNodeInfo.get(edgelabel);
						tmppcktlist.add(brdcstNodepckt);
						commonDateNodeInfo.put(edgelabel, tmppcktlist);
						
					} else {
						List<DataNodeInfoPacket> nodelist = new ArrayList<DataNodeInfoPacket>();
						nodelist.add(brdcstNodepckt);
						commonDateNodeInfo.put(edgelabel, nodelist);
					}*/
					
				} else if (edgelabel.length() == 6) {
					
					// FILL UP MONTH TABLE
					populateNodeTable(commonMonthNodeInfo, edgelabel, brdcstNodepckt);
					
					/*if(commonMonthNodeInfo.containsKey(edgelabel)) {
						
						List<DataNodeInfoPacket> tmppcktlist = commonMonthNodeInfo.get(edgelabel);
						tmppcktlist.add(brdcstNodepckt);
						commonMonthNodeInfo.put(edgelabel, tmppcktlist);
						
					} else {
						List<DataNodeInfoPacket> nodelist = new ArrayList<DataNodeInfoPacket>();
						nodelist.add(brdcstNodepckt);
						commonMonthNodeInfo.put(edgelabel, nodelist);
					}*/
					
					// ALSO FILL UP YEAR TABLE
					String YrInfo = edgelabel.substring(0,4);
					populateNodeTable(commonYearNodeInfo, YrInfo, brdcstNodepckt);
					
					/*if(commonYearNodeInfo.containsKey(YrInfo)) {
						
						List<DataNodeInfoPacket> tmppcktlist = commonYearNodeInfo.get(YrInfo);
						tmppcktlist.add(brdcstNodepckt);
						commonYearNodeInfo.put(YrInfo, tmppcktlist);
						
					} else {
						List<DataNodeInfoPacket> nodelist = new ArrayList<DataNodeInfoPacket>();
						nodelist.add(brdcstNodepckt);
						commonYearNodeInfo.put(YrInfo, nodelist);
					}*/
					
					
				} else if (edgelabel.length() == 4) {
					
					// FILL UP YEAR TABLE
					populateNodeTable(commonYearNodeInfo, edgelabel, brdcstNodepckt);
					
					/*if(commonYearNodeInfo.containsKey(edgelabel)) {
						
						List<DataNodeInfoPacket> tmppcktlist = commonYearNodeInfo.get(edgelabel);
						tmppcktlist.add(brdcstNodepckt);
						commonYearNodeInfo.put(edgelabel, tmppcktlist);
						
					} else {
						List<DataNodeInfoPacket> nodelist = new ArrayList<DataNodeInfoPacket>();
						nodelist.add(brdcstNodepckt);
						commonYearNodeInfo.put(edgelabel, nodelist);
					}*/
				}
			}
			
			// SHOW UPDATED NODE-INFO TABLE
			ShowNodesNodeInfoTable();
			// TO-DO :::: SAME THING NEEDS TO BE DONE FOR BROADCAST NODE
			DataNodeInfoPacket unicatNodeInfo = new DataNodeInfoPacket(datanodeHexID,dataNodeServerAddr, dataNodeHostName, dataNodePortNo);
			DataNodeUniCastpacket unicstpckt = new DataNodeUniCastpacket(unicatNodeInfo,listofCommonTimes);
			
			try {
				
				Socket unibrdcstNodeSocket = new Socket(brdcstdataNodeIP,brdcstNodePort,null,0);
				
				OutputStream dtnddtndout = unibrdcstNodeSocket.getOutputStream();
				InputStream dtnddtndin = unibrdcstNodeSocket.getInputStream();
				
				DataInputStream dtnddtndccin = new DataInputStream(dtnddtndin);
				DataOutputStream dtnddtndccout = new DataOutputStream(dtnddtndout);
				
				dtnddtndccout.writeInt(PackageFormats.DATA_NODE_UNICAST_INFO_REQUEST);
				ObjectOutputStream dtndbrdcstoout = new ObjectOutputStream(dtnddtndout);
				dtndbrdcstoout.writeObject(unicstpckt);
				
				unibrdcstNodeSocket.close();
				
			} catch (Exception e) {
				System.out.println("UNABLE TO CONTACT BROADCASt NODE");
				e.printStackTrace();
			}
			
		} else {
			System.out.println("!!!!!!!!!!!!! NOTHING COMMON BRD :: "+brdcstNodeHexId+" :: UNI :: "+datanodeHexID+" ARE NOT CONNECTED !!!!!!!!!!!!!!!!");
		}
	}
	
	private void DeleteFromLabelledTable(Map<String,CopyOnWriteArrayList<DataNodeInfoPacket>> labelInfo, String deadDataNDHXID) {
		
		synchronized (labelInfo) {
			if(labelInfo.size() > 0) {	
				for(String timeinfo : labelInfo.keySet()) {
					CopyOnWriteArrayList<DataNodeInfoPacket> nodeInfos = labelInfo.get(timeinfo);
					int indx = 0;
					for(DataNodeInfoPacket ndinf : nodeInfos) {
//						System.out.println(timeinfo+" :: HEX ID :: "+ndinf.getHexID()+" :: NAME :: "+ndinf.getDataNodeHostName());
						if((ndinf.getHexID()).equals(deadDataNDHXID)) {
							// DEAD NODE INFO PRESENT
							nodeInfos.remove(indx);
							System.out.println("DELETED DEAD DATA-NODE :: "+ndinf.getDataNodeHostName()+" :: FROM :: "+dataNodeHostName+"'s NODE TABLE");
						}
						indx++;
					}
				}
			} else {
				System.out.println("NO INFORMATION AVAILABLE AT THIS LABEL :: To DELETE");
			}
		}
		
	}
	
	private void DeleteDeadNodeInfofromNodeTable(String deadnodeHexID) {
		DeleteFromLabelledTable(commonDateNodeInfo,deadnodeHexID);
		DeleteFromLabelledTable(commonMonthNodeInfo, deadnodeHexID);
		DeleteFromLabelledTable(commonYearNodeInfo, deadnodeHexID);
	}
	
	private boolean isContains(CopyOnWriteArrayList<DataNodeInfoPacket> nodeInfoList, DataNodeInfoPacket dtndpckt) {
		
		boolean isContain = false;
		
		for (DataNodeInfoPacket pckt : nodeInfoList) {
			if((pckt.getHexID()).equals(dtndpckt.getHexID())) {
				isContain = true;
				break;
			}
		}
		
		return isContain;
		
	}
	
	private void populateNodeTable(Map<String,CopyOnWriteArrayList<DataNodeInfoPacket>> table, String label, DataNodeInfoPacket nodeInfo) {
		
		synchronized (table) {
			if(table.containsKey(label)) {
				
				CopyOnWriteArrayList<DataNodeInfoPacket> tmppcktlist = table.get(label);
				if(!isContains(tmppcktlist,nodeInfo)) { // tmppcktlist.contains(nodeInfo)
					tmppcktlist.add(nodeInfo);
					table.put(label, tmppcktlist);
				}
				
			} else {
				
				CopyOnWriteArrayList<DataNodeInfoPacket> nodelist = new CopyOnWriteArrayList<DataNodeInfoPacket>();
				nodelist.add(nodeInfo);
				table.put(label, nodelist);
			
			}
		}
	}
	
	
	private void StartPopulatingOwnNodeTable(DataNodeUniCastpacket unipckt) {
		
		DataNodeInfoPacket uninodeInfo = unipckt.getUnicastNodeInfo();
		Set<String> commonFileInfo = unipckt.getListofCommonFileInfo();
		
		for (String edgelabel : commonFileInfo) {
			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			System.out.println("EDGE LABEL BETWEEN BRD :: "+datanodeHexID+" ** "+dataNodeHostName+" :: UNI :: "+uninodeInfo.getHexID()+" ** "+uninodeInfo.getDataNodeHostName()+" :: "+edgelabel);
			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			
			if(edgelabel.length() == 8) {
				
				// FILL UP NODE's TABLE WITH IT's REPLICATION NODE-INFO
				populateNodeTable(commonDateNodeInfo, edgelabel, uninodeInfo);
				
				/*if(commonDateNodeInfo.containsKey(edgelabel)) {
					
					List<DataNodeInfoPacket> tmppcktlist = commonDateNodeInfo.get(edgelabel);
					tmppcktlist.add(uninodeInfo);
					commonDateNodeInfo.put(edgelabel, tmppcktlist);
					
				} else {
					List<DataNodeInfoPacket> nodelist = new ArrayList<DataNodeInfoPacket>();
					nodelist.add(uninodeInfo);
					commonDateNodeInfo.put(edgelabel, nodelist);
				}*/
				
			} else if (edgelabel.length() == 6) {
				
				// FILL UP MONTH TABLE
				populateNodeTable(commonMonthNodeInfo, edgelabel, uninodeInfo);
				
				/*if(commonMonthNodeInfo.containsKey(edgelabel)) {
					
					List<DataNodeInfoPacket> tmppcktlist = commonMonthNodeInfo.get(edgelabel);
					tmppcktlist.add(uninodeInfo);;
					commonMonthNodeInfo.put(edgelabel, tmppcktlist);
					
				} else {
					List<DataNodeInfoPacket> nodelist = new ArrayList<DataNodeInfoPacket>();
					nodelist.add(uninodeInfo);
					commonMonthNodeInfo.put(edgelabel, nodelist);
				}*/
				
				// ALSO FILL UP YEAR TABLE
				String YrInfo = edgelabel.substring(0,4);
				populateNodeTable(commonYearNodeInfo, YrInfo, uninodeInfo);
				
				/*if(commonYearNodeInfo.containsKey(YrInfo)) {
					
					List<DataNodeInfoPacket> tmppcktlist = commonYearNodeInfo.get(YrInfo);
					tmppcktlist.add(uninodeInfo);
					commonYearNodeInfo.put(YrInfo, tmppcktlist);
					
				} else {
					List<DataNodeInfoPacket> nodelist = new ArrayList<DataNodeInfoPacket>();
					nodelist.add(uninodeInfo);
					commonYearNodeInfo.put(YrInfo, nodelist);
				}*/
				
				
			} else if (edgelabel.length() == 4) {
				
				// FILL UP YEAR TABLE
				populateNodeTable(commonYearNodeInfo, edgelabel, uninodeInfo);
				
				/*if(commonYearNodeInfo.containsKey(edgelabel)) {
					
					List<DataNodeInfoPacket> tmppcktlist = commonYearNodeInfo.get(edgelabel);
					tmppcktlist.add(uninodeInfo);
					commonYearNodeInfo.put(edgelabel, tmppcktlist);
					
				} else {
					List<DataNodeInfoPacket> nodelist = new ArrayList<DataNodeInfoPacket>();
					nodelist.add(uninodeInfo);
					commonYearNodeInfo.put(edgelabel, nodelist);
				}*/
			}
		}
		
		// SHOW IT'S OWn NODE-INFo TABLE
		ShowNodesNodeInfoTable();
		
	}
	
	private void ShowEachLabelNodeInfoTable(Map<String,CopyOnWriteArrayList<DataNodeInfoPacket>> labelInfo) {
		if(labelInfo.size() > 0) {
			for(String timeinfo : labelInfo.keySet()) {
				CopyOnWriteArrayList<DataNodeInfoPacket> nodeInfos = labelInfo.get(timeinfo);
				for(DataNodeInfoPacket ndinf : nodeInfos) {
					System.out.println(timeinfo+" :: HEX ID :: "+ndinf.getHexID()+" :: NAME :: "+ndinf.getDataNodeHostName());
				}
			}
		} else {
			System.out.println("NO INFORMATION AVAILABLE AT THIS LABEL");
		}
	}
	
	private void ShowNodesNodeInfoTable() {
		System.out.println("************************** NODE TABLE of "+dataNodeHostName+" ********************************************");
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$ YEAR INFO $$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		ShowEachLabelNodeInfoTable(commonYearNodeInfo);
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$ MONTH INFO $$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		ShowEachLabelNodeInfoTable(commonMonthNodeInfo);
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$ DATE INFO $$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		ShowEachLabelNodeInfoTable(commonDateNodeInfo);
		System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		System.out.println("**************************************************************************************************************");
	}
	
	private String ExecuteQueryonLocalDataNode(String prevQueryResult, String fileName, Map<String,CopyOnWriteArrayList<String>> queryvarblfuncs) {
		String outputString = "";
		String YrInfo = ((fileName.split("_"))[1]).substring(0,4);
		try {
			
			String fileNameWithPath = "/tmp/amchakra_PROJ/"+fileName;
			File fin = new File(fileNameWithPath);
			FileInputStream fis = new FileInputStream(fin);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			
			String line = null;
			CopyOnWriteArrayList<String> typeofQuerys = new CopyOnWriteArrayList<String>();
			Map<String,Integer> variableIndexMap = new ConcurrentHashMap<String, Integer>();
			Map<String,String> funcvariableMap = new ConcurrentHashMap<String, String>();
			
			Double idx = 0.0;
			Double minVal = 0.0;
			Double maxVal = 0.0;
			Double totalval = 0.0;
			Double avgVal = 0.0;
			
			while ((line = br.readLine()) != null) {
				String[] linearr = line.split(",");
				if(linearr.length != 1) {
					if (idx == 0.0) {
						// FIRST LINE POPULATE VARIABLE INDEX MAP
						for (int i = 0; i<linearr.length; i++) {
							for(String variable : queryvarblfuncs.keySet()) {
								if((linearr[i].toUpperCase()).equals(variable)) {
									variableIndexMap.put(variable, i);
								}
							}
						}
					} else {
						
						for(String variable : queryvarblfuncs.keySet()) {
							int varIndx = variableIndexMap.get(variable);
							
//							System.out.println("VARIABLE :: "+variable+" :: INDX :: "+varIndx+" :: RUNNING IDX :: "+idx);
							
							if(varIndx != -1) {
								
								Double varblval = Double.parseDouble(linearr[varIndx]);
								
								for(String func : queryvarblfuncs.get(variable)) {
									
//									System.out.println("FUNCTION :: "+func+" :: VAL :: "+varblval+" :: MAX :: "+maxVal);
									
									if(func.equals(PackageFormats.QUERY_TYPE_MAX)) {
										
										if(varblval <= 1000.0D) { // PROBABALY ISSUE IN INPUT DATA :: DON't TAKE INTo ACCOUNT THEM
											if(maxVal == 0.0) {
												maxVal = varblval;
											} else if (varblval >= maxVal) {
												maxVal = varblval;
											}
										}
										
										if(!typeofQuerys.contains(func)) {
											typeofQuerys.add(func);
										}
										
										if(!funcvariableMap.containsKey(PackageFormats.QUERY_TYPE_MAX)) {
											funcvariableMap.put(PackageFormats.QUERY_TYPE_MAX, variable.toLowerCase());
										}
										
									} else if (func.equals(PackageFormats.QUERY_TYPE_MIN)) {
										
										if(varblval <= 1000.0D) { // PROBABALY ISSUE IN INPUT DATA :: DON't TAKE INTo ACCOUNT THEM
											if(minVal == 0.0) {
												minVal = varblval;
											} else if (varblval <= minVal) {
												minVal = varblval;
											}
										}
										
										if(!typeofQuerys.contains(func)) {
											typeofQuerys.add(func);
										}
										
										if(!funcvariableMap.containsKey(PackageFormats.QUERY_TYPE_MIN)) {
											funcvariableMap.put(PackageFormats.QUERY_TYPE_MIN, variable.toLowerCase());
										}
										
									} else if (func.equals(PackageFormats.QUERY_TYPE_AVG)) {
										
										if(varblval <= 1000.0D) { // PROBABALY ISSUE IN INPUT DATA :: DON't TAKE INTo ACCOUNT THEM
											totalval += varblval;
										}
										
										if(!typeofQuerys.contains(func)) {
											typeofQuerys.add(func);
										}
										
										if(!funcvariableMap.containsKey(PackageFormats.QUERY_TYPE_AVG)) {
											funcvariableMap.put(PackageFormats.QUERY_TYPE_AVG, variable.toLowerCase());
										}
										
									}
								}
							}
						}
					}
					
					idx += 1.0;
				}
			}
			
			if(totalval != 0.0) {
				avgVal = totalval/idx;
			}
			
			System.out.println("PREV QUERY RESULT :: "+prevQueryResult+" :: "+prevQueryResult.isEmpty());
			for(String typefqry : typeofQuerys) {
				System.out.println("TYPE OF QUERY :: "+typefqry);
			}
			
			if(prevQueryResult.isEmpty()) {
				
				for(String typefqry : typeofQuerys) {
					
					System.out.println(typefqry+" :: "+maxVal+" :: "+minVal+" :: "+avgVal);
					
					if(typefqry.equals(PackageFormats.QUERY_TYPE_MAX)) {
						
						outputString += PackageFormats.QUERY_TYPE_MAX+"_"+String.valueOf(maxVal)+"_"+YrInfo+"-"+funcvariableMap.get(PackageFormats.QUERY_TYPE_MAX)+" # ";
					
					} else if (typefqry.equals(PackageFormats.QUERY_TYPE_MIN)) {
						
						outputString += PackageFormats.QUERY_TYPE_MIN+"_"+String.valueOf(minVal)+"_"+YrInfo+"-"+funcvariableMap.get(PackageFormats.QUERY_TYPE_MIN)+" # ";
					
					} else if (typefqry.equals(PackageFormats.QUERY_TYPE_AVG)) {
						
						outputString += PackageFormats.QUERY_TYPE_AVG+"_"+String.valueOf(avgVal)+"_"+YrInfo+"-"+funcvariableMap.get(PackageFormats.QUERY_TYPE_AVG)+" # ";
					
					}
				}
				
			} else {
				String[]prevQueryResarr = prevQueryResult.split("#");
				for(String funcvalinfo : prevQueryResarr) {
					if(funcvalinfo.contains(PackageFormats.QUERY_TYPE_MAX)) {
						
						Double prevval = Double.parseDouble((funcvalinfo.split("_"))[1]);
						
						if((maxVal > prevval) && (maxVal != 0.0)) {
							
							outputString += PackageFormats.QUERY_TYPE_MAX+"_"+String.valueOf(maxVal)+"_"+YrInfo+"-"+funcvariableMap.get(PackageFormats.QUERY_TYPE_MAX)+" # ";
						
						} else {
							
							outputString += PackageFormats.QUERY_TYPE_MAX+"_"+String.valueOf(prevval)+"_"+YrInfo+"-"+funcvariableMap.get(PackageFormats.QUERY_TYPE_MAX)+" # ";
						
						}
						
					} else if(funcvalinfo.contains(PackageFormats.QUERY_TYPE_MIN)) {
						
						Double prevval = Double.parseDouble((funcvalinfo.split("_"))[1]);
						
						if((minVal < prevval) && (minVal != 0.0)) {
							
							outputString += PackageFormats.QUERY_TYPE_MIN+"_"+String.valueOf(minVal)+"_"+YrInfo+"-"+funcvariableMap.get(PackageFormats.QUERY_TYPE_MIN)+" # ";
						
						} else {
							
							outputString += PackageFormats.QUERY_TYPE_MIN+"_"+String.valueOf(prevval)+"_"+YrInfo+"-"+funcvariableMap.get(PackageFormats.QUERY_TYPE_MIN)+" # ";
						
						}
						
					} else if(funcvalinfo.contains(PackageFormats.QUERY_TYPE_AVG)) {
						
						Double prevval = Double.parseDouble((funcvalinfo.split("_"))[1]);
						
						outputString += PackageFormats.QUERY_TYPE_AVG+"_"+String.valueOf((avgVal != 0.0) ? (prevval+avgVal)/2.0 : prevval)+"_"+YrInfo+"-"+funcvariableMap.get(PackageFormats.QUERY_TYPE_AVG)+" # ";
						
					}
					
				}
			}
			br.close();
			
		} catch (Exception e) {
			System.out.println("ERROR DURING EXECUTING QUERY :: ");
			e.printStackTrace();
		}
		System.out.println("OUTPUT :: "+outputString);
		return outputString;
	}
	
	private void StartExecutingandForwardingQuery() {
		try {
			String QueryNodeIp = dtndcin.readUTF();
			int QueryNodePort = dtndcin.readInt();
			String queryResultTrace = dtndcin.readUTF();
			String QueryResult = dtndcin.readUTF();
			String processedNodeHexIDs = dtndcin.readUTF();
			String processedFiles = dtndcin.readUTF();
//			long nofRecords = dtndcin.readLong();
			long QueryInitializeTime = dtndcin.readLong();
			
			ObjectInputStream oin = new ObjectInputStream(dtndin);
			QueryNodeQueryPacket querypckt = (QueryNodeQueryPacket) oin.readObject();
			
			Set<String> rcvdfileNames = querypckt.getAvailableNetworkQueryFileList();
			Map<String,CopyOnWriteArrayList<String>> queryInfos = querypckt.getQueryFuctionVariableMap();
			
			// TRY NOT TO USE THIS LIST
//			CopyOnWriteArrayList<DataNodeInfoPacket> NodeList = querypckt.gettoContactDataNodeList();
			
			queryResultTrace += dataNodeHostName+" -- ";
			CopyOnWriteArrayList<String> toProcessFiles = new CopyOnWriteArrayList<String>();
			CopyOnWriteArrayList<String> UnProcessedFiles = new CopyOnWriteArrayList<String>();
			boolean isDestination = false;
			boolean isFileCorrupted = false;
			
			for(String fltminfo : NodeFileListMap.keySet()) {
				System.out.println("STORED FILE INFO :: "+fltminfo+" :: "+NodeFileListMap.get(fltminfo));
			}
			
			for(String rcvdFilenm : rcvdfileNames) {
				String yrmmdtTimeInfo = (rcvdFilenm.split("_"))[1];
				System.out.println("RECEIVED FILE's YEAR/DATE :: "+yrmmdtTimeInfo);
				if(NodeFileListMap.containsKey(yrmmdtTimeInfo)) {
					toProcessFiles.add(rcvdFilenm);
				} else {
					UnProcessedFiles.add(rcvdFilenm);
				}
			}
			
			// FIRST :: :: EXECUTE THE QUERY ::
			// IF QUERy COMES TO THIS NODE JUSt ADD IT's HEX-ID as PROCESSED WHETHER THE FILE PROCESSED OR NOT
			if(toProcessFiles.size() > 0) {
				for(String flnm : toProcessFiles) {
					System.out.println("TO PROCESS FILE NAME :: "+flnm+" :: QUERY RESULT :: "+QueryResult);
					QueryResult = ExecuteQueryonLocalDataNode(QueryResult,flnm,queryInfos);
					
					if(QueryResult.isEmpty()) {
						
						toProcessFiles.remove(toProcessFiles.indexOf(flnm));
						UnProcessedFiles.add(flnm);
						isFileCorrupted = true;
						
					} else {
						
						processedFiles += " - " + (flnm.split("_"))[1];
						
					}
				}
				
				processedNodeHexIDs +="-"+datanodeHexID;
				
			} else {
				
				processedNodeHexIDs +="-"+datanodeHexID;
			
			}
			
			// REMOVE THE PROCESSED FILES 
			for(String flnm : toProcessFiles) {
				rcvdfileNames.remove(flnm);
			}
						
			for(String flnm : UnProcessedFiles) {
				System.out.println("UNPROCESSED FILES :: "+flnm);
			}
			
			
			if(UnProcessedFiles.size() > 0) {
				// STILL NEED TO ROUTE QUERY
				
				// SECOND :: :: ROUTE/FORWARD QUERY
				DataNodeInfoPacket toForwardDtaNode = null;
				
				for(String unprcsdfilenm : UnProcessedFiles) {
					
					if(toForwardDtaNode != null) {
						if(!(toForwardDtaNode.getHexID()).equals(datanodeHexID)) { // PROBABALY :: REDUNDANT CHECK 
							break;
						}
					}
					
					toForwardDtaNode = null;
					
					String firstUnprocessedFile = unprcsdfilenm; //UnProcessedFiles.get(0);
					String firstUnprocessedFileTimeInfo = (firstUnprocessedFile.split("_"))[1];
					
					if(!isFileCorrupted) {
						if(commonMonthNodeInfo.size() > 0) {
							// MONTH LEVEl INFORMATION AVAILABLE :: GET THE NODE HAVING COMMON FILE-PREFIX
							String yrmnthInfo = firstUnprocessedFileTimeInfo.substring(0,6);
							for(String timeInfo : commonMonthNodeInfo.keySet()) {
								if(timeInfo.equals(yrmnthInfo)) {
									for(DataNodeInfoPacket dtnd : commonMonthNodeInfo.get(timeInfo)) {
										 //(commonMonthNodeInfo.get(timeInfo)).get(0);
										if(!processedNodeHexIDs.contains(dtnd.getHexID())) {
											System.out.println("FROM COMMON MONTH :: FORWARD NODE "+dtnd.getDataNodeHostName());
											toForwardDtaNode = dtnd;
											break;
										} else {
											toForwardDtaNode = null;
										}
									}
								}
							}
							
						} 
						
						// THAT MEANS MONTH LEVEL INFO of THIS NODE CAN't PROVIDE FORWARD NODE's INFO 
						// THIS MEANS THAT MONTH WISE DISJOINT SET 
						
						if (toForwardDtaNode == null) { 
							// YEAR LEVEL INFORMATION AVAILABLE :: GET THE NODE HAVING COMMON FILE-PREFIX
							
							if(commonYearNodeInfo.size() > 0) {
								String yrInfo = firstUnprocessedFileTimeInfo.substring(0,4);
								
								for(String timeInfo : commonYearNodeInfo.keySet()) {
									if(timeInfo.equals(yrInfo)) {
										for(DataNodeInfoPacket dtnd : commonYearNodeInfo.get(timeInfo)) {
											 //(commonYearNodeInfo.get(timeInfo)).get(0);
											if(!processedNodeHexIDs.contains(dtnd.getHexID())) {
												System.out.println("FROM COMMON YEAR :: :: FORWARD NODE "+dtnd.getDataNodeHostName());
												toForwardDtaNode = dtnd;
												break;
											} else {
												toForwardDtaNode = null;
											}
										}
									}
								}	
							}
						}
					}
					// THAT MEANS THE DATA NODE FORWARDs IT's INFORMATION to IT's REPLICATED NODE SET
					if(toForwardDtaNode == null) {
						if(commonDateNodeInfo.size() > 0) {
							for(String timeInfo : commonDateNodeInfo.keySet()) {
								if(timeInfo.equals(firstUnprocessedFileTimeInfo)) {
									for(DataNodeInfoPacket dtnd : commonDateNodeInfo.get(timeInfo)) {
										if(!processedNodeHexIDs.contains(dtnd.getHexID())) {
											System.out.println("FROM COMMON YEAR|MONTH|DATE :: :: FORWARD NODE "+dtnd.getDataNodeHostName());
											toForwardDtaNode = dtnd;
											break;
										} else {
											toForwardDtaNode = null;
										}
									}
								}
							}
						}
					}
				}
				
				if(toForwardDtaNode != null) {
					// ROUTE QUERY To THAT NODE
					if(!(toForwardDtaNode.getHexID()).equals(datanodeHexID)) {
						try {
							
							System.out.println("TO FORWARD DATA NODE :: "+toForwardDtaNode.getDataNodeHostName());
							
							Socket datanodesckt = new Socket(toForwardDtaNode.getDataNodeAddress(),toForwardDtaNode.getNodePort(),null,0);
							
							OutputStream dtnddout = datanodesckt.getOutputStream();
							InputStream dtnddin = datanodesckt.getInputStream();
							
							DataInputStream dtndccin = new DataInputStream(dtnddin);
							DataOutputStream dtndccout = new DataOutputStream(dtnddout);
							
							QueryNodeQueryPacket qrypckt = new QueryNodeQueryPacket(rcvdfileNames,queryInfos); // ,NodeList
							
							dtndccout.writeInt(PackageFormats.QUERY_NODE_DATA_NODE_QUERY_REQUEST);
							dtndccout.writeUTF(QueryNodeIp);
							dtndccout.writeInt(QueryNodePort);
							dtndccout.writeUTF(queryResultTrace);
							dtndccout.writeUTF(QueryResult);
							dtndccout.writeUTF(processedNodeHexIDs);
							dtndccout.writeUTF(processedFiles);
							dtndccout.writeLong(QueryInitializeTime);
							
							ObjectOutputStream oout = new ObjectOutputStream(dtnddout);
							oout.writeObject(qrypckt);
							
							datanodesckt.close();
							
						} catch (Exception e) {
							System.out.println("UNABLE To ROUTE QUERY To DATANODE :: "+toForwardDtaNode.getDataNodeHostName());
							
							// UNABLE To FORWARD QUERIES :: CONTACT THE QUERY NODE To SEND PARTIAL QUERY RESULT ::
							try {
								
								Socket datanodequerysckt = new Socket(QueryNodeIp,QueryNodePort,null,0);
								
								OutputStream dtndqryout = datanodequerysckt.getOutputStream();
								InputStream dtndqryin = datanodequerysckt.getInputStream();
								
								DataInputStream dtndqryccin = new DataInputStream(dtndqryin);
								DataOutputStream dtndqryccout = new DataOutputStream(dtndqryout);
								
								dtndqryccout.writeInt(PackageFormats.DATA_NODE_QUERY_NODE_QUERY_RESPONSE);
								dtndqryccout.writeUTF(queryResultTrace);
								dtndqryccout.writeUTF(QueryResult.toUpperCase());
								dtndqryccout.writeUTF(processedFiles);
								dtndqryccout.writeUTF("THIS is PARTIAL RESULT :: PLEASE RE-EXECUTE the QUERY AFTER FEW SECONDS");
								dtndqryccout.writeLong(QueryInitializeTime);
								
								datanodequerysckt.close();
								
							} catch(Exception ex) {
								System.out.println("UNABLE To CONTACT QUERY NODE");
								ex.printStackTrace();
							}
							
							e.printStackTrace();
						}
					} else {
						// MAKE itself as DESTINATION NODE
						isDestination = true;
					}
				} else {
					// MAKE itself as DESTINATION NODE
					isDestination = true;
				}
				
			} else {
				// NO NEED TO ROUTE QUERY :: THIS is THE DESTINATION DATA-NODE
				isDestination = true;
			}
			
			if(isDestination) {
				// CONTACT THE QUERY NODE To SEND QUERY RESULT ::
				try {
					
					Socket datanodequerysckt = new Socket(QueryNodeIp,QueryNodePort,null,0);
					
					OutputStream dtndqryout = datanodequerysckt.getOutputStream();
					InputStream dtndqryin = datanodequerysckt.getInputStream();
					
					DataInputStream dtndqryccin = new DataInputStream(dtndqryin);
					DataOutputStream dtndqryccout = new DataOutputStream(dtndqryout);
					
					dtndqryccout.writeInt(PackageFormats.DATA_NODE_QUERY_NODE_QUERY_RESPONSE);
					dtndqryccout.writeUTF(queryResultTrace);
					dtndqryccout.writeUTF(QueryResult.toUpperCase());
					dtndqryccout.writeUTF(processedFiles);
					dtndqryccout.writeUTF("");
					dtndqryccout.writeLong(QueryInitializeTime);
					
					datanodequerysckt.close();
					
				} catch(Exception e) {
					System.out.println("UNABLE To CONTACT QUERY NODE");
					e.printStackTrace();
				}
				
			}
			
		} catch (Exception e) {
			System.out.println("ERROR in EXECUTING and FORWARDING QUERY");
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			dtndout = dataNodecckt.getOutputStream();
			dtndin = dataNodecckt.getInputStream();
		
			dtndcin = new DataInputStream(dtndin);
			dtndcout = new DataOutputStream(dtndout);
			
			int type_f_flag = dtndcin.readInt();
			
			System.out.println("TYPE OF REQUEST :: "+type_f_flag);
			
			switch (type_f_flag) {
				
//				case PackageFormats.DATA_NODE_DATA_NODE_FILE_PROPAGATE :
//					//START STORING FILES
//					StartStoringandPropagatingFiles();
//				break;
				
				case PackageFormats.DATA_NODE_DATA_NODE_FILE_FORWARD:
					String deadNDHXID = dtndcin.readUTF();
					DeleteDeadNodeInfofromNodeTable(deadNDHXID);
					//START STORING FILES
					StartStoringandPropagatingFiles();
					// START BROADCASTING IT's UPDATED FILE-LIST
					StartBroadCastingFileList(1,deadNDHXID); // 1 -- AFTER FAILURE COPING
				break;
			
				case PackageFormats.DATA_NODE_DATA_NODE_FILE_PROPAGATE :
				case PackageFormats.UPLOADER_NODE_DATA_NODE_FILE_STAGE_REQUEST :
					String deadNdHXID = "";
					//START STORING FILES
					StartStoringandPropagatingFiles();
					// START BROADCASTING IT's UPDATED FILE-LIST
					StartBroadCastingFileList(0,deadNdHXID);
				break;
				
				case PackageFormats.DATA_NODE_BROADCAST_INFO_REQUEST :
					ObjectInputStream dtndooin = new ObjectInputStream(dtndin);
					rcvdmessage = dtndooin.readObject();
					StartUnicastingEdgeLables((DataNodeBroadCastPacket)rcvdmessage);
				break;
				
				case PackageFormats.DATA_NODE_AFTER_FAULT_TOLERATE_BROADCAST_INFO_REQUEST:
					String ddndhxID = dtndcin.readUTF();
					DeleteDeadNodeInfofromNodeTable(ddndhxID);
					ObjectInputStream datndooin = new ObjectInputStream(dtndin);
					rcvdmessage = datndooin.readObject();
					StartUnicastingEdgeLables((DataNodeBroadCastPacket)rcvdmessage);
				break;
				
				case PackageFormats.DATA_NODE_UNICAST_INFO_REQUEST :
					ObjectInputStream dtnuniooin = new ObjectInputStream(dtndin);
					rcvdmessage = dtnuniooin.readObject();
					StartPopulatingOwnNodeTable((DataNodeUniCastpacket)rcvdmessage);
				break;
				
				case PackageFormats.SHOW_NODE_INFO_TABLE :
					ShowNodesNodeInfoTable();
				break;
				
				case PackageFormats.MASTER_NODE_DATA_NODE_HEARTBEAT:
					String rcvdstr = dtndcin.readUTF();
					System.out.println("%%%% HEARTBEAT MESSAGE FROM MASTER :: "+rcvdstr+" %%%%");
				break;
				
				case PackageFormats.MATER_NODE_DATA_NODE_FILE_REPLICATION_REQUEST:
					String fileName = dtndcin.readUTF();
					String deadnodeHexID = dtndcin.readUTF();
					DeleteDeadNodeInfofromNodeTable(deadnodeHexID);
//					int filebufferSize = dtndcin.readInt();
					String to_contact_dataNode_IP = dtndcin.readUTF();
					int to_contact_dataNode_Port = dtndcin.readInt();
					StartFileForwarding(fileName,to_contact_dataNode_IP,to_contact_dataNode_Port,deadnodeHexID);
				break;
				
				case PackageFormats.QUERY_NODE_DATA_NODE_QUERY_REQUEST:
					StartExecutingandForwardingQuery();
				break;
					
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
