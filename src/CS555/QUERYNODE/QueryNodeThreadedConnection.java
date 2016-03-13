/**
 * 
 */
package CS555.QUERYNODE;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import CS555.PACKAGES.DataNodeInfoPacket;
import CS555.PACKAGES.MasterNodeQueryNodeFileNodeInfoPacket;
import CS555.PACKAGES.QueryNodeQueryPacket;
import CS555.PACKAGE_FORMATS.PackageFormats;

/**
 * @author amchakra
 *
 */
public class QueryNodeThreadedConnection implements Runnable{

	private Socket querydataChannel = null;
	private String queryNodeIP = null;
	private int queryNodePort = 0;
	private String queryNodehexID = "";
	
	private OutputStream qryndout = null;
	private InputStream qryndin = null;
	
	private DataInputStream qryndcin = null;
	private DataOutputStream qryndcout = null;
	// private ObjectOutputStream oos = null;
	private Object rcvdmessage = null;
	private long CurrentTimeStamp = new Date().getTime();
	
	public QueryNodeThreadedConnection(Socket querydataChannel, String queryNodeIP, int queryNodePort, String queryNodehexID) {
		this.querydataChannel = querydataChannel;
		this.queryNodeIP = queryNodeIP;
		this.queryNodePort = queryNodePort;
		this.queryNodehexID = queryNodehexID;
	}
	
	private boolean isValidQuery(String Query) {
		
		boolean isValid = true;
		String query = (Query.trim()).toUpperCase();
		
		if(!query.contains("SELECT") || !query.contains("OF")) {
			
			isValid = false;
			
		} else {
			
			int frst = query.indexOf("SELECT");
			int scnd = query.indexOf("OF");
			
			System.out.println(frst+" ** "+scnd);
			
			String queryfuncStr = query.substring(frst,scnd);
			String qyueryTimeInfoStr = query.substring(scnd);
			
			System.out.println(queryfuncStr);
			System.out.println(qyueryTimeInfoStr);
			
			if(queryfuncStr.contains("MAX(") || queryfuncStr.contains("MIN(") || queryfuncStr.contains("AVG(")) {
				
				isValid = true;
				
			} else {
				
				isValid = false;
				
			}
			
			if(qyueryTimeInfoStr.contains("YEAR") || qyueryTimeInfoStr.contains("MONTH") || qyueryTimeInfoStr.contains("DATE")) {
				
				isValid = true;
				
			} else {
				
				isValid = false;
				
			}
			
		}
		
		return isValid;
		
	}
	
	private void StartContactingMasterNode(String Query) {
		try {
			Map<String,CopyOnWriteArrayList<String>> QueryVariableFuncMap = new ConcurrentHashMap<String,CopyOnWriteArrayList<String>>();
			
			String query = (Query.trim()).toUpperCase();
			System.out.println("QUERY :: "+query);
			
			if(query.contains("SELECT") && query.contains("OF")) {
				String[] queryarr = query.split(" ");
				
//				System.out.println("QUERY ARR LEN :: "+queryarr.length);
				
				int frst = query.indexOf("SELECT");
				int scnd = query.indexOf("OF");
				
//				System.out.println(frst+" ** "+scnd);
				
				String queryfuncStr = query.substring(frst,scnd);
				
//				System.out.println(queryfuncStr);
				
				String[] queryfuncarr = queryfuncStr.split(" ");
				for (String str : queryfuncarr) {
					if((!str.equals("SELECT"))) {
						if(!(str.equals("AND"))) {
							
							int openbrcIndx = str.indexOf("(");
							int closebrcIndx = str.indexOf(")");
							String func = str.substring(0,openbrcIndx);
							String variable = str.substring(openbrcIndx+1,closebrcIndx);
							
//							System.out.println("STR :: "+str+" :: FUNC :: "+func+" ::  VARIABLE :: "+variable);
							
							if(QueryVariableFuncMap.containsKey(variable)) {
								
								CopyOnWriteArrayList<String> funcs = QueryVariableFuncMap.get(variable);
								funcs.add(func);
								if((func.equals(PackageFormats.QUERY_TYPE_MAX)) || (func.equals(PackageFormats.QUERY_TYPE_MIN)) || (func.equals(PackageFormats.QUERY_TYPE_AVG))) {
									QueryVariableFuncMap.put(variable, funcs);
								}
								
							} else {
								
								CopyOnWriteArrayList<String> funcs = new CopyOnWriteArrayList<String>();
								funcs.add(func);
								if((func.equals(PackageFormats.QUERY_TYPE_MAX)) || (func.equals(PackageFormats.QUERY_TYPE_MIN)) || (func.equals(PackageFormats.QUERY_TYPE_AVG))) {
									QueryVariableFuncMap.put(variable,funcs);
								}
								
							}
						}
					}
				}
				
				if(QueryVariableFuncMap.size() > 0) {
					
					String timeInfo = "";
					boolean yearPresent = false;
					boolean monthPresent = false;
					boolean datePresent = false;
					
					for(String queryelem : queryarr) {
						if(queryelem.contains("YEAR")) {
							yearPresent = true;
							timeInfo +=  ((queryelem.split("="))[1]).trim();
						} else  if (queryelem.contains("MONTH")) {
							monthPresent = true;
							timeInfo +=  ((queryelem.split("="))[1]).trim();
						} else if (queryelem.contains("DATE")) {
							datePresent = true;
							timeInfo +=  ((queryelem.split("="))[1]).trim();
						}
					}
					
					System.out.println(" YR :: "+yearPresent+" :: MM :: "+monthPresent+" :: DD :: "+datePresent+" :: "+timeInfo);
					for(String var : QueryVariableFuncMap.keySet()) {
						for(String func : QueryVariableFuncMap.get(var)) {
							System.out.println("QUERY :: VAR :: "+var+" :: FUNC :: "+func);
						}
					}
					
					try {
						MasterNodeQueryNodeFileNodeInfoPacket rcvdpckt = null;
						
						// CONTACT MASTER NODE TO GET a LIST of AVAILABLE FILE's WITH the QUERY's TIME-INFO and a LIST of DATA-NODE's INFORMATION
						Socket queryMasterNodeSckt = new Socket(PackageFormats.MASTER_NODE_IP_ADDRESS, PackageFormats.MASTER_NODE_IP_PORT, null, 0);
						
						OutputStream qrymstrout = queryMasterNodeSckt.getOutputStream();
						InputStream qrymstrin = queryMasterNodeSckt.getInputStream();
						
						DataInputStream qrymstrccin = new DataInputStream(qrymstrin);
						DataOutputStream qrymstrccout = new DataOutputStream(qrymstrout);
						
						qrymstrccout.writeInt(PackageFormats.QUERY_NODE_MASTER_NODE_QUERY_REQUEST);
						qrymstrccout.writeUTF(timeInfo);
						qrymstrccout.writeBoolean(yearPresent);
						qrymstrccout.writeBoolean(monthPresent);
						qrymstrccout.writeBoolean(datePresent);
						qrymstrccout.writeUTF(query);
						qrymstrccout.writeUTF(queryNodehexID);
						qrymstrccout.writeUTF(queryNodeIP);
						qrymstrccout.writeInt(queryNodePort);
						
						
						int typeofOprtn = qrymstrccin.readInt();
						Map<String,Set<String>> yrWiseavailableFileList = null;
						Map<String,DataNodeInfoPacket> yrWiseDataNodeInfoList = null;
						
						if (typeofOprtn == PackageFormats.MASTER_NODE_QUERY_NODE_QUERY_RESPONSE) {
							ObjectInputStream oin = new ObjectInputStream(qrymstrin);
							rcvdpckt = (MasterNodeQueryNodeFileNodeInfoPacket) oin.readObject();
							
							yrWiseavailableFileList = rcvdpckt.getYearWiseAvailableFileList();
							yrWiseDataNodeInfoList = rcvdpckt.getYearWiseAvailableDataNodeList();
							
							if(yrWiseavailableFileList.keySet().size() != yrWiseDataNodeInfoList.keySet().size()) {
								System.err.println(" ERROR :: INCONSISTENT OUTPUT :::::::::: KEYSET SHOULD BE SAME ::::::::::::::::::::::::::::::::");
							}
							
							System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ RECEIVED QUERY INFORMATION FROM MASTER @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		//					for (String fileName : rcvdpckt.getAvailableFileList()) {
		//						System.out.println("****** FILE NAME :: "+fileName+" ******");
		//					}
							for(String yrInfo : yrWiseavailableFileList.keySet()) {
								for(String fileName : yrWiseavailableFileList.get(yrInfo)) {
									System.out.println("FOR YEAR :: "+yrInfo+" ****** FILE NAME :: "+fileName+" ******");
								}
							}
							System.out.println("-------------------------------------------------------------------------------------------------------");
		//					for (DataNodeInfoPacket dtnd : rcvdpckt.getDataNodeList()) {
		//						System.out.println("****** DATA NODE :: "+dtnd.getDataNodeHostName()+" :: "+dtnd.getHexID()+" ******");
		//					}
							for(String yrInfo : yrWiseDataNodeInfoList.keySet()) {
								DataNodeInfoPacket dtnd = yrWiseDataNodeInfoList.get(yrInfo);
								System.out.println("FOR YEAR :: "+yrInfo+" ****** DATA NODE :: "+dtnd.getDataNodeHostName()+" :: "+dtnd.getHexID()+" ******");
							}
							System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
							
						}
						
						queryMasterNodeSckt.close();
						
						// START CONTACT ANY of THE RECEIVED DATA-NODE LIST :: NOW THE FIST ONE in THE DATA-NODE LIST
		//				if (((rcvdpckt.getDataNodeList()).size() > 0) && ((rcvdpckt.getAvailableFileList()).size() > 0)) {
						if (((yrWiseavailableFileList).size() > 0) && ((yrWiseDataNodeInfoList).size() > 0)) {
		//					for(DataNodeInfoPacket toContactDataNode : rcvdpckt.getDataNodeList()) {
							for(String yrInfo : yrWiseDataNodeInfoList.keySet()) {
								DataNodeInfoPacket toContactDataNode = yrWiseDataNodeInfoList.get(yrInfo);
								System.out.println("FOR YEAR :: "+yrInfo+" ****** TO CONTACT DATA NODE :: "+toContactDataNode.getDataNodeHostName()+" :: "+toContactDataNode.getHexID()+" ******");
								try {
									
									Socket datanodesckt = new Socket(toContactDataNode.getDataNodeAddress(),toContactDataNode.getNodePort(),null,0);
									
									OutputStream qrydtndout = datanodesckt.getOutputStream();
									InputStream qrydtndin = datanodesckt.getInputStream();
									
									DataInputStream qrydtndccin = new DataInputStream(qrydtndin);
									DataOutputStream qrydtndccout = new DataOutputStream(qrydtndout);
									
		//							QueryNodeQueryPacket qrypckt = new QueryNodeQueryPacket(rcvdpckt.getAvailableFileList(),QueryVariableFuncMap,rcvdpckt.getDataNodeList()); 
									QueryNodeQueryPacket qrypckt = new QueryNodeQueryPacket(yrWiseavailableFileList.get(yrInfo),QueryVariableFuncMap);
									
									qrydtndccout.writeInt(PackageFormats.QUERY_NODE_DATA_NODE_QUERY_REQUEST);
									qrydtndccout.writeUTF(queryNodeIP);
									qrydtndccout.writeInt(queryNodePort);
									qrydtndccout.writeUTF(" -- "); // queryResultTrace
									qrydtndccout.writeUTF(""); // QueryResult
									qrydtndccout.writeUTF(""); // processedNodeHexIDs
									qrydtndccout.writeUTF(""); // processedFileList
		//							qrydtndccout.writeLong(0);
									qrydtndccout.writeLong(CurrentTimeStamp);
									
									ObjectOutputStream oout = new ObjectOutputStream(qrydtndout);
									oout.writeObject(qrypckt);
									
									datanodesckt.close();
		//							break;
								} catch (Exception e) {
									System.out.println("UNABLE To CONTACT DATA-NODE :: TRY THE QUERY :: " + query + " AFTER SOME TIME");
									continue;
								}
							}
						} else {
							System.out.println("NO FILE EXISTS IN THE SYSTEM WITH THE QUERY's YEAR,MONTH and DATE INFORMATION");
						}
					} catch (Exception e){
						System.out.println("ERROR TO CONTACT MASTER-NODE");
						e.printStackTrace();
					}
				} else {
					System.out.println("NOT A VALID QUERY :: QUERY FORMAT :: SELECT MAX/MIN/AVG(VARIABLE NAME) of YEAR=YYYY/MONTH=MM/DATE=DD");
				}
			}
		} catch (Exception e) {
			System.out.println("NOT A VALID QUERY :: QUERY FORMAT :: SELECT MAX/MIN/AVG(VARIABLE NAME) of YEAR=YYYY/MONTH=MM/DATE=DD");
//			System.out.println("or ERROR IN CONTACTING MASTER NODE FROM QUERY NODE");
			e.printStackTrace();
		}
	}
	
	private void ShowProperQueryResponse(String qryRes) {
		String[] qeryResArr = qryRes.split("#");
		
		for(String qryrs : qeryResArr) {
//			System.out.println(qryrs);
			String[] qryResarr = qryrs.split("_");
			
			String funcNM = "";
			String outputVal = "";
			String yrandVariableName = "";
			String yrInfo = "";
			String variableNM = "";
			
			for(int i = 0; i < qryResarr.length; i++) {
//				System.out.println(" :: "+qryResarr[i]);
				if(i == 0) { // FIRSt FUNCTION
					funcNM = qryResarr[i];
				} else if (i == 1) { // SECOND OUTPUT VALUE
					outputVal = qryResarr[i];
				} else {
					if(i == (qryResarr.length-1)) {
						yrandVariableName += qryResarr[i];
					} else {
						yrandVariableName += qryResarr[i]+"_";
					}
				}
			}
			
//			System.out.println(yrandVariableName);
			
			String[] yrandVariableNamearr = yrandVariableName.split("-");
			yrInfo = yrandVariableNamearr[0];
			variableNM = yrandVariableNamearr[1];
			
			System.out.println("FOR YEAR :: "+yrInfo+ " : VARIABLE : "+variableNM+" : FUNC : "+funcNM+" : VALUE : "+outputVal);
		}
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true) {
			try {
				qryndout = querydataChannel.getOutputStream();
				qryndin = querydataChannel.getInputStream();
				
				qryndcout = new DataOutputStream(qryndout);
				qryndcin = new DataInputStream(qryndin);
				
				int type_f_flg = qryndcin.readInt();
				
				switch(type_f_flg) {
					
					case PackageFormats.QUERY_NODE_QUERY_INFO :
						String query = qryndcin.readUTF();
						CurrentTimeStamp = new Date().getTime();
						System.out.println("QUERY TO DO :: "+query);
						
						if(isValidQuery(query)) {
							
							StartContactingMasterNode(query);
							
						} else {
							
							System.out.println("NOT A VALID QUERY :: QUERY FORMAT :: SELECT MAX/MIN/AVG(VARIABLE NAME) of YEAR=YYYY/MONTH=MM/DATE=DD");
						
						}
						
					break;
					
					case PackageFormats.DATA_NODE_QUERY_NODE_QUERY_RESPONSE:
						String queryTracePath = qryndcin.readUTF();
						String QueryResult = qryndcin.readUTF();
						String ProcessedFiles = qryndcin.readUTF();
						String progressInfo = qryndcin.readUTF();
						long QueryInitTime = qryndcin.readLong();
						
						long ReceivedQueryResultTimeStamp = new Date().getTime();
						long QueryResponseTime = (ReceivedQueryResultTimeStamp - QueryInitTime);
						
						System.out.println("QUERY INIT TIMESTAMP :: "+QueryInitTime+ " :: RESPONSE TIMESTAMP :: "+ReceivedQueryResultTimeStamp+" :: ELAPSED TIME :: "+QueryResponseTime);
						System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
						System.out.println(progressInfo);
						System.out.println("QUERY TRACE PATH :: "+queryTracePath);
						System.out.println("FILES PROCESSED :: "+ProcessedFiles);
						
						System.out.println("QUERY RESULT :: "+QueryResult);
						System.out.println("PROPER RESULT :: ");
						
						ShowProperQueryResponse(QueryResult);
						
						System.out.println("QUERY RESPONSE TIME :: "+(QueryResponseTime/1000));
						System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");

					break;
					
					case PackageFormats.MASTER_NODE_QUERY_NODE_QUERY_NOTIFICATION:
						String queryEffected = qryndcin.readUTF();
						System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
						System.out.println("RECEIVED NOTIFICATION :: :: ");
						System.out.println("THE FOLLOWING QUERY CAN BE UPDATED IF IT's EXECUTED NOW :: ");
						System.out.println(queryEffected);
						System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
					break;
					
				}
				
			} catch (Exception e) {
				
			}
		}
		
	}

}
