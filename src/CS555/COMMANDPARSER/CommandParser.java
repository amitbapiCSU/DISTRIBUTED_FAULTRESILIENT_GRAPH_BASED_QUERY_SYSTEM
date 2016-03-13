/**
 * 
 */
package CS555.COMMANDPARSER;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Scanner;

import CS555.DATANODE.DataNodeInfoMain;
import CS555.MASTERNODE_INFO.MasterInfoNodeMain;
import CS555.PACKAGE_FORMATS.PackageFormats;
import CS555.QUERYNODE.QueryNodeMain;
import CS555.UPLOADERNODE.UploaderNodeMain;

/**
 * @author amchakra
 *
 */
public class CommandParser implements Runnable{

	private Object classObj;
	
	public CommandParser(Object obj) {
		this.classObj = obj;
	}
	
	private void StartUploadingFiles(UploaderNodeMain upldrndObj, String filename) {
		
		String uploadernodeIp = upldrndObj.getUploaderNodeIp();
		int uploadernodePort = upldrndObj.getUploaderNodePort();
		System.out.println(uploadernodeIp+" ----- "+uploadernodePort);
		try {
			
			Socket upldrnodeConn = new Socket(uploadernodeIp,uploadernodePort, null,0);
			
			OutputStream upldrscktout = upldrnodeConn.getOutputStream();
			InputStream upldrscktin = upldrnodeConn.getInputStream();
			DataOutputStream sddout = new DataOutputStream(upldrscktout);
			DataInputStream sddin = new DataInputStream(upldrscktin);
			
			sddout.writeInt(PackageFormats.UPLOADER_NODE_FILE_STAGE_INFO);
			sddout.writeUTF(filename);
			
			upldrnodeConn.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	private void ShowNodeInfoTable(DataNodeInfoMain dtndobj) {
		String IP = dtndobj.getDataNodeIPInfo();
		int port = dtndobj.getDataNodePortNo();
		
		try {
			
			Socket datanodeConn = new Socket(IP,port, null,0);
			
			OutputStream datandscktout = datanodeConn.getOutputStream();
			InputStream datandscktin = datanodeConn.getInputStream();
			DataOutputStream sddout = new DataOutputStream(datandscktout);
			DataInputStream sddin = new DataInputStream(datandscktin);
			
			sddout.writeInt(PackageFormats.SHOW_NODE_INFO_TABLE);
			
			datanodeConn.close();
			
		} catch (Exception e){
			System.out.println("UNABLE TO CONTACt DATA-NODE To SHOW INFORMATION");
			e.printStackTrace();
		}
	}
	
	private void ShowInformationFromMasterNode(String oprtntype) {
		try {
			
			Socket masterchannel = new Socket(PackageFormats.MASTER_NODE_IP_ADDRESS, PackageFormats.MASTER_NODE_IP_PORT, null, 0);
			
			OutputStream mstrout = masterchannel.getOutputStream();
			InputStream mstrin = masterchannel.getInputStream();
			
			DataInputStream mstrccin = new DataInputStream(mstrin);
			DataOutputStream mstrccout = new DataOutputStream(mstrout);
			
			int rqstType = 0;
			
			if (oprtntype.contains("FILE")) {
				rqstType = PackageFormats.MASTER_NODE_FILE_INFO_SHOW;
			} else if (oprtntype.contains("LIVE")) {
				rqstType = PackageFormats.MASTER_NODE_LIVE_NODE_SHOW;
			} else if (oprtntype.contains("THROUGHPUT")) {
				rqstType = PackageFormats.SYSTEM_QUERY_THROUGHPUT_SHOW;
			}
			
			if (rqstType != 0) {
				
				mstrccout.writeInt(rqstType);
				
			} else {
				
				System.out.println("PLEASE ENTER PROPER QUERY TYPE :: ");
				System.out.println("HINT :: SHOW-FILE or SHOW-LIVE-NODES or QUERY-THROUGHPUT");
				
			}
			
			
			masterchannel.close();
			
		} catch (Exception e) {
			System.out.println("UNABLE TO CONTACT MASTER NODE FROm COMMAND PASSER");
			e.printStackTrace();
		}
	}
	
	private void StartExecutingQuery(QueryNodeMain quryndObj, String query) {
		
		try {
			String queryNodeIP = quryndObj.getQueryNodeIP();
			int queryNodePort = quryndObj.getQueryNodePort();
			
			Socket querychannel = new Socket(queryNodeIP, queryNodePort, null, 0);
			
			OutputStream qryout = querychannel.getOutputStream();
			InputStream qryin = querychannel.getInputStream();
			
			DataInputStream qryccin = new DataInputStream(qryin);
			DataOutputStream qryccout = new DataOutputStream(qryout);
			
			qryccout.writeInt(PackageFormats.QUERY_NODE_QUERY_INFO);
			qryccout.writeUTF(query);
			
			querychannel.close();
			
		} catch (Exception e) {
			System.out.println("ERROR IN CONTACTING QUERY NODE");
			e.printStackTrace();
		}
	}
	
	private void StartReadingFromFileListandUpload(String fileName) {
		
		try {
			
			File fin = new File(fileName);
			FileInputStream fis = new FileInputStream(fin);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			
			while ((line = br.readLine()) != null) {
				if(!line.isEmpty()) {
				StartUploadingFiles((UploaderNodeMain)classObj, line);
				}
			}
			
			br.close();
			
		} catch (Exception e) {
			System.out.println("ERROR IN COMMAND PARSER :: READINg FILE LIST and UPLOADING");
			e.printStackTrace();
		}
		
	}
	
	private void StartReadingFromFileandQuery(String QueryFileName) {
		
		try {
			
			File fin = new File(QueryFileName);
			FileInputStream fis = new FileInputStream(fin);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			
			while ((line = br.readLine()) != null) {
				if(!line.isEmpty()) {
					StartExecutingQuery((QueryNodeMain)classObj, line);
				}
			}
			
			br.close();
			
		} catch (Exception e) {
			System.out.println("ERROR IN COMMAND PARSER :: READING FILE LIST and QUERING");
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (true) {
			
			try {
				
				Scanner sc = new Scanner(System.in);
				
				if (classObj instanceof UploaderNodeMain) {
					
					// NEED TO UPLOAD FILES
					
					System.out.println("PLEASE ENTER :: 0 for BULK-UPLOAD and 1 for SINGLE-FILE UPLOAD");
					int flag = sc.nextInt();
					System.out.println("FLAG :: " + flag);
					
					if(flag == 0) {
					
						System.out.println("BULK :: PLEASE ENTER PROPER FILE NAME (ENTER FILE NAME :: WITH PROPER PATHNAME)");
						String fileNm = sc.next();
						
						// TO UPLOAD SET OF FILES
						StartReadingFromFileListandUpload(fileNm);
					
					} else {
						
						// TO UPLOAD A SINGLE FILE
						System.out.println("PLEASE ENTER PROPER FILE NAME (ENTER FILE NAME :: WITH PROPER PATHNAME)");
						String fileNm = sc.next();
						
						StartUploadingFiles((UploaderNodeMain)classObj, fileNm);
						
					}
					
				} else if (classObj instanceof DataNodeInfoMain) {
					
					String opertntype = sc.next();
					if(opertntype.equals("SHOW-TABLE")) {
						ShowNodeInfoTable((DataNodeInfoMain)classObj);
					}
					
				} else if (classObj instanceof MasterInfoNodeMain) {
					
					String operationtype = sc.next();
					ShowInformationFromMasterNode(operationtype);
					
				} else if (classObj instanceof QueryNodeMain) {
					
					System.out.println("PLEASE ENTER :: 0 for BULK-QUERY and 1 for SINGLE-QUERY");
					int flag = sc.nextInt();
					
					if(flag == 0) {
						
						// FOR QUERY a SET of QUERIES
						String fileNm = sc.next();
						StartReadingFromFileandQuery(fileNm);
						
					} else {
						
						// ONLY FOR SINGLE QUERY
						while(sc.hasNext()) {
							String Query = sc.nextLine();
							System.out.println("CMD PRSR :: "+Query);
							if(!Query.isEmpty()) {
								StartExecutingQuery((QueryNodeMain)classObj, Query);
								break;
							}
						}
						
					}
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
	}

}
