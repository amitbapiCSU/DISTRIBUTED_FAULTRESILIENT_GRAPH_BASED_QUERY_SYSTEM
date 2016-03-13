/**
 * 
 */
package CS555.UPLOADERNODE;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

import CS555.PACKAGES.DataNodeInfoPacket;
import CS555.PACKAGES.ForwardDataNodeInfoPacket;
import CS555.PACKAGES.UploaderDataNodeFileInfo;
import CS555.PACKAGE_FORMATS.PackageFormats;

/**
 * @author amchakra
 *
 */
public class UploaderNodeThreadedConnection implements Runnable{

	private Socket UploaderNodeScoket = null;
	private String UploaderNodeIPAddr = "";
	private int UploaderNodePort = 0;
	
	private OutputStream upldrout = null;
	private InputStream upldrin = null;
	
	private DataInputStream upldrccin = null;
	private DataOutputStream upldrccout = null;
	
	UploaderNodeThreadedConnection(Socket UploaderNodeScoket, String UploaderNodeIPAddr, int UploaderNodePort) {
		this.UploaderNodeScoket = UploaderNodeScoket;
		this.UploaderNodeIPAddr = UploaderNodeIPAddr;
		this.UploaderNodePort = UploaderNodePort;
	}
	
	private void StartUploadingFile(String fl_nm) {
		
		// START RECEIVING 2 DATA-NODE's INFO FROM MASTER-INFO NODE
		System.out.println("FL NAME : "+fl_nm);
		try {
			
			// GET FILE BUFFER
			File inf = new File(fl_nm);
			long fileSize_KB = inf.length();
			System.out.println("To UPLOAD FILE SIZE : "+fileSize_KB+" FILE NAME : "+fl_nm);
			byte[] flbytebfr = new byte[(int) fileSize_KB];
			FileInputStream fin = new FileInputStream(inf);
			BufferedInputStream bin = new BufferedInputStream(fin);
			bin.read(flbytebfr);
			fin.close();
			
			// CONTACT MASTER NODE TO GET 3 DATA-NODE's INFORMATION
			Socket uploaderMasterNodeSckt = new Socket(PackageFormats.MASTER_NODE_IP_ADDRESS, PackageFormats.MASTER_NODE_IP_PORT, null, 0);
			
			OutputStream upldrmstrout = uploaderMasterNodeSckt.getOutputStream();
			InputStream upldrmstrin = uploaderMasterNodeSckt.getInputStream();
			
			DataInputStream upldrmstrccin = new DataInputStream(upldrmstrin);
			DataOutputStream upldrmstrccout = new DataOutputStream(upldrmstrout);
			
			System.out.println("1111111");
			
			upldrmstrccout.writeInt(PackageFormats.UPLOADER_NODE_MASTER_NODE_FILE_STAGE_REQUEST);
			upldrmstrccout.writeUTF(inf.getName());
			
			int pcktflg = upldrmstrccin.readInt();
			
			System.out.println("RECEIVED PACKET FLAG In UPLOADER: "+pcktflg);
			
			if(pcktflg == PackageFormats.MASTER_NODE_UPLOADER_NODE_FILE_STAGE_RESPONSE) {
				
				ObjectInputStream upooin = new ObjectInputStream(upldrmstrin);
				ForwardDataNodeInfoPacket rcvdpckt = (ForwardDataNodeInfoPacket)upooin.readObject();
//				System.out.println("22222222222222 :: "+(rcvdpckt.getDataNodesInfo()).size());
				if(rcvdpckt != null) {
					List<DataNodeInfoPacket> dataNodeInfos = rcvdpckt.getDataNodesInfo();
					
					for(DataNodeInfoPacket dtndpckt :  dataNodeInfos) {
						System.out.println("*****************************************************************************************************");
						System.out.println("DATA NODE NAME : "+dtndpckt.getDataNodeHostName());
						System.out.println("DATA NODE IP : "+dtndpckt.getDataNodeAddress());
						System.out.println("DATA NODE PORT : "+dtndpckt.getNodePort());
						System.out.println("DATA NODE HEX-ID : "+dtndpckt.getHexID());
						System.out.println("*****************************************************************************************************");
	
					}
					
					String to_connect_dataNode_IP = (dataNodeInfos.get(0)).getDataNodeAddress();
					int to_connect_dataNode_Port = (dataNodeInfos.get(0)).getNodePort();
					dataNodeInfos.remove(0);
					// START CONTACTINg DATA-NODEs TO UPLOAD FILES 
					System.out.println(to_connect_dataNode_IP+" :: "+to_connect_dataNode_Port+" :: ");
					try {
						
						Socket toConnectDataNode = new Socket(to_connect_dataNode_IP,to_connect_dataNode_Port,null,0);
						
						OutputStream upldrdtndout = toConnectDataNode.getOutputStream();
						InputStream upldrdtndin = toConnectDataNode.getInputStream();
						
						DataInputStream upldrdtndccin = new DataInputStream(upldrdtndin);
						DataOutputStream upldrdtndccout = new DataOutputStream(upldrdtndout);
						
						upldrdtndccout.writeInt(PackageFormats.UPLOADER_NODE_DATA_NODE_FILE_STAGE_REQUEST);
						upldrdtndccout.writeUTF(inf.getName());
						upldrdtndccout.writeInt(flbytebfr.length);
						upldrdtndccout.write(flbytebfr);
						ObjectOutputStream upldrdtndoout = new ObjectOutputStream(upldrdtndout);
						UploaderDataNodeFileInfo flupldpckt = new UploaderDataNodeFileInfo(inf.getName(),flbytebfr.length,dataNodeInfos);
						upldrdtndoout.writeObject(flupldpckt);
						
						toConnectDataNode.close();
						
					} catch(Exception e) {
						System.out.println("UNABLE TO CONTACT DATA-NODE");
						e.printStackTrace();
					}
					
				} else {
					System.out.println("%%%%%%%%%%%%% NOT RECEIVED ANY DATA-NODE INFORMATION FROM MASTEr-NODE");
				}
			}
			uploaderMasterNodeSckt.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		try {
			upldrout = UploaderNodeScoket.getOutputStream();
			upldrin = UploaderNodeScoket.getInputStream();
			
			upldrccin = new DataInputStream(upldrin);
			upldrccout = new DataOutputStream(upldrout);
			
			int type_f_flag = upldrccin.readInt();
			
			System.out.println("TYPE oF REQUEST :: UPLOADER :: "+type_f_flag);
			
			switch (type_f_flag) {
			
				case PackageFormats.UPLOADER_NODE_FILE_STAGE_INFO:
					String fileName = upldrccin.readUTF();
					StartUploadingFile(fileName);
					
			}
			
		} catch (Exception e){
			System.out.println("ERROR IN UPLODER NODE THREADED CONNNECTION");
			e.printStackTrace();
		}
	}

}
