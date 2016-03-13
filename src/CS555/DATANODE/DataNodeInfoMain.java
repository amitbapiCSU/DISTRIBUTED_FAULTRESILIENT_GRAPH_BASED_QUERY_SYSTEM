/**
 * 
 */
package CS555.DATANODE;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import CS555.COMMANDPARSER.CommandParser;
import CS555.PACKAGES.DataNodeInfoPacket;
import CS555.PACKAGE_FORMATS.PackageFormats;

/**
 * @author amchakra
 *
 */
public class DataNodeInfoMain {

	/**
	 * @param args
	 */
	
	private String DataNodeHostName = "";
	private String DataNodeServerAddr = "";
	private int DataNodePortNo = 0;
	private String DataNodeHexID = "";
	
	private Map<String, String> FileListMap = new ConcurrentHashMap<String, String>();
	private Map<String,CopyOnWriteArrayList<DataNodeInfoPacket>> CommonYearNodeInfo = new ConcurrentHashMap<String, CopyOnWriteArrayList<DataNodeInfoPacket>>();
	private Map<String,CopyOnWriteArrayList<DataNodeInfoPacket>> CommonMonthNodeInfo = new ConcurrentHashMap<String, CopyOnWriteArrayList<DataNodeInfoPacket>>();
	private Map<String,CopyOnWriteArrayList<DataNodeInfoPacket>> CommonDateNodeInfo = new ConcurrentHashMap<String, CopyOnWriteArrayList<DataNodeInfoPacket>>();
	
	private String getSum(int ip) {
		int sum = 0;
		while (ip%10 != 0) {
			sum += ip%10;
			ip = ip/10;
		}
		if(sum > 15) {
			sum =  sum%15;
		}
		return Integer.toHexString(sum);
	}
	
	private String getIdentifierbyTimeStamp() {
		Date date = new Date();
		Long luid = (Long) date.getTime();
		
		//System.out.println(luid);
		int lstdigit = (int) (luid%10);
		//System.out.println(lstdigit);
		int last4digit = (int) ((luid/10)%10000);
		//System.out.println(last4digit);
		int scndlst4digit = (int) (((luid/10)/10000))%10000;
		//System.out.println(scndlst4digit);
		int first4digit = (int) ((((luid/10)/10000))/10000)%10000;
		//System.out.println(first4digit);
		//System.out.println(getSum(first4digit));
		
		List<String> HexIDXs = new ArrayList<String>();
		HexIDXs.add(getSum(lstdigit));
		HexIDXs.add(getSum(last4digit));
		HexIDXs.add(getSum(scndlst4digit));
		HexIDXs.add(getSum(first4digit));
		Collections.shuffle(HexIDXs);
		String HexIdentifier = "";
		for(String s : HexIDXs) {
			HexIdentifier = HexIdentifier+s;
		}
		
		return HexIdentifier;
	}
	
	public String getDataNodeIPInfo() {
		return this.DataNodeServerAddr;
	}
	
	public int getDataNodePortNo() {
		return this.DataNodePortNo;
	}
	
	private void start(String[] args) {
		
		//START THE DATA-NODE
		ServerSocket DataNodeSrvrSocket = null;
		Socket DataNodeSrvrchannel = null;
		
		Socket DataNodeMaterChannel = null;
		
		int DataNodePortNumber = (args.length >= 1) ? Integer.parseInt(args[0]) : 0;
		
		try {
			
			//START THE SERVER
			DataNodeSrvrSocket = new ServerSocket(DataNodePortNumber);
			
			// GET NECESSARY INFORMATIOn REGARDING NEWLY STARTED DATA-NODE
			DataNodeHostName = DataNodeSrvrSocket.getInetAddress().getLocalHost().getHostName();
			DataNodeServerAddr = DataNodeSrvrSocket.getInetAddress().getLocalHost().getHostAddress();
			DataNodePortNo = DataNodeSrvrSocket.getLocalPort();
			//GET HEX-ID of THE DATA-NODE
			DataNodeHexID = getIdentifierbyTimeStamp();
			System.out.println("HEX ID : "+DataNodeHexID);
			System.out.println("DATA-NODE HOST NAME : "+DataNodeHostName);
			System.out.println("DATA-NODE ADDRESS : "+DataNodeServerAddr);
			System.out.println("DATA-NODE PORT NO : "+DataNodePortNo);
			
			//CREATE DATA-NODE JOINING PACKET 
			DataNodeInfoPacket nodePckt = new DataNodeInfoPacket(DataNodeHexID, DataNodeServerAddr, DataNodeHostName, DataNodePortNo);
			
			// START COMMUNICATING WITH MATER-NODE
			try {
				DataNodeMaterChannel = new Socket(PackageFormats.MASTER_NODE_IP_ADDRESS, PackageFormats.MASTER_NODE_IP_PORT, null, 0);
				
				OutputStream DMout = DataNodeMaterChannel.getOutputStream();
				InputStream DMin = DataNodeMaterChannel.getInputStream();
				
				DataOutputStream dout = new DataOutputStream(DMout);
				DataInputStream din = new DataInputStream(DMin);
				
				dout.writeInt(PackageFormats.DATA_NODE_JOIN_REQUEST);
				ObjectOutputStream oout = new ObjectOutputStream(DMout);
				oout.writeObject(nodePckt);
				
				int joinedorNotRespeonse = din.readInt();
				
				System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
				System.out.println("DATA NODE : "+DataNodeHostName+" : WITH HEX ID : "+DataNodeHexID+" JOINED PROPERLY : "+joinedorNotRespeonse);
				System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
				
				DataNodeMaterChannel.close();
				
			} catch (Exception e) {
				System.out.println("UNABLE TO CONTACT MATSER-NODE");
				e.printStackTrace();
			}
			
			System.out.println("IIN");
			
			// START INERACTIVE-COMMAND PASSER
			new Thread(new CommandParser(this)).start();
			
			// START THE DATA NODE MAIN THREAD 
			while (true) {
				System.out.println("@@@@ LISTENING In DATA-NODE :::: "+DataNodeHostName+" :: "+DataNodeHexID+" @@@@");
				
				DataNodeSrvrchannel = DataNodeSrvrSocket.accept();
				
				// STARTING THREAD
				new Thread(new DataNodeThreadedConnection(DataNodeHexID,DataNodeHostName,DataNodeSrvrchannel,DataNodeServerAddr,DataNodePortNo,FileListMap,CommonYearNodeInfo,CommonMonthNodeInfo,CommonDateNodeInfo)).start();
			}
			
			
		} catch (Exception e) {
			System.out.println("ERROR STARTING DATA NODE SERVER");
			e.printStackTrace();
		}
		
		
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new DataNodeInfoMain().start(args);
	}

}
