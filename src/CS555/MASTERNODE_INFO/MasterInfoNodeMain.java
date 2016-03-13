/**
 * 
 */
package CS555.MASTERNODE_INFO;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import CS555.COMMANDPARSER.CommandParser;
import CS555.PACKAGES.DataNodeInfoPacket;
import CS555.PACKAGES.QueryNodeInfoPacket;

/**
 * @author amchakra
 *
 */
public class MasterInfoNodeMain {

	/**
	 * @param args
	 */
	
	private Map<String, DataNodeInfoPacket> NodeIdInfoMap = new ConcurrentHashMap<String, DataNodeInfoPacket>();
	private CopyOnWriteArrayList<DataNodeInfoPacket> LiveDataNodeInfo = new CopyOnWriteArrayList<DataNodeInfoPacket>();
	private Map<String, Long> FileNameStoreTimeMap = new ConcurrentHashMap<String, Long>();
	private Map<String, CopyOnWriteArrayList<DataNodeInfoPacket>> FileDataNodeMap = new ConcurrentHashMap<String, CopyOnWriteArrayList<DataNodeInfoPacket>>();
	private Map<String, CopyOnWriteArrayList<String>> DataNodeFileInfoMap = new ConcurrentHashMap<String, CopyOnWriteArrayList<String>>();
	private Map<String, CopyOnWriteArrayList<QueryNodeInfoPacket>> QueryNodeTimeQueryMap = new ConcurrentHashMap<String, CopyOnWriteArrayList<QueryNodeInfoPacket>>();
	private CopyOnWriteArrayList<String> QueryNodeQueryInfo = new CopyOnWriteArrayList<String>();
	
	private void Start() {
		
		ServerSocket masterSocket = null;
		Socket masterChannel = null;
		long MasterNodeStartTime = new Date().getTime()/1000;
		
		try {
			masterSocket = new ServerSocket(8081);
			System.out.println(" MASTER NODE ADDRESS : ");
			System.out.println(masterSocket.getInetAddress().getLocalHost().getHostAddress());
		} catch (Exception e) {
			System.out.println("UNABLE TO START MASTER-NODE");
			e.printStackTrace();
		}
		
		
		// START HEARTBEAT COMMUNICATION THREAD
		new Thread(new MasterNodeHeartBeatCommunication(NodeIdInfoMap, LiveDataNodeInfo, FileDataNodeMap, DataNodeFileInfoMap, FileNameStoreTimeMap)).start();
		
		// START COMMAND PARSER THREAD
		new Thread(new CommandParser(this)).start();
		
		// START QUERY NODE NOTIFICATION THREAD
		new Thread(new MasterQueryNodeNotificationThread(QueryNodeTimeQueryMap, FileNameStoreTimeMap)).start();
		
		while(true) {
			
			try {
				
				masterChannel = masterSocket.accept();
				
				System.out.println("MASTER NODE PORT : "+ masterChannel.getLocalPort());
				
				// START a THREAD for EVERY CONNECTION
				new Thread(new MasterInfoNodeThreadedConnection(masterChannel, NodeIdInfoMap, FileDataNodeMap, DataNodeFileInfoMap, FileNameStoreTimeMap, QueryNodeTimeQueryMap, QueryNodeQueryInfo, MasterNodeStartTime)).start();
				
			} catch (Exception e) {
				System.out.println("UNABLE TO LISTEN TO CONNECTIONS");
				e.printStackTrace();
			}
			
		}
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new MasterInfoNodeMain().Start();
	}

}
