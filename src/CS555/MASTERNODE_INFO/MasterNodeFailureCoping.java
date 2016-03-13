/**
 * 
 */
package CS555.MASTERNODE_INFO;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
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
public class MasterNodeFailureCoping implements Runnable{
	
	private String DeadNodeHexID;
	private CopyOnWriteArrayList<String> ForwardFilelist;
	private Map<String, Long> FileNamestoreTimeMap;
	private Map<String, DataNodeInfoPacket> DataNodehexIDInfoMap;
	private Map<String, CopyOnWriteArrayList<DataNodeInfoPacket>> FileDataNodemap;
	private Map<String, CopyOnWriteArrayList<String>> DataNodeFileInfomap;
	
	MasterNodeFailureCoping(String DeadNodeHexID, CopyOnWriteArrayList<String> ForwardFilelist, Map<String, DataNodeInfoPacket> DataNodehexIDInfoMap, Map<String, CopyOnWriteArrayList<DataNodeInfoPacket>> FileDataNodemap, Map<String, CopyOnWriteArrayList<String>> DataNodeFileInfomap, Map<String, Long> FileNamesizeMap) {
		this.DeadNodeHexID = DeadNodeHexID;
		this.ForwardFilelist = ForwardFilelist;
		this.DataNodehexIDInfoMap = DataNodehexIDInfoMap;
		this.FileDataNodemap = FileDataNodemap;
		this.DataNodeFileInfomap = DataNodeFileInfomap;
		this.FileNamestoreTimeMap = FileNamesizeMap;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			if(ForwardFilelist != null) {
				for (String filenm : ForwardFilelist) {
					
					DataNodeInfoPacket fileContainDataNode = null;
					DataNodeInfoPacket fileDontContainDataNode = null;
					
					String fileyrmmdtInfo = (filenm.split("_"))[1];
					
					CopyOnWriteArrayList<String> fileContainsDataNodeList = new CopyOnWriteArrayList<String>();
					
					for(DataNodeInfoPacket dtnd : FileDataNodemap.get(fileyrmmdtInfo)) {
						
						fileContainsDataNodeList.add(dtnd.getHexID());
						
						if(fileContainDataNode == null) {
							fileContainDataNode = dtnd;
						}
					}
					
					for (String hxID : DataNodehexIDInfoMap.keySet()) {
						if(!fileContainsDataNodeList.contains(hxID)) {
							fileDontContainDataNode = DataNodehexIDInfoMap.get(hxID);
							break;
						}
					}
					
					System.out.println("CONTAIN :: "+fileContainDataNode.getDataNodeHostName()+" :: DON'T :: "+fileDontContainDataNode.getDataNodeHostName());
					
					if((fileContainDataNode != null) && (fileDontContainDataNode != null)) {
						System.out.println(" TO FORWARD FILE NAME :: "+filenm+" :: "+fileyrmmdtInfo+" :: FROM :: "+fileContainDataNode.getDataNodeHostName()+" :: "+fileDontContainDataNode.getDataNodeHostName());
						try {
							
							Socket toConnectDataNode = new Socket(fileContainDataNode.getDataNodeAddress(),fileContainDataNode.getNodePort(),null,0);
							
							OutputStream dtndout = toConnectDataNode.getOutputStream();
							InputStream dtndin = toConnectDataNode.getInputStream();
							
							DataInputStream dtndccin = new DataInputStream(dtndin);
							DataOutputStream dtndccout = new DataOutputStream(dtndout);
							
							dtndccout.writeInt(PackageFormats.MATER_NODE_DATA_NODE_FILE_REPLICATION_REQUEST);
							dtndccout.writeUTF(filenm);
							dtndccout.writeUTF(DeadNodeHexID);
	//						dtndccout.writeInt(FileNamesizeMap.get(filenm));
							dtndccout.writeUTF(fileDontContainDataNode.getDataNodeAddress());
							dtndccout.writeInt(fileDontContainDataNode.getNodePort());
							
							toConnectDataNode.close();
							
						} catch (Exception e) {
							System.out.println("UNABLE TO CONTACT FILE CONTAINS DATA-NODE");
							e.printStackTrace();
						}
						
					}
					fileContainsDataNodeList.clear();
				}
			}
		} catch (Exception e){
			System.out.println("ERROR IN MAKING REPLICATION LEVEL CONSISTENT :: ");
			e.printStackTrace();
		}
	}

}
