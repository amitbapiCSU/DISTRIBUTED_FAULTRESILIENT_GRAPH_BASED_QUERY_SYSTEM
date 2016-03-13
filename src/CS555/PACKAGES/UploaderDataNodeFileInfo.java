/**
 * 
 */
package CS555.PACKAGES;

import java.io.Serializable;
import java.util.List;

/**
 * @author amchakra
 *
 */
public class UploaderDataNodeFileInfo implements Serializable{

	private String FileName;
	private int FileSize ;
	private List<DataNodeInfoPacket> LisofDataNodes;
	
	public UploaderDataNodeFileInfo (String fileName, int FileSize, List<DataNodeInfoPacket> lisofDataNodes) {
		this.FileName = fileName;
		this.FileSize = FileSize;
		this.LisofDataNodes = lisofDataNodes;
	}
	
	public String getFileName() {
		return this.FileName;
	}
	
	public int getFileSize() {
		return this.FileSize;
	}
	
	public List<DataNodeInfoPacket> gettoUploadDataNodeInfo() {
		return this.LisofDataNodes;
	}
}
