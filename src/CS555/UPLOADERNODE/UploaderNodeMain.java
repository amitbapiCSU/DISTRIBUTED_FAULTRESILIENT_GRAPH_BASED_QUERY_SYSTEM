/**
 * 
 */
package CS555.UPLOADERNODE;

import java.net.ServerSocket;
import java.net.Socket;

import CS555.COMMANDPARSER.CommandParser;

/**
 * @author amchakra
 *
 */
public class UploaderNodeMain {
	
	private String UploaderNodeIPAddr = "";
	private int UploaderNodePort = 0;
	
	public String getUploaderNodeIp() {
		return this.UploaderNodeIPAddr;
	}
	
	public int getUploaderNodePort(){
		return this.UploaderNodePort;
	}
	
	private void start() {
		
		try {
			ServerSocket UploaderServersckt = null;
			Socket UploaderdataChannel = null;
			
			try {
				
				UploaderServersckt = new ServerSocket(0);
				UploaderNodeIPAddr = UploaderServersckt.getInetAddress().getLocalHost().getHostAddress();
				UploaderNodePort = UploaderServersckt.getLocalPort();
				
			} catch (Exception e) {
				System.out.println("ERROR IN UPLOADER NODE");
				e.printStackTrace();
			}
			
			new Thread(new CommandParser(this)).start();
			
			while (true) {
				
				System.out.println("LISTENING IN UPLOADER");
				
				UploaderdataChannel = UploaderServersckt.accept();
				
				new Thread(new UploaderNodeThreadedConnection(UploaderdataChannel,UploaderNodeIPAddr,UploaderNodePort)).start();
				
			}
		} catch (Exception e) {
			
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		new UploaderNodeMain().start();
		
	}
}
