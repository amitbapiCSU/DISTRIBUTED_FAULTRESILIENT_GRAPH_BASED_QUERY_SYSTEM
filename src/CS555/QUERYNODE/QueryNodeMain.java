/**
 * 
 */
package CS555.QUERYNODE;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import CS555.COMMANDPARSER.CommandParser;

/**
 * @author amchakra
 *
 */
public class QueryNodeMain {

	private String QueryNodeIP = "";
	private int QueryNodePort = 0;
	private String QueryNodeHexID = "";
	
	public String getQueryNodeIP() {
		return this.QueryNodeIP;
	}
	
	public int getQueryNodePort() {
		return this.QueryNodePort;
	}
	
	public String getQueryNodeHexID() {
		return this.QueryNodeHexID;
	}
	
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
		/*Date date = new Date();
		Long luid = (Long) date.getTime();
		String suid = Long.toHexString(luid);*/ //luid.toString();
		
		/*Random generator = new Random(System.nanoTime());
		String suid = Integer.toHexString(generator.hashCode()/ 65536);
		
		return suid;*/
		
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
		
//		System.out.println("1 : "+HexIdentifier);
		
		// String HexIdentifier = getSum(lstdigit)+getSum(last4digit)+getSum(scndlst4digit)+getSum(first4digit);
		
		//System.out.println(HexIdentifier);
		
		return HexIdentifier;
	}
	
	private void start() {
		try {
			
			ServerSocket QueryServersckt = null;
			Socket QuerydataChannel = null;
			
			try {
				
				QueryServersckt = new ServerSocket(0);
				QueryNodeIP = QueryServersckt.getInetAddress().getLocalHost().getHostAddress();
				QueryNodePort = QueryServersckt.getLocalPort();
				
			} catch (Exception e) {
				System.out.println("ERROR IN UPLOADER NODE");
				e.printStackTrace();
			}
			
			// CREATE QUERY NODE HEXID FROm IT's TIMESTAMP
			QueryNodeHexID = getIdentifierbyTimeStamp();
			
			// START COMMAND PARSER
			new Thread(new CommandParser(this)).start();
			
			
			while (true) {
				
				System.out.println("LISTENING IN QUERY NODE :: OF HEX ID :: "+QueryNodeHexID);
				
				QuerydataChannel = QueryServersckt.accept();
				
				new Thread(new QueryNodeThreadedConnection(QuerydataChannel, QueryNodeIP, QueryNodePort, QueryNodeHexID)).start();
				
			}
			
		} catch (Exception e) {
			System.out.println("ERROR In START OF QUERY NODE");
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new QueryNodeMain().start();
	}

}
