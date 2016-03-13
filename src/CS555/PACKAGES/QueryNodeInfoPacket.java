/**
 * 
 */
package CS555.PACKAGES;

import java.io.Serializable;

/**
 * @author amchakra
 *
 */
public class QueryNodeInfoPacket implements Serializable {
	
	private long QuryNodeQueryTimeStamp;
	private String QueryNodeHEXID;
	private String QueryNodeQueryTimeInfo;
	private String QueryNodeQuery;
	private String QueryNodeIP;
	private int QueryNodePort;
	private boolean YearPresent;
	private boolean MonthPresent;
	private boolean DatePresent;
	
	public QueryNodeInfoPacket (String QueryNodeHEXID, long QuryNodeQueryTimeStamp, String QueryNodeQueryTimeInfo, String QueryNodeIP, int QueryNodePort, String QueryNodeQuery, boolean YearPresent, boolean MonthPresent, boolean DatePresent) {
		this.QueryNodeHEXID = QueryNodeHEXID;
		this.QuryNodeQueryTimeStamp = QuryNodeQueryTimeStamp;
		this.QueryNodeQueryTimeInfo = QueryNodeQueryTimeInfo;
		this.QueryNodeIP = QueryNodeIP;
		this.QueryNodePort = QueryNodePort;
		this.QueryNodeQuery = QueryNodeQuery;
		this.YearPresent = YearPresent;
		this.MonthPresent = MonthPresent;
		this.DatePresent = DatePresent;
	}
	
	public boolean isYearPresentinQuery() {
		return this.YearPresent;
	}
	
	public boolean isMonthPresentinQuery() {
		return this.MonthPresent;
	}
	
	public boolean isDatePresentinQuery() {
		return this.DatePresent;
	}
	
	public String getQueryNodeHexID() {
		return this.QueryNodeHEXID;
	}
	
	public long getQueryNodesQueryTimeStamp() {
		return this.QuryNodeQueryTimeStamp;
	}
	
	public String getQueryNodeQueryTimeInfo() {
		return this.QueryNodeQueryTimeInfo;
	}
	
	public String getQueryNodeIP() {
		return this.QueryNodeIP;
	}
	
	public int getQueryNodePort() {
		return this.QueryNodePort;
	}
	
	public String getQueryNodesQuery() {
		return this.QueryNodeQuery;
	}

}
