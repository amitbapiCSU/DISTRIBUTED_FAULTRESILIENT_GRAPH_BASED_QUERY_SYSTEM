/**
 * 
 */
package CS555.PACKAGE_FORMATS;

/**
 * @author amchakra
 *
 */
public class PackageFormats {
	
	public static final String MASTER_NODE_IP_ADDRESS = "129.82.46.217"; // "129.82.47.63"; //"129.82.46.229"; // "129.82.46.216"; // "129.82.47.54";
	public static final int MASTER_NODE_IP_PORT = 8081;
	
	public static final int DATA_NODE_JOIN_REQUEST = 1;
	
	public static final int UPLOADER_NODE_FILE_STAGE_INFO = 2;
	public static final int UPLOADER_NODE_MASTER_NODE_FILE_STAGE_REQUEST = 3;
	public static final int MASTER_NODE_UPLOADER_NODE_FILE_STAGE_RESPONSE = 4;
	public static final int UPLOADER_NODE_DATA_NODE_FILE_STAGE_REQUEST = 5;
	public static final int DATA_NODE_DATA_NODE_FILE_PROPAGATE = 6;
	
	public static final int DATA_NODE_MASTER_NODE_LIVE_NODE_INFO_REQUEST = 7;
	public static final int MASTER_NODE_DATA_NODE_LIVE_NODE_INFO_RESPONSE = 8;
	
	public static final int DATA_NODE_BROADCAST_INFO_REQUEST = 9;
	public static final int DATA_NODE_UNICAST_INFO_REQUEST = 10;
	
	public static final int SHOW_NODE_INFO_TABLE = 11;
	
	public static final int MASTER_NODE_DATA_NODE_HEARTBEAT = 12;
	public static final int DATA_NODE_MASTER_NODE_STAGED_FILE_INFO_ACKNOWLEDGEMENT = 13;
	
	public static final int MASTER_NODE_LIVE_NODE_SHOW = 14;
	public static final int MASTER_NODE_FILE_INFO_SHOW = 15;
	
	public static final int MATER_NODE_DATA_NODE_FILE_REPLICATION_REQUEST = 16;
	public static final int DATA_NODE_DATA_NODE_FILE_FORWARD = 17;
	
	public static final int DATA_NODE_AFTER_FAULT_TOLERATE_BROADCAST_INFO_REQUEST = 18;
	public static final int QUERY_NODE_QUERY_INFO = 19;
	
	public static final int QUERY_NODE_MASTER_NODE_QUERY_REQUEST = 20;
	public static final int MASTER_NODE_QUERY_NODE_QUERY_RESPONSE = 21;
	
	public static final int QUERY_NODE_DATA_NODE_QUERY_REQUEST = 22;
	public static final int DATA_NODE_QUERY_NODE_QUERY_RESPONSE = 23;
	
	public static final int MASTER_NODE_QUERY_NODE_QUERY_NOTIFICATION = 24;
	
	public static final int SYSTEM_QUERY_THROUGHPUT_SHOW = 25;
	
	public static final String QUERY_TYPE_MAX = "MAX";
	public static final String QUERY_TYPE_MIN = "MIN";
	public static final String QUERY_TYPE_AVG = "AVG";
	
}
