package WeChat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WeChatw implements WritableComparable<WeChatw>{

	/**
	 * @param args
	 */
	private String chatid;
	private String userid;
	private long clicknum;
	private long prisenum;
	
	public WeChatw() {
		super();
		// TODO Auto-generated constructor stub
	}

	public WeChatw(String chatid, String userid, long clicknum, long prisenum) {
		super();
		this.chatid = chatid;
		this.userid = userid;
		this.clicknum = clicknum;
		this.prisenum = prisenum;
	}

	public String getChatid() {
		return chatid;
	}

	public void setChatid(String chatid) {
		this.chatid = chatid;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public long getClicknum() {
		return clicknum;
	}

	public void setClicknum(long clicknum) {
		this.clicknum = clicknum;
	}

	public long getPrisenum() {
		return prisenum;
	}

	public void setPrisenum(long prisenum) {
		this.prisenum = prisenum;
	}
    
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.chatid=in.readUTF();
		this.userid=in.readUTF();
		this.clicknum=in.readLong();
		this.prisenum=in.readLong();
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(chatid);
		out.writeUTF(userid);
		out.writeLong(clicknum);
		out.writeLong(prisenum);
		
	}

	public int compareTo(WeChatw o) {
		// TODO Auto-generated method stub
		return (int) (o.clicknum-this.clicknum);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

