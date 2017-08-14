package hive;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
//UDF是作用于单个数据行，产生一个数据行
//用户必须要继承UDF，且必须至少实现一个evalute方法，该方法并不在UDF中
//但是Hive会检查用户的UDF是否拥有一个evalute方法
public class Strip extends UDF{
	private Text result=new Text();
	//自定义方法
	public Text evaluate(Text str)
	{
		if(str==null)
		return null;
		result.set(StringUtils.strip(str.toString()));
		return result;
	}
	public Text evaluate(Text str,String stripChars)
	{
		if(str==null)
		return null;
		result.set(StringUtils.strip(str.toString(),stripChars));
		return result;
	}
}