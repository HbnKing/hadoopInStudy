package hive;


import org.apache.hadoop.hive.ql.exec.UDF;
//UDF是作用于单个数据行，产生一个数据行
//用户必须要继承UDF，且必须至少实现一个evalute方法，该方法并不在UDF中
//但是Hive会检查用户的UDF是否拥有一个evalute方法
/**
* @function 自定义UDF统计最大值
* @author John
*
*/
public class Min  extends UDF{

	public Double evaluate(Double a, Double b) {
	
		if(a==null)
			a=0.0;
		if(b==null)
			b=0.0;
		if(a>=b){
			return b;
		}else{
			return a;
		}
	}
}