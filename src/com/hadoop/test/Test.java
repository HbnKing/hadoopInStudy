package com.hadoop.test;

import java.util.Arrays;
import java.util.Date;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String line = "bdsufgsda    ".trim();
		String line2 = "sdbgsdauf";
		String [] strings  = new String[10];
		strings[0]="aa";
		System.out.println(strings.length);
		//int length = line.length();
		//System.out.println(length);
		//char [] ch = line.toCharArray();
		String  [] ch = line.split("");  //使用split进行拆分操作,变为string数组
		String s= String.valueOf(ch);
		//System.out.println(s);
				 
		char [] ch2 = line2.toCharArray(); 
		//System.out.println(ch);
		
		Arrays.sort(ch);
		Arrays.sort(ch2);
		//toString(ch,a);
		
		
		
		String s2= String.valueOf(ch2);
		
		System.out.println(ch);
		System.out.println(ch2);
		System.out.println(s2);
		
		
		
	}
  
}
